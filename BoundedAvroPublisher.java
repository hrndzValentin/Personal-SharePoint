public class BoundedAvroPublisher {

    private static final Logger log = LoggerFactory.getLogger(BoundedAvroPublisher.class);

    private final KafkaTemplate<String, SpecificRecord> kafkaTemplate;
    private final DlqPublisher dlqPublisher;
    private final int maxInFlight;
    private final Semaphore inFlight;

    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicReference<Throwable> firstFatal = new AtomicReference<>();

    public BoundedAvroPublisher(KafkaTemplate<String, SpecificRecord> kafkaTemplate,
                                DlqPublisher dlqPublisher,
                                int maxInFlight) {
        this.kafkaTemplate = kafkaTemplate;
        this.dlqPublisher = dlqPublisher;
        this.maxInFlight = maxInFlight;
        this.inFlight = new Semaphore(maxInFlight);
    }

    /**
     * Publica de forma asíncrona pero con ventana acotada.
     * Bloquea (backpressure) si ya hay maxInFlight mensajes sin confirmar.
     * Lanza si un fallo previo irrecuperable marcó el flujo como abortado.
     */
    public void publish(String topic, String key, SpecificRecord record)
            throws InterruptedException {

        // fail-fast: si algo irrecuperable ya pasó, no sigas procesando el archivo
        Throwable fatal = firstFatal.get();
        if (fatal != null) {
            throw new PublishingAbortedException(
                    "Abortando procesamiento: fallo irrecuperable previo", fatal);
        }

        inFlight.acquire();   // BACKPRESSURE: bloquea si la ventana está llena
        try {
            kafkaTemplate.send(topic, key, record)
                .whenComplete((result, ex) -> {
                    try {
                        if (ex != null) {
                            handleFailure(topic, key, record, ex);
                        } else {
                            sent.incrementAndGet();
                        }
                    } finally {
                        inFlight.release();   // SIEMPRE libera el permiso
                    }
                });
        } catch (RuntimeException e) {
            // si send() lanza síncronamente (ej. serialización Avro falla,
            // schema incompatible), el callback NO correrá: libera aquí
            inFlight.release();
            failed.incrementAndGet();
            // un fallo de serialización es del registro, no del cluster:
            // mándalo al DLQ y sigue, no abortes todo el archivo
            if (!dlqPublisher.tryPublish(topic, key, record, e)) {
                firstFatal.compareAndSet(null, e);
            }
            throw new PublishingException("Fallo síncrono publicando key=" + key, e);
        }
    }

    private void handleFailure(String topic, String key, SpecificRecord record, Throwable ex) {
        failed.incrementAndGet();
        log.error("Fallo async publicando key={}: {}", key, ex.toString());
        // el DLQ es parte de la garantía de no-pérdida
        boolean recovered = dlqPublisher.tryPublish(topic, key, record, ex);
        if (!recovered) {
            // si ni el DLQ acepta el mensaje, no hay dónde ponerlo: es fatal
            log.error("DLQ también falló para key={}. Marcando flujo como fatal.", key);
            firstFatal.compareAndSet(null, ex);
        }
    }

    /**
     * Punto de sincronización determinista. Se llama UNA vez al terminar el archivo.
     * Al retornar sin excepción, CADA mensaje fue confirmado en el topic o en el DLQ.
     */
    public void awaitCompletion() throws InterruptedException {
        // 1. fuerza el envío de lo que quede bufferizado
        kafkaTemplate.flush();

        // 2. espera a que TODOS los permisos vuelvan = todos los callbacks corrieron
        //    con timeout para no colgarse indefinidamente si algo quedó atascado
        boolean allDone = inFlight.tryAcquire(maxInFlight, 5, TimeUnit.MINUTES);
        if (!allDone) {
            throw new PublishingException(
                    "Timeout esperando confirmación de mensajes en vuelo. " +
                    "Confirmados: " + sent.get() + ", fallidos: " + failed.get());
        }
        inFlight.release(maxInFlight);

        // 3. si hubo un fallo irrecuperable, propágalo para abortar con exit code != 0
        Throwable fatal = firstFatal.get();
        if (fatal != null) {
            throw new PublishingException(
                    "Publicación terminó con fallo irrecuperable. " +
                    "Confirmados: " + sent.get() + ", fallidos: " + failed.get(), fatal);
        }

        log.info("Publicación completa. Confirmados: {}, enviados a DLQ: {}",
                sent.get(), failed.get());
    }

    public long getSentCount()   { return sent.get(); }
    public long getFailedCount() { return failed.get(); }
}
