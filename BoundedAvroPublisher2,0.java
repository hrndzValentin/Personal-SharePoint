package com.gloss.publishtokafka;

import net.logstash.logback.argument.StructuredArguments;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Publisher asíncrono con control de entrega.
 *
 * ESTRATEGIA:
 *  · Backpressure por semáforo: ventana acotada de mensajes en vuelo.
 *  · Reintento acotado (5) con backoff exponencial para fallos transitorios.
 *  · Distinción transitorio / permanente: no reintenta lo que no sirve reintentar.
 *  · Validación de reglas de negocio ANTES de publicar.
 *  · RESILIENCIA POR REGISTRO: un nodo malo se loguea con detalle y se salta.
 *    El archivo CONTINÚA. Los fallos los revisa una persona vía log + email.
 *  · CIRCUIT BREAKER: si N mensajes CONSECUTIVOS fallan por causas de
 *    infraestructura (sin ningún éxito en medio), se concluye que el cluster
 *    está caído y se ABORTA el job. Es el único caso de parada.
 *
 * GARANTÍA: at-least-once para lo publicado. Los fallos NO se reprocesan
 * automáticamente: quedan documentados en log estructurado + email para
 * intervención humana. El consumidor debe ser idempotente (dedup por partyRef)
 * porque un reintento del job completo republica todo.
 */
public class BoundedAvroPublisher {

    private static final Logger log = LoggerFactory.getLogger(BoundedAvroPublisher.class);

    // --- reintentos ---
    private static final int    MAX_RETRIES     = 5;
    private static final long   INITIAL_BACKOFF = 500;    // ms
    private static final double BACKOFF_FACTOR  = 2.0;    // 500, 1000, 2000, 4000, 8000

    /**
     * Umbral del circuit breaker. N fallos de infraestructura CONSECUTIVOS
     * (sin un solo éxito entre ellos) ⇒ el cluster está caído, no es un dato malo.
     * Un registro corrupto falla aislado; un broker caído hace fallar todo.
     */
    private static final int CONSECUTIVE_FAILURES_THRESHOLD = 25;

    private final KafkaTemplate<String, SpecificRecord> kafkaTemplate;
    private final BusinessRuleValidator validator;
    private final PublishMetrics metrics;
    private final ScheduledExecutorService retryScheduler;
    private final int maxInFlight;
    private final Semaphore inFlight;
    private final String batchId;

    private final AtomicInteger consecutiveInfraFailures = new AtomicInteger();
    private final AtomicReference<Throwable> brokerDown  = new AtomicReference<>();

    public BoundedAvroPublisher(KafkaTemplate<String, SpecificRecord> kafkaTemplate,
                                BusinessRuleValidator validator,
                                PublishMetrics metrics,
                                ScheduledExecutorService retryScheduler,
                                String batchId,
                                int maxInFlight) {
        this.kafkaTemplate  = kafkaTemplate;
        this.validator      = validator;
        this.metrics        = metrics;
        this.retryScheduler = retryScheduler;
        this.batchId        = batchId;
        this.maxInFlight    = maxInFlight;
        this.inFlight       = new Semaphore(maxInFlight);
    }

    /**
     * Punto de entrada desde el loop StAX.
     *
     * @param topic  topic destino (dinámico por tipo de archivo)
     * @param key    key determinista = partyRef; habilita dedup del consumidor
     * @param record el SpecificRecord Avro ya construido
     * @throws BrokerUnavailableException si el circuit breaker detectó caída del cluster
     */
    public void publish(String topic, String key, SpecificRecord record)
            throws InterruptedException {

        // 1. ¿el cluster se cayó? único motivo para detener el archivo
        Throwable down = brokerDown.get();
        if (down != null) {
            throw new BrokerUnavailableException(
                    "Cluster Kafka no disponible: " + CONSECUTIVE_FAILURES_THRESHOLD
                    + " fallos de infraestructura consecutivos. Abortando batch " + batchId, down);
        }

        // 2. reglas de negocio ANTES de publicar
        ValidationResult validation = validator.validate(record);
        if (!validation.valid()) {
            handleInvalid(key, validation);
            return;   // no consume permiso, no publica; el archivo CONTINÚA
        }

        // 3. backpressure: bloquea si la ventana está llena
        long blockStart = System.nanoTime();
        inFlight.acquire();
        metrics.recordBlockedTime(System.nanoTime() - blockStart);

        sendWithRetry(topic, key, record, 0);
    }

    /**
     * Envía y, ante fallo transitorio, se reprograma con backoff exponencial.
     * El permiso del semáforo se mantiene tomado durante TODA la cadena de
     * reintentos: el mensaje sigue "en vuelo" hasta alcanzar estado terminal.
     */
    private void sendWithRetry(String topic, String key, SpecificRecord record, int attempt) {

        ProducerRecord<String, SpecificRecord> pr = new ProducerRecord<>(topic, key, record);
        enrichHeaders(pr.headers(), attempt);

        kafkaTemplate.send(pr)
            .whenComplete((result, ex) -> {
                if (ex == null) {
                    onSuccess(key, attempt, result);
                    inFlight.release();                       // terminal: ÉXITO
                    return;
                }

                if (isPermanent(ex)) {
                    // dato/schema malo: reintentar no sirve. Log + métrica, sigue el archivo.
                    onPermanentFailure(key, attempt, ex);
                    inFlight.release();                       // terminal: FALLIDO
                    return;
                }

                if (attempt < MAX_RETRIES) {
                    scheduleRetry(topic, key, record, attempt, ex);
                    // NO libera el permiso: sigue en vuelo durante el backoff
                } else {
                    onRetriesExhausted(key, ex);
                    inFlight.release();                       // terminal: FALLIDO
                }
            });
    }

    // ─── manejo de resultados ────────────────────────────────────────────────

    private void onSuccess(String key, int attempt, Object result) {
        consecutiveInfraFailures.set(0);   // un éxito prueba que el cluster está vivo
        metrics.recordPublished();
        if (attempt > 0) {
            // solo logueamos éxito si hubo reintentos: evita ruido en el caso feliz
            log.info("Publicado tras reintentos",
                    StructuredArguments.kv("event", "publish_recovered"),
                    StructuredArguments.kv("batch_id", batchId),
                    StructuredArguments.kv("ref_id", key),
                    StructuredArguments.kv("attempts", attempt + 1));
        }
    }

    private void onPermanentFailure(String key, int attempt, Throwable ex) {
        Throwable cause = unwrap(ex);
        // NO cuenta para el circuit breaker: es problema del dato, no del cluster
        metrics.recordFailure(FailurePhase.SERIALIZATION, cause.getClass().getSimpleName());
        log.error("Fallo permanente: el registro requiere revisión manual",
                StructuredArguments.kv("event", "publish_failed_permanent"),
                StructuredArguments.kv("batch_id", batchId),
                StructuredArguments.kv("ref_id", key),
                StructuredArguments.kv("phase", FailurePhase.SERIALIZATION),
                StructuredArguments.kv("attempts", attempt + 1),
                StructuredArguments.kv("error_class", cause.getClass().getName()),
                StructuredArguments.kv("error_message", msg(cause)),
                StructuredArguments.kv("action", "revisar schema/dato en el archivo origen"));
    }

    private void scheduleRetry(String topic, String key, SpecificRecord record,
                               int attempt, Throwable ex) {
        long backoff = (long) (INITIAL_BACKOFF * Math.pow(BACKOFF_FACTOR, attempt));
        Throwable cause = unwrap(ex);
        metrics.recordRetry();
        log.warn("Fallo transitorio, reintentando",
                StructuredArguments.kv("event", "publish_retry"),
                StructuredArguments.kv("batch_id", batchId),
                StructuredArguments.kv("ref_id", key),
                StructuredArguments.kv("attempt", attempt + 1),
                StructuredArguments.kv("max_attempts", MAX_RETRIES),
                StructuredArguments.kv("backoff_ms", backoff),
                StructuredArguments.kv("error_class", cause.getClass().getSimpleName()),
                StructuredArguments.kv("error_message", msg(cause)));

        retryScheduler.schedule(
                () -> sendWithRetry(topic, key, record, attempt + 1),
                backoff, TimeUnit.MILLISECONDS);
    }

    private void onRetriesExhausted(String key, Throwable ex) {
        Throwable cause = unwrap(ex);
        metrics.recordFailure(FailurePhase.BROKER_RETRIES_EXHAUSTED,
                cause.getClass().getSimpleName());

        log.error("Reintentos agotados: el registro NO se publicó",
                StructuredArguments.kv("event", "publish_failed_exhausted"),
                StructuredArguments.kv("batch_id", batchId),
                StructuredArguments.kv("ref_id", key),
                StructuredArguments.kv("phase", FailurePhase.BROKER_RETRIES_EXHAUSTED),
                StructuredArguments.kv("attempts", MAX_RETRIES + 1),
                StructuredArguments.kv("error_class", cause.getClass().getName()),
                StructuredArguments.kv("error_message", msg(cause)),
                StructuredArguments.kv("action", "reprocesar este ref_id manualmente"));

        // CIRCUIT BREAKER: fallos de infraestructura consecutivos ⇒ cluster caído
        int consecutive = consecutiveInfraFailures.incrementAndGet();
        if (consecutive >= CONSECUTIVE_FAILURES_THRESHOLD) {
            if (brokerDown.compareAndSet(null, ex)) {
                log.error("CLUSTER KAFKA CAÍDO: abortando el procesamiento del archivo",
                        StructuredArguments.kv("event", "broker_unavailable"),
                        StructuredArguments.kv("batch_id", batchId),
                        StructuredArguments.kv("consecutive_failures", consecutive),
                        StructuredArguments.kv("threshold", CONSECUTIVE_FAILURES_THRESHOLD),
                        StructuredArguments.kv("error_class", cause.getClass().getName()),
                        StructuredArguments.kv("action", "verificar salud del cluster y relanzar el job"));
            }
        }
    }

    private void handleInvalid(String key, ValidationResult validation) {
        if (validation.severity() == Severity.HARD) {
            metrics.recordFailure(FailurePhase.VALIDATION, "BusinessRuleViolation");
            log.error("Registro inválido: no se publica, requiere revisión manual",
                    StructuredArguments.kv("event", "validation_failed"),
                    StructuredArguments.kv("batch_id", batchId),
                    StructuredArguments.kv("ref_id", key),
                    StructuredArguments.kv("phase", FailurePhase.VALIDATION),
                    StructuredArguments.kv("violations", validation.errors()),
                    StructuredArguments.kv("action", "corregir el dato en el archivo origen"));
        } else {
            metrics.recordSoftReject();
            log.info("Registro descartado por regla de negocio",
                    StructuredArguments.kv("event", "record_skipped"),
                    StructuredArguments.kv("batch_id", batchId),
                    StructuredArguments.kv("ref_id", key),
                    StructuredArguments.kv("reason", validation.errors()));
        }
    }

    // ─── cierre ──────────────────────────────────────────────────────────────

    /**
     * Barrera de cierre. Al retornar, TODO mensaje alcanzó estado terminal:
     * publicado, fallido (logueado), o descartado.
     *
     * El semáforo es la prueba contable: solo se recuperan los N permisos
     * cuando todos los callbacks corrieron. No queda nada en el limbo.
     */
    public void awaitCompletion(long timeoutMinutes) throws InterruptedException {
        kafkaTemplate.flush();   // empuja lo bufferizado; dispara callbacks pendientes

        boolean allSettled = inFlight.tryAcquire(maxInFlight, timeoutMinutes, TimeUnit.MINUTES);
        if (!allSettled) {
            throw new PublishingException(String.format(
                    "Timeout esperando confirmaciones tras %d min. publicados=%d fallidos=%d",
                    timeoutMinutes, metrics.getPublished(), metrics.getFailedHard()));
        }
        inFlight.release(maxInFlight);

        Throwable down = brokerDown.get();
        if (down != null) {
            throw new BrokerUnavailableException(
                    "Batch " + batchId + " abortado por caída del cluster. publicados="
                    + metrics.getPublished(), down);
        }
    }

    // ─── helpers ─────────────────────────────────────────────────────────────

    private void enrichHeaders(Headers headers, int attempt) {
        headers.add("batch.id",        batchId.getBytes(StandardCharsets.UTF_8));
        headers.add("publish.attempt", Integer.toString(attempt).getBytes(StandardCharsets.UTF_8));
        headers.add("publish.ts",      Instant.now().toString().getBytes(StandardCharsets.UTF_8));
    }

    /** Fallos que NO se resuelven reintentando: son del dato o del schema, no del cluster. */
    private boolean isPermanent(Throwable ex) {
        String name = unwrap(ex).getClass().getName();
        return name.contains("SerializationException")
            || name.contains("RecordTooLargeException")
            || name.contains("InvalidTopicException")
            || name.contains("SchemaException")
            || name.contains("AuthorizationException")
            || name.contains("AuthenticationException");
    }

    private Throwable unwrap(Throwable ex) {
        return (ex instanceof CompletionException && ex.getCause() != null) ? ex.getCause() : ex;
    }

    private String msg(Throwable t) {
        return t.getMessage() != null ? t.getMessage() : "(sin mensaje)";
    }

    public boolean isBrokerDown() { return brokerDown.get() != null; }
}
