
package com.gloss.publishtokafka;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Publisher asíncrono con:
 *  - Backpressure por semáforo (ventana acotada de mensajes en vuelo).
 *  - Reintento acotado con backoff exponencial para fallos transitorios.
 *  - Distinción transitorio / permanente (no reintenta lo que no sirve reintentar).
 *  - Validación de reglas de negocio ANTES de publicar.
 *  - Estrategia OPCIÓN B: al fallar definitivamente, marca el flujo como fatal
 *    para abortar el job. El orquestador reprocesa el archivo completo.
 *
 * GARANTÍA: at-least-once. La ausencia de duplicados en el estado final
 * la provee el CONSUMIDOR idempotente (dedup por key = partyRef).
 */
public class BoundedAvroPublisher {

    private static final Logger log = LoggerFactory.getLogger(BoundedAvroPublisher.class);

    // --- parámetros de reintento ---
    private static final int    MAX_RETRIES     = 5;
    private static final long   INITIAL_BACKOFF = 500;    // ms
    private static final double BACKOFF_FACTOR  = 2.0;    // 500, 1000, 2000, 4000, 8000

    private final KafkaTemplate<String, SpecificRecord> kafkaTemplate;
    private final BusinessRuleValidator validator;
    private final ScheduledExecutorService retryScheduler;
    private final int maxInFlight;
    private final Semaphore inFlight;
    private final String batchId;   // determinista por archivo

    // --- contadores para reconciliación y exit code ---
    private final AtomicLong sent      = new AtomicLong();
    private final AtomicLong rejected   = new AtomicLong();   // rechazados por reglas de negocio
    private final AtomicReference<Throwable> firstFatal = new AtomicReference<>();

    public BoundedAvroPublisher(KafkaTemplate<String, SpecificRecord> kafkaTemplate,
                                BusinessRuleValidator validator,
                                ScheduledExecutorService retryScheduler,
                                String batchId,
                                int maxInFlight) {
        this.kafkaTemplate  = kafkaTemplate;
        this.validator      = validator;
        this.retryScheduler = retryScheduler;
        this.batchId        = batchId;
        this.maxInFlight    = maxInFlight;
        this.inFlight       = new Semaphore(maxInFlight);
    }

    /**
     * Punto de entrada desde el loop StAX. Valida, y si pasa, publica async.
     *
     * @param topic  topic destino (dinámico por tipo de archivo)
     * @param key    key determinista = partyRef (habilita dedup del consumidor)
     * @param record el SpecificRecord Avro ya construido
     * @throws InterruptedException      si el hilo se interrumpe esperando cupo
     * @throws PublishingAbortedException si un fallo previo dejó el flujo en estado fatal
     */
    public void publish(String topic, String key, SpecificRecord record)
            throws InterruptedException {

        // 1. fail-fast: si ya hubo un fallo irrecuperable, aborta el archivo (Opción B)
        Throwable fatal = firstFatal.get();
        if (fatal != null) {
            throw new PublishingAbortedException(
                    "Flujo abortado por fallo previo. Se reprocesará el archivo completo.", fatal);
        }

        // 2. VALIDACIÓN DE REGLAS DE NEGOCIO antes de publicar
        ValidationResult validation = validator.validate(record);
        if (!validation.valid()) {
            // un registro que viola reglas de negocio NO se publica.
            // Decisión de diseño: ¿esto aborta o solo se cuenta y se salta?
            // Para "no data loss" estricto lo tratamos como error del dato:
            // se registra, se cuenta, y se DECIDE según severidad (ver handleInvalid).
            handleInvalid(key, record, validation);
            return;   // no consume permiso, no publica
        }

        // 3. backpressure: bloquea si la ventana está llena
        inFlight.acquire();
        sendWithRetry(topic, key, record, 0);
    }

    /**
     * Envía y, ante fallo transitorio, se reprograma con backoff exponencial
     * hasta MAX_RETRIES. El permiso del semáforo se mantiene tomado durante
     * TODA la cadena de reintentos (el mensaje sigue "en vuelo").
     */
    private void sendWithRetry(String topic, String key,
                               SpecificRecord record, int attempt) {

        ProducerRecord<String, SpecificRecord> producerRecord =
                new ProducerRecord<>(topic, key, record);
        enrichHeaders(producerRecord.headers(), attempt);

        kafkaTemplate.send(producerRecord)
            .whenComplete((result, ex) -> {
                if (ex == null) {
                    sent.incrementAndGet();
                    inFlight.release();                    // estado terminal: ÉXITO
                    return;
                }

                if (isPermanent(ex)) {
                    // reintentar es inútil → Opción B: fatal, abortar y reprocesar
                    log.error("Fallo PERMANENTE key={} attempt={}: {}. Abortando job.",
                            key, attempt, rootMsg(ex));
                    firstFatal.compareAndSet(null, ex);
                    inFlight.release();                    // estado terminal: FATAL
                    return;
                }

                if (attempt < MAX_RETRIES) {
                    long backoff = (long) (INITIAL_BACKOFF * Math.pow(BACKOFF_FACTOR, attempt));
                    log.warn("Fallo transitorio key={} reintento {}/{} en {}ms: {}",
                            key, attempt + 1, MAX_RETRIES, backoff, rootMsg(ex));
                    // NO se libera el permiso: el mensaje sigue en vuelo durante el backoff
                    retryScheduler.schedule(
                        () -> sendWithRetry(topic, key, record, attempt + 1),
                        backoff, TimeUnit.MILLISECONDS);
                } else {
                    // agotados los 5 reintentos → Opción B: fatal, abortar y reprocesar
                    log.error("Reintentos AGOTADOS key={}: {}. Abortando job para reproceso.",
                            key, rootMsg(ex));
                    firstFatal.compareAndSet(null, ex);
                    inFlight.release();                    // estado terminal: FATAL
                }
            });
    }

    /**
     * Manejo de un registro que falla las reglas de negocio.
     * Severidad HARD  → aborta el job (dato estructuralmente inválido, no debería existir).
     * Severidad SOFT  → se cuenta como rechazado y se salta (regla de filtrado esperada).
     */
    private void handleInvalid(String key, SpecificRecord record, ValidationResult validation) {
        if (validation.severity() == Severity.HARD) {
            log.error("Registro key={} viola regla HARD: {}. Abortando job.",
                    key, validation.errors());
            firstFatal.compareAndSet(null,
                    new BusinessRuleViolationException(key, validation.errors()));
        } else {
            // SOFT: p.ej. "este PARTY no aplica para este topic" — descarte legítimo
            rejected.incrementAndGet();
            log.info("Registro key={} descartado por regla SOFT: {}", key, validation.errors());
        }
    }

    /**
     * Barrera de cierre. Se llama UNA vez al terminar el archivo.
     * Al retornar sin excepción: cada mensaje llegó a un estado terminal
     * (publicado, rechazado SOFT) y no hubo fatal → el archivo se procesó completo.
     */
    public void awaitCompletion(long timeoutMinutes) throws InterruptedException {
        kafkaTemplate.flush();   // empuja lo bufferizado; dispara callbacks pendientes

        boolean allSettled = inFlight.tryAcquire(maxInFlight, timeoutMinutes, TimeUnit.MINUTES);
        if (!allSettled) {
            throw new PublishingException(
                "Timeout esperando confirmación. Publicados=" + sent.get()
                + " rechazados=" + rejected.get());
        }
        inFlight.release(maxInFlight);

        Throwable fatal = firstFatal.get();
        if (fatal != null) {
            throw new PublishingException(
                "Job abortado (Opción B): se reprocesará el archivo. "
                + "Publicados antes de abortar=" + sent.get(), fatal);
        }

        log.info("Batch {} completo. Publicados={} rechazados(SOFT)={}",
                batchId, sent.get(), rejected.get());
    }

    /** Agrega batchId y metadata determinista a cada mensaje (contexto para el consumidor). */
    private void enrichHeaders(Headers headers, int attempt) {
        headers.add("batch.id",      batchId.getBytes(StandardCharsets.UTF_8));
        headers.add("publish.attempt", Integer.toString(attempt).getBytes(StandardCharsets.UTF_8));
        headers.add("publish.ts",    Instant.now().toString().getBytes(StandardCharsets.UTF_8));
    }

    /** Fallos que NO se resuelven reintentando: son del dato o del schema, no del cluster. */
    private boolean isPermanent(Throwable ex) {
        Throwable cause = (ex instanceof CompletionException && ex.getCause() != null)
                ? ex.getCause() : ex;
        String name = cause.getClass().getName();
        return name.contains("SerializationException")
            || name.contains("RecordTooLargeException")
            || name.contains("InvalidTopicException")
            || name.contains("SchemaException")           // incompatibilidad de schema registry
            || name.contains("AuthorizationException");
    }

    private String rootMsg(Throwable ex) {
        Throwable c = (ex instanceof CompletionException && ex.getCause() != null)
                ? ex.getCause() : ex;
        return c.getClass().getSimpleName() + ": "
                + (c.getMessage() != null ? c.getMessage() : "(sin mensaje)");
    }

    // --- getters para el runner (exit code, reconciliación) ---
    public long getSentCount()     { return sent.get(); }
    public long getRejectedCount() { return rejected.get(); }
    public boolean hasFatalError() { return firstFatal.get() != null; }
}
