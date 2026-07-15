package com.gloss.publishtokafka;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Métricas del job. Doble propósito:
 *  1. Exportadas a Datadog/Prometheus vía Micrometer (dashboards, alertas).
 *  2. Fuente de datos para el email de resumen al final del job.
 *
 * Todos los contadores son thread-safe: los callbacks de Kafka llegan
 * desde el hilo de I/O, no desde el loop de procesamiento.
 */
public class PublishMetrics {

    private final MeterRegistry registry;
    private final String batchId;
    private final String fileType;

    // --- contadores de negocio ---
    private final AtomicLong nodesRead      = new AtomicLong();
    private final AtomicLong published      = new AtomicLong();
    private final AtomicLong failedHard     = new AtomicLong();   // requieren revisión humana
    private final AtomicLong rejectedSoft   = new AtomicLong();   // descartes esperados
    private final AtomicLong retriesTotal   = new AtomicLong();

    // --- diagnóstico de backpressure ---
    private final AtomicLong blockedNanos   = new AtomicLong();
    private final AtomicLong startNanos     = new AtomicLong();

    /** Desglose de fallos por causa, para que el email diga QUÉ revisar. */
    private final Map<String, AtomicLong> failuresByReason = new ConcurrentHashMap<>();

    private final Counter publishedCounter;
    private final Counter failedCounter;
    private final Counter retryCounter;
    private final Timer   publishTimer;

    public PublishMetrics(MeterRegistry registry, String batchId, String fileType) {
        this.registry = registry;
        this.batchId  = batchId;
        this.fileType = fileType;

        this.publishedCounter = Counter.builder("gloss.publish.success")
                .tag("file_type", fileType).register(registry);
        this.failedCounter = Counter.builder("gloss.publish.failed")
                .tag("file_type", fileType).register(registry);
        this.retryCounter = Counter.builder("gloss.publish.retry")
                .tag("file_type", fileType).register(registry);
        this.publishTimer = Timer.builder("gloss.publish.latency")
                .tag("file_type", fileType).register(registry);

        this.startNanos.set(System.nanoTime());
    }

    public void recordNodeRead()  { nodesRead.incrementAndGet(); }

    public void recordPublished() {
        published.incrementAndGet();
        publishedCounter.increment();
    }

    public void recordRetry() {
        retriesTotal.incrementAndGet();
        retryCounter.increment();
    }

    public void recordFailure(FailurePhase phase, String errorClass) {
        failedHard.incrementAndGet();
        failedCounter.increment();
        // tag por fase para poder filtrar en el dashboard
        Counter.builder("gloss.publish.failed.by_phase")
                .tag("file_type", fileType)
                .tag("phase", phase.name())
                .tag("error", errorClass)
                .register(registry)
                .increment();
        failuresByReason
                .computeIfAbsent(phase.name() + " / " + errorClass, k -> new AtomicLong())
                .incrementAndGet();
    }

    public void recordSoftReject() { rejectedSoft.incrementAndGet(); }

    public void recordBlockedTime(long nanos) { blockedNanos.addAndGet(nanos); }

    // --- getters para el email / exit code ---
    public String getBatchId()      { return batchId; }
    public String getFileType()     { return fileType; }
    public long getNodesRead()      { return nodesRead.get(); }
    public long getPublished()      { return published.get(); }
    public long getFailedHard()     { return failedHard.get(); }
    public long getRejectedSoft()   { return rejectedSoft.get(); }
    public long getRetriesTotal()   { return retriesTotal.get(); }
    public Map<String, AtomicLong> getFailuresByReason() { return failuresByReason; }

    public long getElapsedMillis() {
        return (System.nanoTime() - startNanos.get()) / 1_000_000;
    }

    /**
     * % del tiempo que el loop pasó bloqueado esperando cupo en la ventana.
     * <5%  → el cuello de botella es la transformación (CPU).
     * >70% → estás estrangulado por Kafka o por MAX_IN_FLIGHT.
     */
    public double getBackpressurePercent() {
        long elapsed = System.nanoTime() - startNanos.get();
        return elapsed == 0 ? 0.0 : (blockedNanos.get() * 100.0) / elapsed;
    }

    public double getThroughputPerSecond() {
        long ms = getElapsedMillis();
        return ms == 0 ? 0.0 : (published.get() * 1000.0) / ms;
    }
}
