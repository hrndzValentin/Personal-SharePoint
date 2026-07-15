package com.gloss.publishtokafka;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Contadores del job. Sin dependencias externas: solo estructuras de java.util.concurrent.
 *
 * Doble propósito:
 *  1. Fuente de datos para el log de resumen final (una línea estructurada con todo).
 *  2. Fuente de datos para el email de resumen.
 *
 * Thread-safe: los callbacks de Kafka incrementan desde el hilo de I/O,
 * mientras el loop de procesamiento incrementa desde el hilo principal.
 * Todos los contadores son atómicos, por eso no hace falta sincronizar.
 */
public class PublishMetrics {

    private final String batchId;
    private final String fileType;

    // --- contadores de negocio ---
    private final AtomicLong nodesRead    = new AtomicLong();
    private final AtomicLong published    = new AtomicLong();
    private final AtomicLong failedHard   = new AtomicLong();   // requieren revisión humana
    private final AtomicLong rejectedSoft = new AtomicLong();   // descartes esperados
    private final AtomicLong retriesTotal = new AtomicLong();

    // --- diagnóstico de backpressure ---
    private final AtomicLong blockedNanos = new AtomicLong();
    private final long startNanos;

    /**
     * Desglose de fallos por causa: "FASE / ClaseDeError" -> conteo.
     * Es lo que hace que el email diga QUÉ revisar, no solo cuántos fallaron.
     */
    private final Map<String, AtomicLong> failuresByReason = new ConcurrentHashMap<>();

    public PublishMetrics(String batchId, String fileType) {
        this.batchId    = batchId;
        this.fileType   = fileType;
        this.startNanos = System.nanoTime();
    }

    // ─── registro de eventos ─────────────────────────────────────────────────

    public void recordNodeRead() {
        nodesRead.incrementAndGet();
    }

    public void recordPublished() {
        published.incrementAndGet();
    }

    public void recordRetry() {
        retriesTotal.incrementAndGet();
    }

    /** Fallo que requiere revisión humana. Se agrupa por fase + tipo de error. */
    public void recordFailure(FailurePhase phase, String errorClass) {
        failedHard.incrementAndGet();
        failuresByReason
                .computeIfAbsent(phase.name() + " / " + errorClass, k -> new AtomicLong())
                .incrementAndGet();
    }

    public void recordSoftReject() {
        rejectedSoft.incrementAndGet();
    }

    /** Nanosegundos que el loop pasó bloqueado esperando cupo en la ventana. */
    public void recordBlockedTime(long nanos) {
        blockedNanos.addAndGet(nanos);
    }

    // ─── lectura ─────────────────────────────────────────────────────────────

    public String getBatchId()    { return batchId; }
    public String getFileType()   { return fileType; }
    public long getNodesRead()    { return nodesRead.get(); }
    public long getPublished()    { return published.get(); }
    public long getFailedHard()   { return failedHard.get(); }
    public long getRejectedSoft() { return rejectedSoft.get(); }
    public long getRetriesTotal() { return retriesTotal.get(); }

    public Map<String, AtomicLong> getFailuresByReason() {
        return failuresByReason;
    }

    public long getElapsedMillis() {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }

    public double getThroughputPerSecond() {
        long ms = getElapsedMillis();
        return ms == 0 ? 0.0 : (published.get() * 1000.0) / ms;
    }

    /**
     * % del tiempo total que el loop pasó bloqueado esperando cupo.
     * Es la métrica más informativa del pipeline y sale gratis:
     *   &lt;5%  → el cuello de botella es la transformación (CPU), no Kafka.
     *   >70% → estrangulado por Kafka o por un MAX_IN_FLIGHT muy bajo.
     */
    public double getBackpressurePercent() {
        long elapsed = System.nanoTime() - startNanos;
        return elapsed == 0 ? 0.0 : (blockedNanos.get() * 100.0) / elapsed;
    }
}
