package com.gloss.publishtokafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Email de resumen al terminar el job.
 *
 * FILOSOFÍA: el email es ALERTA y RESUMEN, no un mecanismo de recuperación.
 * Le dice a una persona QUÉ pasó y DÓNDE mirar. El detalle completo de cada
 * fallo (con ref_id, causa y acción sugerida) vive en el log estructurado,
 * que es donde se hace el triage real.
 *
 * Se envía SIEMPRE que haya algo que reportar:
 *  · fallos que requieren revisión humana
 *  · el job abortó por caída del cluster
 * En un job 100% limpio, opcionalmente se omite para no generar ruido.
 */
public class JobSummaryMailer {

    private static final Logger log = LoggerFactory.getLogger(JobSummaryMailer.class);

    private final JavaMailSender mailSender;
    private final String from;
    private final String[] to;

    public JobSummaryMailer(JavaMailSender mailSender, String from, String[] to) {
        this.mailSender = mailSender;
        this.from = from;
        this.to   = to;
    }

    /**
     * @param metrics     contadores del job
     * @param sourceFile  archivo procesado
     * @param aborted     true si el job se detuvo por caída del cluster
     * @param abortCause  causa del aborto, si aplica
     * @param onlyOnIssues si true, no envía nada cuando el job fue 100% limpio
     */
    public void send(PublishMetrics metrics, String sourceFile,
                     boolean aborted, Throwable abortCause, boolean onlyOnIssues) {

        boolean clean = !aborted
                && metrics.getFailedHard() == 0
                && metrics.getRejectedSoft() == 0;

        if (clean && onlyOnIssues) {
            log.info("Job limpio: no se envía email de resumen.");
            return;
        }

        try {
            SimpleMailMessage message = new SimpleMailMessage();
            message.setFrom(from);
            message.setTo(to);
            message.setSubject(buildSubject(metrics, sourceFile, aborted, clean));
            message.setText(buildBody(metrics, sourceFile, aborted, abortCause));
            mailSender.send(message);

            log.info("Email de resumen enviado",
                    org.slf4j.helpers.MessageFormatter.arrayFormat(
                            "batch={} destinatarios={}",
                            new Object[]{metrics.getBatchId(), String.join(",", to)}
                    ).getMessage());

        } catch (Exception e) {
            // El email NO es parte de la garantía: si falla, se loguea y se sigue.
            // El log estructurado ya tiene todo el detalle; el email es conveniencia.
            log.error("No se pudo enviar el email de resumen (el detalle está en los logs)", e);
        }
    }

    private String buildSubject(PublishMetrics m, String sourceFile,
                                boolean aborted, boolean clean) {
        String prefix;
        if (aborted)      prefix = "[ABORTADO]";
        else if (!clean)  prefix = "[CON FALLOS]";
        else              prefix = "[OK]";

        return String.format("%s GLOSS %s — %s — %d publicados, %d fallidos",
                prefix, m.getFileType(), sourceFile,
                m.getPublished(), m.getFailedHard());
    }

    private String buildBody(PublishMetrics m, String sourceFile,
                             boolean aborted, Throwable abortCause) {
        StringBuilder sb = new StringBuilder();

        sb.append("RESUMEN DE PROCESAMIENTO\n");
        sb.append("========================\n\n");
        sb.append("Archivo    : ").append(sourceFile).append('\n');
        sb.append("Tipo       : ").append(m.getFileType()).append('\n');
        sb.append("Batch ID   : ").append(m.getBatchId()).append('\n');
        sb.append("Duración   : ").append(m.getElapsedMillis() / 1000).append(" s\n");
        sb.append('\n');

        if (aborted) {
            sb.append("!! JOB ABORTADO !!\n");
            sb.append("-------------------\n");
            sb.append("El cluster de Kafka se detectó como NO DISPONIBLE.\n");
            sb.append("El archivo NO se procesó completo.\n\n");
            sb.append("Causa: ").append(abortCause != null
                    ? abortCause.getClass().getSimpleName() + " — " + abortCause.getMessage()
                    : "desconocida").append("\n\n");
            sb.append("ACCIÓN REQUERIDA:\n");
            sb.append("  1. Verificar la salud del cluster de Kafka.\n");
            sb.append("  2. Relanzar el job una vez restablecido.\n");
            sb.append("  3. El consumidor deduplicará los ").append(m.getPublished())
              .append(" mensajes ya publicados.\n\n");
        }

        sb.append("CONTEO\n");
        sb.append("------\n");
        sb.append(String.format("  Nodos leídos          : %,d%n", m.getNodesRead()));
        sb.append(String.format("  Publicados OK         : %,d%n", m.getPublished()));
        sb.append(String.format("  Fallidos (revisar)    : %,d%n", m.getFailedHard()));
        sb.append(String.format("  Descartados (regla)   : %,d%n", m.getRejectedSoft()));
        sb.append(String.format("  Reintentos totales    : %,d%n", m.getRetriesTotal()));
        sb.append('\n');

        Map<String, AtomicLong> byReason = m.getFailuresByReason();
        if (!byReason.isEmpty()) {
            sb.append("FALLOS POR CAUSA (requieren revisión)\n");
            sb.append("-------------------------------------\n");
            byReason.entrySet().stream()
                    .sorted((a, b) -> Long.compare(b.getValue().get(), a.getValue().get()))
                    .forEach(e -> sb.append(String.format("  %-45s %,d%n",
                            e.getKey(), e.getValue().get())));
            sb.append('\n');
            sb.append("El detalle de cada fallo (ref_id, causa, intentos) está en los logs.\n");
            sb.append("Filtrar por: batch_id=").append(m.getBatchId()).append('\n');
            sb.append("             event=publish_failed_permanent | publish_failed_exhausted\n");
            sb.append("                 | validation_failed\n");
            sb.append('\n');
        }

        sb.append("RENDIMIENTO\n");
        sb.append("-----------\n");
        sb.append(String.format("  Throughput            : %.1f msg/s%n", m.getThroughputPerSecond()));
        sb.append(String.format("  Tiempo en backpressure: %.1f %%%n", m.getBackpressurePercent()));
        sb.append(diagnoseBackpressure(m.getBackpressurePercent()));

        return sb.toString();
    }

    /** Traduce el % de backpressure a una recomendación accionable. */
    private String diagnoseBackpressure(double pct) {
        if (pct < 5)  return "    → El cuello de botella es la transformación (CPU), no Kafka.\n";
        if (pct < 50) return "    → Balance saludable entre transformación y publicación.\n";
        if (pct < 70) return "    → Kafka marca el ritmo. Aceptable, monitorear.\n";
        return "    → Estrangulado por Kafka. Revisar MAX_IN_FLIGHT, particiones del topic\n"
             + "      o salud del cluster.\n";
    }
}
