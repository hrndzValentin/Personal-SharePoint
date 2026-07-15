package com.gloss.publishtokafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.sf.saxon.s9api.DocumentBuilder;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.Serializer;
import net.sf.saxon.s9api.XdmNode;
import net.sf.saxon.s9api.Xslt30Transformer;
import net.sf.saxon.s9api.XsltExecutable;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.events.XMLEvent;
import javax.xml.transform.stream.StreamSource;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Runner del JAR efímero. Un archivo por ejecución, luego termina.
 *
 * PIPELINE:
 *   StAX loop (secuencial, O(1) memoria)
 *     → extrae subárbol <PARTY>
 *     → XSLT (Saxon-HE) → JSON
 *     → JSON → SpecificRecord Avro
 *     → validación de reglas de negocio
 *     → publish async con semáforo + reintentos
 *   → awaitCompletion (barrera contable)
 *   → email de resumen
 *   → exit code para el orquestador
 */
@Component
public class XsltRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(XsltRunner.class);

    /** Ventana de mensajes en vuelo. REGLA: MAX_IN_FLIGHT × tamaño_p99 << buffer.memory */
    private static final int  MAX_IN_FLIGHT      = 500;
    private static final long AWAIT_TIMEOUT_MIN  = 5;    // > delivery.timeout.ms (120s)
    /** Guarda contra un nodo patológicamente grande que ningún otro límite cubre. */
    private static final int  MAX_RECORD_BYTES   = 5 * 1024 * 1024;

    private static final XMLOutputFactory XOF = XMLOutputFactory.newInstance();

    private final ProcessingConfigResolver configResolver;
    private final KafkaProducerBuilder producerBuilder;
    private final BusinessRuleValidator validator;
    private final JobSummaryMailer mailer;
    private final io.micrometer.core.instrument.MeterRegistry meterRegistry;
    private final Processor processor;
    private final ObjectMapper objectMapper;

    public XsltRunner(ProcessingConfigResolver configResolver,
                      KafkaProducerBuilder producerBuilder,
                      BusinessRuleValidator validator,
                      JobSummaryMailer mailer,
                      io.micrometer.core.instrument.MeterRegistry meterRegistry,
                      Processor processor,
                      ObjectMapper objectMapper) {
        this.configResolver  = configResolver;
        this.producerBuilder = producerBuilder;
        this.validator       = validator;
        this.mailer          = mailer;
        this.meterRegistry   = meterRegistry;
        this.processor       = processor;
        this.objectMapper    = objectMapper;
    }

    @Override
    public void run(String... args) throws Exception {
        ProcessingConfig config = configResolver.resolve(args);

        // batchId DETERMINISTA: el mismo archivo produce el mismo id en cada reproceso.
        // Esto permite al consumidor reconocer un reintento del job y reconciliar.
        String batchId = buildDeterministicBatchId(config.inputPath());

        PublishMetrics metrics = new PublishMetrics(meterRegistry, batchId, config.fileType());

        log.info("Iniciando procesamiento",
                net.logstash.logback.argument.StructuredArguments.kv("event", "job_start"),
                net.logstash.logback.argument.StructuredArguments.kv("batch_id", batchId),
                net.logstash.logback.argument.StructuredArguments.kv("file", config.inputPath()),
                net.logstash.logback.argument.StructuredArguments.kv("topic", config.topic()));

        KafkaTemplate<String, SpecificRecord> template =
                producerBuilder.build(config.bootstrapServers(), config.schemaRegistryUrl());
        ScheduledExecutorService retryScheduler =
                Executors.newScheduledThreadPool(2, r -> {
                    Thread t = new Thread(r, "kafka-retry");
                    t.setDaemon(true);
                    return t;
                });

        BoundedAvroPublisher publisher = new BoundedAvroPublisher(
                template, validator, metrics, retryScheduler, batchId, MAX_IN_FLIGHT);

        boolean aborted = false;
        Throwable abortCause = null;

        try {
            processFile(config, publisher, metrics, batchId);
            publisher.awaitCompletion(AWAIT_TIMEOUT_MIN);

        } catch (BrokerUnavailableException e) {
            // ÚNICO caso de parada: el cluster está caído, todos los nodos fallarían
            aborted = true;
            abortCause = e;
            log.error("Job abortado por caída del cluster", e);

        } catch (Exception e) {
            aborted = true;
            abortCause = e;
            log.error("Job abortado por error inesperado", e);

        } finally {
            // el producer y el scheduler deben cerrarse SIEMPRE, o el JAR no termina
            try { template.destroy(); } catch (Exception ignored) { }
            retryScheduler.shutdown();
            try {
                if (!retryScheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                    retryScheduler.shutdownNow();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                retryScheduler.shutdownNow();
            }
        }

        // resumen final
        logJobSummary(metrics, batchId, aborted);
        mailer.send(metrics, config.inputPath().getFileName().toString(),
                aborted, abortCause, false);

        // exit code honesto: el orquestador decide si relanza
        int exitCode = resolveExitCode(aborted, metrics);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    // ─── pipeline ────────────────────────────────────────────────────────────

    private void processFile(ProcessingConfig config, BoundedAvroPublisher publisher,
                             PublishMetrics metrics, String batchId) throws Exception {

        XMLInputFactory xif = XMLInputFactory.newInstance();
        xif.setProperty(XMLInputFactory.IS_COALESCING, true);
        xif.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        xif.setProperty("javax.xml.stream.isSupportingExternalEntities", false);

        DocumentBuilder builder = processor.newDocumentBuilder();
        // single-thread ⇒ un solo Transformer reusado para todo el archivo
        Xslt30Transformer transformer = config.executable().load30();

        try (InputStream in = Files.newInputStream(config.inputPath())) {
            XMLEventReader reader = xif.createXMLEventReader(in);

            while (reader.hasNext()) {
                XMLEvent peek = reader.peek();
                boolean isRecordStart = peek.isStartElement()
                        && config.recordElement().equals(
                                peek.asStartElement().getName().getLocalPart());

                if (!isRecordStart) {
                    reader.nextEvent();
                    continue;
                }

                metrics.recordNodeRead();
                String recordXml = readSubtree(reader);

                // guarda contra el nodo gigante: ningún otro límite lo cubre
                if (recordXml.length() > MAX_RECORD_BYTES) {
                    log.error("Nodo excede el tamaño máximo: se omite",
                            net.logstash.logback.argument.StructuredArguments.kv(
                                    "event", "record_oversized"),
                            net.logstash.logback.argument.StructuredArguments.kv(
                                    "batch_id", batchId),
                            net.logstash.logback.argument.StructuredArguments.kv(
                                    "size_bytes", recordXml.length()),
                            net.logstash.logback.argument.StructuredArguments.kv(
                                    "max_bytes", MAX_RECORD_BYTES));
                    metrics.recordFailure(FailurePhase.VALIDATION, "OversizedRecord");
                    continue;
                }

                // XSLT: XML → JSON canónico
                StringWriter sw = new StringWriter(2048);
                Serializer serializer = processor.newSerializer(sw);
                XdmNode node = builder.build(new StreamSource(new StringReader(recordXml)));
                transformer.applyTemplates(node, serializer);
                String json = sw.toString();

                // JSON → Avro
                SpecificRecord record = toAvro(json, config);
                String key = extractKey(record);

                // valida + publica (async, acotado). Un nodo malo NO detiene el archivo.
                publisher.publish(config.topic(), key, record);
            }
            reader.close();
        }
    }

    /** Consume eventos desde el START_ELEMENT actual hasta su END_ELEMENT balanceado. */
    private static String readSubtree(XMLEventReader reader) throws Exception {
        StringWriter sw = new StringWriter(4096);
        XMLEventWriter writer = XOF.createXMLEventWriter(sw);
        int depth = 0;
        try {
            while (reader.hasNext()) {
                XMLEvent event = reader.nextEvent();
                writer.add(event);
                if (event.isStartElement()) {
                    depth++;
                } else if (event.isEndElement()) {
                    depth--;
                    if (depth == 0) {
                        writer.flush();
                        return sw.toString();
                    }
                }
            }
        } finally {
            writer.close();
        }
        throw new IllegalStateException("Elemento sin cierre balanceado");
    }

    // ─── batchId determinista ────────────────────────────────────────────────

    /**
     * El batchId DEBE ser determinista: el mismo archivo produce el mismo id
     * aunque el job se relance. Un UUID aleatorio rompería la reconciliación
     * del consumidor tras un reintento.
     *
     * nombre + hash del contenido detecta además el caso peligroso de un archivo
     * regenerado con el mismo nombre pero contenido distinto.
     */
    private String buildDeterministicBatchId(Path file) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        try (InputStream is = Files.newInputStream(file)) {
            byte[] buf = new byte[8192];   // O(1) memoria: solo hashea, no acumula
            int n;
            while ((n = is.read(buf)) > 0) {
                md.update(buf, 0, n);
            }
        }
        String hash = HexFormat.of().formatHex(md.digest()).substring(0, 12);
        return file.getFileName().toString() + ":" + hash;
    }

    // ─── cierre ──────────────────────────────────────────────────────────────

    private void logJobSummary(PublishMetrics m, String batchId, boolean aborted) {
        log.info("Procesamiento finalizado",
                net.logstash.logback.argument.StructuredArguments.kv("event", "job_end"),
                net.logstash.logback.argument.StructuredArguments.kv("batch_id", batchId),
                net.logstash.logback.argument.StructuredArguments.kv("aborted", aborted),
                net.logstash.logback.argument.StructuredArguments.kv("nodes_read", m.getNodesRead()),
                net.logstash.logback.argument.StructuredArguments.kv("published", m.getPublished()),
                net.logstash.logback.argument.StructuredArguments.kv("failed", m.getFailedHard()),
                net.logstash.logback.argument.StructuredArguments.kv("rejected_soft", m.getRejectedSoft()),
                net.logstash.logback.argument.StructuredArguments.kv("retries", m.getRetriesTotal()),
                net.logstash.logback.argument.StructuredArguments.kv("duration_ms", m.getElapsedMillis()),
                net.logstash.logback.argument.StructuredArguments.kv("throughput_per_sec",
                        String.format("%.1f", m.getThroughputPerSecond())),
                net.logstash.logback.argument.StructuredArguments.kv("backpressure_pct",
                        String.format("%.1f", m.getBackpressurePercent())));
    }

    /**
     * 0 → todo bien.
     * 1 → hubo fallos que requieren revisión humana (NO relanzar: el dato está mal).
     * 2 → el cluster estaba caído (SÍ relanzar cuando se restablezca).
     */
    private int resolveExitCode(boolean aborted, PublishMetrics m) {
        if (aborted)                  return 2;
        if (m.getFailedHard() > 0)    return 1;
        return 0;
    }

    // ─── por implementar según tu schema ─────────────────────────────────────

    private SpecificRecord toAvro(String json, ProcessingConfig config) {
        // Mapea el JSON del XSLT a la clase Avro generada desde tu .avsc.
        // Con Jackson: objectMapper.readValue(json, config.avroClass())
        // o construyendo el builder generado campo a campo.
        throw new UnsupportedOperationException("Implementar según el schema Avro");
    }

    private String extractKey(SpecificRecord record) {
        // key determinista = partyRef, viene del dato mismo.
        // Con la clase generada: ((PartyRecord) record).getPartyRef().toString()
        Object v = record.get(record.getSchema().getField("partyRef").pos());
        return v == null ? null : v.toString();
    }
}
