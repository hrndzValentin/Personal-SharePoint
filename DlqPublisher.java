@Component
public class DlqPublisher {

    private static final Logger log = LoggerFactory.getLogger(DlqPublisher.class);
    private static final Duration DLQ_SEND_TIMEOUT = Duration.ofSeconds(30);

    private final KafkaTemplate<String, String> dlqTemplate;  // DLQ como String, no Avro
    private final String dlqTopic;
    private final LocalFallbackWriter fallbackWriter;
    private final String sourceFile;

    public DlqPublisher(KafkaTemplate<String, String> dlqTemplate,
                        String dlqTopic,
                        LocalFallbackWriter fallbackWriter,
                        String sourceFile) {
        this.dlqTemplate = dlqTemplate;
        this.dlqTopic = dlqTopic;
        this.fallbackWriter = fallbackWriter;
        this.sourceFile = sourceFile;
    }

    /**
     * Intenta salvar un mensaje fallido de forma DURABLE.
     * Estrategia en cascada: DLQ topic (síncrono) → fallback local en disco.
     *
     * @return true si el mensaje quedó a salvo en algún medio durable;
     *         false si NADA lo pudo persistir (irrecuperable → fatal).
     */
    public boolean tryPublish(String originalTopic, String key,
                              String payload, Throwable cause) {
        // 1er intento: DLQ topic, SÍNCRONO (bloqueante con timeout)
        try {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(dlqTopic, key, payload);
            enrichHeaders(record, originalTopic, cause);

            // .get() bloquea hasta el ack: garantía de que sí llegó al DLQ
            dlqTemplate.send(record).get(DLQ_SEND_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

            log.warn("Mensaje key={} enviado al DLQ topic '{}'. Causa: {}",
                    key, dlqTopic, cause.toString());
            return true;

        } catch (Exception dlqEx) {
            // el DLQ topic falló: caemos al fallback local
            log.error("DLQ topic '{}' falló para key={}. Intentando fallback local. Causa DLQ: {}",
                    dlqTopic, key, dlqEx.toString());

            return writeToLocalFallback(originalTopic, key, payload, cause);
        }
    }

    /**
     * Sobrecarga para cuando el fallo fue de un SpecificRecord Avro.
     * Convierte el record a una representación String siempre serializable.
     */
    public boolean tryPublish(String originalTopic, String key,
                              SpecificRecord record, Throwable cause) {
        String payload = safeToString(record, key);
        return tryPublish(originalTopic, key, payload, cause);
    }

    private void enrichHeaders(ProducerRecord<String, String> record,
                               String originalTopic, Throwable cause) {
        record.headers()
            .add("dlq.original-topic", originalTopic.getBytes(StandardCharsets.UTF_8))
            .add("dlq.error-class",   cause.getClass().getName().getBytes(StandardCharsets.UTF_8))
            .add("dlq.error-message", safeMsg(cause).getBytes(StandardCharsets.UTF_8))
            .add("dlq.failed-at",     Instant.now().toString().getBytes(StandardCharsets.UTF_8))
            .add("dlq.source-file",   sourceFile.getBytes(StandardCharsets.UTF_8));
    }

    private boolean writeToLocalFallback(String originalTopic, String key,
                                         String payload, Throwable cause) {
        try {
            fallbackWriter.write(new FailedRecord(
                    originalTopic, key, payload,
                    cause.getClass().getName(), safeMsg(cause),
                    Instant.now(), sourceFile));
            log.warn("Mensaje key={} escrito al fallback local en disco.", key);
            return true;   // a salvo en disco: recuperable manualmente, NO se perdió
        } catch (Exception diskEx) {
            // ni Kafka DLQ ni disco: esto es realmente irrecuperable
            log.error("FATAL: fallback local también falló para key={}. Mensaje EN RIESGO. Causa: {}",
                    key, diskEx.toString(), diskEx);
            return false;
        }
    }

    private String safeToString(SpecificRecord record, String key) {
        try {
            return record.toString();   // Avro toString → JSON-ish, siempre funciona
        } catch (Exception e) {
            // hasta el toString falló: preserva al menos la key para rastreo
            return "{\"__unserializable__\":true,\"key\":\"" + key + "\"}";
        }
    }

    private String safeMsg(Throwable t) {
        return t.getMessage() != null ? t.getMessage() : t.getClass().getSimpleName();
    }
}
