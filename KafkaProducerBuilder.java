@Component
public class KafkaProducerBuilder {

    /**
     * Construye el KafkaTemplate en runtime porque bootstrap y registry
     * llegan como config dinámica del job, no se conocen al arranque de Spring.
     */
    public KafkaTemplate<String, SpecificRecord> build(String bootstrapServers,
                                                       String schemaRegistryUrl) {
        Map<String, Object> props = new HashMap<>();

        // --- Conexión (dinámica) ---
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        // --- Schema Registry ---
        props.put("schema.registry.url", schemaRegistryUrl);
        // en prod, valida compatibilidad pero NO auto-registres desde el producer
        // si tu governance lo exige (registra el schema por separado/CI):
        props.put("auto.register.schemas", false);
        props.put("use.latest.version", true);

        // --- Durabilidad / exactly-once en el producer ---
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);

        // --- Throughput ---
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65_536);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        // --- Backpressure ---
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60_000);

        DefaultKafkaProducerFactory<String, SpecificRecord> pf =
                new DefaultKafkaProducerFactory<>(props);
        return new KafkaTemplate<>(pf);
    }
}
