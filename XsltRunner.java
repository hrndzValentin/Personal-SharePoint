@Component
public class XsltRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(XsltRunner.class);
    private static final int MAX_IN_FLIGHT = 500;   // ventana; ajústala midiendo

    private final ProcessingConfigResolver configResolver;
    private final KafkaProducerBuilder producerBuilder;
    private final DlqPublisher dlqPublisher;
    private final Processor processor;
    private final ObjectMapper objectMapper;

    // ... constructor ...

    @Override
    public void run(String... args) throws Exception {
        ProcessingConfig config = configResolver.resolve(args);

        KafkaTemplate<String, SpecificRecord> template =
                producerBuilder.build(config.bootstrapServers(), config.schemaRegistryUrl());
        BoundedAvroPublisher publisher =
                new BoundedAvroPublisher(template, dlqPublisher, MAX_IN_FLIGHT);

        boolean success = false;
        try {
            processFile(config, publisher);
            publisher.awaitCompletion();   // bloquea hasta confirmar TODO (topic o DLQ)
            success = true;
        } catch (Exception e) {
            log.error("Procesamiento fallido para {}: {}", config.inputPath(), e.getMessage(), e);
        } finally {
            // el producer debe cerrarse siempre; close() hace un flush final defensivo
            template.destroy();
        }

        // exit code honesto: el orquestador reintenta el job si != 0
        if (!success || publisher.getFailedCount() > 0) {
            log.error("Job terminó con fallos. Confirmados: {}, fallidos: {}",
                    publisher.getSentCount(), publisher.getFailedCount());
            System.exit(1);
        }
        log.info("Job completado OK. Total confirmados: {}", publisher.getSentCount());
    }

    private void processFile(ProcessingConfig config, BoundedAvroPublisher publisher)
            throws Exception {

        XMLInputFactory xif = XMLInputFactory.newInstance();
        xif.setProperty(XMLInputFactory.IS_COALESCING, true);
        xif.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        xif.setProperty("javax.xml.stream.isSupportingExternalEntities", false);

        DocumentBuilder builder = processor.newDocumentBuilder();
        Xslt30Transformer transformer = config.executable().load30();  // reusar: single-thread

        try (InputStream in = Files.newInputStream(config.inputPath())) {
            XMLEventReader reader = xif.createXMLEventReader(in);

            while (reader.hasNext()) {
                XMLEvent peek = reader.peek();
                if (peek.isStartElement()
                        && config.recordElement().equals(peek.asStartElement().getName().getLocalPart())) {

                    String recordXml = readSubtree(reader);

                    // XSLT: XML → JSON canónico
                    StringWriter sw = new StringWriter(2048);
                    Serializer serializer = processor.newSerializer(sw);
                    transformer.applyTemplates(
                            builder.build(new StreamSource(new StringReader(recordXml))),
                            serializer);

                    // JSON → SpecificRecord Avro (valida forma antes de publicar)
                    SpecificRecord record = toAvro(sw.toString());
                    String key = record.get("partyRef").toString();  // ajusta al campo real

                    publisher.publish(config.topic(), key, record);  // async acotado

                } else {
                    reader.nextEvent();
                }
            }
            reader.close();
        }
    }

    private SpecificRecord toAvro(String json) {
        // Jackson→POJO Avro, o un mapper JSON→Avro (ej. avro-json o construcción manual
        // del builder generado). Depende de cómo generes tus clases Avro.
        // ...
    }
}
