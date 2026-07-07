@Component
public class LocalFallbackWriter {

    private static final Logger log = LoggerFactory.getLogger(LocalFallbackWriter.class);

    private final Path fallbackFile;
    private final ObjectMapper objectMapper;
    private final Object lock = new Object();   // writes atómicos si algún día es multi-thread

    public LocalFallbackWriter(
            @Value("${dlq.fallback-dir:/var/gloss/dlq-fallback}") String dir,
            ObjectMapper objectMapper) throws IOException {
        this.objectMapper = objectMapper;
        Path dirPath = Path.of(dir);
        Files.createDirectories(dirPath);
        // un archivo por ejecución del JAR, con timestamp para no pisar
        this.fallbackFile = dirPath.resolve(
                "dlq-fallback-" + System.currentTimeMillis() + ".jsonl");
    }

    /** Escribe el registro fallido como una línea JSON, con fsync para durabilidad real. */
    public void write(FailedRecord record) throws IOException {
        synchronized (lock) {
            String line = objectMapper.writeValueAsString(record) + "\n";
            // APPEND + SYNC: fuerza escritura a disco físico, sobrevive a un crash del proceso
            Files.writeString(fallbackFile, line,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND,
                    StandardOpenOption.SYNC);
        }
    }
}
