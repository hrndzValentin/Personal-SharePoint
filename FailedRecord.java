public record FailedRecord(
        String originalTopic,
        String key,
        String payload,          // JSON del XSLT — siempre serializable
        String errorClass,
        String errorMessage,
        Instant failedAt,
        String sourceFile) {
}
