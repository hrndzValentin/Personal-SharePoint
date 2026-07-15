package com.gloss.publishtokafka;

import java.util.List;

// ============================================================================
//  Tipos de soporte. En el proyecto real, cada uno en su propio archivo.
// ============================================================================

/** Resultado de validar un registro contra las reglas de negocio. */
public record ValidationResult(boolean valid, Severity severity, List<String> errors) {

    public static ValidationResult ok() {
        return new ValidationResult(true, Severity.NONE, List.of());
    }

    public static ValidationResult hard(List<String> errors) {
        return new ValidationResult(false, Severity.HARD, errors);
    }

    public static ValidationResult soft(List<String> errors) {
        return new ValidationResult(false, Severity.SOFT, errors);
    }
}

/**
 * NONE → sin problemas.
 * HARD → dato inválido que requiere intervención humana. Se loguea, NO se publica,
 *        el archivo continúa.
 * SOFT → descarte legítimo esperado (el registro no aplica). Se cuenta, se salta.
 */
enum Severity { NONE, HARD, SOFT }

/** Etapa del pipeline donde murió el registro. Clave para el triage humano. */
enum FailurePhase {
    VALIDATION,                  // no pasó las reglas de negocio
    SERIALIZATION,               // Avro / schema incompatible — no se arregla reintentando
    BROKER_RETRIES_EXHAUSTED     // fallo transitorio que agotó los reintentos
}

/** Se lanza cuando el cluster de Kafka se considera caído: el job debe abortar. */
class BrokerUnavailableException extends RuntimeException {
    BrokerUnavailableException(String message, Throwable cause) { super(message, cause); }
}

/** Error general de publicación (timeout de cierre). */
class PublishingException extends RuntimeException {
    PublishingException(String message) { super(message); }
    PublishingException(String message, Throwable cause) { super(message, cause); }
}

/** Violación de una regla de negocio HARD. */
class BusinessRuleViolationException extends RuntimeException {
    BusinessRuleViolationException(String key, List<String> errors) {
        super("ref=" + key + " viola reglas de negocio: " + errors);
    }
}
