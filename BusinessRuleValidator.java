package com.gloss.publishtokafka;

import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Verifica cada objeto contra las reglas de negocio ANTES de publicarlo.
 *
 * Separa dos niveles de severidad:
 *   HARD → el dato está estructuralmente mal / no debería existir. Aborta el job.
 *   SOFT → descarte legítimo esperado (ej. registro que no aplica). Se salta y cuenta.
 *
 * Esta clase es el "helper de revisión" que valida reglas de negocio. Se mantiene
 * en Java (no en XSLT) porque son reglas que pueden requerir catálogos, estado o
 * lógica que el XSLT no debería cargar.
 */
public class BusinessRuleValidator {

    private static final Logger log = LoggerFactory.getLogger(BusinessRuleValidator.class);

    // Ejemplo de catálogo cargado en memoria al arranque (inmutable durante el job).
    // En producción vendría de BD/config; aquí ilustra el patrón.
    private final Set<String> validCategories;

    public BusinessRuleValidator(Set<String> validCategories) {
        this.validCategories = validCategories;
    }

    /**
     * Punto único de validación por registro. Acumula TODOS los errores
     * en vez de fallar en el primero, para dar un diagnóstico completo.
     */
    public ValidationResult validate(SpecificRecord record) {
        List<String> errors = new ArrayList<>();
        Severity severity = Severity.SOFT;   // se eleva a HARD si alguna regla dura falla

        // --- Regla 1: partyRef obligatorio (HARD: sin key no hay dedup posible) ---
        Object partyRef = safeGet(record, "partyRef");
        if (isBlank(partyRef)) {
            errors.add("partyRef ausente o vacío");
            severity = Severity.HARD;   // sin key el consumidor no puede deduplicar → crítico
        }

        // --- Regla 2: category dentro del catálogo permitido (HARD si presente pero inválida) ---
        Object category = safeGet(record, "category");
        if (isBlank(category)) {
            errors.add("category ausente");
            severity = Severity.HARD;
        } else if (!validCategories.contains(category.toString())) {
            errors.add("category no permitida: " + category);
            severity = Severity.HARD;
        }

        // --- Regla 3: longDesc no debería exceder límite del consumidor (SOFT: se trunca aguas abajo) ---
        Object longDesc = safeGet(record, "longDesc");
        if (longDesc != null && longDesc.toString().length() > 500) {
            errors.add("longDesc excede 500 chars (se marcará para revisión)");
            // no eleva severidad: es SOFT, informativo
        }

        // --- Regla 4: ejemplo de regla de consistencia entre campos ---
        // (agrega aquí las validaciones reales que hoy hace tu código Java viejo)

        boolean valid = errors.isEmpty()
                // los SOFT-only no invalidan la publicación; solo los que elevaron a HARD
                || severity == Severity.SOFT;

        return new ValidationResult(valid && severity != Severity.HARD, severity, errors);
    }

    private Object safeGet(SpecificRecord record, String field) {
        try {
            // Avro SpecificRecord expone get(String) vía índice; si el campo no existe lanza.
            // Envolvemos para que un campo faltante no rompa la validación.
            return record.get(record.getSchema().getField(field).pos());
        } catch (Exception e) {
            return null;
        }
    }

    private boolean isBlank(Object o) {
        return o == null || o.toString().isBlank();
    }
}
