package com.gloss.publishtokafka;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Verifica cada objeto contra las reglas de negocio ANTES de publicarlo.
 *
 * Vive en Java (no en XSLT) porque estas reglas pueden requerir catálogos,
 * estado o lógica que el stylesheet no debería cargar. El XSLT valida la
 * FORMA del registro; esta clase valida el NEGOCIO.
 *
 * Severidad:
 *   HARD → el dato requiere intervención humana. Se loguea con detalle, no se
 *          publica, y el archivo CONTINÚA con el siguiente nodo.
 *   SOFT → descarte legítimo y esperado. Se cuenta, se salta, sin ruido.
 *
 * IMPORTANTE: acumula TODOS los errores en vez de fallar en el primero.
 * Quien revise el log necesita el diagnóstico completo, no el primer síntoma.
 */
public class BusinessRuleValidator {

    private static final int MAX_LONGDESC_LENGTH = 500;

    private final Set<String> validCategories;
    private final Set<String> validCountries;

    public BusinessRuleValidator(Set<String> validCategories, Set<String> validCountries) {
        this.validCategories = validCategories;
        this.validCountries  = validCountries;
    }

    public ValidationResult validate(SpecificRecord record) {
        List<String> hardErrors = new ArrayList<>();
        List<String> softErrors = new ArrayList<>();

        // ── Regla 1: partyRef obligatorio ────────────────────────────────────
        // HARD: sin key el consumidor no puede deduplicar. Es crítico.
        String partyRef = str(record, "partyRef");
        if (isBlank(partyRef)) {
            hardErrors.add("partyRef ausente o vacío");
        } else if (!partyRef.matches("^[A-Za-z0-9_\\-]+$")) {
            hardErrors.add("partyRef con formato inválido: '" + partyRef + "'");
        }

        // ── Regla 2: category dentro del catálogo permitido ──────────────────
        String category = str(record, "category");
        if (isBlank(category)) {
            hardErrors.add("category ausente");
        } else if (!validCategories.contains(category)) {
            hardErrors.add("category fuera de catálogo: '" + category
                    + "' (permitidas: " + validCategories + ")");
        }

        // ── Regla 3: country contra catálogo ─────────────────────────────────
        String country = str(record, "country");
        if (!isBlank(country) && !validCountries.contains(country)) {
            hardErrors.add("country fuera de catálogo: '" + country + "'");
        }

        // ── Regla 4: longDesc dentro de límite ───────────────────────────────
        // SOFT: no invalida el registro, pero conviene que alguien lo sepa.
        String longDesc = str(record, "longDesc");
        if (longDesc != null && longDesc.length() > MAX_LONGDESC_LENGTH) {
            softErrors.add("longDesc excede " + MAX_LONGDESC_LENGTH
                    + " chars (actual: " + longDesc.length() + ")");
        }

        // ── Regla 5: consistencia entre campos ───────────────────────────────
        // TODO: portar aquí las validaciones del código Java legado.
        //       Clasificar cada una como HARD (bloquea publicación, requiere
        //       revisión humana) o SOFT (informativa, se publica igual).

        if (!hardErrors.isEmpty()) {
            // los soft se anexan para dar contexto completo en el log
            hardErrors.addAll(softErrors);
            return ValidationResult.hard(hardErrors);
        }
        if (!softErrors.isEmpty()) {
            return ValidationResult.soft(softErrors);
        }
        return ValidationResult.ok();
    }

    /** Acceso seguro a un campo Avro por nombre. Devuelve null si no existe. */
    private String str(SpecificRecord record, String fieldName) {
        try {
            Schema.Field field = record.getSchema().getField(fieldName);
            if (field == null) return null;
            Object value = record.get(field.pos());
            return value == null ? null : value.toString();
        } catch (Exception e) {
            return null;
        }
    }

    private boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
