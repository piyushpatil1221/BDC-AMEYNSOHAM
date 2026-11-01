package newsrec.cleaner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

import java.util.Set;
import java.util.stream.Collectors;

/** Parses a raw string and validates it against a JSON Schema bundled in the jar. */
final class SchemaValidator {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final JsonSchema schema;

    SchemaValidator(String resource) {
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
        this.schema = factory.getSchema(SchemaValidator.class.getResourceAsStream(resource));
    }

    /** @return the parsed node, or throws IllegalArgumentException with a DLQ-friendly reason. */
    JsonNode parse(String raw) {
        JsonNode node;
        try {
            node = MAPPER.readTree(raw);
        } catch (Exception e) {
            throw new IllegalArgumentException("malformed_json: " + e.getClass().getSimpleName());
        }
        if (node == null || !node.isObject()) {
            throw new IllegalArgumentException("malformed_json: not an object");
        }
        Set<ValidationMessage> errors = schema.validate(node);
        if (!errors.isEmpty()) {
            throw new IllegalArgumentException("schema: " + errors.stream()
                    .map(ValidationMessage::getMessage).sorted().collect(Collectors.joining("; ")));
        }
        return node;
    }
}
