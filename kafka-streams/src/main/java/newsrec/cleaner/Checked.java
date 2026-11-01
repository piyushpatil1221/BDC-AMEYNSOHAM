package newsrec.cleaner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Outcome of cleaning one raw record: either a clean JSON payload or an error destined for the DLQ. */
public record Checked(String json, String error, String raw) {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static Checked ok(String json) { return new Checked(json, null, null); }

    static Checked bad(String error, String raw) { return new Checked(null, error, raw); }

    boolean isOk() { return error == null; }

    /** Dead-letter envelope: where it came from, why it failed, and the original bytes. */
    String toDlq(String sourceTopic) {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("source_topic", sourceTopic);
        n.put("error", error);
        n.put("raw", raw);
        n.put("failed_at", System.currentTimeMillis());
        return n.toString();
    }
}
