package playground.sagafault;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Kafka로 오가는 메시지 하나. 명령·응답·이벤트 모두 같은 모양이다.
 * id는 메시지마다 새로 만들고(Inbox 중복 제거용), saga는 같은 Saga의 모든 메시지가 공유한다.
 */
record Msg(UUID id, UUID saga, String type, Map<String, Object> data) {
    private static final ObjectMapper JSON = new ObjectMapper();

    static Msg of(UUID saga, String type, Object... keyValues) {
        var data = new LinkedHashMap<String, Object>();
        for (int i = 0; i < keyValues.length; i += 2) data.put((String) keyValues[i], keyValues[i + 1]);
        return new Msg(UUID.randomUUID(), saga, type, data);
    }

    long num(String key) {
        return ((Number) data.get(key)).longValue();
    }

    String str(String key) {
        Object v = data.get(key);
        return v == null ? null : v.toString();
    }

    String encode() {
        try {
            return JSON.writeValueAsString(Map.of("id", id.toString(), "saga", saga.toString(), "type", type, "data", data));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static Msg decode(String json) {
        try {
            Map<String, Object> m = JSON.readValue(json, new TypeReference<>() {});
            @SuppressWarnings("unchecked")
            var data = (Map<String, Object>) m.get("data");
            return new Msg(UUID.fromString((String) m.get("id")), UUID.fromString((String) m.get("saga")),
                    (String) m.get("type"), data);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
