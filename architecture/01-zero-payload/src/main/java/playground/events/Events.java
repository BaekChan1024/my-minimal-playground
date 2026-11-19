package playground.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.UUID;

/** 같은 변경을 세 가지 이벤트 형식으로 만든다. */
final class Events {
    enum Format {
        /** 소비자가 쓸 상태를 전부 싣는다. */
        FAT,
        /** 무엇이 바뀌었는지(ID와 종류)만 싣는다. */
        ZERO,
        /** ID와 종류에 "몇 번째 변경인지"만 더한다. */
        ZERO_VERSION
    }

    /** 소비자가 받는 이벤트. 형식에 따라 비어 있는 필드가 있다. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record Event(String eventId, String type, long contentId, Long version,
                 String status, String title, String body) {}

    static final ObjectMapper JSON = new ObjectMapper();

    private Events() {}

    static String encode(Format format, Article article) {
        var event = new LinkedHashMap<String, Object>();
        event.put("eventId", UUID.randomUUID().toString());
        event.put("type", article.status().equals(Article.DELETED) ? "ContentDeleted" : "ContentChanged");
        event.put("contentId", article.id());
        if (format != Format.ZERO) event.put("version", article.version());
        if (format == Format.FAT) {
            event.put("status", article.status());
            if (!article.status().equals(Article.DELETED)) {
                event.put("title", article.title());
                event.put("body", article.body());
            }
        }
        try {
            return JSON.writeValueAsString(event);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static Event decode(String json) {
        try {
            return JSON.readValue(json, Event.class);
        } catch (Exception e) {
            throw new IllegalStateException("Invalid event: " + json, e);
        }
    }
}
