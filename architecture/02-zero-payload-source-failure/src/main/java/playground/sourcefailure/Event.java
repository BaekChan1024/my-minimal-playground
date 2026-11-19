package playground.sourcefailure;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.UUID;

/**
 * 소비자가 받는 이벤트. zero payload는 글 ID와 version만, fat event는 제목까지 싣는다.
 * 1편의 결과대로 zero payload에도 version을 남긴다(조회 결과가 오래됐는지 판단하는 데 쓴다).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
record Event(String eventId, String type, long contentId, long version, String title) {
    static Event zero(ContentSource.Article a) {
        return new Event(UUID.randomUUID().toString(), "ContentChanged", a.id(), a.version(), null);
    }

    static Event fat(ContentSource.Article a) {
        return new Event(UUID.randomUUID().toString(), "ContentChanged", a.id(), a.version(), a.title());
    }

    String json() {
        try {
            return ContentSource.JSON.writeValueAsString(this);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static Event parse(String json) {
        try {
            return ContentSource.JSON.readValue(json, Event.class);
        } catch (Exception e) {
            throw new IllegalStateException("Invalid event: " + json, e);
        }
    }
}
