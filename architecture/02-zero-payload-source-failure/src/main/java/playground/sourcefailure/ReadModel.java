package playground.sourcefailure;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** 소비자의 조회 모델. 글 ID → (version, 제목). */
final class ReadModel {
    record View(long version, String title) {}

    final Map<Long, View> views = new ConcurrentHashMap<>();

    Optional<View> view(long id) { return Optional.ofNullable(views.get(id)); }

    /** zero payload: 조회한 최신 상태를 그대로 반영한다. 404면 지운다. */
    void applyLatest(long id, Optional<ContentSource.Article> article) {
        if (article.isPresent()) views.put(id, new View(article.get().version(), article.get().title()));
        else views.remove(id);
    }

    /** fat event를 받은 대로 덮어쓴다. */
    void applyFat(Event e) {
        views.put(e.contentId(), new View(e.version(), e.title()));
    }

    /** fat event지만 이미 반영한 version 이하이면 버린다. 버렸으면 false. */
    boolean applyFatGuarded(Event e) {
        var current = views.get(e.contentId());
        if (current != null && current.version() >= e.version()) return false;
        applyFat(e);
        return true;
    }
}
