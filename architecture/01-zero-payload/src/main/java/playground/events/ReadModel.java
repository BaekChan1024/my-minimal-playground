package playground.events;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import playground.events.Events.Event;

/**
 * 소비자가 만드는 조회 모델(예: 공개 글 목록, 알림용 캐시). 공개 상태인 글만 보관한다.
 * 이벤트를 어떻게 해석하는지에 따라 네 가지 소비자를 만든다.
 */
final class ReadModel {
    record View(long version, String title, String body) {}

    /** 다시 시도해야 하는 처리. 소비자는 이 레코드의 오프셋을 커밋하지 않는다. */
    static final class RetryLater extends Exception {
        RetryLater(String message) { super(message); }
    }

    interface Handler {
        void handle(Event event) throws RetryLater;
    }

    final String name;
    final Map<Long, View> views = new HashMap<>();
    /** 적용한 가장 높은 version. 삭제·비공개도 기록해야 늦게 온 옛 변경이 글을 되살리지 않는다. */
    final Map<Long, Long> appliedVersion = new HashMap<>();
    /** 이벤트 하나를 처리한 직후 해당 글의 노출 상태. 중간에 무엇이 보였는지 확인한다. */
    final List<String> steps = new ArrayList<>();
    /** zero payload 소비자가 조회 API로 받은 version. */
    final List<Long> fetchedVersions = new ArrayList<>();
    int skipped;

    ReadModel(String name) { this.name = name; }

    Optional<View> view(long id) { return Optional.ofNullable(views.get(id)); }

    private void apply(long id, long version, boolean visible, String title, String body) {
        if (visible) views.put(id, new View(version, title, body));
        else views.remove(id);
        steps.add(visible ? "v" + version + ":" + title : "absent");
    }

    /** fat event를 받은 대로 덮어쓴다. */
    Handler fatNaive() {
        return e -> apply(e.contentId(), e.version(), Article.PUBLISHED.equals(e.status()), e.title(), e.body());
    }

    /** fat event지만 version이 이미 적용한 값 이하이면 버린다. */
    Handler fatGuarded() {
        return e -> {
            long current = appliedVersion.getOrDefault(e.contentId(), 0L);
            if (e.version() <= current) {
                skipped++;
                steps.add("skip v" + e.version());
                return;
            }
            appliedVersion.put(e.contentId(), e.version());
            apply(e.contentId(), e.version(), Article.PUBLISHED.equals(e.status()), e.title(), e.body());
        };
    }

    /** 이벤트에는 ID만 있다. 원본 API에서 지금 상태를 읽어 반영한다. */
    Handler zero(ContentSource source) {
        return e -> {
            var article = fetch(source, e.contentId());
            article.ifPresent(a -> fetchedVersions.add(a.version()));
            applyFetched(e.contentId(), article);
        };
    }

    /** ID와 version을 받는다. 조회 결과가 이벤트보다 오래됐으면 다시 시도한다. */
    Handler zeroVersion(ContentSource source) {
        return e -> {
            var article = fetch(source, e.contentId());
            article.ifPresent(a -> fetchedVersions.add(a.version()));
            if (article.isPresent() && article.get().version() < e.version()) {
                throw new RetryLater("stale read v" + article.get().version() + " < event v" + e.version());
            }
            applyFetched(e.contentId(), article);
        };
    }

    /**
     * poll 한 번에 받은 이벤트를 글 ID별로 합쳐 한 번만 조회한다.
     * 같은 글의 이벤트가 열 번 와도 결국 최신 상태 하나만 필요하다는 zero payload의 성질을 이용한다.
     */
    void zeroCoalesced(ContentSource source, List<Event> batch) throws RetryLater {
        var ids = new LinkedHashSet<Long>();
        for (var e : batch) ids.add(e.contentId());
        for (long id : ids) {
            var article = fetch(source, id);
            article.ifPresent(a -> fetchedVersions.add(a.version()));
            applyFetched(id, article);
        }
    }

    private void applyFetched(long id, Optional<Article> article) {
        if (article.isPresent()) {
            var a = article.get();
            apply(id, a.version(), a.visible(), a.title(), a.body());
        } else {
            apply(id, 0, false, null, null);
        }
    }

    private static Optional<Article> fetch(ContentSource source, long id) throws RetryLater {
        try {
            return source.fetch(id);
        } catch (ContentSource.Unavailable e) {
            throw new RetryLater(e.getMessage());
        }
    }
}
