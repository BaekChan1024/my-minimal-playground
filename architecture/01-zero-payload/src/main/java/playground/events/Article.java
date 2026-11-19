package playground.events;

/** 원본 서비스(글 저장소)가 가진 상태. version은 저장할 때마다 1씩 증가한다. */
public record Article(long id, long version, String status, String title, String body) {
    static final String PUBLISHED = "PUBLISHED";
    static final String PRIVATE = "PRIVATE";
    static final String DELETED = "DELETED";

    boolean visible() {
        return PUBLISHED.equals(status);
    }
}
