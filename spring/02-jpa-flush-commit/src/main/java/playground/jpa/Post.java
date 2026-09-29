package playground.jpa;

import jakarta.persistence.*;

@Entity
@Table(name = "post_record")
public class Post {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @Version private long version;
    @Column(nullable = false, unique = true) private String slug;
    @Column(nullable = false) private String title;
    protected Post() { }
    public Post(String slug, String title) { this.slug = slug; this.title = title; }
    public Long id() { return id; }
    public long version() { return version; }
    public void title(String title) { this.title = title; }
    public void slug(String slug) { this.slug = slug; }
}
