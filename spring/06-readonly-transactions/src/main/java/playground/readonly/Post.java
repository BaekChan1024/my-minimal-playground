package playground.readonly;
import jakarta.persistence.*;
@Entity @Table(name="post_record")
public class Post {
    @Id private Long id;
    private String title;
    @Version private long version;
    protected Post() {}
    public Post(long id,String title) { this.id=id;this.title=title; }
    public String title() { return title; }
    public void rename(String title) { this.title=title; }
}
