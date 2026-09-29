package playground.reads;
import jakarta.persistence.*;
@Entity @Table(name="post_record")
public class Post {
    @Id private Long id;
    private String title;
    @Version private long version;
    public String title() { return title; }
    public long version() { return version; }
}
