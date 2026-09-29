package playground.locking;
import jakarta.persistence.*;

@Entity
@Table(name = "versioned_post")
public class VersionedPost {
    @Id private Long id;
    private String title;
    @Version private long version;
    public long version() { return version; }
    public String title() { return title; }
    public void title(String value) { title = value; }
}
