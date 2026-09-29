package playground.locking;
import jakarta.persistence.*;

@Entity
@Table(name = "plain_post")
public class PlainPost {
    @Id private Long id;
    private String title;
    public String title() { return title; }
    public void title(String value) { title = value; }
}
