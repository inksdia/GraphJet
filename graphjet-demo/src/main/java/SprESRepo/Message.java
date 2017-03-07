package SprESRepo;

/**
 * Created by saurav on 06/03/17.
 */
public class Message {
    Long Id;
    ProfileUser user;
    String message;

    public Long getId() {
        return Id;
    }

    public void setId(Long id) {
        Id = id;
    }

    public ProfileUser getUser() {
        return user;
    }

    public void setUser(ProfileUser user) {
        this.user = user;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
