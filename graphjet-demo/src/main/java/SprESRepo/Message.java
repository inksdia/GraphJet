package SprESRepo;

/**
 * Created by saurav on 06/03/17.
 */
public class Message {
    String Id;
    String type;
    ProfileUser user;
    String message;

    public String getId() {
        return Id;
    }

    public void setId(String id) {
        Id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
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
