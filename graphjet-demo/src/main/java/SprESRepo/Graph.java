package SprESRepo;

import org.elasticsearch.common.collect.Tuple;
import sun.plugin.util.UserProfile;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by dekorate on 06/03/17.
 */
public class Graph {
    Map<Long, ProfileUser> userProfileMap;
    Map<Long, Message> messages;
    List<Tuple<Long, Long>> links;

    public Graph() {
        this.userProfileMap = new HashMap<>();
        this.messages = new HashMap<>();
        this.links = new ArrayList<>();
    }

    public Graph(Map<Long, ProfileUser> userProfileMap, Map<Long, Message> messages, List<Tuple<Long, Long>> edges) {
        this.userProfileMap = userProfileMap;
        this.messages = messages;
        this.links = edges;
    }

    public void addLink(Message message) {
        ProfileUser user = message.getUser();
        this.userProfileMap.put(user.getId(), user);
        this.messages.put(message.getId(), message);
        this.links.add(Tuple.tuple(user.getId(), message.getId()));
    }


    public Map<Long, ProfileUser> getUserProfileMap() {
        return userProfileMap;
    }

    public void setUserProfileMap(Map<Long, ProfileUser> userProfileMap) {
        this.userProfileMap = userProfileMap;
    }

    public Map<Long, Message> getMessages() {
        return messages;
    }

    public void setMessages(Map<Long, Message> messages) {
        this.messages = messages;
    }

    public List<Tuple<Long, Long>> getLinks() {
        return links;
    }

    public void setLinks(List<Tuple<Long, Long>> links) {
        this.links = links;
    }
}
