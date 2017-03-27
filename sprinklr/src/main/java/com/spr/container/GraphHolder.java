package com.spr.container;

import SprESRepo.Message;
import org.elasticsearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by saurav on 27/03/17.
 */
public class GraphHolder {
    private Map<Long, SprESRepo.ProfileUser> userProfileMap;
    private Map<Long, SprESRepo.Message> messages;
    private List<Tuple<Long, Long>> links;

    public GraphHolder() {
        this.userProfileMap = new HashMap<>();
        this.messages = new HashMap<>();
        this.links = new ArrayList<>();
    }

    public GraphHolder(Map<Long, SprESRepo.ProfileUser> userProfileMap, Map<Long, SprESRepo.Message> messages, List<Tuple<Long, Long>> edges) {
        this.userProfileMap = userProfileMap;
        this.messages = messages;
        this.links = edges;
    }

    public void addLink(SprESRepo.Message message) {
        SprESRepo.ProfileUser user = message.getUser();
        this.userProfileMap.put(user.getId(), user);
        this.messages.put(message.getId(), message);
        this.links.add(Tuple.tuple(user.getId(), message.getId()));
    }


    public Map<Long, SprESRepo.ProfileUser> getUserProfileMap() {
        return userProfileMap;
    }

    public void setUserProfileMap(Map<Long, SprESRepo.ProfileUser> userProfileMap) {
        this.userProfileMap = userProfileMap;
    }

    public Map<Long, SprESRepo.Message> getMessages() {
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
