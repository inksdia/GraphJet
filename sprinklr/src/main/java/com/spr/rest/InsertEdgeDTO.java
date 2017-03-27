package com.spr.rest;

import SprESRepo.Message;

import java.util.List;

/**
 * Created by saurav on 27/03/17.
 */
public class InsertEdgeDTO {

    private List<Message> messages;
    private String key;

    public List<Message> getMessages() {
        return messages;
    }

    public void setMessages(List<Message> messages) {
        this.messages = messages;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
