package com.graph.rest;


import com.graph.beans.Message;

import java.util.List;

/**
 * Created by saurav on 27/03/17.
 */
public class IngestMessageDTO {

    private List<Message> messages;

    public List<Message> getMessages() {
        return messages;
    }

    public void setMessages(List<Message> messages) {
        this.messages = messages;
    }
}