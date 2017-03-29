package com.graph.service;

import com.graph.beans.Message;
import com.graph.container.Graph;

import java.util.List;


/**
 * Created by saurav on 27/03/17.
 */
public class EdgeInsertion implements Runnable {

    private final List<Message> messages;
    private final Graph graph;

    public EdgeInsertion(Graph graph, List<Message> messages) {
        this.graph = graph;
        this.messages = messages;
    }

    @Override
    public void run() {
        if (this.messages == null) {
            return;
        }
        this.messages.forEach(graph::indexMessage);
    }
}
