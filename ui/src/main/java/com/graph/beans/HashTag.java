package com.graph.beans;

import twitter4j.HashtagEntity;

/**
 * Created by saurav on 07/03/17.
 */
public class HashTag implements HashtagEntity {

    private final Long id; //hashcode of text

    private final String text;

    public HashTag(Long id, String text) {
        this.id = id;
        this.text = text;
    }

    public Long getId() {
        return id;
    }

    @Override
    public String getText() {
        return text;
    }

    @Override
    public int getStart() {
        return 0;
    }

    @Override
    public int getEnd() {
        return 0;
    }

    @Override
    public String toString() {
        return "HashTag{" +
                "text='" + text + '\'' +
                '}';
    }
}
