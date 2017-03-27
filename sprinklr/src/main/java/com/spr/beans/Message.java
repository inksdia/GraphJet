package com.spr.beans;

import twitter4j.HashtagEntity;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by saurav on 27/03/17.
 */
@XmlRootElement
public class Message implements Serializable {

    Long Id;
    SprESRepo.ProfileUser user;
    String message;
    boolean isRetweet;
    long retweetId;
    HashtagEntity[] hashTags;

    public Long getId() {
        return Id;
    }

    public void setId(Long id) {
        Id = id;
    }

    public SprESRepo.ProfileUser getUser() {
        return user;
    }

    public void setUser(SprESRepo.ProfileUser user) {
        this.user = user;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public boolean isRetweet() {
        return isRetweet;
    }

    public void setRetweet(boolean retweet) {
        isRetweet = retweet;
    }

    public long getRetweetId() {
        return retweetId;
    }

    public void setRetweetId(long retweetId) {
        this.retweetId = retweetId;
    }

    public HashtagEntity[] getHashTags() {
        return hashTags;
    }

    public void setHashTags(HashtagEntity[] hashTags) {
        this.hashTags = hashTags;
    }

    @Override
    public String toString() {
        return "Message{" +
                "Id=" + Id +
                ", user=" + user +
                ", message='" + message + '\'' +
                ", isRetweet=" + isRetweet +
                ", retweetId=" + retweetId +
                ", hashTags=" + Arrays.toString(hashTags) +
                '}';
    }

}
