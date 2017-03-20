package SprESRepo;

import twitter4j.HashtagEntity;

/**
 * Created by saurav on 07/03/17.
 */
public class HashTag implements HashtagEntity {

    private String text;

    public HashTag(String text) {
        this.text = text;
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
}
