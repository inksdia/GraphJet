package com.twitter.graphjet.demo;

import SprESRepo.HashTag;
import SprESRepo.Message;
import SprESRepo.ProfileUser;
import com.google.gson.reflect.TypeToken;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by saurav on 21/03/17.
 */
public class InsertEdge extends AbstractServlet {

    public InsertEdge(Map<String, SprGraph> graphs) {
        super(graphs);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        try {
            if (checkIdentifier(req, resp)) {
                return;
            }

            StringBuffer jb = new StringBuffer();
            String line;
            try {
                BufferedReader reader = req.getReader();
                while ((line = reader.readLine()) != null)
                    jb.append(line);
            } catch (Exception e) {

            }

            if (jb != null) {
                final TypeToken<List<Map<String, Object>>> typeToken = new TypeToken<List<Map<String, Object>>>() {
                };
                final List<Map<String, Object>> messages = GSON_INSTANCE.fromJson(jb.toString(), typeToken.getType());
                for (Map<String, Object> message : messages) {
                    try {
                        Message msg = convertToMessage(message);
                        sprGraph.indexMessage(msg);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

        } finally {
            // do nothing for now
        }

    }

    private Message convertToMessage(Map<String, Object> sourceMap) {
        Message message = new Message();
        message.setId(Long.valueOf((String) sourceMap.get("snMsgId")));
        message.setMessage((String) sourceMap.get("message"));
        ProfileUser profile = new ProfileUser();
        Map user = (Map) sourceMap.get("fromSnUser");
        profile.setId(Long.valueOf((String) user.get("userId")));
        profile.setName((String) user.get("screenName"));
        message.setUser(profile);
        List<String> hTs = (List<String>) sourceMap.get("hashTags");
        HashTag[] hashTags = new HashTag[hTs.size()];
        for (int i = 0; i < hTs.size(); i++) {
            hashTags[i] = new HashTag(hTs.get(i));
        }
        message.setHashTags(hashTags);

        if (sourceMap.get("msgType").equals(8)) {
            message.setRetweet(true);
            message.setRetweetId(Long.valueOf((String) ((Map) sourceMap.get("additional")).get("quotedStatusId")));
        } else {
            message.setRetweet(false);
        }
        return message;
    }

}
