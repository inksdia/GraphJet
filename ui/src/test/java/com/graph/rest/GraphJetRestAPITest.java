package com.graph.rest;


import com.graph.BaseSpringTestCase;
import com.graph.beans.HashTag;
import com.graph.beans.Message;
import com.graph.beans.ProfileUser;
import com.graph.service.GraphJetService;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.util.Map;

import static com.graph.GraphHelper.getMessages;

/**
 * Created by saurav on 28/03/17.
 */
public class GraphJetRestAPITest extends BaseSpringTestCase {

    @Autowired
    private GraphJetService graphJetService;
    public static final String graphIdentifier = "58dce7f5e4b00fd7124b47f4";

    @Test
    public void testSpring() {
        if (graphJetService != null) {
            System.out.println("cool");
        } else {
            System.out.println("oops");
        }
    }

    @Test
    public void insertEdge() throws IOException {

        graphJetService.createGraph(graphIdentifier);

        for (int i = 0; i < 100000; i += 10000) {
            IngestMessageDTO dto = createInsertDTO(i);
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            System.out.println(graphJetService.insertEdge(graphIdentifier, dto));
            stopWatch.stop();
            System.out.println("Time: " + stopWatch.getLastTaskTimeMillis());
        }
        final Map<Message, Double> messages = graphJetService.topMessages(graphIdentifier, 10);
        System.out.println("******MESSAGE*******");
        System.out.println(messages);
        if (messages != null && !messages.isEmpty() && messages.keySet().iterator().next() != null) {
            System.out.println(graphJetService.topUsersByMsgId(graphIdentifier, messages.keySet().iterator().next().getId(), 10));
        }
        final Map<ProfileUser, Double> profileUsers = graphJetService.topUsers(graphIdentifier, 10);
        System.out.println("******PROFILE*******");
        System.out.println(profileUsers);
        if (profileUsers != null && !profileUsers.isEmpty()) {
            System.out.println(graphJetService.topMessagesByUserId(graphIdentifier, profileUsers.keySet().iterator().next().getId(), 10));
        }
        final Map<HashTag, Double> hashTags = graphJetService.topHashTags(graphIdentifier, 10);
        System.out.println("******HASHTAGS*******");
        System.out.println(hashTags);
        if (hashTags != null && !hashTags.isEmpty() && hashTags.keySet().iterator().next() != null) {
            System.out.println(graphJetService.topMessagesByHashTags(graphIdentifier, hashTags.keySet().iterator().next().getId(), 10));
        }

        System.out.println("******INFLUENCER*******");
        System.out.println(graphJetService.topInfluencers(graphIdentifier, 100));

    }

    /*private static List<Message> getMessages() {
        List<Message> messages = new ArrayList<>();

        ProfileUser p1 = new ProfileUser();
        p1.setId(11L);
        p1.setName("A");

        ProfileUser p2 = new ProfileUser();
        p2.setId(12L);
        p2.setName("B");

        ProfileUser p3 = new ProfileUser();
        p3.setId(13L);
        p3.setName("C");

        Message m1 = new Message();
        m1.setId(1L);
        m1.setMessage("Hello");
        m1.setUser(p1);

        Message m2 = new Message();
        m2.setId(2L);
        m2.setMessage("World");
        m2.setUser(p2);
        m2.setRetweet(true);
        m2.setRetweetId(1L);

        Message m3 = new Message();
        m3.setId(3L);
        m3.setMessage("Hi");
        m3.setUser(p3);
        m3.setRetweet(true);
        m3.setRetweetId(1L);

        return Lists.newArrayList(m1, m2, m3);
    }*/

    private IngestMessageDTO createInsertDTO(int i) throws IOException {
        IngestMessageDTO dto = new IngestMessageDTO();
        dto.setMessages(getMessages(graphIdentifier, i));
        return dto;
    }

}