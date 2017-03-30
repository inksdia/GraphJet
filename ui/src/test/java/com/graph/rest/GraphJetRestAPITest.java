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

import static com.graph.Helper.getMessages;

/**
 * Created by saurav on 28/03/17.
 */
public class GraphJetRestAPITest extends BaseSpringTestCase {

    @Autowired
    private GraphJetService graphJetService;

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
        IngestMessageDTO dto = createInsertDTO();
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        System.out.println(graphJetService.insertEdge(dto));
        stopWatch.stop();
        System.out.println("Time: " + stopWatch.getLastTaskTimeMillis());
        final Map<Message, Long> messages = graphJetService.topMessages(10);
        System.out.println("******MESSAGE*******");
        System.out.println(messages);
        if (messages != null && !messages.isEmpty()) {
            System.out.println(graphJetService.topUsersByMsgId(messages.keySet().iterator().next().getId(), 10));
        }
        final Map<ProfileUser, Long> profileUsers = graphJetService.topUsers(10);
        System.out.println("******PROFILE*******");
        System.out.println(profileUsers);
        if (profileUsers != null && !profileUsers.isEmpty()) {
            System.out.println(graphJetService.topMessagesByUserId(profileUsers.keySet().iterator().next().getId(), 10));
        }
        final Map<HashTag, Long> hashTags = graphJetService.topHashTags(10);
        System.out.println("******HASHTAGS*******");
        System.out.println(hashTags);
        if (hashTags != null && !hashTags.isEmpty()) {
            System.out.println(graphJetService.topMessagesByHashTags(hashTags.keySet().iterator().next().getId(), 10));
        }

        System.out.println("******INFLUENCER*******");
        System.out.println(graphJetService.topInfluencers(100));

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

    private IngestMessageDTO createInsertDTO() throws IOException {
        IngestMessageDTO dto = new IngestMessageDTO();
        dto.setMessages(getMessages(0));
        return dto;
    }

}