package com.graph.rest;


import com.graph.BaseSpringTestCase;
import com.graph.beans.HashTag;
import com.graph.beans.Message;
import com.graph.beans.ProfileUser;
import com.graph.service.GraphJetService;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StopWatch;

import java.io.FileNotFoundException;
import java.util.List;

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
    public void insertEdge() throws FileNotFoundException {
        IngestMessageDTO dto = createInsertDTO();
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        System.out.println(graphJetService.insertEdge(dto));
        stopWatch.stop();
        System.out.println("Time: " + stopWatch.getLastTaskTimeMillis());
        final List<Message> messages = graphJetService.topMessages(10);
        System.out.println("******MESSAGE*******");
        System.out.println(messages);
        if (messages != null && !messages.isEmpty()) {
            System.out.println(graphJetService.topUsersByMsgId(messages.get(0).getId(), 10));
        }
        final List<ProfileUser> profileUsers = graphJetService.topUsers(10);
        System.out.println("******PROFILE*******");
        System.out.println(profileUsers);
        if (profileUsers != null && !profileUsers.isEmpty()) {
            System.out.println(graphJetService.topMessagesByUserId(profileUsers.get(0).getId(), 10));
        }
        final List<HashTag> hashTags = graphJetService.topHashTags(10);
        System.out.println("******HASHTAGS*******");
        System.out.println(hashTags);
        if (hashTags != null && !hashTags.isEmpty()) {
            System.out.println(graphJetService.topMessagesByHashTags(hashTags.get(0).getId(), 10));
        }
    }

    private IngestMessageDTO createInsertDTO() throws FileNotFoundException {
        IngestMessageDTO dto = new IngestMessageDTO();
        dto.setMessages(getMessages());
        return dto;
    }

}