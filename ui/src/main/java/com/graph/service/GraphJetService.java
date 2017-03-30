package com.graph.service;

import com.graph.beans.HashTag;
import com.graph.beans.Message;
import com.graph.beans.ProfileUser;
import com.graph.rest.IngestMessageDTO;

import java.util.Map;

/**
 * Created by saurav on 27/03/17.
 */
public interface GraphJetService {

    String insertEdge(String identifier, IngestMessageDTO ingestMessageDTO);

    String createGraph(String identifier);

    Map<ProfileUser, Long> topUsers(String identifier, int count);

    Map<Message, Long> topMessages(String identifier, int count);

    Map<HashTag, Long> topHashTags(String identifier, int count);

    Map<Message, Long> topMessagesByUserId(String identifier, Long userId, int count);

    Map<ProfileUser, Long> topUsersByMsgId(String identifier, Long msgId, int count);

    Map<HashTag, Long> topMessagesByHashTags(String identifier, Long hashTagId, int count);

    Map<Message, Long> topHashTagsByTweets(String identifier, Long msgId);

    Map<ProfileUser, Long> topInfluencers(String identifier, int count);

    Map<HashTag, Long> similarHashTags(String identifier, String hashTagId, int count);
}
