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

    Map<ProfileUser, Double> topUsers(String identifier, int count);

    Map<Message, Double> topMessages(String identifier, int count);

    Map<HashTag, Double> topHashTags(String identifier, int count);

    Map<Message, Double> topMessagesByUserId(String identifier, Long userId, int count);

    Map<ProfileUser, Double> topUsersByMsgId(String identifier, Long msgId, int count);

    Map<HashTag, Double> topMessagesByHashTags(String identifier, Long hashTagId, int count);

    Map<Message, Double> topHashTagsByTweets(String identifier, Long msgId);

    Map<ProfileUser, Double> topInfluencers(String identifier, int count);

    Map<HashTag, Double> similarHashTags(String identifier, String hashTagId, int count);
}
