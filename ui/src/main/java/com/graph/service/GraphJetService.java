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

    String insertEdge(IngestMessageDTO ingestMessageDTO);

    String createGraph(String identifier);

    Map<ProfileUser, Long> topUsers(int count);

    Map<Message, Long> topMessages(int count);

    Map<HashTag, Long> topHashTags(int count);

    Map<Message, Long> topMessagesByUserId(Long userId, int count);

    Map<ProfileUser, Long> topUsersByMsgId(Long msgId, int count);

    Map<HashTag, Long> topMessagesByHashTags(Long hashTagId, int count);

    Map<Message, Long> topHashTagsByTweets(Long msgId);

    Map<ProfileUser, Long> topInfluencers(int count);

    Map<HashTag, Long> similarHashTags(String hashTagId, int count);
}
