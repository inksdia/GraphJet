package com.graph.service;

import com.graph.beans.HashTag;
import com.graph.beans.Message;
import com.graph.beans.ProfileUser;
import com.graph.rest.IngestMessageDTO;
import it.unimi.dsi.fastutil.Hash;

import java.util.List;

/**
 * Created by saurav on 27/03/17.
 */
public interface GraphJetService {

    String insertEdge(IngestMessageDTO ingestMessageDTO);

    String createGraph();

    List<ProfileUser> topUsers(int count);

    List<Message> topMessages(int count);

    List<HashTag> topHashTags(int count);

    List<Message> topMessagesByUserId(Long userId, int count);

    List<ProfileUser> topUsersByMsgId(Long msgId, int count);

    List<HashTag> topMessagesByHashTags(Long hashTagId, int count);

    List<Message> topHashTagsByTweets(Long msgId);

    List<HashTag> similarHashTags(String hashTagId, int count);
}
