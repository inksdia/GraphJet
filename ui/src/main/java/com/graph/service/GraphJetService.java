package com.graph.service;

import com.graph.beans.HashTag;
import com.graph.beans.Message;
import com.graph.beans.ProfileUser;
import com.graph.rest.InsertEdgeDTO;

import java.util.List;

/**
 * Created by saurav on 27/03/17.
 */
public interface GraphJetService {

    String insertEdge(InsertEdgeDTO insertEdgeDTO);

    String createGraph();

    List<ProfileUser> topUsers(int count);

    List<Message> topMessages(int count);

    List<HashTag> topHashTags(int count);

    List<Message> topMessagesByUserId(Long userId, int count);

    List<ProfileUser> topUsersByMsgId(Long msgId, int count);

    List<HashTag> topMessagesByHashTags(Long hashTagId, int count);

    List<Message> topHashTagsByTweets(Long msgId);
}
