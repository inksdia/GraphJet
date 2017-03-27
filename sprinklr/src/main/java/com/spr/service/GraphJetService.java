package com.spr.service;

import com.spr.beans.Message;
import com.spr.beans.ProfileUser;
import com.spr.rest.InsertEdgeDTO;

import java.util.List;

/**
 * Created by saurav on 27/03/17.
 */
public interface GraphJetService {

    String insertEdge(InsertEdgeDTO insertEdgeDTO);

    String createGraph();

    List<ProfileUser> topUsers(int count);

    List<Message> topMessages(int count);

    List<String> topHashTags(int count);

    List<Message> topMessages(Long userId, int count);

    List<ProfileUser> topUsers(Long msgId, int count);
}
