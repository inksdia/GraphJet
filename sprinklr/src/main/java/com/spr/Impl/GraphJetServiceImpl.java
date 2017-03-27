package com.spr.Impl;

import com.spr.beans.Message;
import com.spr.beans.ProfileUser;
import com.spr.container.SprGraph;
import com.spr.rest.InsertEdgeDTO;
import com.spr.service.GraphJetService;
import com.spr.service.GraphOps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;

/**
 * Created by saurav on 27/03/17.
 */
public class GraphJetServiceImpl implements GraphJetService {

    final static Logger logger = LoggerFactory.getLogger(GraphJetService.class);

    private SprGraph graph;

    @Override
    public String insertEdge(InsertEdgeDTO insertEdgeDTO) {
        SprGraph sprGraph = getGraph(insertEdgeDTO.getKey());
        if (sprGraph == null) {
            return "Unexpected Error occured";
        }
    }

    @Override
    public String createGraph() {
        return null;
    }

    @Override
    public List<ProfileUser> topUsers(int count) {
        return null;
    }

    @Override
    public List<Message> topMessages(int count) {
        return null;
    }

    @Override
    public List<String> topHashTags(int count) {
        return null;
    }

    @Override
    public List<Message> topMessages(Long userId, int count) {
        return null;
    }

    @Override
    public List<ProfileUser> topUsers(Long msgId, int count) {
        return null;
    }

    private String getKeyOrDefault(String key) {
        if (key == null) {
            return GraphOps.defaultGraph;
        }
        return key;
    }

    public SprGraph getGraph(final String key) {
        final String keyOrDefault = getKeyOrDefault(key);
        final SprGraph sprGraph = GraphOps.getInstance().getGraph(keyOrDefault);
        if (sprGraph == null) {
            logger.error("Graph not found");
            return null;
        }
        return sprGraph;
    }
}
