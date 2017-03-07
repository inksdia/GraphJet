package com.twitter.graphjet.demo;

import SprESRepo.Graph;
import SprESRepo.Message;
import SprESRepo.ProfileUser;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.twitter.graphjet.bipartite.MultiSegmentPowerLawBipartiteGraph;
import com.twitter.graphjet.bipartite.api.EdgeIterator;
import org.eclipse.jetty.http.HttpStatus;
import org.elasticsearch.common.collect.Tuple;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;

/**
 * Created by dekorate on 06/03/17.
 */
public class GraphRenderer extends HttpServlet {
    MultiSegmentPowerLawBipartiteGraph bigraph;
    Map<Long, ProfileUser> userMap;
    Map<Long, Message> messageMap;

    public GraphRenderer(MultiSegmentPowerLawBipartiteGraph graph, Map<Long, ProfileUser> userMap, Map<Long, Message> messageMap) {
        this.bigraph = graph;
        this.userMap = userMap;
        this.messageMap = messageMap;
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        List<Tuple<Long, Long>> edges = new ArrayList<>();
        for (Map.Entry<Long, ProfileUser> user : userMap.entrySet()) {
            Long userId = user.getKey();
            for (EdgeIterator it = bigraph.getLeftNodeEdges(userId); it != null && it.hasNext(); ) {
                Long id = it.nextLong();
                edges.add(Tuple.tuple(user.getKey(), id));
            }
        }
        Gson gson = new Gson();
        Graph graph = new Graph(this.userMap, this.messageMap, edges);

        response.setStatus(HttpStatus.OK_200);
        response.getWriter().println(gson.toJson(graph, Graph.class));
    }

}
