package com.twitter.graphjet.demo;

import com.twitter.graphjet.bipartite.MultiSegmentPowerLawBipartiteGraph;
import com.twitter.graphjet.bipartite.api.EdgeIterator;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.graphstream.graph.*;
import org.graphstream.graph.implementations.DefaultGraph;
import org.graphstream.graph.implementations.MultiGraph;
import org.graphstream.ui.graphicGraph.GraphicGraph;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by dekorate on 06/03/17.
 */
public class GraphRenderer extends HttpServlet {
    MultiSegmentPowerLawBipartiteGraph bigraph;
    Long2ObjectOpenHashMap<String> users;

    public GraphRenderer(MultiSegmentPowerLawBipartiteGraph graph, Long2ObjectOpenHashMap<String> userMap) {
        this.bigraph = graph;
        this.users = userMap;
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        Graph graph = new DefaultGraph("0");
        Set<String> nodeSet = new HashSet<>();
        for (Map.Entry<Long, String> user : users.entrySet()) {
            Long userId = user.getKey();
            graph.addNode(user.getValue());
            nodeSet.add(user.getValue());
            for (EdgeIterator it = bigraph.getLeftNodeEdges(userId); it != null && it.hasNext(); ) {
                String id = String.valueOf(it.nextLong());
                if (!nodeSet.contains(id))
                    graph.addNode(id);
                byte type = it.currentEdgeType();
                graph.addEdge(String.valueOf(type), user.getValue(), String.valueOf(id));
            }

        }
        graph.display();
    }

}
