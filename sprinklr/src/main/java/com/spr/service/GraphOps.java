package com.spr.service;

import com.spr.container.SprGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by saurav on 27/03/17.
 */
public class GraphOps {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphOps.class);
    private static final GraphOps INSTANCE = new GraphOps();
    private final static Map<String, SprGraph> graphs = new HashMap<>();

    public final static String defaultGraph = "_default";

    static {
        graphs.put(defaultGraph, new SprGraph()); //default graph
    }

    public static GraphOps getInstance() {
        return INSTANCE;
    }

    //private constructor
    private GraphOps() {

    }

    public Map<String, SprGraph> getGraphs() {
        return graphs;
    }

    public String getDefaultGraph() {
        return defaultGraph;
    }

    public void addGraph(String key, SprGraph graph) {
        graphs.put(key, graph);
    }

    public SprGraph getGraph(String key) {
        return graphs.get(key);
    }

}
