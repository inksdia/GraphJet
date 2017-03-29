package com.graph.service;

import com.graph.container.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by saurav on 27/03/17.
 */
public class GraphOps {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphOps.class);
    private static final GraphOps INSTANCE = new GraphOps();
    private static final ConcurrentMap<String, Graph> graphs = new ConcurrentHashMap<>();

    public final static String defaultGraph = "_default";

    static {
        graphs.put(defaultGraph, new Graph()); //default graph
    }

    public static GraphOps getInstance() {
        return INSTANCE;
    }

    //private constructor
    private GraphOps() {

    }

    public Map<String, Graph> getGraphs() {
        return graphs;
    }

    public Graph getDefaultGraph() {
        return graphs.get(defaultGraph);
    }

    public void addGraph(String key, Graph graph) {
        graphs.put(key, graph);
    }

    public Graph getGraph(String key) {
        return graphs.get(key);
    }

}
