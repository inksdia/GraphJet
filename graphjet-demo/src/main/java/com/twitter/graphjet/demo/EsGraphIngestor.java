package com.twitter.graphjet.demo;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by dekorate on 06/03/17.
 */
public class EsGraphIngestor {

    final static int port = 8888;
    final static String defaultGraph = "_default";

    public static void main(String[] argv) throws Exception {

        final Map<String, SprGraph> graphs = new HashMap<>();
        graphs.put(defaultGraph, new SprGraph()); //default graph

        ServletContextImpl context = new ServletContextImpl(graphs.get(defaultGraph), ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        Server jettyServer = new Server(port);
        jettyServer.setHandler(context);

        context.addServlet(new ServletHolder(new CreateGraph(graphs)), "/createGraph");
        context.addServlet(new ServletHolder(new InsertEdge(graphs)), "/insertEdge");
        context.addServlet(new ServletHolder(new TopUsersServlet(graphs)),
                "/userTweetGraph/topUsers");
        context.addServlet(new ServletHolder(new TopTweetsServlet(graphs, TopTweetsServlet.GraphType.USER_TWEET)), "/userTweetGraph/topTweets");
        context.addServlet(new ServletHolder(new TopTweetsServlet(graphs, TopTweetsServlet.GraphType.TWEET_HASHTAG)), "/tweetHashtagGraph/topTweets");
        context.addServlet(new ServletHolder(new TopHashtagsServlet(graphs)),
                "/tweetHashtagGraph/topHashtags");
        context.addServlet(new ServletHolder(new GetEdgesServlet(graphs, GetEdgesServlet.Side.LEFT, 0)),
                "/userTweetGraphEdges/users");
        context.addServlet(new ServletHolder(new GetEdgesServlet(graphs, GetEdgesServlet.Side.RIGHT, 0)),
                "/userTweetGraphEdges/tweets");
        context.addServlet(new ServletHolder(new GetEdgesServlet(graphs, GetEdgesServlet.Side.LEFT, 1)),
                "/tweetHashtagGraphEdges/tweets");
        context.addServlet(new ServletHolder(new GetEdgesServlet(graphs, GetEdgesServlet.Side.RIGHT, 1)),
                "/tweetHashtagGraphEdges/hashtags");
        context.addServlet(new ServletHolder(new GetSimilarHashtagsServlet(graphs)),
                "/similarHashtags");

        System.out.println(String.format("%tc: Starting service on port %d", new Date(), port));
        try {
            jettyServer.start();
            jettyServer.join();
        } finally {
            jettyServer.destroy();
        }
    }

}
