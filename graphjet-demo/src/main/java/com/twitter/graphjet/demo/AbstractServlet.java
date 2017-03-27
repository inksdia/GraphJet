package com.twitter.graphjet.demo;

import com.google.gson.Gson;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

/**
 * Created by saurav on 22/03/17.
 */
public abstract class AbstractServlet extends HttpServlet {

    public static final Gson GSON_INSTANCE = new Gson();
    protected Map<String, SprGraph> graphs;
    protected String identifier = null;
    protected SprGraph sprGraph = null;

    public AbstractServlet(Map<String, SprGraph> graphs) {
        this.graphs = graphs;
    }

    //true-false is opposite
    protected boolean checkIdentifier(HttpServletRequest req, HttpServletResponse response) throws IOException {
        identifier = req.getParameter("key");
        if (identifier == null) {
            System.out.println("Identifier not found " + identifier);
            response.getWriter().println("Graph not found, First create graph");
            return true;
        }

        sprGraph = graphs.get(identifier);
        if (sprGraph == null) {
            System.out.println("Timeout graph deleted " + identifier);
            response.getWriter().println("Graph not found, First create graph");
            return true;
        }
        return false;
    }

}
