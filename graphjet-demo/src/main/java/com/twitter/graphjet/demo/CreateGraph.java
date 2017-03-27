package com.twitter.graphjet.demo;


import org.elasticsearch.common.joda.time.DateTimeUtils;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Map;

/**
 * Created by saurav on 22/03/17.
 */
public class CreateGraph extends AbstractServlet {

    private static final SecureRandom random = new SecureRandom();

    public CreateGraph(Map<String, SprGraph> graphs) {
        super(graphs);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        SprGraph sprGraph = new SprGraph();
        String identifier = DateTimeUtils.currentTimeMillis() + "" + random.nextLong();
        graphs.put(identifier, sprGraph);

        System.out.println("new graph created with Identifier: " + identifier);

        response.getWriter().println(identifier);

    }

}
