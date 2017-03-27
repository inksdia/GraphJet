/**
 * Copyright 2016 Twitter. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.graphjet.demo;

import com.twitter.graphjet.bipartite.MultiSegmentPowerLawBipartiteGraph;
import com.twitter.graphjet.bipartite.api.EdgeIterator;
import org.eclipse.jetty.http.HttpStatus;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

/**
 * Servlet of {@link TwitterStreamReader} that fetches the edges incident to either the left (users) or the right
 * (tweets) side of the user-tweet bipartite graph.
 */
public class GetEdgesServlet extends AbstractServlet {
    public enum Side {LEFT, RIGHT}

    private final Side side;
    private final int graphType;

    public GetEdgesServlet(Map<String, SprGraph> graphs, Side side, int graphType) {
        super(graphs);
        this.side = side;
        this.graphType = graphType;
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        if (checkIdentifier(request, response)) {
            return;
        }

        final MultiSegmentPowerLawBipartiteGraph bigraph;
        if (graphType == 0) {
            bigraph = sprGraph.userTweetBigraph;
        } else {
            bigraph = sprGraph.tweetHashtagBigraph;
        }

        long id;
        String p = request.getParameter("id");
        try {
            id = Long.parseLong(p);
        } catch (NumberFormatException e) {
            response.setStatus(HttpStatus.BAD_REQUEST_400); // Signal client error.
            response.getWriter().println("[]");             // Return empty results.
            return;
        }

        StringBuffer output = new StringBuffer();
        output.append("[");
        EdgeIterator iter = side.equals(Side.LEFT) ? bigraph.getLeftNodeEdges(id) :
                bigraph.getRightNodeEdges(id);

        while (iter.hasNext()) {
            output.append("\"");
            output.append(iter.nextLong());
            output.append(iter.hasNext() ? "\", " : "\"");
        }
        output.append("]");

        response.setStatus(HttpStatus.OK_200);
        response.getWriter().println(output.toString());
    }
}
