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

import SprESRepo.ProfileUser;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.twitter.graphjet.bipartite.MultiSegmentPowerLawBipartiteGraph;
import org.eclipse.jetty.http.HttpStatus;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;

/**
 * Servlet of {@link TwitterStreamReader} that returns the top <i>k</i> users in terms of degree in the user-tweet
 * bipartite graph.
 */
public class TopUsersServlet extends AbstractServlet {
    private static final Joiner JOINER = Joiner.on(",\n");

    public TopUsersServlet(Map<String, SprGraph> graphs) {
        super(graphs);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        if (checkIdentifier(request, response)) {
            return;
        }

        final MultiSegmentPowerLawBipartiteGraph bigraph = sprGraph.userTweetBigraph;
        final Map<Long, ProfileUser> users = sprGraph.graph.getUserProfileMap();

        int k = 10;
        String p = request.getParameter("k");
        if (p != null) {
            try {
                k = Integer.parseInt(p);
            } catch (NumberFormatException e) {
                // Just eat it, don't need to worry.
            }
        }

        PriorityQueue<NodeValueEntry> queue = new PriorityQueue<>(k);
        Iterator<Long> iter = users.keySet().iterator();
        while (iter.hasNext()) {
            long user = iter.next();
            int cnt = bigraph.getLeftNodeDegree(user);
            if (cnt == 1) continue;

            if (queue.size() < k) {
                queue.add(new NodeValueEntry(user, cnt));
            } else {
                NodeValueEntry peek = queue.peek();
                // Break ties by preferring higher userid (i.e., more recent user)
                if (cnt > peek.getValue() || (cnt == peek.getValue() && user > peek.getNode())) {
                    queue.poll();
                    queue.add(new NodeValueEntry(user, cnt));
                }
            }
        }

        if (queue.size() == 0) {
            response.getWriter().println("[]\n");
            return;
        }

        NodeValueEntry e;
        List<String> entries = new ArrayList<>(queue.size());
        while ((e = queue.poll()) != null) {
            // Note that we explicitly use id_str and treat the tweet id as a String. See:
            // https://dev.twitter.com/overview/api/twitter-ids-json-and-snowflake
            entries.add(String.format("{\"Users\": " + users.get(e.getNode()) + ",\"cnt\":" + (int) e.getValue() + "}"));
        }

        response.setStatus(HttpStatus.OK_200);
        response.getWriter().println("[\n" + JOINER.join(Lists.reverse(entries)) + "\n]");
    }
}
