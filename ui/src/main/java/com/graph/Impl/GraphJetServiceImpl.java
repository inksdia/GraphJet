package com.graph.Impl;

import com.graph.beans.HashTag;
import com.graph.beans.Message;
import com.graph.beans.NodeValueEntry;
import com.graph.beans.ProfileUser;
import com.graph.container.Graph;
import com.graph.rest.InsertEdgeDTO;
import com.graph.service.EdgeInsertion;
import com.graph.service.GraphJetService;
import com.graph.service.GraphOps;
import com.twitter.graphjet.bipartite.MultiSegmentPowerLawBipartiteGraph;
import com.twitter.graphjet.bipartite.api.EdgeIterator;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Created by saurav on 27/03/17.
 */
@Service
public class GraphJetServiceImpl implements GraphJetService {

    final static Logger logger = LoggerFactory.getLogger(GraphJetService.class);

    /**
     * Insert edge in background
     *
     * @param insertEdgeDTO
     * @return
     */
    @Override
    public String insertEdge(InsertEdgeDTO insertEdgeDTO) {
        try {
            new Thread(new EdgeInsertion(getGraph(), insertEdgeDTO.getMessages())).start();
        } catch (Throwable throwable) {
            logger.error("Unexpected error while getting graph", throwable);
            return "Unexpected error occurred, check graph";
        } finally {

        }
        return "Edge insertion Started";
    }

    @Override
    public String createGraph() {
        return null;
    }

    @Override
    public List<ProfileUser> topUsers(int count) {
        try {
            Iterator<Long> iter = getGraph().users.keySet().iterator();
            List<Long> ids = getTopEntities(iter, true, getGraph().userTweetBigraph, count);
            return findEntitiesByIds(ids, getGraph().users);
        } catch (Throwable e) {

        }
        return null;
    }

    @Override
    public List<Message> topMessages(int count) {
        try {
            Iterator<Long> iter = getGraph().tweets.keySet().iterator();
            List<Long> ids = getTopEntities(iter, false, getGraph().userTweetBigraph, count);
            return findEntitiesByIds(ids, getGraph().tweets);
        } catch (Throwable e) {

        }
        return null;
    }

    @Override
    public List<HashTag> topHashTags(int count) {
        try {
            Iterator<Long> iter = getGraph().hashtags.keySet().iterator();
            List<Long> ids = getTopEntities(iter, false, getGraph().tweetHashtagBigraph, count);
            return findEntitiesByIds(ids, getGraph().hashtags);
        } catch (Throwable e) {

        }
        return null;
    }

    @Override
    public List<Message> topMessagesByUserId(Long userId, int count) {
        try {
            List<Long> ids = getTopEntities(userId, true, getGraph().userTweetBigraph);
            //trim by count or change getTopEntities method, will see
            return findEntitiesByIds(ids, getGraph().tweets);
        } catch (Throwable t) {

        }
        return null;
    }

    @Override
    public List<ProfileUser> topUsersByMsgId(Long msgId, int count) {
        try {
            List<Long> ids = getTopEntities(msgId, false, getGraph().userTweetBigraph);
            return findEntitiesByIds(ids, getGraph().users);
        } catch (Throwable t) {

        }
        return null;
    }

    @Override
    public List<HashTag> topMessagesByHashTags(Long hashTagId, int count) {
        try {
            List<Long> ids = getTopEntities(hashTagId, true, getGraph().tweetHashtagBigraph);
            return findEntitiesByIds(ids, getGraph().hashtags);
        } catch (Throwable t) {

        }
        return null;
    }

    @Override
    public List<Message> topHashTagsByTweets(Long msgId) {
        try {
            List<Long> ids = getTopEntities(msgId, true, getGraph().tweetHashtagBigraph);
            return findEntitiesByIds(ids, getGraph().tweets);
        } catch (Throwable t) {

        }
        return null;
    }


    /***
     * Return top entities which is connected with Id, it can be on either side and
     * returns top entites from other side
     *
     * @param id
     * @param side
     * @param bigraph
     * @return
     */
    private List<Long> getTopEntities(final Long id, final boolean side, final MultiSegmentPowerLawBipartiteGraph bigraph) {
        List<Long> entities = new ArrayList<>();
        EdgeIterator iter = side ? bigraph.getLeftNodeEdges(id) : bigraph.getRightNodeEdges(id);

        if (iter != null) {
            while (iter.hasNext()) {
                entities.add(iter.nextLong());
            }
        }
        return entities;
    }

    /***
     * Return top entities on same side of iter
     *
     * @param iter
     * @param side
     * @param bigraph
     * @param count
     * @return
     */
    @SuppressWarnings("Duplicates")
    private List<Long> getTopEntities(Iterator<Long> iter, final boolean side, final MultiSegmentPowerLawBipartiteGraph bigraph, int count) {

        PriorityQueue<NodeValueEntry> queue = new PriorityQueue<>(count);
        while (iter.hasNext()) {
            long currId = iter.next();
            int cnt;
            if (side) {
                cnt = bigraph.getLeftNodeDegree(currId);
            } else {
                cnt = bigraph.getRightNodeDegree(currId);
            }
            if (cnt == 1) continue;

            if (queue.size() < count) {
                queue.add(new NodeValueEntry(currId, cnt));
            } else {
                NodeValueEntry peek = queue.peek();
                if (cnt > peek.getValue()) {
                    queue.poll();
                    queue.add(new NodeValueEntry(currId, cnt));
                }
            }
        }

        if (queue.size() == 0) {
            return Collections.EMPTY_LIST;
        }

        NodeValueEntry e;
        List<Long> entries = new ArrayList<>(queue.size());
        while ((e = queue.poll()) != null) {
            entries.add(e.getNode());
        }
        return entries;

    }

    /***
     * Search Id from map
     *
     * @param ids
     * @param hashMap
     * @param <T>
     * @return
     */
    private <T> List<T> findEntitiesByIds(List<Long> ids, Long2ObjectOpenHashMap<T> hashMap) {
        List<T> entities = new ArrayList<>();
        for (Long id : ids) {
            T obj = hashMap.get(id);
            if (obj instanceof Message) {
                Long resolvedId = getResolvedId((Message) obj);
                entities.add(hashMap.get(resolvedId));
            } else {
                entities.add(hashMap.get(id));
            }
        }
        return entities;
    }

    private Long getResolvedId(Message msg) {
        if (msg.isRetweet()) {
            return msg.getRetweetId();
        }
        return msg.getId();
    }

    /***
     * @param key
     * @return
     */
    private String getKeyOrDefault(String key) {
        if (key == null) {
            return GraphOps.defaultGraph;
        }
        return key;
    }

    /***
     * Get graph according to user logged in
     * In Future will use something like UserContext
     *
     * @param key
     * @throws Throwable when no graph found!
     */
    private Graph getGraph(final String key) throws Throwable {
        final String keyOrDefault = getKeyOrDefault(key);
        Graph graph = GraphOps.getInstance().getGraph(keyOrDefault);
        if (graph == null) {
            logger.error("Graph not found");
            throw new Throwable("Unexpected exception occur");
        }
        return graph;
    }

    private Graph getGraph() throws Throwable {
        return getGraph(null);
    }
}
