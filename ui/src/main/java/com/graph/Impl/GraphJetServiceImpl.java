package com.graph.Impl;

import com.graph.beans.HashTag;
import com.graph.beans.Message;
import com.graph.beans.NodeValueEntry;
import com.graph.beans.ProfileUser;
import com.graph.container.Graph;
import com.graph.rest.IngestMessageDTO;
import com.graph.service.EdgeInsertion;
import com.graph.service.GraphJetService;
import com.graph.service.GraphOps;
import com.twitter.graphjet.algorithms.SimilarityInfo;
import com.twitter.graphjet.algorithms.SimilarityResponse;
import com.twitter.graphjet.algorithms.intersection.CosineUpdateNormalization;
import com.twitter.graphjet.algorithms.intersection.IntersectionSimilarity;
import com.twitter.graphjet.algorithms.intersection.IntersectionSimilarityRequest;
import com.twitter.graphjet.algorithms.intersection.RelatedTweetUpdateNormalization;
import com.twitter.graphjet.bipartite.MultiSegmentPowerLawBipartiteGraph;
import com.twitter.graphjet.bipartite.api.EdgeIterator;
import com.twitter.graphjet.stats.NullStatsReceiver;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;

import static java.util.Collections.EMPTY_MAP;

/**
 * Created by saurav on 27/03/17.
 */
@Service
public class GraphJetServiceImpl implements GraphJetService {

    final static Logger logger = LoggerFactory.getLogger(GraphJetService.class);

    /**
     * Insert edge in background
     *
     * @param ingestMessageDTO
     * @return
     */
    @Override
    public String insertEdge(String identifier, IngestMessageDTO ingestMessageDTO) {
        try {
            new EdgeInsertion(getGraph(identifier), ingestMessageDTO.getMessages()).run();
        } catch (Throwable throwable) {
            logger.error("Unexpected error while getting graph", throwable);
            return "Unexpected error occurred, check graph";
        } finally {

        }
        return "Edge insertion Started";
    }

    @Override
    public String createGraph(String identifier) {
        Graph graph = new Graph();
        GraphOps.getInstance().addGraph(identifier, graph);
        logger.error("Graph created with Identifier:" + identifier);
        return identifier;
    }

    @Override
    public Map<ProfileUser, Double> topUsers(String identifier, int count) {
        try {
            Iterator<Long> iter = getGraph(identifier).users.keySet().iterator();
            Map<Long, Double> ids = getTopEntities(iter, true, getGraph(identifier).userTweetBigraph, count, false);
            return findEntitiesByIds(ids, getGraph(identifier).users);
        } catch (Throwable e) {

        }
        return null;
    }

    @Override
    public Map<Message, Double> topMessages(String identifier, int count) {
        try {
            Iterator<Long> iter = getGraph(identifier).tweets.keySet().iterator();
            Map<Long, Double> ids = getTopEntities(iter, false, getGraph(identifier).userTweetBigraph, count, false);
            return findEntitiesByIds(ids, getGraph(identifier).tweets);
        } catch (Throwable e) {

        }
        return null;
    }

    @Override
    public Map<HashTag, Double> topHashTags(String identifier, int count) {
        try {
            Iterator<Long> iter = getGraph(identifier).hashtags.keySet().iterator();
            Map<Long, Double> ids = getTopEntities(iter, false, getGraph(identifier).tweetHashtagBigraph, count, false);
            return findEntitiesByIds(ids, getGraph(identifier).hashtags);
        } catch (Throwable e) {

        }
        return null;
    }

    @Override
    public Map<Message, Double> topMessagesByUserId(String identifier, Long userId, int count) {
        try {
            Map<Long, Double> ids = getTopEntities(userId, true, getGraph(identifier).userTweetBigraph);
            //trim by count or change getTopEntities method, will see
            return findEntitiesByIds(ids, getGraph(identifier).tweets);
        } catch (Throwable t) {

        }
        return null;
    }

    @Override
    public Map<ProfileUser, Double> topUsersByMsgId(String identifier, Long msgId, int count) {
        try {
            Map<Long, Double> ids = getTopEntities(msgId, false, getGraph(identifier).userTweetBigraph);
            return findEntitiesByIds(ids, getGraph(identifier).users);
        } catch (Throwable t) {

        }
        return null;
    }

    @Override
    public Map<HashTag, Double> topMessagesByHashTags(String identifier, Long hashTagId, int count) {
        try {
            Map<Long, Double> ids = getTopEntities(hashTagId, true, getGraph(identifier).tweetHashtagBigraph);
            return findEntitiesByIds(ids, getGraph(identifier).hashtags);
        } catch (Throwable t) {

        }
        return null;
    }

    @Override
    public Map<Message, Double> topHashTagsByTweets(String identifier, Long msgId) {
        try {
            Map<Long, Double> ids = getTopEntities(msgId, true, getGraph(identifier).tweetHashtagBigraph);
            return findEntitiesByIds(ids, getGraph(identifier).tweets);
        } catch (Throwable t) {

        }
        return null;
    }


    @Override
    public Map<HashTag, Double> similarHashTags(String identifier, String hashTag, int count) {
        try {
            return getSimilarHashTags(hashTag, count, getGraph(identifier).tweetHashtagBigraph, getGraph(identifier).hashtags);
        } catch (Throwable throwable) {
            logger.error("Error in finding graph ... ", throwable.getStackTrace());
        }
        return null;
    }

    @Override
    public Map<ProfileUser, Double> topInfluencers(String identifier, int count) {
        try {
            Iterator<Long> iter = getGraph(identifier).users.keySet().iterator();
            Map<Long, Double> ids = getTopEntities(iter, true, getGraph(identifier).userTweetBigraph, count, true);
            return findEntitiesByIds(ids, getGraph(identifier).users);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return null;
    }

    private Map<HashTag, Double> getSimilarHashTags(String hashTag, int count, final MultiSegmentPowerLawBipartiteGraph bigraph, Long2ObjectOpenHashMap<HashTag> hashTags) {
        Long id = Long.valueOf(hashTag);
        int maxNumNeighbors = 100;
        int minNeighborDegree = 1;
        int maxNumSamplesPerNeighbor = 100;
        int minCooccurrence = 2;
        int minDegree = 2;
        double maxLowerMultiplicativeDeviation = 5.0;
        double maxUpperMultiplicativeDeviation = 5.0;

        if (bigraph.getRightNodeDegree(id) == 0) {
            logger.debug("Hashtag #%s not found", hashTag);
            return EMPTY_MAP;
        }

        logger.debug("Running similarity for node " + id);
        IntersectionSimilarityRequest intersectionSimilarityRequest = new IntersectionSimilarityRequest(
                id,
                count,
                new LongOpenHashSet(),
                maxNumNeighbors,
                minNeighborDegree,
                maxNumSamplesPerNeighbor,
                minCooccurrence,
                minDegree,
                maxLowerMultiplicativeDeviation,
                maxUpperMultiplicativeDeviation,
                false);

        RelatedTweetUpdateNormalization cosineUpdateNormalization = new CosineUpdateNormalization();
        IntersectionSimilarity cosineSimilarity = new IntersectionSimilarity(bigraph,
                cosineUpdateNormalization, new NullStatsReceiver());
        SimilarityResponse similarityResponse =
                cosineSimilarity.getSimilarNodes(intersectionSimilarityRequest, new Random());
        ;
        logger.debug("Related hashtags for hashtag: " + hashTag);
        Map<HashTag, Double> similarHashTags = new HashMap<>();
        for (SimilarityInfo sim : similarityResponse.getRankedSimilarNodes()) {
            logger.debug(hashTags.get(sim.getSimilarNode()) + ": " + sim.toString());
            similarHashTags.put(hashTags.get(sim.getSimilarNode()), sim.getWeight());
        }
        return similarHashTags;
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
    private Map<Long, Double> getTopEntities(final Long id, final boolean side, final MultiSegmentPowerLawBipartiteGraph bigraph) {
        Map<Long, Double> entities = new HashMap<>();
        EdgeIterator iter = side ? bigraph.getLeftNodeEdges(id) : bigraph.getRightNodeEdges(id);

        if (iter != null) {
            while (iter.hasNext()) {
                long currId = iter.nextLong();
                long cnt = !side ? bigraph.getLeftNodeDegree(currId) : bigraph.getRightNodeDegree(currId);
                entities.put(iter.nextLong(), (double) cnt);
            }
        }
        return entities;
    }

    private void addToPriorityQueue(Queue<NodeValueEntry> queue, NodeValueEntry nodeValueEntry, int count) {
        if (queue.size() < count) {
            queue.add(nodeValueEntry);
        } else {
            NodeValueEntry peek = queue.peek();
            if (nodeValueEntry.getValue() > peek.getValue()) {
                queue.poll();
                queue.add(nodeValueEntry);
            }
        }
    }

    private int getNodeDegree(final MultiSegmentPowerLawBipartiteGraph bigraph, long id, boolean side) {
        if (side) {
            return bigraph.getLeftNodeDegree(id);
        }
        return bigraph.getRightNodeDegree(id);
    }

    private EdgeIterator getEdges(final MultiSegmentPowerLawBipartiteGraph bigraph, long id, boolean side) {
        if (side) {
            return bigraph.getLeftNodeEdges(id);
        }
        return bigraph.getRightNodeEdges(id);
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
    private Map<Long, Double> getTopEntities(Iterator<Long> iter, final boolean side, final MultiSegmentPowerLawBipartiteGraph bigraph, int count, boolean secondOrder) {

        PriorityQueue<NodeValueEntry> queue = new PriorityQueue<>(count);
        while (iter.hasNext()) {
            long currId = iter.next();
            int normalizer = 1;
            int cnt = 0;
            if (secondOrder) {
                EdgeIterator edgeIterator = getEdges(bigraph, currId, side);
                if (edgeIterator != null) {
                    while (edgeIterator.hasNext()) {
                        Long rightNode = edgeIterator.nextLong();
                        cnt += getNodeDegree(bigraph, rightNode, !side) - 1;
                    }
                }
                normalizer = getNodeDegree(bigraph, currId, side);
            } else {
                cnt = getNodeDegree(bigraph, currId, side);
            }
            if (cnt == 1) continue;
            addToPriorityQueue(queue, new NodeValueEntry(currId, cnt * 1.0 / normalizer), count);
        }

        if (queue.size() == 0) {
            return EMPTY_MAP;
        }

        NodeValueEntry e;
        Map<Long, Double> entries = new HashMap<>(queue.size());
        while ((e = queue.poll()) != null) {
            entries.put(e.getNode(), e.getValue());
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
    private <T> Map<T, Double> findEntitiesByIds(Map<Long, Double> ids, Long2ObjectOpenHashMap<T> hashMap) {
        ids = sortByValue(ids);
        Map<T, Double> entities = new LinkedHashMap<>();
        for (Map.Entry<Long, Double> entry : ids.entrySet()) {
            T obj = hashMap.get(entry.getKey());
            if (obj instanceof Message) {
                Long resolvedId = getResolvedId((Message) obj);
                entities.put(hashMap.get(resolvedId), entry.getValue());
            } else {
                entities.put(hashMap.get(entry.getKey()), entry.getValue());
            }
        }
        return entities;
    }

    private Map<Long, Double> sortByValue(Map<Long, Double> map) {
        List<Map.Entry<Long, Double>> list = new LinkedList<>(map.entrySet());
        Collections.sort(list, (o1, o2) -> -1 * o1.getValue().compareTo(o2.getValue()));

        Map<Long, Double> result = new LinkedHashMap<>();
        for (Map.Entry<Long, Double> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
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
