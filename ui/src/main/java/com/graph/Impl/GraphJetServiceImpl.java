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
    public String insertEdge(IngestMessageDTO ingestMessageDTO) {
        try {
            new Thread(new EdgeInsertion(getGraph(), ingestMessageDTO.getMessages())).start();
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
            List<Long> ids = getTopEntities(iter, true, getGraph().userTweetBigraph, count, false);
            return findEntitiesByIds(ids, getGraph().users);
        } catch (Throwable e) {

        }
        return null;
    }

    @Override
    public List<Message> topMessages(int count) {
        try {
            Iterator<Long> iter = getGraph().tweets.keySet().iterator();
            List<Long> ids = getTopEntities(iter, false, getGraph().userTweetBigraph, count, false);
            return findEntitiesByIds(ids, getGraph().tweets);
        } catch (Throwable e) {

        }
        return null;
    }

    @Override
    public List<HashTag> topHashTags(int count) {
        try {
            Iterator<Long> iter = getGraph().hashtags.keySet().iterator();
            List<Long> ids = getTopEntities(iter, false, getGraph().tweetHashtagBigraph, count, false);
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


    @Override
    public List<HashTag> similarHashTags(String hashTag, int count) {
        try {
            return getSimilarHashTags(hashTag, count, getGraph().tweetHashtagBigraph, getGraph().hashtags);
        } catch (Throwable throwable) {
            logger.error("Error in finding graph ... ", throwable.getStackTrace());
        }
        return null;
    }

    @Override
    public List<ProfileUser> topInfluencers(int count) {
        try {
            Iterator<Long> iter = getGraph().users.keySet().iterator();
            List<Long> ids = getTopEntities(iter, true, getGraph().userTweetBigraph, count, true);
            return findEntitiesByIds(ids, getGraph().users);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return null;
    }

    private List<HashTag> getSimilarHashTags(String hashTag, int count, final MultiSegmentPowerLawBipartiteGraph bigraph, Long2ObjectOpenHashMap<HashTag> hashTags) {
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
            return Collections.EMPTY_LIST;
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
        List<HashTag> similarHashTags = new ArrayList<>();
        for (SimilarityInfo sim : similarityResponse.getRankedSimilarNodes()) {
            logger.debug(hashTags.get(sim.getSimilarNode()) + ": " + sim.toString());
            similarHashTags.add(hashTags.get(sim.getSimilarNode()));
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
    @SuppressWarnings("Duplicates")
    private List<Long> getTopEntities(Iterator<Long> iter, final boolean side, final MultiSegmentPowerLawBipartiteGraph bigraph, int count, boolean secondOrder) {

        PriorityQueue<NodeValueEntry> queue = new PriorityQueue<>(count);
        while (iter.hasNext()) {
            long currId = iter.next();
            int cnt = 0;
            if (secondOrder) {
                EdgeIterator edgeIterator = getEdges(bigraph, currId, side);
                if (edgeIterator != null) {
                    while (edgeIterator.hasNext()) {
                        Long rightNode = edgeIterator.nextLong();
                        cnt += getNodeDegree(bigraph, rightNode, !side);
                    }
                }
            } else {
                cnt = getNodeDegree(bigraph, currId, side);
            }
            if (cnt == 1) continue;
            addToPriorityQueue(queue, new NodeValueEntry(currId, cnt), count);
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
