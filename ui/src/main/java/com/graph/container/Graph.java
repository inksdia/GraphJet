package com.graph.container;

import com.graph.beans.HashTag;
import com.graph.beans.Message;
import com.graph.beans.ProfileUser;
import com.twitter.graphjet.bipartite.MultiSegmentPowerLawBipartiteGraph;
import com.twitter.graphjet.bipartite.segment.IdentityEdgeTypeMask;
import com.twitter.graphjet.stats.NullStatsReceiver;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import org.kohsuke.args4j.Option;
import twitter4j.HashtagEntity;

import java.util.Date;

/**
 * Created by saurav on 22/03/17.
 */
public class Graph {

    public MultiSegmentPowerLawBipartiteGraph userTweetBigraph;
    public MultiSegmentPowerLawBipartiteGraph tweetHashtagBigraph;
    public Long2ObjectOpenHashMap<ProfileUser> users = new Long2ObjectOpenHashMap<>();
    public Long2ObjectOpenHashMap<Message> tweets = new Long2ObjectOpenHashMap<>();
    public Long2ObjectOpenHashMap<HashTag> hashtags = new Long2ObjectOpenHashMap<>();
    int statusCnt = 0;
    public int minorUpdateInterval = 1000;
    public int majorUpdateInterval = 10000;
    private Date start;
    public int msgCount = 0;

    private static class props {
        @Option(name = "-port", metaVar = "[port]", usage = "port")
        int port = 8888;

        // For the month of July 2016, analysis of the sample stream shows approximately 150k tweets per hour.
        // The demo parameters are guesstimates based on this value. Heuristically, we tune the settings so that each
        // segment spans roughly an hour. Since the observations from the sample stream are sparse, we just assume the
        // expected number of left and right nodes to be the same as the number of edges. This is obviously not true,
        // but close enough for demo purposes.

        // Keep track of around eight hours worth of the sample stream
        @Option(name = "-maxSegments", metaVar = "[value]", usage = "maximum number of segments")
        int maxSegments = 8;

        @Option(name = "-maxEdgesPerSegment", metaVar = "[value]", usage = "maximum number of edges in each segment")
        int maxEdgesPerSegment = 150000;

        @Option(name = "-leftSize", metaVar = "[value]", usage = "expected number of nodes on left side")
        int leftSize = 150000;

        @Option(name = "-leftDegree", metaVar = "[value]", usage = "expected degree on left side")
        int leftDegree = 2;

        @Option(name = "-leftPowerLawExponent", metaVar = "[value]", usage = "left side Power Law exponent")
        float leftPowerLawExponent = 2.0f;

        @Option(name = "-rightSize", metaVar = "[value]", usage = "expected number of nodes on right side")
        int rightSize = 150000;

        @Option(name = "-rightDegree", metaVar = "[value]", usage = "expected degree on right side")
        int rightDegree = 2;

        @Option(name = "-rightPowerLawExponent", metaVar = "[value]", usage = "right side Power Law exponent")
        float rightPowerLawExponent = 2.0f;

        @Option(name = "-minorUpdateInterval", metaVar = "[value]", usage = "number of statuses before minor status update")
        int minorUpdateInterval = 1000;

        @Option(name = "-majorUpdateInterval", metaVar = "[value]", usage = "number of statuses before major status update")
        int majorUpdateInterval = 10000;
    }

    public Graph() {
        final props args = new props();

        start = new Date();
        userTweetBigraph =
                new MultiSegmentPowerLawBipartiteGraph(args.maxSegments, args.maxEdgesPerSegment,
                        args.leftSize, args.leftDegree, args.leftPowerLawExponent,
                        args.rightSize, args.rightDegree, args.rightPowerLawExponent,
                        new IdentityEdgeTypeMask(),
                        new NullStatsReceiver());

        tweetHashtagBigraph =
                new MultiSegmentPowerLawBipartiteGraph(args.maxSegments, args.maxEdgesPerSegment,
                        args.leftSize, args.leftDegree, args.leftPowerLawExponent,
                        args.rightSize, args.rightDegree, args.rightPowerLawExponent,
                        new IdentityEdgeTypeMask(),
                        new NullStatsReceiver());
    }

    @SuppressWarnings("Duplicates")
    public void indexMessage(Message message) {
        ++msgCount;
        ProfileUser user = message.getUser();
        long userId = message.getUser().getId();
        long tweetId = message.getId();
        long resolvedTweetId = message.isRetweet() ? message.getRetweetId() : message.getId();
        HashtagEntity[] hashtagEntities = message.getHashTags();

        userTweetBigraph.addEdge(userId, resolvedTweetId, (byte) 0);

        if (!users.containsKey(userId)) {
            users.put(userId, user);
        }

        if (!tweets.containsKey(tweetId)) {
            tweets.put(tweetId, message);
        }
        if (!tweets.containsKey(resolvedTweetId)) {
            tweets.put(resolvedTweetId, message);
        }

        if (hashtagEntities != null) {
            for (HashtagEntity entity : hashtagEntities) {
                long hashtagHash = (long) entity.getText().toLowerCase().hashCode();
                tweetHashtagBigraph.addEdge(tweetId, hashtagHash, (byte) 0);
                if (!hashtags.containsKey(hashtagHash)) {
                    hashtags.put(hashtagHash, new HashTag(hashtagHash, entity.getText().toLowerCase()));
                }
            }
        }

        statusCnt++;

        // Note that status updates are currently performed synchronously (i.e., blocking). Best practices dictate that
        // they should happen on another thread so as to not interfere with ingest, but this is okay for the pruposes
        // of the demo and the volume of the sample stream.

        // Minor status update: just print counters.

        if (statusCnt % minorUpdateInterval == 0) {
            long duration = (new Date().getTime() - start.getTime()) / 1000;

            System.out.println(String.format("%tc: %,d statuses, %,d unique tweets, %,d unique hashtags (observed); " +
                            "%.2f edges/s; totalMemory(): %,d bytes, freeMemory(): %,d bytes",
                    new Date(), statusCnt, tweets.size(), hashtags.size(), (float) statusCnt / duration,
                    Runtime.getRuntime().totalMemory(), Runtime.getRuntime().freeMemory()));
        }

        // Major status update: iterate over right and left nodes.
        if (statusCnt % majorUpdateInterval == 0) {
            int leftCnt = 0;
            LongIterator leftIter = tweets.keySet().iterator();
            while (leftIter.hasNext()) {
                if (userTweetBigraph.getLeftNodeDegree(leftIter.nextLong()) != 0)
                    leftCnt++;
            }

            int rightCnt = 0;
            LongIterator rightIter = hashtags.keySet().iterator();
            while (rightIter.hasNext()) {
                if (userTweetBigraph.getRightNodeDegree(rightIter.nextLong()) != 0)
                    rightCnt++;
            }
            System.out.println(String.format("%tc: Current user-tweet graph state: %,d left nodes (users), " +
                            "%,d right nodes (tweets)",
                    new Date(), leftCnt, rightCnt));
        }

    }

}
