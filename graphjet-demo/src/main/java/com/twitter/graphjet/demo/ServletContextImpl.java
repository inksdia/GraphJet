package com.twitter.graphjet.demo;

import SprESRepo.ESRepo;
import SprESRepo.Message;
import org.apache.commons.lang.time.StopWatch;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

/**
 * Created by saurav on
 */
public class ServletContextImpl extends ServletContextHandler {

    private SprGraph sprGraph;

    public ServletContextImpl(SprGraph graph, int sessions) {
        super(sessions);
        this.sprGraph = graph;
        init();
    }

    private void init() {
        new Thread(() -> {
            /*try {
                Thread.sleep(10000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/
            ESRepo esRepo = new ESRepo();
            SearchRequestBuilder searchRequestBuilder = esRepo.getBuilder("hello");
            SearchResponse scrollResp = searchRequestBuilder.get();
            int iter = 0;
            long msgCount = 0;
            final StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            do {
                msgCount += scrollResp.getHits().totalHits();
                for (SearchHit hit : scrollResp.getHits().getHits()) {
                    Message message = esRepo.parseHit(hit);
                    if (message != null) {
                        sprGraph.indexMessage(message);
                    }
                }
                scrollResp = esRepo.doScroll(scrollResp.getScrollId());
                ++iter;
            }
            while (scrollResp.getHits().getHits().length != 0 && iter <= 100);
            stopWatch.stop();
            System.out.println("Message received: [" + msgCount + "], Processed: [" + sprGraph.msgCount + "]");
            System.out.println("Time taken: " + stopWatch.getTime());
            esRepo.close();

        }).start();
    }
}
