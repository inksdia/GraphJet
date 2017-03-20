package com.twitter.graphjet.demo;

import SprESRepo.ESRepo;
import SprESRepo.Message;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

/**
 * Created by saurav on 07/03/17.
 */
public class ServletContextImpl extends ServletContextHandler {

    public ServletContextImpl(int options) {
        super(options);
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
            do {
                msgCount += scrollResp.getHits().totalHits();
                for (SearchHit hit : scrollResp.getHits().getHits()) {
                    Message message = esRepo.parseHit(hit);
                    if (message != null) {
                        EsGraphIngestor.indexMessage(message);
                    }
                }
                System.out.println("Message received: [" + msgCount + "]");
                scrollResp = esRepo.doScroll(scrollResp.getScrollId());
                ++iter;
            }
            while (scrollResp.getHits().getHits().length != 0 && iter <= 100);
            esRepo.close();

        }).start();
    }
}
