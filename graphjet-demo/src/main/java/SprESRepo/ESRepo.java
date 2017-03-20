package SprESRepo;

import com.google.common.base.Splitter;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.rest.RestClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by saurav on 06/03/17.
 */
public class ESRepo {

    private String clusterName;
    private String ipPortCSV;

    private Client restClient;

    public ESRepo(String ipPortCSV, String clusterName) {
        this.ipPortCSV = ipPortCSV;
        this.clusterName = clusterName;
        restClient = createElasticSearchRestClient(ipPortCSV);
    }

    public ESRepo() {
        this.ipPortCSV = "localhost:5000";
        this.clusterName = "nqa-es-listening3";
        restClient = createElasticSearchRestClient(ipPortCSV);
    }

    public void close() {
        restClient.close();
    }

    private Client createElasticSearchRestClient(String hostPortsCsv) {
        if (restClient == null) {
            Set<HttpHost> hosts = parseHostPortsCsv(hostPortsCsv);
            RestClient restClient =
                    RestClient.builder(hosts.toArray(new HttpHost[hosts.size()])).setMaxRetryTimeout(new TimeValue(1, TimeUnit.DAYS))
                            .setSocketTimeout(TimeValue.timeValueHours(1)).setConnectTimeout(TimeValue.timeValueHours(1))
                            .setConnectionRequestTimeout(TimeValue.timeValueHours(1))
                            .setMaxResponseSize(new ByteSizeValue(2, ByteSizeUnit.GB)).build();
            String connectedClusterName = restClient.getClusterName();
            if (!clusterName.equals(connectedClusterName)) {
                throw new IllegalStateException(
                        String.format("The given cluster name: '%s' doesn't match the connected cluster: '%s'", clusterName, connectedClusterName));

            }
            this.restClient = restClient;
        }
        return restClient;
    }

    private Set<HttpHost> parseHostPortsCsv(String hostPortsCsv) {
        Set<HttpHost> hosts = new HashSet<>();
        List<String> hostPortsList = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(hostPortsCsv);
        for (String hostPort : hostPortsList) {
            List<String> hostPortSplit = Splitter.on(':').trimResults().omitEmptyStrings().splitToList(hostPort);
            String host = hostPortSplit.get(0);
            int port = Integer.valueOf(hostPortSplit.get(1));
            String scheme = null;
            hosts.add(new HttpHost(host, port, scheme));
        }
        return hosts;
    }

    public SearchRequestBuilder getBuilder(String query) {
        SearchRequestBuilder searchRequestBuilder = this.restClient.prepareSearch("lst_p45_v_5_20151220_0000");
        searchRequestBuilder.setScroll(new TimeValue(60000));
        searchRequestBuilder.setSize(100);
        if (query != null) {

            QueryStringQueryBuilder queryBuilder = QueryBuilders.queryString(query).useDisMax(false).analyzer("spr_standard");
            String[] searchFields = new String[]{"t", "mVadd.m_x", "mVadd.t_x", "mVadd.m_other", "mVadd.m_en", "mVadd.t_en"};

            for (String field : searchFields) {
                queryBuilder.field(field);
            }

            searchRequestBuilder.setPostFilter(FilterBuilders.queryFilter(queryBuilder));
        }
        return searchRequestBuilder;
    }

    public SearchResponse doScroll(String scrollId) {
        return this.restClient.prepareSearchScroll(scrollId).setScroll(new TimeValue(60000)).execute().actionGet();
    }

    public Message parseHit(SearchHit hit) {
        try {
            Map<String, Object> sourceMap = hit.getSource();
            Message message = new Message();
            message.setId(Long.valueOf((String) sourceMap.get("snMId")));
            message.setMessage((String) sourceMap.get("m"));
            ProfileUser profile = new ProfileUser();
            Map user = (Map) sourceMap.get("fU");
            profile.setId(Long.valueOf((String) user.get("uI")));
            profile.setName((String) user.get("sN"));
            message.setUser(profile);
            List<String> hTs = (List<String>) sourceMap.get("hT");
            HashTag[] hashTags = new HashTag[hTs.size()];
            for (int i = 0; i < hTs.size(); i++) {
                hashTags[i] = new HashTag(hTs.get(i));
            }
            message.setHashTags(hashTags);

            if (sourceMap.get("mTp").equals(8)) {
                message.setRetweet(true);
                message.setRetweetId(Long.valueOf((String) ((Map) sourceMap.get("add")).get("qsId")));
            } else {
                message.setRetweet(false);
            }
            return message;
        } catch (Throwable t) {
            t.printStackTrace();
        }
        return null;
    }

    public List<Message> getdocs(String query, int maxIter) {
        SearchRequestBuilder searchRequestBuilder = getBuilder(query);

        SearchResponse scrollResp = searchRequestBuilder.get();
        List<Message> messages = new ArrayList<>();
        int iter = 0;
        do {
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                Message message = parseHit(hit);
                messages.add(message);
            }
            scrollResp = doScroll(scrollResp.getScrollId());
            iter++;
        }
        while (scrollResp.getHits().getHits().length != 0 && iter <= maxIter); // Zero hits mark the end of the scroll and the while loop.
        return messages;
    }

    public static void main(String args[]) {
        ESRepo esRepo = new ESRepo("localhost:5000", "nqa-es-listening3");
        List<Message> messages = esRepo.getdocs("hello", 10);
        System.out.println(messages.size());
    }
}
