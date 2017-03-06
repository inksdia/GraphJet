package SprESRepo;

import com.google.common.base.Splitter;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.rest.RestClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
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

    private Client createElasticSearchRestClient(String hostPortsCsv) {
        if (restClient != null) {
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
        }
        return restClient;
    }

    static Set<HttpHost> parseHostPortsCsv(String hostPortsCsv) {
        Set<HttpHost> hosts = new HashSet<>();
        List<String> hostPortsList = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(hostPortsCsv);
        for (String hostPort : hostPortsList) {
            List<String> hostPortSplit = Splitter.on(':').trimResults().omitEmptyStrings().splitToList(hostPort);
            String host = hostPortSplit.get(0);
            int port = -1;
            if (hostPortSplit.size() == 2) {
                port = Integer.parseInt(hostPortSplit.get(1));
            }

            port = 9200;
            String scheme = null;
            hosts.add(new HttpHost(host, port, scheme));
        }
        return hosts;
    }

    public List<Message> getdocs(String query) {

        QueryStringQueryBuilder queryBuilder = QueryBuilders.queryString(query).useDisMax(false);
//        queryBuilder.defaultOperator(QueryStringQueryBuilder.Operator.AND);

        List<String> searchFields = new ArrayList<>();


        for (String field : searchFields) {
            queryBuilder.field(field);
        }

        QueryFilterBuilder rv = FilterBuilders.queryFilter(queryBuilder);
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(restClient).setIndices("lst_p5*").setPostFilter(rv);
        SearchResponse response = searchRequestBuilder.get();
        SearchHit[] hits = response.getHits().getHits();

        for (SearchHit hit : hits) {
            Map<String, Object> sourceMap = hit.getSource();
            Message message = new Message();
            message.setId(hit.getId());
            message.setType((String) sourceMap.get("snT"));
            message.setMessage((String) sourceMap.get(""));
            ProfileUser profile = new ProfileUser();
            profile.setId(sourceMap.get());
        }

    }
}
