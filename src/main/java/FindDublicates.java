import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;

import static org.elasticsearch.index.query.FilterBuilders.andFilter;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

/**
 * Created by adyachenko on 14.01.16.
 */
public class FindDublicates {
    static Node node;
    static Client client;

    public static SearchResponse scrollES(String[] indexName, QueryBuilder fqb, Integer size, String[] fields) {
        SearchResponse scrollResp = client.prepareSearch(indexName)
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(60000))
                .setQuery(fqb)
                .addFields(fields)
                .setSize(size).execute().actionGet();
        return scrollResp;
    }
    public void deleteFromES(FilteredQueryBuilder findDoc, String[] indexWhereFind) throws IOException {
        DeleteByQueryResponse response = client.prepareDeleteByQuery(indexWhereFind)
                .setQuery(findDoc)
                .execute()
                .actionGet();
    }

    public void equalHostSearch(FilteredQueryBuilder searchFor, String[] index, String[] indexWhereFind) throws IOException {
        Integer counter = 0;
        Long deleteCounter = 0L;
        String[] returnFields = {"domain"};
        SearchResponse scrollResp = scrollES(index, searchFor, 10, returnFields);
        System.out.println("TOTAL RECORDS: " + scrollResp.getHits().totalHits());
        while (true) {
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                String hostToFind = hit.field("domain").getValue().toString();
                System.out.println("Searching for: " + hostToFind);
                FilteredQueryBuilder findDoc = filteredQuery(matchAllQuery(), termFilter("domain", hostToFind));
                SearchResponse scrollResp2 = scrollES(indexWhereFind, findDoc, 10, returnFields);
                if (scrollResp2.getHits().totalHits() != 0) {
                    System.out.println("EQUALS DOCS FOUND: " + scrollResp2.getHits().totalHits());
                    deleteFromES(findDoc, indexWhereFind);
                    deleteCounter = deleteCounter + scrollResp2.getHits().totalHits();
//                    while (true) {
//                        for (SearchHit hit2 : scrollResp2.getHits().getHits()) {
//                            System.out.println("Index: " + hit2.getIndex());
//                            System.out.println("_ID: " + hit2.getId());
//                        }
//                        scrollResp2 = client.prepareSearchScroll(scrollResp2.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
//                        if (scrollResp2.getHits().getHits().length == 0) {
//                            break;
//                        }
//                    }
                } else {
                    System.out.println(hostToFind + " UNIQUE");
                }
                System.out.println("Processed: " + ++counter + " Deleted: " + deleteCounter);
            }
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        FindDublicates findHosts = new FindDublicates();
        String[] index = {"marketing008"};
        String[] indexWhereFind = {"marketing001", "marketing002", "marketing003", "marketing004", "marketing005", "marketing006", "marketing007", "marketing009"};
        node = ConnectAsNode.connectASNode();
        client = node.client();
//        client = ConnectAsTransport.connectToEs("main-cluster", "10.32.18.31", 9303);
//        FilteredQueryBuilder searchFor = filteredQuery(matchAllQuery(), andFilter(termFilter("scanMode", 0), termFilter("domain", "cetrom.net")));
        FilteredQueryBuilder searchFor = filteredQuery(matchAllQuery(), andFilter(termFilter("scanMode", 0), termFilter("reader_id", 1)));
        findHosts.equalHostSearch(searchFor, index, indexWhereFind);
        Thread.sleep(5000);
        client.close();
        node.close();
    }
}
