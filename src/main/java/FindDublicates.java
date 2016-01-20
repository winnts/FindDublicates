import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

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
    static LongAdder counter = new LongAdder();
    static LongAdder deleteCounter = new LongAdder();

    static final BulkProcessor bulkProcessor = BulkProcessor.builder(
            client,
            new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {

                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                    for (BulkItemResponse bulkItemResponse : response) {
                        System.out.println("Deleted: " + deleteCounter);
                        if (bulkItemResponse.getFailureMessage() != null) {
                            System.err.println(Thread.currentThread().getName() + " " + bulkItemResponse.getId() + "" +
                                    " " + bulkItemResponse.getFailureMessage());
                        }
                    }

                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                }
            })
            .setBulkActions(500)
            //.setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
            .setBulkSize(new ByteSizeValue(-1))
            .setFlushInterval(TimeValue.timeValueSeconds(60))
            .setConcurrentRequests(1)
            .build();


    public static SearchResponse scrollES(String[] indexName, QueryBuilder fqb, Integer size, String[] fields) {
        SearchResponse scrollResp = client.prepareSearch(indexName)
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(60000))
                .setQuery(fqb)
                .addFields(fields)
                .setSize(size)
                .execute().actionGet();
        return scrollResp;
    }

    public void deleteFromES(FilteredQueryBuilder findDoc, String[] indexWhereFind) throws IOException {
        DeleteByQueryResponse response = client.prepareDeleteByQuery(indexWhereFind)
                .setQuery(findDoc)
                .execute()
                .actionGet();
    }

    public void equalHostSearch(FilteredQueryBuilder searchFor, String[] index, String[] indexWhereFind, Integer reader_id) throws IOException {
        String[] returnFields = {"domain"};
        SearchResponse scrollResp = scrollES(index, searchFor, 10, returnFields);
        System.out.println("TOTAL RECORDS: " + scrollResp.getHits().totalHits());
        while (true) {
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                String hostToFind = hit.field("domain").getValue().toString();
                System.out.println("Searching for: " + hostToFind + " reader_id: " + reader_id);
                FilteredQueryBuilder findDoc = filteredQuery(matchAllQuery(), termFilter("domain", hostToFind));
                SearchResponse scrollResp2 = scrollES(indexWhereFind, findDoc, 10, returnFields);
                if (scrollResp2.getHits().totalHits() != 0) {
                    System.out.println("EQUALS DOCS FOUND: " + scrollResp2.getHits().totalHits());
//                    deleteFromES(findDoc, indexWhereFind);
//                    deleteCounter.add(scrollResp2.getHits().totalHits());
                    while (true) {
                        for (SearchHit hit2 : scrollResp2.getHits().getHits()) {
                            System.out.println("Will be DELETED: Index: " + hit2.getIndex() + " _ID: " + hit2.getId());
                            bulkProcessor.add(new DeleteRequest(hit2.getIndex(), "doc", hit2.getId()));
                            deleteCounter.increment();
                        }
                        scrollResp2 = client.prepareSearchScroll(scrollResp2.getScrollId()).setScroll(new TimeValue(60000))
                                .execute().actionGet();
                        if (scrollResp2.getHits().getHits().length == 0) {
                            break;
                        }
                    }
                } else {
                    System.out.println(hostToFind + " UNIQUE");
                }
                counter.increment();
                System.out.println("Processed: " + counter);
            }
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000))
                    .execute().actionGet();
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        counter.add(0);
        deleteCounter.add(0);
        Integer[] reader_id = {2, 3, 4, 5};
        String[] index = {"marketing008"};
        String[] indexWhereFind = {"marketing001", "marketing002", "marketing003", "marketing004", "marketing005",
                "marketing006", "marketing007", "marketing009"};
        node = ConnectAsNode.connectASNode();
        client = node.client();
//        client = ConnectAsTransport.connectToEs("main-cluster", "10.32.18.31", 9303);
//        FilteredQueryBuilder searchFor = filteredQuery(matchAllQuery(), andFilter(termFilter("scanMode", 0), termFilter("domain", "cetrom.net")));
        List<Thread> threads = new ArrayList<>();
        for (Integer readers_id : reader_id) {
            Thread find = new Thread() {
                public void run() {
                    setName("Tread number by id: " + readers_id);
                    FindDublicates findHosts = new FindDublicates();
                    try {
                        FilteredQueryBuilder searchFor = filteredQuery(matchAllQuery(), andFilter(termFilter
                                ("scanMode", 0), termFilter("reader_id", readers_id)));
                        findHosts.equalHostSearch(searchFor, index, indexWhereFind, readers_id);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    System.out.println(Thread.currentThread().getName() + " finished");
                }
            };
            find.setDaemon(true);
            find.start();
            threads.add(find);
        }
        for (Thread thread : threads) {
            thread.join();
        }
        Thread.sleep(5000);
        bulkProcessor.flush();
        bulkProcessor.close();
        Thread.sleep(5000);
        client.close();
        node.close();
    }
}
