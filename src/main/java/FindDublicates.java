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
import org.elasticsearch.search.sort.SortOrder;

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
    static Node node = ConnectAsNode.connectASNode();
    static Client client = node.client();
    static LongAdder counter = new LongAdder();
    static LongAdder deleteCounter = new LongAdder();
    static String[] returnFields = {"domain"};
    static String searchField = "domain";
    static String[] index = {"marketing008"};
    static String[] indexWhereFind = {"marketing001", "marketing002", "marketing003", "marketing004", "marketing005",
            "marketing006", "marketing007", "marketing009"};

    static final BulkProcessor bulkProcessor = BulkProcessor.builder(
            client,
            new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {

                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                    for (BulkItemResponse bulkItemResponse : response) {
                        if (bulkItemResponse.getFailureMessage() != null) {
                            System.err.println(Thread.currentThread().getName() + " " + bulkItemResponse.getId() + "" +
                                    " " + bulkItemResponse.getFailureMessage());
                        }
                    }
                    System.out.println("TOTAL DELETED: " + deleteCounter);
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                }
            })
            .setBulkActions(500)
            //.setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
            .setBulkSize(new ByteSizeValue(-1))
            .setFlushInterval(TimeValue.timeValueSeconds(300))
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
        SearchResponse scrollResp = scrollES(index, searchFor, 10, returnFields);
        System.out.println(Thread.currentThread().getName() + " TOTAL RECORDS: " + scrollResp.getHits().totalHits());
        while (true) {
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                String hostToFind = hit.field(searchField).getValue().toString();
                FilteredQueryBuilder findDoc = filteredQuery(matchAllQuery(), termFilter(searchField, hostToFind));
                SearchResponse scrollResp2 = scrollES(indexWhereFind, findDoc, 10, returnFields);
                if (scrollResp2.getHits().totalHits() != 0) {
                    System.out.println(Thread.currentThread().getName() + " Document: " + hostToFind + " EQUALS FOUND: "
                            + scrollResp2.getHits()
                            .totalHits());
                    while (true) {
                        for (SearchHit hit2 : scrollResp2.getHits().getHits()) {
                            System.out.println(Thread.currentThread().getName() + " Will be DELETED: Index: " +
                                    hit2.getIndex() + " _ID: " + hit2.getId());
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
                    System.out.println(Thread.currentThread().getName() +" Document: " + hostToFind + " is UNIQUE");
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
        Integer[] reader_ids = {2, 3, 4, 5};
        List<Thread> threads = new ArrayList<>();
        for (Integer reader_id : reader_ids) {
            Thread find = new Thread() {
                public void run() {
                    setName("Thread : " + reader_id);
                    FindDublicates findHosts = new FindDublicates();
                    try {
                        FilteredQueryBuilder searchFor = filteredQuery(matchAllQuery(), andFilter(termFilter
                                ("scanMode", 0), termFilter("reader_id", reader_id)));
                        findHosts.equalHostSearch(searchFor, index, indexWhereFind, reader_id);
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
