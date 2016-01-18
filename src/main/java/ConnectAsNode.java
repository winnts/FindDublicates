import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;

import java.io.Closeable;
import java.io.IOException;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Created by adyachenko on 21.10.15.
 */
public class ConnectAsNode implements Closeable {

    static Node node;

    public static Node connectASNode () {
        if(node == null) {
            node = nodeBuilder()
                    .clusterName("main-cluster")
                    .settings(ImmutableSettings.settingsBuilder()
                                    .put("http.enabled", false)
                                    //.put("node.name", "Copy_Indexes")
                                    .put("discovery.zen.ping.multicast.enabled", false)
                                    .put("discovery.zen.ping.unicast.hosts", "10.32.18.31:9303")
                    )
                    .client(true)
                    .node();
        }
        return node;
    }

    @Override
    public void close() throws IOException {
        node.close();
    }
}
