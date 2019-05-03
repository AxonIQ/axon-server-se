package io.axoniq.axonserver;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.config.TagConfiguration;
import io.axoniq.axonserver.grpc.internal.DeleteNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Stores and maintains the tags that are defined for this nodes and any other node which joins the cluster
 * @author Greg Woods
 * @since 4.2
 */
@Component("ClusterTagsCache")
public class ClusterTagsCache {

    private final Logger logger = LoggerFactory.getLogger(ClusterTagsCache.class);

    private Map<String, Map<String,String>> clusterTags = new HashMap<>();

    public ClusterTagsCache(MessagingPlatformConfiguration platformConfiguration, TagConfiguration tagConfiguration){
        clusterTags.put(platformConfiguration.getName(),tagConfiguration.getTags());
    }

    @EventListener
    public void on(ClusterEvents.AxonServerNodeReceived event){
        logger.debug("Adding tags: {} for node: {}", event.getNodeInfo().getTagsMap(),event.getNodeInfo().getNodeName());

        clusterTags.put(event.getNodeInfo().getNodeName(),event.getNodeInfo().getTagsMap());
    }

    @EventListener
    public void on(ClusterEvents.AxonServerInstanceDisconnected event){
        logger.debug("Removing tags for disconnected node: {}",event.getNodeName());

        clusterTags.remove(event.getNodeName());
    }

    public Map<String, Map<String,String>> getClusterTags(){
        return clusterTags;
    }
}
