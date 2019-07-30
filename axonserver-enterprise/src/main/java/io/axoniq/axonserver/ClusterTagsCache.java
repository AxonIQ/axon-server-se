package io.axoniq.axonserver;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.config.TagsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Stores and maintains the tags that are defined for this node and any other node which joins the cluster
 * @author Greg Woods
 * @since 4.2
 */
@Component("ClusterTagsCache")
public class ClusterTagsCache {

    private static final Logger logger = LoggerFactory.getLogger(ClusterTagsCache.class);

    private Map<String, Map<String,String>> clusterTags = new HashMap<>();

    /**
     * Constructs a new tags cache on startup and adds this node (itself) to the cache from configuration.
     * @param platformConfiguration
     * @param tagsConfiguration
     */
    public ClusterTagsCache(MessagingPlatformConfiguration platformConfiguration, TagsConfiguration tagsConfiguration){
        logger.debug("This node is configured with tag names: {}", tagsConfiguration.getTags().keySet());

        clusterTags.put(platformConfiguration.getName(), tagsConfiguration.getTags());
    }

    /**
     * Receives the new node that has been added/connected to the cluster and stores/updates the tags in the cache
     * @param event new node that has been added
     */
    @EventListener
    public void on(ClusterEvents.AxonServerNodeConnected event){
        logger.debug("Adding tags with names: {} for node: {}", event.getNodeInfo().getTagsMap().keySet(), event.getNodeInfo().getNodeName());

        clusterTags.put(event.getNodeInfo().getNodeName(),event.getNodeInfo().getTagsMap());
    }

    /**
     * Receives that a node has been disconnected and removes its tags from the cache.
     * @param event the node which has been disconnected
     */
    @EventListener
    public void on(ClusterEvents.AxonServerInstanceDisconnected event){
        logger.debug("Removing tags for disconnected node: {}",event.getNodeName());

        clusterTags.remove(event.getNodeName());
    }

    /**
     * @return the current tags for the cluster
     */
    public Map<String, Map<String,String>> getClusterTags(){
        return clusterTags;
    }
}
