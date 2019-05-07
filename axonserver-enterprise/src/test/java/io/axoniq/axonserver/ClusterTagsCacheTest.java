package io.axoniq.axonserver;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.config.TagsConfiguration;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import org.junit.*;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class ClusterTagsCacheTest {

    private ClusterTagsCache testSubject;

    @Before
    public void setUp(){
        MessagingPlatformConfiguration platformConfiguration = new MessagingPlatformConfiguration(null);
        platformConfiguration.setName("ClusterNode1");

        Map<String,String> tagMap = new HashMap<String,String>(){{put("region","Amsterdam-East");}};
        TagsConfiguration tagsConfiguration = new TagsConfiguration();
        tagsConfiguration.setTags(tagMap);

        testSubject = new ClusterTagsCache(platformConfiguration, tagsConfiguration);
    }

    @Test
    public void nodeJoiningAndLeaving() {
        Map<String,String> newNodeTags = new HashMap<String,String>(){{put("region","Utrecht");}};

        NodeInfo nodeInfo = NodeInfo.newBuilder()
                                    .putAllTags(new HashMap<String,String>(){{put("region","Utrecht");}})
                                    .setNodeName("ClusterNode2")
                                    .build();
        ClusterEvents.AxonServerNodeConnected nodeReceived = new ClusterEvents.AxonServerNodeConnected(nodeInfo);

        testSubject.on(nodeReceived);

        assertTrue(testSubject.getClusterTags().containsKey("ClusterNode2"));
        assertEquals(newNodeTags,testSubject.getClusterTags().get("ClusterNode2"));

        testSubject.on(new ClusterEvents.AxonServerInstanceDisconnected("ClusterNode2"));

        assertFalse(testSubject.getClusterTags().containsKey("ClusterNode2"));
    }

    @Test
    public void nonExistantNodeLeaving() {
        testSubject.on(new ClusterEvents.AxonServerInstanceDisconnected("DoesNotExistNode"));

        assertFalse(testSubject.getClusterTags().containsKey("DoesNotExistNode"));
    }

    @Test
    public void nodeTagsOverwritten() {
        Map<String,String> newNodeTags = new HashMap<String,String>(){{put("region","Utrecht");}};

        NodeInfo nodeInfo = NodeInfo.newBuilder()
                                    .putAllTags(new HashMap<String,String>(){{put("region","Utrecht");}})
                                    .setNodeName("ClusterNode1")
                                    .build();
        ClusterEvents.AxonServerNodeConnected nodeReceived = new ClusterEvents.AxonServerNodeConnected(nodeInfo);

        testSubject.on(nodeReceived);
        assertEquals(newNodeTags,testSubject.getClusterTags().get("ClusterNode1"));
    }

}