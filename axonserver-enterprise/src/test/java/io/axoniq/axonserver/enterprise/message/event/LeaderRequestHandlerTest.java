package io.axoniq.axonserver.enterprise.message.event;

import io.axoniq.axonserver.enterprise.cluster.manager.EventStoreManager;
import io.axoniq.axonserver.enterprise.cluster.manager.LeaderRequestHandler;
import io.axoniq.axonserver.enterprise.cluster.manager.RequestLeaderEvent;
import io.axoniq.axonserver.grpc.internal.NodeContextInfo;
import junit.framework.TestCase;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.runners.*;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Author: marc
 */
@RunWith(MockitoJUnitRunner.class)
public class LeaderRequestHandlerTest {
    private LeaderRequestHandler testSubject;

    @Before
    public void setUp() {

        testSubject = new LeaderRequestHandler("test",
                context -> null,
                connectorCommand -> {},
                context -> 10L,
                name -> 0
        );
    }

    @Test
    public void leaderRequestLowerSequenceNumber() {
        AtomicBoolean resultHolder = new AtomicBoolean();
        NodeContextInfo request = NodeContextInfo.newBuilder()
                                                 .setNodeName("other")
                                                 .setNrOfMasterContexts(0)
                                                 .setHashKey(EventStoreManager.hash("default", "other"))
                                                 .setContext("default")
                                                 .setMasterSequenceNumber(9)
                                                 .build();
        RequestLeaderEvent event = new RequestLeaderEvent(request, resultHolder::set);
        testSubject.on(event);
        Assert.assertFalse(resultHolder.get());
    }

    @Test
    public void leaderRequestHigerSequenceNumber() {
        AtomicBoolean resultHolder = new AtomicBoolean();
        NodeContextInfo request = NodeContextInfo.newBuilder()
                                                 .setNodeName("other")
                                                 .setNrOfMasterContexts(0)
                                                 .setHashKey(EventStoreManager.hash("default", "other"))
                                                 .setContext("default")
                                                 .setMasterSequenceNumber(11)
                                                 .build();
        RequestLeaderEvent event = new RequestLeaderEvent(request, resultHolder::set);
        testSubject.on(event);
        TestCase.assertTrue(resultHolder.get());
    }

    @Test
    public void leaderRequestSameSequenceNumberLowerHash() {
        AtomicBoolean resultHolder = new AtomicBoolean();
        NodeContextInfo request = NodeContextInfo.newBuilder()
                                                 .setNodeName("other")
                                                 .setNrOfMasterContexts(0)
                                                 .setHashKey(Integer.MIN_VALUE)
                                                 .setContext("default")
                                                 .setMasterSequenceNumber(10)
                                                 .build();
        RequestLeaderEvent event = new RequestLeaderEvent(request, resultHolder::set);
        testSubject.on(event);
        TestCase.assertTrue(resultHolder.get());
    }

    @Test
    public void leaderRequestSameSequenceNumberMoreContexts() {
        AtomicBoolean resultHolder = new AtomicBoolean();
        NodeContextInfo request = NodeContextInfo.newBuilder()
                                                 .setNodeName("other")
                                                 .setNrOfMasterContexts(1)
                                                 .setHashKey(Integer.MIN_VALUE)
                                                 .setContext("default")
                                                 .setMasterSequenceNumber(10)
                                                 .build();
        RequestLeaderEvent event = new RequestLeaderEvent(request, resultHolder::set);
        testSubject.on(event);
        Assert.assertFalse(resultHolder.get());
    }

    @Test
    public void leaderRequestMasterSet() {
        AtomicInteger messageCount = new AtomicInteger();
        testSubject = new LeaderRequestHandler("test",
                context -> "node2",
                connectorCommand -> messageCount.incrementAndGet(),
                context -> 10L,
                name -> 0
        );
        AtomicBoolean resultHolder = new AtomicBoolean();
        NodeContextInfo request = NodeContextInfo.newBuilder()
                                                 .setNodeName("other")
                                                 .setNrOfMasterContexts(0)
                                                 .setHashKey(Integer.MAX_VALUE)
                                                 .setContext("default")
                                                 .setMasterSequenceNumber(10)
                                                 .build();
        RequestLeaderEvent event = new RequestLeaderEvent(request, resultHolder::set);
        testSubject.on(event);
        Assert.assertFalse(resultHolder.get());
        Assert.assertEquals(1, messageCount.get());
    }
}