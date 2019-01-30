package io.axoniq.axonserver.enterprise.message.event;

import io.axoniq.axonserver.LifecycleController;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.TestMessagingClusterService;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.cluster.internal.MessagingClusterServiceInterface;
import io.axoniq.axonserver.enterprise.cluster.internal.StubFactory;
import io.axoniq.axonserver.enterprise.cluster.internal.SyncStatusController;
import io.axoniq.axonserver.enterprise.cluster.manager.EventStoreManager;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.grpc.Confirmation;
import io.axoniq.axonserver.grpc.internal.NodeContextInfo;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.util.AssertUtils;
import io.grpc.stub.StreamObserver;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;
import org.springframework.context.ApplicationEventPublisher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * Author: marc
 */
@RunWith(MockitoJUnitRunner.class)
public class EventStoreManagerTest {
    private EventStoreManager testSubject;

    @Mock
    private MessagingPlatformConfiguration messagingPlatformConfiguration;

    @Mock
    private LocalEventStore localEventStore;

    @Mock
    private ApplicationEventPublisher applicationEventPublisher;

    @Mock
    private LifecycleController lifecycleController;

    @Mock
    private SyncStatusController syncStatusController;

    private List<Context> contexts = new ArrayList<>();

    @Before
    public void setup() {
        StubFactory stubFactory =  new StubFactory() {
            @Override
            public MessagingClusterServiceInterface messagingClusterServiceStub(
                    ClusterNode node) {
                return new TestMessagingClusterService() {
                    @Override
                    public void requestLeader(NodeContextInfo nodeContextInfo,
                                              StreamObserver<Confirmation> confirmationStreamObserver) {
                        if( node.getName().equals("node2") || node.getName().equals("node3")) {
                            confirmationStreamObserver.onNext(Confirmation.newBuilder().setSuccess(true).build());
                            confirmationStreamObserver.onCompleted();
                            return;
                        }
                        if( node.getName().equals("node4") || node.getName().equals("node5")) {
                            confirmationStreamObserver.onNext(Confirmation.newBuilder().setSuccess(false).build());
                            confirmationStreamObserver.onCompleted();
                            return;
                        }

                        confirmationStreamObserver.onError(new RuntimeException("Failed to check node: " + node.getName()));
                    }
                };
            }
        };
        testSubject = new EventStoreManager( messagingPlatformConfiguration,
                                            stubFactory, lifecycleController, localEventStore, syncStatusController, applicationEventPublisher, () -> contexts.iterator(), false, "me", true, 10, n->new ClusterNode());
    }

    private Context createContext(String name, String... nodes) {
        Context defaultContext = new Context(name);
        addNode(defaultContext, "me");
        for (String node : nodes) {
            addNode(defaultContext, node);
        }
        return defaultContext;


    }
    private void addNode(Context context, String name) {
        ClusterNode node = new ClusterNode();
        node.setName(name);
        node.addContext(context, true, true);
    }

    @Test
    public void becomeMasterWith3Nodes() throws InterruptedException {
        Context defaultContext = createContext("default", "node2", "node3");
        contexts.add(defaultContext);
        testSubject.start();
        testSubject.on(new ClusterEvents.InternalServerReady());
        AssertUtils.assertWithin(5, TimeUnit.SECONDS, () -> Assert.assertTrue(testSubject.getEventStore("default") instanceof LocalEventStore));
    }

    @Test
    public void becomeMasterWith3NodesOneWithError() throws InterruptedException {
        Context defaultContext = createContext("default", "node2", "node7");
        contexts.add(defaultContext);

        testSubject.start();
        testSubject.on(new ClusterEvents.InternalServerReady());
        AssertUtils.assertWithin(5, TimeUnit.SECONDS, () -> Assert.assertTrue(testSubject.getEventStore("default") instanceof LocalEventStore));
    }

    @Test
    public void becomeMasterWith2Nodes() throws InterruptedException {
        Context defaultContext = createContext("default", "node2");
        contexts.add(defaultContext);

        testSubject.start();
        testSubject.on(new ClusterEvents.InternalServerReady());
        AssertUtils.assertWithin(5, TimeUnit.SECONDS, () -> Assert.assertTrue(testSubject.getEventStore("default") instanceof LocalEventStore));
    }

    @Test
    public void noMasterWithNotRespondingNodes() throws InterruptedException {
        Context defaultContext = createContext("default", "node6", "node7");
        contexts.add(defaultContext);

        testSubject.start();
        testSubject.on(new ClusterEvents.InternalServerReady());
        Thread.sleep(2000);
        Assert.assertNull(testSubject.getEventStore("default"));
    }

    @Test
    public void noMasterWithOneNegativeResponse() throws InterruptedException {
        Context defaultContext = createContext("default", "node2", "node4");
        contexts.add(defaultContext);

        testSubject.start();
        testSubject.on(new ClusterEvents.InternalServerReady());
        Thread.sleep(2000);
        Assert.assertNull(testSubject.getEventStore("default"));
    }}