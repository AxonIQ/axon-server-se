package io.axoniq.axonhub.message.event;

import io.axoniq.axonhub.Confirmation;
import io.axoniq.axonhub.LifecycleController;
import io.axoniq.axonhub.cluster.TestMessagingClusterService;
import io.axoniq.axonhub.cluster.jpa.ClusterNode;
import io.axoniq.axonhub.config.MessagingPlatformConfiguration;
import io.axoniq.axonhub.context.jpa.Context;
import io.axoniq.axonhub.grpc.DataSychronizationServiceInterface;
import io.axoniq.axonhub.grpc.StubFactory;
import io.axoniq.axonhub.grpc.internal.MessagingClusterServiceInterface;
import io.axoniq.axonhub.internal.grpc.NodeContextInfo;
import io.axoniq.axonhub.localstorage.LocalEventStore;
import io.grpc.stub.StreamObserver;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;
import org.springframework.context.ApplicationEventPublisher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.axoniq.axonhub.util.AssertUtils.assertWithin;
import static org.junit.Assert.*;

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

    private List<Context> contexts = new ArrayList<>();

    @Before
    public void setup() {
        StubFactory stubFactory =  new StubFactory() {
            @Override
            public MessagingClusterServiceInterface messagingClusterServiceStub(
                    MessagingPlatformConfiguration messagingPlatformConfiguration, ClusterNode node) {
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

            @Override
            public MessagingClusterServiceInterface messagingClusterServiceStub(
                    MessagingPlatformConfiguration messagingPlatformConfiguration, String host, int port) {
                return null;
            }

            @Override
            public DataSychronizationServiceInterface dataSynchronizationServiceStub(
                    MessagingPlatformConfiguration messagingPlatformConfiguration, ClusterNode clusterNode) {
                return null;
            }
        };
        testSubject = new EventStoreManager( messagingPlatformConfiguration,
                                            stubFactory, lifecycleController, localEventStore, applicationEventPublisher, () -> contexts.iterator(), false, "me", true, 10, n->new ClusterNode());
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
        assertWithin(5, TimeUnit.SECONDS, () -> assertTrue(testSubject.getEventStore("default") instanceof LocalEventStore));
    }

    @Test
    public void becomeMasterWith3NodesOneWithError() throws InterruptedException {
        Context defaultContext = createContext("default", "node2", "node7");
        contexts.add(defaultContext);

        testSubject.start();
        assertWithin(5, TimeUnit.SECONDS, () -> assertTrue(testSubject.getEventStore("default") instanceof LocalEventStore));
    }

    @Test
    public void becomeMasterWith2Nodes() throws InterruptedException {
        Context defaultContext = createContext("default", "node2");
        contexts.add(defaultContext);

        testSubject.start();
        assertWithin(5, TimeUnit.SECONDS, () -> assertTrue(testSubject.getEventStore("default") instanceof LocalEventStore));
    }

    @Test
    public void noMasterWithNotRespondingNodes() throws InterruptedException {
        Context defaultContext = createContext("default", "node6", "node7");
        contexts.add(defaultContext);

        testSubject.start();
        Thread.sleep(2000);
        assertNull(testSubject.getEventStore("default"));
    }

    @Test
    public void noMasterWithOneNegativeResponse() throws InterruptedException {
        Context defaultContext = createContext("default", "node2", "node4");
        contexts.add(defaultContext);

        testSubject.start();
        Thread.sleep(2000);
        assertNull(testSubject.getEventStore("default"));
    }}