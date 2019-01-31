package io.axoniq.axonserver.enterprise.cluster.snapshot;

import io.axoniq.axonserver.AxonServer;
import io.axoniq.axonserver.access.application.ApplicationController;
import io.axoniq.axonserver.access.application.ApplicationRepository;
import io.axoniq.axonserver.access.application.ShaHasher;
import io.axoniq.axonserver.access.jpa.Application;
import io.axoniq.axonserver.access.jpa.ApplicationContext;
import io.axoniq.axonserver.access.jpa.ApplicationContextRole;
import io.axoniq.axonserver.access.jpa.User;
import io.axoniq.axonserver.access.jpa.UserRole;
import io.axoniq.axonserver.access.user.UserRepository;
import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.component.processor.balancing.jpa.LoadBalancingStrategy;
import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.ProcessorLoadBalancing;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.LoadBalanceStrategyRepository;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.ProcessorLoadBalancingRepository;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.EventStore;
import io.axoniq.axonserver.localstorage.EventStoreFactory;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;
import io.axoniq.axonserver.localstorage.TransactionInformation;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonserver.localstorage.file.IndexManager;
import io.axoniq.axonserver.localstorage.file.PrimaryEventStore;
import io.axoniq.axonserver.localstorage.transaction.PreparedTransaction;
import io.axoniq.axonserver.localstorage.transformation.DefaultEventTransformerFactory;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import org.junit.*;
import org.junit.rules.*;
import org.junit.runner.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests the snapshot mechanism as a whole.
 *
 * @author Milan Savic
 */
@RunWith(SpringRunner.class)
@DataJpaTest
@Transactional
@ComponentScan(basePackages = {
        "io.axoniq.axonserver.component.processor.balancing.jpa",
        "io.axoniq.axonserver.access.jpa"
}, lazyInit = true)
@ContextConfiguration(classes = AxonServer.class)
public class SnapshotManagerIntegrationTest {

    private static final String CONTEXT = "junit";

    @Autowired
    private ApplicationRepository applicationRepository;
    @Autowired
    private UserRepository userRepository;
    @Autowired
    private LoadBalanceStrategyRepository loadBalanceStrategyRepository;
    @Autowired
    private ProcessorLoadBalancingRepository processorLoadBalancingRepository;

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    private AxonServerSnapshotManager leaderSnapshotManager;
    private AxonServerSnapshotManager followerSnapshotManager;

    private EventStore leaderEventStore;
    private EventStore leaderSnapshotStore;
    private EventStore followerEventStore;
    private EventStore followerSnapshotStore;

    @Before
    public void setUp() {
        EmbeddedDBProperties embeddedDBProperties = embeddedDBProperties();
        List<LoadBalancingStrategy> existingLoadBalancingStrategies = loadBalanceStrategyRepository.findAll();
        existingLoadBalancingStrategies.forEach(e -> loadBalanceStrategyRepository.delete(e));
        leaderEventStore = eventStore(embeddedDBProperties);
        leaderSnapshotStore = snapshotStore(embeddedDBProperties);
        leaderSnapshotManager = axonServerSnapshotManager(leaderEventStore, leaderSnapshotStore);

        followerEventStore = eventStore(embeddedDBProperties);
        followerSnapshotStore = snapshotStore(embeddedDBProperties);
        followerSnapshotManager = axonServerSnapshotManager(followerEventStore, followerSnapshotStore);
    }

    @Test
    public void testSnapshot() throws InterruptedException {
        setupEventStore(leaderEventStore, 10, 10, false);
        setupEventStore(leaderSnapshotStore, 3, 3, true);
        ApplicationContextRole applicationContextRole1 = new ApplicationContextRole("READ");
        ApplicationContextRole applicationContextRole2 = new ApplicationContextRole("WRITE");
        ApplicationContext junitAppContext =
                new ApplicationContext(CONTEXT, singletonList(applicationContextRole1));
        ApplicationContext defaultAppContext =
                new ApplicationContext("default", singletonList(applicationContextRole2));
        Application app1 = new Application("app1", "app1Desc", "tokenPrefix", "hashedToken1", junitAppContext);
        Application app2 = new Application("app2", "app2Desc", "tokenPrefix", "hashedToken2", defaultAppContext);
        applicationRepository.save(app1);
        applicationRepository.save(app2);

        User user = new User("username", "password", new String[]{"role1", "role2"});
        userRepository.save(user);

        LoadBalancingStrategy loadBalancingStrategy = new LoadBalancingStrategy("loadBalancingStrategy",
                                                                                "label",
                                                                                "factoryBean");
        loadBalanceStrategyRepository.save(loadBalancingStrategy);

        TrackingEventProcessor tep1 = new TrackingEventProcessor("tep1", "component", CONTEXT);
        TrackingEventProcessor tep2 = new TrackingEventProcessor("tep2", "component", "default");
        ProcessorLoadBalancing processorLoadBalancing1 = new ProcessorLoadBalancing(tep1, "strategy1");
        ProcessorLoadBalancing processorLoadBalancing2 = new ProcessorLoadBalancing(tep2, "strategy2");
        processorLoadBalancingRepository.save(processorLoadBalancing1);
        processorLoadBalancingRepository.save(processorLoadBalancing2);

        List<io.axoniq.axonserver.grpc.cluster.SerializedObject> snapshotChunks =
                leaderSnapshotManager.streamSnapshotData(0, 95)
                                     .collectList()
                                     .block();

        assertNotNull(snapshotChunks);
        assertEquals(17, snapshotChunks.size());
        assertEquals(Application.class.getName(), snapshotChunks.get(0).getType());
        assertEquals(User.class.getName(), snapshotChunks.get(1).getType());
        assertEquals(LoadBalancingStrategy.class.getName(), snapshotChunks.get(2).getType());
        assertEquals(ProcessorLoadBalancing.class.getName(), snapshotChunks.get(3).getType());
        for (int i = 4; i < 14; i++) {
            assertEquals("eventsTransaction", snapshotChunks.get(i).getType());
        }
        for (int i = 14; i < 17; i++) {
            assertEquals("snapshotsTransaction", snapshotChunks.get(i).getType());
        }

        assertEquals(2, processorLoadBalancingRepository.findAll().size());
        followerSnapshotManager.clear();
        assertEquals(1, processorLoadBalancingRepository.findAll().size());
        followerSnapshotManager.applySnapshotData(snapshotChunks)
                               .block();
        assertEquals(2, processorLoadBalancingRepository.findAll().size());

        assertEventStores(leaderEventStore, followerEventStore, 0, 95);
        assertEventStores(leaderSnapshotStore, followerSnapshotStore, 0, 9);

        List<Application> applications = applicationRepository.findAllByContextsContext(CONTEXT);
        assertEquals(1, applications.size());
        assertApplications(app1, applications.get(0));

        List<User> users = userRepository.findAll();
        assertEquals(1, users.size());
        assertUsers(user, users.get(0));

        List<LoadBalancingStrategy> loadBalancingStrategies = loadBalanceStrategyRepository.findAll();
        assertEquals(1, loadBalancingStrategies.size());
        assertLoadBalancingStrategies(loadBalancingStrategy, loadBalancingStrategies.get(0));

        List<ProcessorLoadBalancing> processorLoadBalancingList = processorLoadBalancingRepository
                .findByContext(CONTEXT);
        assertEquals(1, processorLoadBalancingList.size());
        assertProcessorLoadBalancing(processorLoadBalancing1, processorLoadBalancingList.get(0));
    }

    private EventStore eventStore(EmbeddedDBProperties embeddedDBProperties) {
        IndexManager eventIndexManager = new IndexManager(CONTEXT, embeddedDBProperties.getEvent());
        EventTransformerFactory eventTransformerFactory = new DefaultEventTransformerFactory();
        EventStore eventStore = new PrimaryEventStore(new EventTypeContext(CONTEXT, EventType.EVENT),
                                                      eventIndexManager,
                                                      eventTransformerFactory,
                                                      embeddedDBProperties.getEvent());
        eventStore.init(false);
        return eventStore;
    }

    private EventStore snapshotStore(EmbeddedDBProperties embeddedDBProperties) {
        IndexManager snapshotIndexManager = new IndexManager(CONTEXT, embeddedDBProperties.getSnapshot());
        EventTransformerFactory eventTransformerFactory = new DefaultEventTransformerFactory();
        EventStore snapshotStore = new PrimaryEventStore(new EventTypeContext(CONTEXT, EventType.SNAPSHOT),
                                                         snapshotIndexManager,
                                                         eventTransformerFactory,
                                                         embeddedDBProperties.getSnapshot());
        snapshotStore.init(false);
        return snapshotStore;
    }

    private AxonServerSnapshotManager axonServerSnapshotManager(EventStore eventStore, EventStore snapshotStore) {
        EventStoreFactory eventStoreFactory = mock(EventStoreFactory.class);
        when(eventStoreFactory.createEventManagerChain(CONTEXT)).thenReturn(eventStore);
        when(eventStoreFactory.createSnapshotManagerChain(CONTEXT)).thenReturn(snapshotStore);

        LocalEventStore localEventStore = new LocalEventStore(eventStoreFactory);
        localEventStore.initContext(CONTEXT, false);
        EventTransactionsSnapshotDataStore eventTransactionsSnapshotDataProvider =
                new EventTransactionsSnapshotDataStore(CONTEXT, localEventStore);
        SnapshotTransactionsSnapshotDataStore snapshotTransactionsSnapshotDataProvider =
                new SnapshotTransactionsSnapshotDataStore(CONTEXT, localEventStore);
        ApplicationController applicationController = new ApplicationController(applicationRepository, new ShaHasher());
        ApplicationSnapshotDataStore applicationSnapshotDataProvider =
                new ApplicationSnapshotDataStore(CONTEXT, applicationController);
        UserSnapshotDataStore userSnapshotDataProvider = new UserSnapshotDataStore(userRepository);
        LoadBalanceStrategySnapshotDataStore loadBalanceStrategySnapshotDataProvider =
                new LoadBalanceStrategySnapshotDataStore(loadBalanceStrategyRepository);
        ProcessorLoadBalancingSnapshotDataStore processorLoadBalancingSnapshotDataProvider =
                new ProcessorLoadBalancingSnapshotDataStore(CONTEXT, processorLoadBalancingRepository);

        List<SnapshotDataStore> dataProviders = new ArrayList<>();
        dataProviders.add(eventTransactionsSnapshotDataProvider);
        dataProviders.add(snapshotTransactionsSnapshotDataProvider);
        dataProviders.add(applicationSnapshotDataProvider);
        dataProviders.add(userSnapshotDataProvider);
        dataProviders.add(loadBalanceStrategySnapshotDataProvider);
        dataProviders.add(processorLoadBalancingSnapshotDataProvider);

        return new AxonServerSnapshotManager(dataProviders);
    }

    private EmbeddedDBProperties embeddedDBProperties() {
        EmbeddedDBProperties embeddedDBProperties = new EmbeddedDBProperties(new SystemInfoProvider() {});
        embeddedDBProperties.getEvent().setStorage(
                tempFolder.getRoot().getAbsolutePath() + "/" + UUID.randomUUID().toString());
        embeddedDBProperties.getEvent().setSegmentSize(512 * 1024L);
        embeddedDBProperties.getSnapshot().setStorage(tempFolder.getRoot().getAbsolutePath());
        embeddedDBProperties.getEvent().setPrimaryCleanupDelay(0);
        return embeddedDBProperties;
    }

    private void setupEventStore(EventStore eventStore, int numOfTransactions, int numOfEvents, boolean snapshot)
            throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(numOfTransactions);
        IntStream.range(0, numOfTransactions).forEach(j -> {
            String aggId = UUID.randomUUID().toString();
            List<SerializedEvent> newEvents = new ArrayList<>();
            IntStream.range(0, numOfEvents).forEach(i -> newEvents.add(new SerializedEvent(Event.newBuilder()
                                                                                            .setAggregateIdentifier(aggId)
                                                                                            .setAggregateSequenceNumber(i)
                                                                                            .setSnapshot(snapshot)
                                                                                            .setAggregateType("Demo")
                                                                                            .setPayload(SerializedObject.newBuilder().build())
                                                                                            .build())));
            PreparedTransaction preparedTransaction = eventStore.prepareTransaction(new TransactionInformation(eventStore.transactionVersion(),
                                                                                                               0),
                                                                                    newEvents);
            eventStore.store(preparedTransaction).thenAccept(t -> latch.countDown());
        });

        latch.await(5, TimeUnit.SECONDS);
    }

    private void assertEventStores(EventStore eventStore1, EventStore eventStore2, long firstToken, long limitToken) {
        Iterator<SerializedTransactionWithToken> iterator1 = eventStore1.transactionIterator(firstToken, limitToken);
        Iterator<SerializedTransactionWithToken> iterator2 = eventStore2.transactionIterator(firstToken, limitToken);

        while (iterator1.hasNext()) {
            assertTransactions(iterator1.next(), iterator2.next());
        }
        assertFalse(iterator2.hasNext());
    }

    private void assertTransactions(SerializedTransactionWithToken transaction1, SerializedTransactionWithToken transaction2) {
        assertEquals(transaction1.getToken(), transaction2.getToken());
        assertEquals(transaction1.getVersion(), transaction2.getVersion());
        assertEquals(transaction1.getEventsCount(), transaction2.getEventsCount());
        for (int i = 0; i < transaction1.getEventsCount(); i++) {
            assertEvents(transaction1.getEvents(i), transaction2.getEvents(i));
        }
    }

    private void assertEvents(Event event1, Event event2) {
        assertEquals(event1.getAggregateSequenceNumber(), event2.getAggregateSequenceNumber());
        assertEquals(event1.getAggregateIdentifier(), event2.getAggregateIdentifier());
        assertEquals(event1.getAggregateType(), event2.getAggregateType());
        assertEquals(event1.getTimestamp(), event2.getTimestamp());
        assertEquals(event1.getMessageIdentifier(), event2.getMessageIdentifier());
        assertEquals(event1.getSnapshot(), event2.getSnapshot());
        assertEquals(event1.getMetaDataCount(), event2.getMetaDataCount());
        assertEquals(event1.getMetaDataMap(), event2.getMetaDataMap());
        assertEquals(event1.getPayload(), event2.getPayload());
    }

    private void assertApplications(Application app1, Application app2) {
        assertEquals(app1.getName(), app2.getName());
        assertEquals(app1.getDescription(), app2.getDescription());
        assertEquals(app1.getHashedToken(), app2.getHashedToken());
        assertEquals(app1.getTokenPrefix(), app2.getTokenPrefix());
        assertEquals(app1.getContexts().size(), app2.getContexts().size());
        Iterator<ApplicationContext> context1Iterator = app1.getContexts().iterator();
        Iterator<ApplicationContext> context2Iterator = app2.getContexts().iterator();
        while (context1Iterator.hasNext()) {
            assertApplicationContexts(context1Iterator.next(), context2Iterator.next());
        }
    }

    private void assertApplicationContexts(ApplicationContext appContext1, ApplicationContext appContext2) {
        assertEquals(appContext1.getContext(), appContext2.getContext());
        assertEquals(appContext1.getRoles().size(), appContext2.getRoles().size());
        for (int i = 0; i < appContext1.getRoles().size(); i++) {
            assertEquals(appContext1.getRoles().get(i).getRole(), appContext2.getRoles().get(i).getRole());
        }
    }

    private void assertUsers(User user1, User user2) {
        assertEquals(user1.getUserName(), user2.getUserName());
        assertEquals(user1.getPassword(), user2.getPassword());
        assertEquals(user1.getRoles().size(), user2.getRoles().size());
        List<String> roles1 = user1.getRoles()
                                   .stream()
                                   .map(UserRole::getRole)
                                   .collect(Collectors.toList());
        List<String> roles2 = user2.getRoles()
                                   .stream()
                                   .map(UserRole::getRole)
                                   .collect(Collectors.toList());
        assertTrue(roles1.containsAll(roles2));
    }

    private void assertLoadBalancingStrategies(LoadBalancingStrategy lbs1, LoadBalancingStrategy lbs2) {
        assertEquals(lbs1.name(), lbs2.name());
        assertEquals(lbs1.factoryBean(), lbs2.factoryBean());
        assertEquals(lbs1.label(), lbs2.label());
    }

    private void assertProcessorLoadBalancing(ProcessorLoadBalancing plb1, ProcessorLoadBalancing plb2) {
        assertEquals(plb1.strategy(), plb2.strategy());
        assertProcessors(plb1.processor(), plb2.processor());
    }

    private void assertProcessors(TrackingEventProcessor tep1, TrackingEventProcessor tep2) {
        assertEquals(tep1.name(), tep2.name());
        assertEquals(tep1.context(), tep2.context());
        assertEquals(tep1.component(), tep2.component());
    }
}
