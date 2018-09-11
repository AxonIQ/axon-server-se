package io.axoniq.axonserver.localstorage;

import io.axoniq.axondb.Event;
import io.axoniq.axonserver.enterprise.storage.file.DatafileEventStoreFactory;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonserver.localstorage.transaction.DefaultStorageTransactionManagerFactory;
import io.axoniq.axonserver.enterprise.storage.transaction.ReplicationManager;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;
import io.axoniq.axonserver.localstorage.transformation.DefaultEventTransformerFactory;
import io.axoniq.platform.SerializedObject;
import org.junit.*;
import org.junit.rules.*;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Author: marc
 */
public class EventWriteStorageClusterTest {

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();
    private EventWriteStorage testSubject;
    private EventStore datafileManagerChain;
    private FakeReplicationManager fakeReplicationManager;

    @Before
    public void setUp() {
        EmbeddedDBProperties embeddedDBProperties = new EmbeddedDBProperties();
        embeddedDBProperties.getEvent().setStorage(tempFolder.getRoot().getAbsolutePath() + "/" + UUID.randomUUID().toString());
        embeddedDBProperties.getEvent().setSegmentSize(5120 * 1024L);
        embeddedDBProperties.getSnapshot().setStorage(tempFolder.getRoot().getAbsolutePath());
        fakeReplicationManager = new FakeReplicationManager();
        DefaultStorageTransactionManagerFactory defaultStorageTransactionManagerFactory = new DefaultStorageTransactionManagerFactory(
                true, fakeReplicationManager);
        EventStoreFactory eventStoreFactory = new DatafileEventStoreFactory(embeddedDBProperties, new DefaultEventTransformerFactory(),
                                                                            defaultStorageTransactionManagerFactory);
        datafileManagerChain = eventStoreFactory.createEventManagerChain("default");
        datafileManagerChain.init(false);
        StorageTransactionManager transactionManager = defaultStorageTransactionManagerFactory.createTransactionManager(datafileManagerChain);
        testSubject = new EventWriteStorage(transactionManager);
    }

    @After
    public void tearDown() {
        datafileManagerChain.cleanup();
    }

    @Test
    public void stepDown() throws ExecutionException, InterruptedException {
        Event event = Event.newBuilder().setAggregateIdentifier("1").setAggregateSequenceNumber(0).setAggregateType(
                "Demo").setPayload(SerializedObject.newBuilder().build()).build();
        CompletableFuture<Void> first = testSubject.store(Collections.singletonList(event));

        event = Event.newBuilder().setAggregateIdentifier("1").setAggregateSequenceNumber(1).setAggregateType(
                "Demo").setPayload(SerializedObject.newBuilder().build()).build();

        CompletableFuture<Void> second = testSubject.store(Collections.singletonList(event));
        fakeReplicationManager.completed(0);
        first.get();
        assertEquals(0, testSubject.getLastCommittedToken());
        assertEquals(1, testSubject.getLastToken());

        testSubject.cancelPendingTransactions();
        assertEquals(0, testSubject.getLastCommittedToken());
        assertEquals(0, testSubject.getLastToken());
        assertTrue(second.isCompletedExceptionally());
    }


    private class FakeReplicationManager implements ReplicationManager {
        private Consumer<Long> replicationCompleted;

        @Override
        public int getQuorum(String context) {
            return 2;
        }

        @Override
        public void registerListener(EventTypeContext type, Consumer<Long> replicationCompleted) {
            this.replicationCompleted = replicationCompleted;
        }

        @Override
        public void publish(EventTypeContext type, List<Event> eventList, long token) {

        }

        public void completed(long transaction) {
            replicationCompleted.accept(transaction);
        }

    }
}