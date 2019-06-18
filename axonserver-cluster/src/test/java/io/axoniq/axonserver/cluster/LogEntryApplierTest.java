package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.cluster.election.InMemoryElectionStore;
import io.axoniq.axonserver.cluster.replication.InMemoryLogEntryStore;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.cluster.scheduler.DefaultScheduler;
import io.axoniq.axonserver.grpc.cluster.Config;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import org.junit.*;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.axoniq.axonserver.cluster.TestUtils.assertWithin;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link LogEntryApplier}.
 *
 * @author Milan Savic
 */
public class LogEntryApplierTest {

    private static final String GROUP_ID = "groupId";
    private static final String LOCAL_NODE_ID = "localNodeId";

    private LogEntryApplier logEntryApplier;
    private LogEntryStore logEntryStore;
    private ProcessorStore processorStore;
    private Consumer<Entry> lastLogEntryAppliedConsumer;
    private NewConfigurationConsumer newConfigurationConsumer;

    @Before
    public void setUp() {
        processorStore = new InMemoryProcessorStore();
        LogEntryProcessor logEntryProcessor = new LogEntryProcessor(processorStore);

        logEntryStore = new InMemoryLogEntryStore("log-entry-applier-test");

        RaftGroup raftGroup = new RaftGroup() {
            @Override
            public LogEntryStore localLogEntryStore() {
                return logEntryStore;
            }

            @Override
            public ElectionStore localElectionStore() {
                return new InMemoryElectionStore();
            }

            @Override
            public RaftConfiguration raftConfiguration() {
                return new FakeRaftConfiguration(GROUP_ID, LOCAL_NODE_ID);
            }

            @Override
            public LogEntryProcessor logEntryProcessor() {
                return logEntryProcessor;
            }

            @Override
            public RaftPeer peer(Node node) {
                return null;
            }

            @Override
            public RaftNode localNode() {
                return null;
            }
        };

        lastLogEntryAppliedConsumer = mock(Consumer.class);
        newConfigurationConsumer = mock(NewConfigurationConsumer.class);

        logEntryApplier = new LogEntryApplier(raftGroup,
                                              new DefaultScheduler(),
                                              lastLogEntryAppliedConsumer,
                                              newConfigurationConsumer);
    }

    @After
    public void tearDown() {
        logEntryApplier.stop();
    }

    @Test
    public void testEntryConsumption() throws IOException, InterruptedException {
        String entryType = "theType";
        LogEntryConsumer aLogEntryConsumer = mock(LogEntryConsumer.class);
        when(aLogEntryConsumer.entryType()).thenReturn(entryType);

        logEntryApplier.registerLogEntryConsumer(aLogEntryConsumer);
        logEntryApplier.start();

        SerializedObject aSerializedObject = SerializedObject.newBuilder()
                                                             .setType(entryType)
                                                             .build();
        Entry anEntry = Entry.newBuilder()
                             .setIndex(1L)
                             .setTerm(0L)
                             .setSerializedObject(aSerializedObject)
                             .build();

        logEntryStore.appendEntry(Collections.singletonList(anEntry));
        processorStore.updateCommit(1, 0);

        assertWithin(100, TimeUnit.MILLISECONDS, () -> {
            try {
                verify(aLogEntryConsumer).consumeLogEntry(GROUP_ID, anEntry);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        verify(lastLogEntryAppliedConsumer).accept(anEntry);
    }

    @Test
    public void testNewConfConsumption() throws IOException, InterruptedException {
        logEntryApplier.start();

        Config config = Config.newBuilder().build();
        Entry anEntry = Entry.newBuilder()
                             .setIndex(1L)
                             .setTerm(0L)
                             .setNewConfiguration(config)
                             .build();

        logEntryStore.appendEntry(Collections.singletonList(anEntry));
        processorStore.updateCommit(1, 0);

        assertWithin(100, TimeUnit.MILLISECONDS, () -> {
            try {
                verify(newConfigurationConsumer).consume(config);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        verify(lastLogEntryAppliedConsumer).accept(anEntry);
    }

    @Test
    public void testSuccessfulRetry() throws Exception {
        String entryType = "theType";
        LogEntryConsumer aLogEntryConsumer = mock(LogEntryConsumer.class);
        when(aLogEntryConsumer.entryType()).thenReturn(entryType);

        SerializedObject aSerializedObject = SerializedObject.newBuilder()
                                                             .setType(entryType)
                                                             .build();
        Entry anEntry = Entry.newBuilder()
                             .setIndex(1L)
                             .setTerm(0L)
                             .setSerializedObject(aSerializedObject)
                             .build();
        doThrow(new RuntimeException("oops")).doAnswer(invocation -> null)
                                             .when(aLogEntryConsumer)
                                             .consumeLogEntry(GROUP_ID, anEntry);

        logEntryApplier.registerLogEntryConsumer(aLogEntryConsumer);
        logEntryApplier.start();

        logEntryStore.appendEntry(Collections.singletonList(anEntry));
        processorStore.updateCommit(1, 0);

        assertWithin(100, TimeUnit.MILLISECONDS, () -> verify(lastLogEntryAppliedConsumer).accept(anEntry));
    }

    @Test
    public void testUnsuccessfulRetry() throws Exception {
        String entryType = "theType";
        LogEntryConsumer aLogEntryConsumer = mock(LogEntryConsumer.class);
        when(aLogEntryConsumer.entryType()).thenReturn(entryType);

        SerializedObject aSerializedObject = SerializedObject.newBuilder()
                                                             .setType(entryType)
                                                             .build();
        Entry anEntry = Entry.newBuilder()
                             .setIndex(1L)
                             .setTerm(0L)
                             .setSerializedObject(aSerializedObject)
                             .build();
        doThrow(new RuntimeException("oops")).when(aLogEntryConsumer)
                                       .consumeLogEntry(GROUP_ID, anEntry);

        logEntryApplier.registerLogEntryConsumer(aLogEntryConsumer);
        logEntryApplier.start();

        logEntryStore.appendEntry(Collections.singletonList(anEntry));
        processorStore.updateCommit(1, 0);

        Thread.sleep(100);

        verify(lastLogEntryAppliedConsumer, never()).accept(anEntry);
    }
}