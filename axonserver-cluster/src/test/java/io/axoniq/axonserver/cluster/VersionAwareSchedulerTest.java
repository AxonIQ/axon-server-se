package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.scheduler.DefaultScheduler;
import io.axoniq.axonserver.cluster.snapshot.SnapshotManager;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import org.junit.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests schedules during starting/stopping of a Raft state.
 *
 * @author Milan Savic
 */
public class VersionAwareSchedulerTest {

    private TestState testState;

    @Before
    public void setUp() {
        testState = TestState.builder()
                             .raftGroup(mock(RaftGroup.class))
                             .transitionHandler(mock(StateTransitionHandler.class))
                             .termUpdateHandler(mock(BiConsumer.class))
                             .stateFactory(mock(MembershipStateFactory.class))
                             .currentConfiguration(mock(CurrentConfiguration.class))
                             .registerConfigurationListenerFn(mock(Function.class))
                             .snapshotManager(mock(SnapshotManager.class))
                             .schedulerFactory(DefaultScheduler::new)
                             .build();
    }

    @Test
    public void testScheduleNotTriggeredInDifferentVersion() throws InterruptedException {
        AtomicReference<String> result = new AtomicReference<>();

        CountDownLatch notInvokedLatch = new CountDownLatch(1);
        testState.schedule(s -> s.schedule(() -> {
            result.set("not reached");
            notInvokedLatch.countDown();
        }, 100, TimeUnit.MILLISECONDS));
        notInvokedLatch.await(200, TimeUnit.MILLISECONDS);
        assertNull(result.get());

        testState.start();
        CountDownLatch latch = new CountDownLatch(1);
        testState.schedule(s -> s.schedule(() -> {
            result.set("done");
            latch.countDown();
        }, 100, TimeUnit.MILLISECONDS));
        latch.await(200, TimeUnit.MILLISECONDS);
        assertEquals("done", result.get());
        testState.stop();

        testState.schedule(s -> s.schedule(() -> {
            result.set("done again");
            notInvokedLatch.countDown();
        }, 100, TimeUnit.MILLISECONDS));

        notInvokedLatch.await(200, TimeUnit.MILLISECONDS);
        assertEquals("done", result.get());
    }

    @Test
    public void testExecuteNotTriggeredInDifferentVersion() throws InterruptedException {
        AtomicReference<String> result = new AtomicReference<>();

        CountDownLatch notInvokedLatch = new CountDownLatch(1);
        testState.execute(() -> {
            result.set("not reached");
            notInvokedLatch.countDown();
        });
        notInvokedLatch.await(200, TimeUnit.MILLISECONDS);
        assertNull(result.get());

        testState.start();
        CountDownLatch latch = new CountDownLatch(1);
        testState.execute(() -> {
            result.set("done");
            latch.countDown();
        });
        latch.await(200, TimeUnit.MILLISECONDS);
        assertEquals("done", result.get());
        testState.stop();

        testState.execute(() -> {
            result.set("done again");
            notInvokedLatch.countDown();
        });

        notInvokedLatch.await(100, TimeUnit.MILLISECONDS);
        assertEquals("done", result.get());
    }

    private static class TestState extends AbstractMembershipState {

        public TestState(Builder builder) {
            super(builder);
        }

        @Override
        public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
            return AppendEntriesResponse.newBuilder().build();
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder extends AbstractMembershipState.Builder<Builder> {

            public TestState build() {
                return new TestState(this);
            }
        }
    }
}
