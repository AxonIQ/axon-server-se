package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import org.junit.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

/**
 * Author: marc
 */
public class LeaderStateTest {

    private LeaderState testSubject;
    private RaftGroup raftGroup;
    private boolean running = true;
    private RaftClusterTestFixture fixture;

    @Before
    public void setup() {
        fixture = new RaftClusterTestFixture("node1", "node2");
        raftGroup =fixture.getGroup("node1");
        testSubject = new DefaultStateFactory(raftGroup, n -> {}).leaderState();
        Executors.newCachedThreadPool().submit(() -> applyEntries(raftGroup.localLogEntryStore()));
    }

    private void applyEntries(LogEntryStore entryStore) {
        entryStore.registerCommitListener(Thread.currentThread());
        while(running) {
            int retries = 10;
            while( retries > 0) {
                int applied = raftGroup.localLogEntryStore().applyEntries(e -> this.testSubject.applied(e));
                if( applied > 0 ) {
                    retries = 0;
                } else {
                    retries--;
                }
                LockSupport.parkNanos(1000);
            }
        }
    }

    @Test
    public void startAndStop() throws InterruptedException, TimeoutException, ExecutionException {
        testSubject.appendEntry("Test", "Hello".getBytes());
        testSubject.appendEntry("Test", "Hello".getBytes());
        testSubject.start();
        testSubject.appendEntry("Test", "Hello".getBytes());
        testSubject.appendEntry("Test", "Hello".getBytes());
        testSubject.appendEntry("Test", "Hello".getBytes());
        testSubject.appendEntry("Test", "Hello".getBytes());
        CompletableFuture<Void> done =  testSubject.appendEntry("Test", "Hello".getBytes());

        done.get(5, TimeUnit.SECONDS);
        testSubject.stop();
    }
}