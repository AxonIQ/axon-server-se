package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.AppendEntryFailure;
import io.axoniq.axonserver.grpc.cluster.AppendEntrySuccess;
import io.axoniq.axonserver.grpc.cluster.Entry;

import java.util.Iterator;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Author: marc
 */
public class DummyReplicationConnection implements ReplicationConnection {

    private final ScheduledExecutorService remoteCommunication = new ScheduledThreadPoolExecutor(2);

    private static final int MAX_ENTRIES_PER_BATCH = 3;
    private final LogEntryStore localLogEntryStore;
    private volatile Iterator<Entry> entryIterator;
    private Consumer<Long> matchIndexListener;

    public DummyReplicationConnection(LogEntryStore localLogEntryStore, Consumer<Long> matchIndexListener) {
        this.localLogEntryStore = localLogEntryStore;
        this.matchIndexListener = matchIndexListener;
    }

    @Override
    public int sendNextEntries(LeaderState.PeerInfo peer) {
        if( entryIterator == null) entryIterator = localLogEntryStore.createIterator(peer.getNextIndex());
        int sent = 0;
        while( sent < MAX_ENTRIES_PER_BATCH && entryIterator.hasNext()) {
            Entry entry = entryIterator.next();
            //
            System.out.println("send "  + entry.getIndex());
            send( AppendEntriesRequest.newBuilder()
                                      .setCommitIndex(localLogEntryStore.commitIndex())
                                      .addEntries(entry)
                                      .build());
            sent++;
        }

        return sent;
    }

    @Override
    public void send(AppendEntriesRequest request) {
        System.out.println("send "  + request);
        remoteCommunication.schedule(() -> reply(request), 10, TimeUnit.MILLISECONDS);
    }

    private synchronized void reply(AppendEntriesRequest request) {
        if( request.getEntriesCount() == 0) {
            on( AppendEntriesResponse.newBuilder().setFailure(AppendEntryFailure.newBuilder()
                                                                                .setLastAppliedIndex(0)
                                                                                .setLastAppliedEventSequence(0)
                                                                                .build()).build());
        } else {
            on( AppendEntriesResponse.newBuilder().setSuccess(AppendEntrySuccess.newBuilder()
                                                                                .setLastLogIndex(request.getEntries(request.getEntriesCount()-1).getIndex())
                                                                                .build()).build());

        }
    }

    public void on(AppendEntriesResponse response) {
        if( response.hasSuccess()) {
            matchIndexListener.accept(response.getSuccess().getLastLogIndex());
        } else {
            entryIterator = localLogEntryStore.createIterator(response.getFailure().getLastAppliedIndex()+1);
        }
    }


}
