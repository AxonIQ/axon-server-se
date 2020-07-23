package io.axoniq.axonserver.enterprise.replication.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.cluster.util.RoleUtils;
import io.axoniq.axonserver.enterprise.messaging.event.LowerTierEventStore;
import io.axoniq.axonserver.enterprise.replication.group.ReplicationGroupController;
import io.axoniq.axonserver.enterprise.storage.multitier.LowerTierEventStoreLocator;
import io.axoniq.axonserver.grpc.SerializedTransactionWithTokenConverter;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import javax.annotation.Nonnull;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;

/**
 * Snapshot data store for snapshot transactions data. Only streams data to nodes that have a role that
 * includes an event store (MESSAGING_ONLY nodes will not get this data).
 *
 * @author Milan Savic
 * @since 4.1
 */
public class SnapshotTransactionsSnapshotDataStore implements SnapshotDataStore {

    private static final Logger logger = LoggerFactory.getLogger(SnapshotTransactionsSnapshotDataStore.class);
    private static final String ENTRY_TYPE = "snapshotsTransaction";

    private final String replicationGroupName;
    private final LocalEventStore localEventStore;
    private final boolean adminContext;
    private final LowerTierEventStoreLocator lowerTierEventStoreLocator;
    private final ReplicationGroupController replicationGroupController;

    /**
     * Creates Snapshot Transaction Snapshot Data Store for streaming/applying snapshot transaction data.
     *
     * @param replicationGroupName       application context
     * @param localEventStore            event store used retrieving/saving snapshot transactions
     * @param lowerTierEventStoreLocator provides a facade to a SECONDARY event store in case the replica needs data
     *                                   that is
     *                                   no longer at the primary node
     * @param replicationGroupController provides the contexts for the replication group
     */
    public SnapshotTransactionsSnapshotDataStore(String replicationGroupName, LocalEventStore localEventStore,
                                                 LowerTierEventStoreLocator lowerTierEventStoreLocator,
                                                 ReplicationGroupController replicationGroupController) {
        this.replicationGroupName = replicationGroupName;
        this.localEventStore = localEventStore;
        this.adminContext = isAdmin(replicationGroupName);
        this.lowerTierEventStoreLocator = lowerTierEventStoreLocator;
        this.replicationGroupController = replicationGroupController;
    }

    @Override
    public int order() {
        return 200;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(SnapshotContext installationContext) {
        if (adminContext
                || installationContext.supportsReplicationGroups()
                || !RoleUtils.hasStorage(installationContext.role())) {
            return Flux.empty();
        }

        return addContext(replicationGroupName, installationContext, Flux.empty());
    }


    @Override
    public Flux<SerializedObject> streamAppendOnlyData(SnapshotContext installationContext) {
        if (adminContext
                || !installationContext.supportsReplicationGroups()
                || !RoleUtils.hasStorage(installationContext.role())) {
            return Flux.empty();
        }
        Flux<SerializedObject> result = Flux.empty();
        for (String context : replicationGroupController.getContextNames(
                replicationGroupName)) {
            result = addContext(context, installationContext, result);
        }
        return result;
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return !adminContext && type.equals(ENTRY_TYPE);
    }

    @Override
    public void applySnapshotData(SerializedObject serializedObject, Role role) {
        try {
            TransactionWithToken transactionWithToken = TransactionWithToken.parseFrom(serializedObject.getData());
            localEventStore.syncSnapshots(transactionWithToken.getContext()
                                                              .isEmpty() ? replicationGroupName : transactionWithToken
                                                  .getContext(),
                                          SerializedTransactionWithTokenConverter
                                                  .asSerializedTransactionWithToken(transactionWithToken));
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize events transaction.", e);
        }
    }

    @Override
    public void clear() {
        // we don't delete snapshots
    }

    @Nonnull
    private SerializedObject toSerializedObject(SerializedTransactionWithToken transactionWithToken,
                                                String replicationGroupName) {
        return SerializedObject.newBuilder()
                               .setType(ENTRY_TYPE)
                               .setData(SerializedTransactionWithTokenConverter.asByteString(
                                       transactionWithToken,
                                       replicationGroupName))
                               .build();
    }

    @NotNull
    private SerializedObject toSerializedObject(TransactionWithToken transactionWithToken) {
        return SerializedObject.newBuilder()
                               .setType(ENTRY_TYPE)
                               .setData(transactionWithToken.toByteString())
                               .build();
    }

    private Flux<SerializedObject> addContext(String context, SnapshotContext installationContext,
                                              Flux<SerializedObject> result) {
        long fromToken = installationContext.fromSnapshotSequence(context);
        long toToken = localEventStore.getLastSnapshot(context) + 1;
        logger.debug("{}: replicate from {} to {} first snapshot token at leader is {}",
                     context,
                     fromToken,
                     toToken,
                     localEventStore.firstSnapshotToken(context));
        if (fromToken < localEventStore.firstSnapshotToken(context)) {
            // Leader no longer has the first requested token
            LowerTierEventStore lowerTier = lowerTierEventStoreLocator.getEventStore(context);
            long lastTokenFromLowerTier = lowerTier.getLastSnapshotToken(context);
            logger.debug("{}: last token at secondary node {}", context, lastTokenFromLowerTier);
            return result.concatWith(lowerTier.snapshotTransactions(context, fromToken, lastTokenFromLowerTier)
                                              .map(this::toSerializedObject))
                         .concatWith(Flux.fromIterable(() -> localEventStore
                                 .eventTransactionsIterator(context, lastTokenFromLowerTier, toToken))
                                         .map(transactionWithToken -> toSerializedObject(transactionWithToken,
                                                                                         context)));
        }
        if (toToken > fromToken) {
            return result.concatWith(
                    Flux.fromIterable(() -> localEventStore.snapshotTransactionsIterator(context, fromToken, toToken))
                        .map(transactionWithToken -> toSerializedObject(transactionWithToken,
                                                                        context)));
        }
        return result;
    }

}
