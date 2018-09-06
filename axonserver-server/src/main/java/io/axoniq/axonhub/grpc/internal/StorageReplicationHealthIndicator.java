package io.axoniq.axonhub.grpc.internal;

import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Author: marc
 */
@Component
@ConditionalOnProperty(value = "axoniq.axonhub.cluster.enabled", havingValue = "true")
public class StorageReplicationHealthIndicator extends AbstractHealthIndicator {
    private final DataSynchronizationMaster dataSynchronizationMaster;
    private final DataSynchronizationReplica dataSynchronizationReplica;

    public StorageReplicationHealthIndicator(
            DataSynchronizationMaster dataSynchronizationMaster,
            DataSynchronizationReplica dataSynchronizationReplica) {
        this.dataSynchronizationMaster = dataSynchronizationMaster;
        this.dataSynchronizationReplica = dataSynchronizationReplica;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) throws Exception {
        dataSynchronizationMaster.getConnectionsPerContext().forEach(
                (context, replicas) -> {
                    builder.withDetail( context, "Master");
                    replicas.forEach(replica -> {
                        builder.withDetail(String.format("%s.%s.lastConfirmedEventTransaction", context, replica.getNodeName()), replica.getLastEventTransaction());
                        builder.withDetail(String.format("%s.%s.lastConfirmedSnapshotTransaction", context, replica.getNodeName()), replica.getLastSnapshotTransaction());
                    });
                }
        );

        dataSynchronizationReplica.getConnectionPerContext().forEach((context, replicaConnection) -> {
            builder.withDetail( context, "Replica");
            builder.withDetail(String.format("%s.master", context), replicaConnection.getNode());
            builder.withDetail(String.format("%s.waitingForEvent", context), replicaConnection.getExpectedEventToken());
            builder.withDetail(String.format("%s.waitingForSnapshot", context), replicaConnection.getExpectedSnapshotToken());
            builder.withDetail(String.format("%s.waitingEvents", context), replicaConnection.waitingEvents());
            builder.withDetail(String.format("%s.waitingSnapshots", context), replicaConnection.waitingSnapshots());
        });

        builder.up();
    }
}
