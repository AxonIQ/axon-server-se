package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.features.Feature;
import io.axoniq.axonserver.features.FeatureChecker;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
 */
@Component
@ConditionalOnProperty(value = "axoniq.axonserver.cluster.enabled", havingValue = "true")
public class StorageReplicationHealthIndicator extends AbstractHealthIndicator {
    private final DataSynchronizationMaster dataSynchronizationMaster;
    private final DataSynchronizationReplica dataSynchronizationReplica;
    private final FeatureChecker featureChecker;

    public StorageReplicationHealthIndicator(
            DataSynchronizationMaster dataSynchronizationMaster,
            DataSynchronizationReplica dataSynchronizationReplica,
            FeatureChecker featureChecker) {
        this.dataSynchronizationMaster = dataSynchronizationMaster;
        this.dataSynchronizationReplica = dataSynchronizationReplica;
        this.featureChecker = featureChecker;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder)  {
        if(!Feature.CLUSTERING.enabled(featureChecker)) {
            builder.down();
            return;
        }
        dataSynchronizationMaster.getConnectionsPerContext().forEach(
                (context, replicas) -> {
                    builder.withDetail( context, "Master");
                    replicas.forEach((n, replica) -> {
                        builder.withDetail(String.format("%s.%s.lastConfirmedEventTransaction", context, n), replica.getLastEventTransaction());
                        builder.withDetail(String.format("%s.%s.lastConfirmedSnapshotTransaction", context, n), replica.getLastSnapshotTransaction());
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
