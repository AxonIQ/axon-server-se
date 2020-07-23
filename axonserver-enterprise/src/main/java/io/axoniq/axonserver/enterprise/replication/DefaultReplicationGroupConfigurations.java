package io.axoniq.axonserver.enterprise.replication;

import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroup;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroupRepository;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupConfiguration;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.function.Function;

/**
 * Iterable of all context configurations persisted in controlDB.
 *
 * @author Sara Pellegrini
 * @since 4.1
 */
@Component
public class DefaultReplicationGroupConfigurations implements ReplicationGroupConfigurations {

    private final Function<AdminReplicationGroup, ReplicationGroupConfiguration> mapping;
    private final AdminReplicationGroupRepository adminReplicationGroupRepository;

    @Autowired
    public DefaultReplicationGroupConfigurations(AdminReplicationGroupRepository adminReplicationGroupRepository) {
        this(adminReplicationGroupRepository, new ReplicationGroupConfigurationMapping());
    }

    public DefaultReplicationGroupConfigurations(AdminReplicationGroupRepository adminReplicationGroupRepository,
                                                 Function<AdminReplicationGroup, ReplicationGroupConfiguration> mapping) {
        this.adminReplicationGroupRepository = adminReplicationGroupRepository;
        this.mapping = mapping;
    }

    @NotNull
    @Override
    public Iterator<ReplicationGroupConfiguration> iterator() {
        return adminReplicationGroupRepository.findAll().stream().map(mapping).iterator();
    }
}

