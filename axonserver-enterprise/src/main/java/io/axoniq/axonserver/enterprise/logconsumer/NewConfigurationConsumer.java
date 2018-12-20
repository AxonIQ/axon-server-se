package io.axoniq.axonserver.enterprise.logconsumer;

import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNode;
import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNodeRepository;
import io.axoniq.axonserver.grpc.cluster.Config;
import io.axoniq.axonserver.grpc.cluster.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * Author: marc
 */
@Component
public class NewConfigurationConsumer implements LogEntryConsumer {
    private final JpaRaftGroupNodeRepository raftGroupNodeRepository;
    private final Logger logger = LoggerFactory.getLogger(NewConfigurationConsumer.class);

    public NewConfigurationConsumer(JpaRaftGroupNodeRepository raftGroupNodeRepository) {
        this.raftGroupNodeRepository = raftGroupNodeRepository;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry e) {
        if( e.hasNewConfiguration()) {
            Config configuration = e.getNewConfiguration();
            logger.warn("{}: received config: {}", groupId, configuration);

            Set<JpaRaftGroupNode> oldNodes = raftGroupNodeRepository.findByGroupId(groupId);
            raftGroupNodeRepository.deleteAll(oldNodes);
            configuration.getNodesList().forEach(node -> {
                JpaRaftGroupNode jpaRaftGroupNode = new JpaRaftGroupNode();
                jpaRaftGroupNode.setGroupId(groupId);
                jpaRaftGroupNode.setNodeId(node.getNodeId());
                jpaRaftGroupNode.setHost(node.getHost());
                jpaRaftGroupNode.setPort(node.getPort());
                raftGroupNodeRepository.save(jpaRaftGroupNode);
            });

        }
    }
}
