package io.axoniq.axonserver.enterprise.init;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNode;
import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNodeRepository;
import io.axoniq.axonserver.enterprise.cluster.ClusterNodeRepository;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.jpa.ContextClusterNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import javax.persistence.EntityManager;

import static io.axoniq.axonserver.util.ObjectUtils.getOrDefault;

/**
 * Perform recovery action on Axon Server nodes, in case of major network change or moving Axon Server cluster
 * to new machines. Updates all references to nodes in the controldb. To use this feature all nodes must be stopped,
 * and restarted with a recovery file.
 *
 * Runs before all other components are started.
 *
 * @author Marc Gathier
 * @since 4.3
 */
@Component
public class Recover implements SmartLifecycle {
    private final Logger logger = LoggerFactory.getLogger(Recover.class);
    private boolean running;

    private final ClusterNodeRepository clusterNodeRepository;
    private final JpaRaftGroupNodeRepository jpaRaftGroupNodeRepository;

    private final EntityManager entityManager;

    private final String recoveryFileName;

    public Recover(EntityManager entityManager,
                   ClusterNodeRepository clusterNodeRepository,
                   JpaRaftGroupNodeRepository jpaRaftGroupNodeRepository,
                   @Value("${axoniq.axonserver.recoveryfile:recovery.json}") String recoveryFileName) {
        this.clusterNodeRepository = clusterNodeRepository;
        this.entityManager = entityManager;
        this.jpaRaftGroupNodeRepository = jpaRaftGroupNodeRepository;
        this.recoveryFileName = recoveryFileName;
    }

    @Override
    @Transactional
    public void start() {
        File recoveryFile = new File(recoveryFileName);
        if( recoveryFile.exists() && recoveryFile.canRead() ) {
            logger.warn("Starting in recover mode with file {}", recoveryFile.getAbsolutePath());
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                RecoverNode[] nodes = objectMapper.readValue(recoveryFile, RecoverNode[].class);
                for (RecoverNode node : nodes) {
                    recover(node);
                }
            } catch (IOException e) {
                logger.warn("Failed to process recover file {}", recoveryFile.getAbsolutePath(), e);
            }
        }
        running = true;
    }

    private void recover(RecoverNode node) {
        if (node.getOldName() != null) {
            clusterNodeRepository.findById(node.getOldName()).ifPresent(clusterNode -> rename(clusterNode, node));
        }
        clusterNodeRepository.findById(node.getName()).ifPresent(clusterNode -> update(clusterNode, node));
    }

    private void update(ClusterNode clusterNode, RecoverNode node) {
        clusterNode.setHostName(getOrDefault(node.getHostName(), clusterNode.getHostName()));
        clusterNode.setInternalHostName(getOrDefault(node.getInternalHostName(), clusterNode.getInternalHostName()));
        clusterNode.setGrpcPort(getOrDefault(node.getGrpcPort(), clusterNode.getGrpcPort()));
        clusterNode.setGrpcInternalPort(getOrDefault(node.getInternalGrpcPort(), clusterNode.getGrpcInternalPort()));
        clusterNode.setHttpPort(getOrDefault(node.getHttpPort(), clusterNode.getHttpPort()));
        clusterNodeRepository.save(clusterNode);

        Set<JpaRaftGroupNode> raftGroups = jpaRaftGroupNodeRepository.findByNodeName(
                clusterNode.getName());
        raftGroups.forEach(raftGroup -> {
            raftGroup.setHost(getOrDefault(node.getInternalHostName(), raftGroup.getNodeName()));
            raftGroup.setPort(getOrDefault(node.getInternalGrpcPort(), raftGroup.getPort()));
        });
        jpaRaftGroupNodeRepository.saveAll(raftGroups);
    }

    private void rename(ClusterNode clusterNode, RecoverNode node) {
        if (clusterNode.getName().equals(node.getName())) return;
        logger.warn("Renaming {} to {}", clusterNode.getName(), node.getName());
        Set<ContextClusterNode> contexts = new HashSet<>(clusterNode.getContexts());

        ClusterNode newNode = new ClusterNode(node.getName(),
                                              clusterNode.getHostName(),
                                              clusterNode.getInternalHostName(),
                                              clusterNode.getGrpcPort(),
                                              clusterNode.getGrpcInternalPort(),
                                              clusterNode.getHttpPort());


        clusterNodeRepository.delete(clusterNode);
        clusterNodeRepository.flush();
        // Hibernates keeps some entries in its cache, so use entityManager.clear() to remove them from cache.
        entityManager.clear();
        for (ContextClusterNode context : contexts) {
            newNode.addContext(context.getContext(), context.getClusterNodeLabel(), context.getRole());
        }
        clusterNodeRepository.saveAndFlush(newNode);

        Set<JpaRaftGroupNode> raftGroups = jpaRaftGroupNodeRepository.findByNodeName(
                clusterNode.getName());
        raftGroups.forEach(raftGroup -> raftGroup.setNodeName(node.getName()));
        jpaRaftGroupNodeRepository.saveAll(raftGroups);
    }

    @Override
    public void stop() {
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return -100;
    }
}
