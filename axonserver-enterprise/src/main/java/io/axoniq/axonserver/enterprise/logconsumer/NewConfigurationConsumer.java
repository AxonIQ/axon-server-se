package io.axoniq.axonserver.enterprise.logconsumer;

import io.axoniq.axonserver.grpc.cluster.Config;
import io.axoniq.axonserver.grpc.cluster.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Author: marc
 */
@Component
public class NewConfigurationConsumer implements LogEntryConsumer {
    private final Logger logger = LoggerFactory.getLogger(NewConfigurationConsumer.class);

    @Override
    public void consumeLogEntry(String groupId, Entry e) {
        if( e.hasNewConfiguration()) {
            Config configuration = e.getNewConfiguration();
            logger.debug("{}: received config: {}", groupId, configuration);
        }
    }
}
