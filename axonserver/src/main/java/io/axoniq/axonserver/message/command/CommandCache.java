package io.axoniq.axonserver.message.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Author: marc
 */
@Component
public class CommandCache extends ConcurrentHashMap<String, CommandInformation> {
    private final Logger logger = LoggerFactory.getLogger(CommandCache.class);
    private final long defaultQueryTimeout;

    @Autowired
    public CommandCache(@Value("${axoniq.axonserver.default-command-timeout:15000}") long defaultQueryTimeout) {
        this.defaultQueryTimeout = defaultQueryTimeout;
    }

    public CommandCache() {
        this(300000);
    }

    @Scheduled(fixedDelayString = "${axoniq.axonserver.cache-cleanup-rate:5000}")
    public void clearOnTimeout() {
        logger.debug("Checking timed out queries");
        long minTimestamp = System.currentTimeMillis() - defaultQueryTimeout;
        Set<Entry<String, CommandInformation>> toDelete = entrySet().stream().filter(e -> e.getValue().getTimestamp() < minTimestamp).collect(
                Collectors.toSet());
        if( ! toDelete.isEmpty()) {
            logger.warn("Found {} waiting commands to delete", toDelete.size());
            toDelete.forEach(e -> {
                remove(e.getKey());
                e.getValue().cancel();
            });
        }
    }

}
