package io.axoniq.axonserver.message.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 */
@Component("QueryCache")
public class QueryCache extends ConcurrentHashMap<String, QueryInformation> {
    private final Logger logger = LoggerFactory.getLogger(QueryCache.class);
    private final long defaultQueryTimeout;

    public QueryCache(@Value("${axoniq.axonserver.default-query-timeout:900000}") long defaultQueryTimeout) {
        this.defaultQueryTimeout = defaultQueryTimeout;
    }

    public QueryInformation remove(String messagId) {
        logger.debug("Remove messageId {}", messagId);
        return super.remove(messagId);
    }

    @Scheduled(fixedDelayString = "${axoniq.axonserver.cache-cleanup-rate:300000}")
    public void clearOnTimeout() {
        logger.debug("Checking timed out queries");
        long minTimestamp = System.currentTimeMillis() - defaultQueryTimeout;
        Set<Entry<String, QueryInformation>> toDelete = entrySet().stream().filter(e -> e.getValue().getTimestamp() < minTimestamp).collect(
                Collectors.toSet());
        if( ! toDelete.isEmpty()) {
            logger.warn("Found {} waiting queries to delete", toDelete.size());
            toDelete.forEach(e -> {
                remove(e.getKey());
                e.getValue().cancel();
            });
        }
    }
}
