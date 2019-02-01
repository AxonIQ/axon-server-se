package io.axoniq.axonserver.migration.mongo;

import io.axoniq.axonserver.migration.DomainEvent;
import io.axoniq.axonserver.migration.EventProducer;
import io.axoniq.axonserver.migration.SnapshotEvent;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

/**
 * @author Marc Gathier
 */
@Component
@Profile({"migrate-from-mongo"})
public class MongoEventProcessor implements EventProducer{
    @Override
    public List<? extends DomainEvent> findEvents(long lastProcessedToken, int batchSize) {
        return Collections.EMPTY_LIST;
    }

    @Override
    public List<? extends SnapshotEvent> findSnapshots(String lastProcessedTimestamp, int batchSize) {
        return Collections.EMPTY_LIST;
    }
}
