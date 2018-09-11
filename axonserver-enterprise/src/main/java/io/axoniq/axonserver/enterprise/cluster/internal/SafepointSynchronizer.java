package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.enterprise.cluster.SafepointRepository;
import io.axoniq.axonserver.enterprise.jpa.Safepoint;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.enterprise.cluster.manager.EventStoreManager;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;

/**
 * Author: marc
 */
@Controller
public class SafepointSynchronizer {
    private final SafepointRepository safepointRepository;
    private final EventStoreManager eventStoreManager;
    private final LocalEventStore localEventStore;
    private final DataSynchronizationMaster dataSynchronizationMaster;


    public SafepointSynchronizer(SafepointRepository safepointRepository,
                                 EventStoreManager eventStoreManager,
                                 LocalEventStore localEventStore,
                                 DataSynchronizationMaster dataSynchronizationMaster) {
        this.safepointRepository = safepointRepository;
        this.eventStoreManager = eventStoreManager;
        this.localEventStore = localEventStore;
        this.dataSynchronizationMaster = dataSynchronizationMaster;
    }

    @Scheduled(fixedRateString = "${axoniq.axonserver.safepoint-synchronization-rate:5000}")
    public void synchronize() {
        eventStoreManager.masterFor().forEach(this::synchronizeContext);
    }

    private void synchronizeContext(String context) {
        long eventToken = localEventStore.getLastCommittedToken(context);
        long snapshotToken = localEventStore.getLastCommittedSnapshot(context);
        dataSynchronizationMaster.publishSafepoints(context, eventToken, snapshotToken);

        safepointRepository.save(new Safepoint(EventType.EVENT.name(), context, eventToken));
        safepointRepository.save(new Safepoint(EventType.SNAPSHOT.name(), context, snapshotToken));
    }
}
