package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.enterprise.cluster.manager.EventStoreManager;
import io.axoniq.axonserver.localstorage.EventType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;

import java.util.Optional;

/**
 * @author Marc Gathier
 */
@Controller
public class SafepointSynchronizer {
    private final SyncStatusController safepointRepository;
    private final EventStoreManager eventStoreManager;
    private final DataSynchronizationMaster dataSynchronizationMaster;


    public SafepointSynchronizer(SyncStatusController safepointRepository,
                                 Optional<EventStoreManager> eventStoreManager,
                                 DataSynchronizationMaster dataSynchronizationMaster) {
        this.safepointRepository = safepointRepository;
        this.eventStoreManager = eventStoreManager.orElse(null);
        this.dataSynchronizationMaster = dataSynchronizationMaster;
    }

    @Scheduled(fixedRateString = "${axoniq.axonserver.safepoint-synchronization-rate:5000}")
    public void synchronize() {
        if( eventStoreManager == null) return;
        eventStoreManager.masterFor().forEach(this::synchronizeContext);
    }

    private void synchronizeContext(String context) {
        long eventToken = safepointRepository.getSafePoint(EventType.EVENT, context);
        long snapshotToken = safepointRepository.getSafePoint(EventType.SNAPSHOT, context);
        dataSynchronizationMaster.publishSafepoints(context, eventToken, snapshotToken);
    }

}
