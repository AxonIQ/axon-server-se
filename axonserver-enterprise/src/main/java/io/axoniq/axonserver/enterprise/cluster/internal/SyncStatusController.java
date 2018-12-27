package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.enterprise.cluster.SafepointRepository;
import io.axoniq.axonserver.enterprise.jpa.Safepoint;
import io.axoniq.axonserver.localstorage.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Author: marc
 */
@Component
public class SyncStatusController {
    private final Logger log = LoggerFactory.getLogger(SyncStatusController.class);
    private final SafepointRepository safepointRepository;

    public SyncStatusController(SafepointRepository safepointRepository) {
        this.safepointRepository = safepointRepository;
    }

    public void storeSafePoint(EventType eventType, String context, long token) {
        log.trace("{}: save {} = {}", context, eventType, token);
        updateSafePoint(eventType,context, token);
    }


    public long getSafePoint(EventType eventType, String context) {
        return lastSyncStatus(eventType, context).safePoint();
    }

    private Safepoint lastSyncStatus(EventType eventType, String context){
        Safepoint syncStatus = safepointRepository.findById(new Safepoint.SafepointKey(context, eventType.name())).orElse(null);
        return syncStatus != null ? syncStatus : new Safepoint(eventType.name(), context);
    }

    public long generation(EventType eventType, String context){
        return lastSyncStatus(eventType,context).generation();
    }

    public void increaseGenerations(String context) {
        Safepoint eventSyncStatus = lastSyncStatus(EventType.EVENT, context);
        eventSyncStatus.increaseGeneration();
        safepointRepository.save(eventSyncStatus);
        Safepoint snapshotSyncStatus = lastSyncStatus(EventType.SNAPSHOT, context);
        snapshotSyncStatus.increaseGeneration();
        safepointRepository.save(snapshotSyncStatus);
    }

    public void updateGeneration(EventType eventType, String context, long generation){
        log.trace("{}: Storing generation for {} = {}", context, eventType, generation);
        Safepoint syncStatus = lastSyncStatus(eventType, context);
        if (generation == syncStatus.generation()){
            return;
        }
        syncStatus.setGeneration(generation);
        safepointRepository.save(syncStatus);
    }

    public void updateSafePoint(EventType eventType, String context, long safePoint){
        log.trace("{}: Storing safePoint for {} = {}", context, eventType, safePoint);
        Safepoint syncStatus = lastSyncStatus(eventType, context);
        if (safePoint == syncStatus.safePoint()){
            return;
        }
        syncStatus.setSafePoint(safePoint);
        safepointRepository.save(syncStatus);
    }

    public long safePoint(EventType eventType, String context){
        return lastSyncStatus(eventType,context).safePoint();
    }

}
