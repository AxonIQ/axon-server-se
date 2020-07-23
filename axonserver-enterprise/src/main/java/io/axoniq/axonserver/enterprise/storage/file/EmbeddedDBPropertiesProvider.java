package io.axoniq.axonserver.enterprise.storage.file;

import io.axoniq.axonserver.enterprise.jpa.ReplicationGroupContext;
import io.axoniq.axonserver.enterprise.jpa.ReplicationGroupContextRepository;
import io.axoniq.axonserver.enterprise.storage.ContextPropertyDefinition;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonserver.localstorage.file.StorageProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * @author Marc Gathier
 */
@Component
public class EmbeddedDBPropertiesProvider {

    public static final String JUMP_SKIP_INDEX = "JUMP_SKIP_INDEX";
    public static final String BLOOM_FILTER_INDEX = "BLOOM_FILTER_INDEX";

    private final EmbeddedDBProperties embeddedDBProperties;
    private final Function<String, Optional<Map<String, String>>> metaDataProvider;

    @Autowired
    public EmbeddedDBPropertiesProvider(
            EmbeddedDBProperties embeddedDBProperties,
            ReplicationGroupContextRepository replicationGroupContextRepository) {
        this.embeddedDBProperties = embeddedDBProperties;
        this.metaDataProvider = contextName -> replicationGroupContextRepository.findById(contextName)
                                                                                .map(ReplicationGroupContext::getMetaDataMap);
    }

    public EmbeddedDBPropertiesProvider(
            EmbeddedDBProperties embeddedDBProperties) {
        this.embeddedDBProperties = embeddedDBProperties;
        this.metaDataProvider = contextName -> Optional.empty();
    }

    public StorageProperties getEventProperties(String contextName) {
        return metaDataProvider.apply(contextName)
                               .map(metaData -> mergeEvent(embeddedDBProperties.getEvent(), metaData))
                               .orElse(embeddedDBProperties.getEvent());
    }

    public StorageProperties getSnapshotProperties(String contextName) {
        return metaDataProvider.apply(contextName)
                               .map(metaData -> mergeSnapshot(embeddedDBProperties.getSnapshot(), metaData))
                               .orElse(embeddedDBProperties.getSnapshot());
    }

    private StorageProperties mergeEvent(StorageProperties defaultProperties, Map<String, String> metaDataMap) {
        StorageProperties merged = defaultProperties;
        for (Map.Entry<String, String> metadataEntry : metaDataMap.entrySet()) {
            ContextPropertyDefinition property = ContextPropertyDefinition.findByKey(metadataEntry.getKey());
            if (property != null && EventType.EVENT.equals(property.scope())) {
                merged = property.apply(merged, metadataEntry.getValue());
            }
        }
        return merged;
    }

    private StorageProperties mergeSnapshot(StorageProperties defaultProperties, Map<String, String> metaDataMap) {
        StorageProperties merged = defaultProperties;
        for (Map.Entry<String, String> metadataEntry : metaDataMap.entrySet()) {
            ContextPropertyDefinition property = ContextPropertyDefinition.findByKey(metadataEntry.getKey());
            if (property != null && EventType.SNAPSHOT.equals(property.scope())) {
                merged = property.apply(merged, metadataEntry.getValue());
            }
        }
        return merged;
    }
}
