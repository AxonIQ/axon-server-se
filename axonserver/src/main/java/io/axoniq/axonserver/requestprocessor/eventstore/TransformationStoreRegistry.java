package io.axoniq.axonserver.requestprocessor.eventstore;

import io.axoniq.axonserver.filestorage.FileStore;
import io.axoniq.axonserver.filestorage.impl.BaseFileStore;
import io.axoniq.axonserver.filestorage.impl.StorageProperties;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
@Component
public class TransformationStoreRegistry {
    private final Logger logger = LoggerFactory.getLogger(TransformationStoreRegistry.class);
    private final Map<String, FileStore> fileStoreMap = new ConcurrentHashMap<>();
    private final EmbeddedDBProperties embeddedDBProperties;

    public TransformationStoreRegistry(EmbeddedDBProperties embeddedDBProperties) {
        this.embeddedDBProperties = embeddedDBProperties;
    }

    public void register(String context, String transformationId) {
        fileStoreMap.computeIfAbsent(transformationId, t -> {
            String baseDirectory = embeddedDBProperties.getEvent().getStorage(context);
            StorageProperties storageProperties = new StorageProperties();
            storageProperties.setStorage(Paths.get(baseDirectory, transformationId).toFile());
            storageProperties.setSuffix(".events");
            logger.warn("{}: creating transformation store for {}", context, transformationId);
            return new BaseFileStore(storageProperties, context + "/" + transformationId);
        }).open(false);
    }

    public FileStore get(String transformationId) {
        return fileStoreMap.get(transformationId);
    }
}
