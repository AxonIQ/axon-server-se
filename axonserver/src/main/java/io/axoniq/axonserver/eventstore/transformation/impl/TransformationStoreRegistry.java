/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.eventstore.transformation.impl;

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
    private final Map<String, TransformationEntryStore> fileStoreMap = new ConcurrentHashMap<>();
    private final EmbeddedDBProperties embeddedDBProperties;

    public TransformationStoreRegistry(EmbeddedDBProperties embeddedDBProperties) {
        this.embeddedDBProperties = embeddedDBProperties;
    }

    public TransformationEntryStore register(String context, String transformationId) {
        TransformationEntryStore store = fileStoreMap.computeIfAbsent(transformationId, t -> {
            String baseDirectory = embeddedDBProperties.getEvent().getStorage(context);
            StorageProperties storageProperties = new StorageProperties();
            storageProperties.setStorage(Paths.get(baseDirectory, transformationId).toFile());
            storageProperties.setSuffix(".events");
            logger.info("{}: creating transformation store for {}", context, transformationId);
            return new TransformationEntryStore(storageProperties, context + "/" + transformationId);
        });
        store.open(false);
        return store;
    }


    public TransformationEntryStore get(String transformationId) {
        return fileStoreMap.get(transformationId);
    }

    public void delete(String transformationId) {
        TransformationEntryStore fileStore = fileStoreMap.remove(transformationId);
        if (fileStore != null) {
            fileStore.delete();
        }
    }
}
