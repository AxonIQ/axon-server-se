/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.requestprocessor.eventstore;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.filestorage.FileStoreEntry;
import io.axoniq.axonserver.filestorage.impl.BaseFileStore;
import io.axoniq.axonserver.filestorage.impl.StorageProperties;
import io.axoniq.axonserver.grpc.event.DeletedEvent;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.TransformEventsRequest;
import io.axoniq.axonserver.grpc.event.TransformedEvent;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
@Component
public class DefaultEventStoreTransformationService implements EventStoreTransformationService {
    private final EmbeddedDBProperties embeddedDBProperties;
    private final Map<String, TransformationDescription> activeTransformations = new ConcurrentHashMap<>();

    public DefaultEventStoreTransformationService(
            EmbeddedDBProperties embeddedDBProperties) {
        this.embeddedDBProperties = embeddedDBProperties;
    }

    @Override
    public Mono<String> startTransformation(String context) {
        try {
            TransformationDescription transformation = activeTransformations.compute(context, (ctx, current) -> {
                if (current != null) {
                    throw new MessagingPlatformException(ErrorCode.OTHER, "transformation in progress");
                }
                return new TransformationDescription(ctx);
            });
            return Mono.just(transformation.id);
        } catch (Exception ex) {
            return Mono.error(ex);
        }
    }

    @Override
    public Mono<Void> deleteEvent(String context, String transformationId, long token, long previousToken) {
        try {
            return transformation(context, transformationId).deleteEvent(token, previousToken);
        } catch (Exception ex) {
            return Mono.error(ex);
        }
    }

    private TransformationDescription transformation(String context, String transformationId) {
        TransformationDescription transformationDescription = activeTransformations.get(context);
        if (transformationDescription == null) {
            throw new MessagingPlatformException(ErrorCode.OTHER, "no transformation in progress");
        }
        if (!transformationId.equals(transformationDescription.id)) {
            throw new MessagingPlatformException(ErrorCode.OTHER, "invalid transformation id");
        }
        return transformationDescription;
    }

    @Override
    public Mono<Void> replaceEvent(String context, String transformationId, long token, Event event, long previousToken) {
        try {
            return transformation(context, transformationId).replaceEvent(token, previousToken, event);
        } catch (Exception ex) {
            return Mono.error(ex);
        }
    }

    @Override
    public Mono<Void> cancelTransformation(String context, String id) {
        try {
            TransformationDescription transformationDescription = transformation(context, id);
            transformationDescription.cancel();
            activeTransformations.remove(context);
            return Mono.empty();
        } catch (Exception ex) {
            return Mono.error(ex);
        }
    }

    @Override
    public Mono<Void> applyTransformation(String context, String id, long lastEventToken, long lastSnapshotToken) {
        try {
            TransformationDescription transformationDescription = transformation(context, id);
            transformationDescription.apply();
            return Mono.empty();
        } catch (Exception ex) {
            return Mono.error(ex);
        }
    }

    @Override
    public Mono<Void> replaceSnapshot(String context, String transformationId, long token, Event event,
                                      long previousToken) {
        return null;
    }

    @Override
    public Mono<Void> deleteSnapshot(String context, String transformationId, long token, long previousToken) {
        return null;
    }

    private class TransformationDescription {

        private final String id;
        private final BaseFileStore eventFileStore;
        private final BaseFileStore snapshotFileStore;
        private final AtomicLong previousEventToken = new AtomicLong(-1);
        private final AtomicLong previousSnapshotToken = new AtomicLong(-1);
        private final AtomicBoolean complete = new AtomicBoolean();

        public TransformationDescription(String context) {
            this.id = UUID.randomUUID().toString();
            String baseDirectory = embeddedDBProperties.getEvent().getStorage(context);
            StorageProperties storageProperties = new StorageProperties();
            storageProperties.setStorage(Paths.get(baseDirectory, id).toFile());
            storageProperties.setSuffix(".events");
            eventFileStore = new BaseFileStore(storageProperties, context + "/" + id);
            StorageProperties snapshotStorageProperties = new StorageProperties();
            snapshotStorageProperties.setStorage(Paths.get(baseDirectory, id).toFile());
            snapshotStorageProperties.setSuffix(".snapshots");
            snapshotFileStore = new BaseFileStore(storageProperties, context + "/" + id);
        }

        public Mono<Void> deleteEvent(long token, long previousToken) {
            if (complete.get()) {
                return Mono.error(new RuntimeException("transformation completed"));
            }
            if (previousToken != previousEventToken.get()) {
                return Mono.error(new RuntimeException("Invalid previous token, expecting " + previousEventToken.get()));
            }
            if (previousEventToken.compareAndSet(-1, token)) {
                eventFileStore.open(false);
            }

            TransformEventsRequest request = TransformEventsRequest.newBuilder()
                                                                   .setDeleteEvent(DeletedEvent.newBuilder()
                                                                                               .setToken(token))
                                                                   .build();
            return eventFileStore.append(new FileStoreEntry() {
                @Override
                public byte[] bytes() {
                    return request.toByteArray();
                }

                @Override
                public byte version() {
                    return 0;
                }
            }).then();
        }

        public void cancel() {
            if (previousEventToken.get() >= 0) {
                eventFileStore.delete();
            }
            if (previousSnapshotToken.get() >= 0) {
                snapshotFileStore.delete();
            }
        }

        public void apply() {
            if( !complete.compareAndSet(false, true)) {
                throw new MessagingPlatformException(ErrorCode.OTHER, "apply already in progress");
            }
        }

        public Mono<Void> replaceEvent(long token, long previousToken, Event event) {
            if (complete.get()) {
                return Mono.error(new RuntimeException("transformation completed"));
            }
            if (previousToken != previousEventToken.get()) {
                return Mono.error(new RuntimeException("Invalid previous token, expecting " + previousEventToken.get()));
            }
            if (previousEventToken.compareAndSet(-1, token)) {
                eventFileStore.open(false);
            }

            TransformEventsRequest request = TransformEventsRequest.newBuilder()
                                                                   .setEvent(TransformedEvent.newBuilder()
                                                                                             .setToken(token)
                                                                                     .setEvent(event))
                                                                   .build();
            return eventFileStore.append(new FileStoreEntry() {
                @Override
                public byte[] bytes() {
                    return request.toByteArray();
                }

                @Override
                public byte version() {
                    return 0;
                }
            }).then();
        }
    }

}
