/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.requestprocessor.eventstore;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.filestorage.FileStore;
import io.axoniq.axonserver.filestorage.FileStoreEntry;
import io.axoniq.axonserver.grpc.event.DeletedEvent;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.TransformEventsRequest;
import io.axoniq.axonserver.grpc.event.TransformedEvent;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.util.CloseableIterator;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
@Component
public class TransformationProcessor {
    private final Logger logger = LoggerFactory.getLogger(TransformationProcessor.class);

    private final EventStoreTransformationRepository eventStoreTransformationRepository;
    private final LocalEventStore localEventStore;
    private final TransformationStoreRegistry transformationStoreRegistry;
    private final PlatformTransactionManager platformTransactionManager;

    public TransformationProcessor(
            EventStoreTransformationRepository eventStoreTransformationRepository,
            LocalEventStore localEventStore,
            TransformationStoreRegistry transformationStoreRegistry,
            PlatformTransactionManager platformTransactionManager) {
        this.eventStoreTransformationRepository = eventStoreTransformationRepository;
        this.localEventStore = localEventStore;
        this.transformationStoreRegistry = transformationStoreRegistry;
        this.platformTransactionManager = platformTransactionManager;
    }

    public void startTransformation(String context, String transformationId) {
        eventStoreTransformationRepository.save(new EventStoreTransformationJpa(transformationId, context));
        transformationStoreRegistry.register(context, transformationId);
    }

    public void deleteEvent(String transformationId, long token) {
        EventStoreTransformationJpa transformation = eventStoreTransformationRepository.findById(
                transformationId).orElseThrow(() -> new RuntimeException(transformationId + ": Transformation not found"));

        FileStore fileStore = transformationStoreRegistry.get(transformationId);
        fileStore.append(deleteEventEntry(token))
                 .block();

        transformation.setLastToken(token);
        eventStoreTransformationRepository.save(transformation);
    }

    public void replaceEvent(String transformationId, long token, Event event) {
        EventStoreTransformationJpa transformation = eventStoreTransformationRepository.findById(
                transformationId).orElseThrow(() -> new RuntimeException(transformationId + ": Transformation not found"));

        FileStore fileStore = transformationStoreRegistry.get(transformationId);
        fileStore.append(replaceEventEntry(token, event))
                 .block();

        transformation.setLastToken(token);
        eventStoreTransformationRepository.save(transformation);
    }

    public void cancel(String transformationId) {
        transformationStoreRegistry.delete(transformationId);
        eventStoreTransformationRepository.deleteById(transformationId);
    }

    public void apply(String transformationId) {
        EventStoreTransformationJpa transformation = eventStoreTransformationRepository.getById(transformationId);
        logger.warn("{}: Start apply transformation from {} to {}", transformation.getContext(), transformation.getFirstToken(), transformation.getLastToken());
        if( transformation.getLastToken() == null) {
            transformation.setStatus(EventStoreTransformationJpa.Status.APPLIED);
            eventStoreTransformationRepository.save(transformation);
        } else {
            FileStore transformationFileStore = transformationStoreRegistry.get(transformationId);
            CloseableIterator<FileStoreEntry> iterator = transformationFileStore.iterator(0);
            if (iterator.hasNext()) {
                transformation.setStatus(EventStoreTransformationJpa.Status.APPLYING);
                eventStoreTransformationRepository.save(transformation);
                FileStoreEntry transformationEntry = iterator.next();
                AtomicReference<TransformEventsRequest> request = new AtomicReference<>(parse(transformationEntry));
                logger.warn("Next token {}", token(request.get()));
                localEventStore.transformEvents(transformation.getContext(),
                                                transformation.getFirstToken(),
                                                transformation.getLastToken(),
                                                (event, token) -> {
                                                    logger.warn("Found token {}", token);
                                                    Event result = event;
                                                    TransformEventsRequest nextRequest = request.get();
                                                    if (token(nextRequest) == token) {
                                                        result = applyTransformation(event, nextRequest);
                                                        if (iterator.hasNext()) {
                                                            request.set(parse(iterator.next()));
                                                            logger.warn("Next token {}", token(request.get()));
                                                        }
                                                    }
                                                    return result;
                                                }).thenAccept(r -> {
                    iterator.close();
                    runInTransaction(() -> {
                        transformation.setStatus(EventStoreTransformationJpa.Status.APPLIED);
                        return eventStoreTransformationRepository.save(transformation);
                    });

                });
            }
        }

    }

    private FileStoreEntry deleteEventEntry(long token) {
        TransformEventsRequest request = TransformEventsRequest.newBuilder()
                                                               .setDeleteEvent(DeletedEvent.newBuilder()
                                                                                           .setToken(token))
                                                               .build();
        return new FileStoreEntry() {
            @Override
            public byte[] bytes() {
                return request.toByteArray();
            }

            @Override
            public byte version() {
                return 0;
            }
        };
    }

    private FileStoreEntry replaceEventEntry(long token, Event event) {
        TransformEventsRequest request = TransformEventsRequest.newBuilder()
                                                               .setEvent(TransformedEvent.newBuilder()
                                                                                         .setToken(token)
                                                                                         .setEvent(event))
                                                               .build();
        return new FileStoreEntry() {
            @Override
            public byte[] bytes() {
                return request.toByteArray();
            }

            @Override
            public byte version() {
                return 0;
            }
        };
    }

    private void runInTransaction(Supplier<EventStoreTransformationJpa> r) {
        new TransactionTemplate(platformTransactionManager).execute(status -> r.get());
    }

    private Event applyTransformation(Event original, TransformEventsRequest transformRequest) {
        switch (transformRequest.getRequestCase()) {
            case EVENT:
                return merge(original, transformRequest.getEvent().getEvent());
            case DELETE_EVENT:
                return nullify(original);
            case REQUEST_NOT_SET:
                break;
        }
        return original;
    }

    private Event merge(Event original, Event updated) {
        return Event.newBuilder(original)
                    .clearMetaData()
                    .setAggregateType(updated.getAggregateType())
                    .setPayload(updated.getPayload())
                    .putAllMetaData(updated.getMetaDataMap())
                    .build();
    }

    private Event nullify(Event event) {
        return Event.newBuilder(event)
                    .clearPayload()
                    .clearMessageIdentifier()
                    .clearMetaData()
                    .build();
    }

    private long token(TransformEventsRequest nextRequest) {
        switch (nextRequest.getRequestCase()) {
            case EVENT:
                return nextRequest.getEvent().getToken();
            case DELETE_EVENT:
                return nextRequest.getDeleteEvent().getToken();
            default:
                throw new IllegalArgumentException("Request without token");
        }
    }

    private TransformEventsRequest parse(FileStoreEntry entry) {
        try {
            return TransformEventsRequest.parseFrom(entry.bytes());
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }


}
