/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.eventstore.transformation.impl.TransformationCache;
import io.axoniq.axonserver.eventstore.transformation.impl.TransformationProcessor;
import io.axoniq.axonserver.eventstore.transformation.impl.TransformationValidator;
import io.axoniq.axonserver.grpc.event.Event;
import org.junit.*;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 * @since
 */
public class DefaultEventStoreTransformationServiceTest {

    private DefaultEventStoreTransformationService testSubject;
    private final TransformationValidator transformationValidator = mock(TransformationValidator.class);
    private final TransformationProcessor transformationProcessor = mock(TransformationProcessor.class);

    @Before
    public void setUp() throws Exception {
        TransformationCache transformationCache = mock(TransformationCache.class);

        when(transformationProcessor.apply(anyString(),
                                           anyBoolean())).thenReturn(CompletableFuture.completedFuture(null));
        testSubject = new DefaultEventStoreTransformationService(transformationCache,
                                                                 transformationValidator,
                                                                 transformationProcessor);
    }

    @Test
    public void startTransformation() {
        StepVerifier.create(testSubject.startTransformation("demo"))
                    .assertNext(UUID::fromString)
                    .expectComplete()
                    .verify();
    }

    @Test
    public void deleteEvent() {
        when(transformationProcessor.deleteEvent(anyString(), anyLong())).thenReturn(Mono.empty());
        StepVerifier.create(
                            testSubject.deleteEvent("demo", "transformationId",
                                                    0, -1))
                    .expectComplete()
                    .verify();
    }

    @Test
    public void replaceEvent() {
        when(transformationProcessor.replaceEvent(anyString(), anyLong(), any())).thenReturn(Mono.empty());
        StepVerifier.create(testSubject.replaceEvent("demo",
                                                     "transformationId",
                                                     0,
                                                     Event.getDefaultInstance(),
                                                     -1))
                    .expectComplete()
                    .verify();
    }

    @Test
    public void cancelTransformation() {
        StepVerifier.create(testSubject.cancelTransformation("demo",
                                                             "transformationId"))
                    .expectComplete()
                    .verify();
    }

    @Test
    public void applyTransformation() {
        StepVerifier.create(testSubject.applyTransformation("demo",
                                                            "1234",
                                                            100,
                                                            false))
                    .expectComplete()
                    .verify();
    }

    @Test
    public void applyTransformationValidationFailed() {
        doAnswer(invocationOnMock -> {
            throw new IllegalStateException();
        }).when(transformationValidator).apply(anyString(), anyString(), anyLong());
        StepVerifier.create(testSubject.applyTransformation("demo",
                                                            "1234",
                                                            100,
                                                            false))
                    .expectError(IllegalStateException.class)
                    .verify();
    }

    @Test
    public void rollbackTransformation() {
        StepVerifier.create(testSubject.rollbackTransformation("demo",
                                                               "transformationId"))
                    .expectComplete()
                    .verify();
    }

    @Test
    public void deleteOldVersions() {
        StepVerifier.create(testSubject.deleteOldVersions("demo",
                                                          "transformationId"))
                    .expectComplete()
                    .verify();
    }
}