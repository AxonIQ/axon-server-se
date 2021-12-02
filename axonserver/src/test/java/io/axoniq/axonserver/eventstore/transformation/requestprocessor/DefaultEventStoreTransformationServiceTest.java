/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.api.Authentication;
import io.axoniq.axonserver.eventstore.transformation.impl.DefaultTransformationValidator;
import io.axoniq.axonserver.eventstore.transformation.impl.TransformationProcessor;
import io.axoniq.axonserver.eventstore.transformation.impl.TransformationStateManager;
import io.axoniq.axonserver.eventstore.transformation.impl.TransformationValidator;
import io.axoniq.axonserver.grpc.event.Event;
import org.junit.*;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

import static io.axoniq.axonserver.test.AssertUtils.assertWithin;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public class DefaultEventStoreTransformationServiceTest {

    private DefaultEventStoreTransformationService testSubject;
    private final TransformationValidator transformationValidator = mock(DefaultTransformationValidator.class);
    private final TransformationProcessor transformationProcessor = mock(TransformationProcessor.class);
    private final TransformationStateManager transformationStateManager = mock(TransformationStateManager.class);
    private final Authentication authentication = new Authentication() {
        @Nonnull
        @Override
        public String username() {
            return "JUNIT";
        }
    };

    @Before
    public void setUp() throws Exception {

        when(transformationProcessor.apply(anyString(),
                                           anyBoolean(),
                                           anyString(),
                                           any(), anyLong(), anyLong()))
                .thenReturn(Mono.empty());
        testSubject = new DefaultEventStoreTransformationService(transformationStateManager,
                                                                 transformationValidator,
                                                                 transformationProcessor);
    }

    @Test
    public void startTransformation() {
        StepVerifier.create(testSubject.startTransformation("demo", "descirption", authentication))
                    .assertNext(UUID::fromString)
                    .expectComplete()
                    .verify();
    }

    @Test
    public void deleteEvent() {
        when(transformationProcessor.deleteEvent(anyString(), anyLong())).thenReturn(Mono.empty());
        StepVerifier.create(
                            testSubject.deleteEvent("demo", "transformationId",
                                                    0, -1, authentication))
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
                                                     -1, authentication))
                    .expectComplete()
                    .verify();
    }

    @Test
    public void cancelTransformation() {
        StepVerifier.create(testSubject.cancelTransformation("demo",
                                                             "transformationId", authentication))
                    .expectComplete()
                    .verify();
    }

    @Test
    public void applyTransformation() {
        when(transformationStateManager.firstToken(anyString())).thenReturn(Mono.just(0L));
        StepVerifier.create(testSubject.applyTransformation("demo",
                                                            "1234",
                                                            100,
                                                            false, authentication))
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
                                                            false, authentication))
                    .expectError(IllegalStateException.class)
                    .verify();
    }

    @Test
    public void rollbackTransformation() {
        StepVerifier.create(testSubject.rollbackTransformation("demo",
                                                               "transformationId", authentication))
                    .expectComplete()
                    .verify();
    }

    @Test
    public void deleteOldVersions() {
        StepVerifier.create(testSubject.deleteOldVersions("demo",
                                                          "transformationId", authentication))
                    .expectComplete()
                    .verify();
    }

    @Test
    public void serialExecution() throws InterruptedException {
        List<String> entries = new CopyOnWriteArrayList<>();
        doAnswer(invocationOnMock -> {
            sleep(500);
            return null;
        }).when(transformationValidator).validateDeleteEvent(anyString(), anyString(), anyLong(), anyLong());

        when(transformationProcessor.deleteEvent(anyString(), anyLong())).thenReturn(Mono.empty());
        when(transformationProcessor.replaceEvent(anyString(), anyLong(), any())).thenReturn(Mono.empty());
        doAnswer(invocationOnMock -> null).when(transformationValidator)
                                          .validateReplaceEvent(anyString(), anyString(), anyLong(), anyLong(), any());

        Scheduler scheduler = Schedulers.parallel();
        testSubject.startTransformation("demo", "description", authentication)
                   .subscribe(id -> {
                       scheduler.schedule(() ->
                                                  testSubject.deleteEvent("demo", id, 0, -1, authentication)
                                                             .subscribe(r -> {
                                                                        },
                                                                        Throwable::printStackTrace,
                                                                        () -> entries.add("DELETE")));
                       scheduler.schedule(() -> testSubject.replaceEvent("demo", id, 1,
                                                                         Event.getDefaultInstance(), 0, authentication)
                                                           .subscribe(r -> {
                                                           }, Throwable::printStackTrace, () ->
                                                                              entries.add("REPLACE")));
                   });

        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(2, entries.size()));
        assertEquals("DELETE", entries.get(0));
        assertEquals("REPLACE", entries.get(1));
    }

    private void sleep(int i) {
        try {
            Thread.sleep(i);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}