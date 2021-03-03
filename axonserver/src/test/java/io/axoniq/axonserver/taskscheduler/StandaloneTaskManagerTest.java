/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.taskscheduler;

import io.axoniq.axonserver.grpc.TaskStatus;
import io.axoniq.axonserver.rest.json.UserInfo;
import io.axoniq.axonserver.test.FakeScheduledExecutorService;
import org.junit.*;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class StandaloneTaskManagerTest {

    private StandaloneTaskManager testSubject;
    private final Map<String, Task> tasks = new HashMap<>();
    private final FakeScheduledExecutorService scheduler = new FakeScheduledExecutorService();
    private final AtomicBoolean transientException = new AtomicBoolean();
    private final AtomicBoolean nonTransientException = new AtomicBoolean();
    private final AtomicInteger executionCounter = new AtomicInteger();

    @Before
    public void setup() {
        TaskRepository repository = mock(TaskRepository.class);
        doAnswer(invocationOnMock -> {
            long after = invocationOnMock.getArgument(1);
            long before = invocationOnMock.getArgument(2);
            return tasks.values()
                        .stream()
                        .filter(t -> t.getTimestamp() < before)
                        .filter(t -> t.getTimestamp() >= after)
                        .filter(t -> TaskStatus.SCHEDULED.equals(t.getStatus()))
                        .collect(Collectors.toList());
        }).when(repository).findScheduled(anyString(), anyLong(), anyLong());

        doAnswer(invocation -> {
                     Task task = invocation.getArgument(0);
                     tasks.put(task.getTaskId(), task);
                     return task;
                 }
        ).when(repository).save(any(Task.class));

        doAnswer(invocation -> {
            Task t = invocation.getArgument(0);
            tasks.remove(t.getTaskId());
            return null;
        }).when(repository).delete(any(Task.class));
        when(repository.findById(any()))
                .then(invocation -> Optional.ofNullable(tasks.get(invocation.getArgument(0))));


        testSubject = new StandaloneTaskManager("context",
                                                t -> {
                                                    executionCounter.getAndIncrement();
                                                    CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                                                    if (transientException.get()) {
                                                        completableFuture.completeExceptionally(new TransientException(
                                                                "Failed, please try again"));
                                                    } else if (nonTransientException.get()) {
                                                        completableFuture.completeExceptionally(new RuntimeException(
                                                                "Failed, don't try again"));
                                                    } else {
                                                        completableFuture.complete(null);
                                                    }
                                                    return completableFuture;
                                                },
                                                repository,
                                                new JacksonTaskPayloadSerializer(),
                                                new PlatformTransactionManager() {
                                                    @Override
                                                    public TransactionStatus getTransaction(
                                                            TransactionDefinition definition)
                                                            throws TransactionException {
                                                        return new SimpleTransactionStatus();
                                                    }

                                                    @Override
                                                    public void commit(TransactionStatus status)
                                                            throws TransactionException {

                                                    }

                                                    @Override
                                                    public void rollback(TransactionStatus status)
                                                            throws TransactionException {

                                                    }
                                                },
                                                scheduler,
                                                scheduler.clock());
        testSubject.start();
        scheduler.timeElapses(0);
    }

    @Test
    public void createLocalTask() {
        testSubject.createTask("DummyHandler",
                               new UserInfo("user", Collections.emptySet()),
                               Duration.ofSeconds(5));
        assertEquals(1, tasks.size());
        scheduler.timeElapses(5, TimeUnit.SECONDS);
        assertEquals(0, tasks.size());
    }

    @Test
    public void createLocalTaskAfter11Minutes() {
        testSubject.createTask("DummyHandler",
                               new UserInfo("user", Collections.emptySet()),
                               Duration.ofMinutes(11));
        assertEquals(1, tasks.size());
        assertEquals(1, scheduler.tasks());
        scheduler.timeElapses(5, TimeUnit.MINUTES);
        assertEquals(1, tasks.size());
        assertEquals(1, scheduler.tasks());
        scheduler.timeElapses(5, TimeUnit.MINUTES);
        assertEquals(1, tasks.size());
        assertEquals(2, scheduler.tasks());
        scheduler.timeElapses(1, TimeUnit.MINUTES);
        assertEquals(0, tasks.size());
        assertEquals(1, scheduler.tasks());
        assertEquals(1, executionCounter.get());
    }


    @Test
    public void createLocalTaskTransientException() {
        transientException.set(true);
        testSubject.createTask("DummyHandler",
                               new UserInfo("user", Collections.emptySet()),
                               Duration.ofSeconds(5));
        assertEquals(1, tasks.size());
        scheduler.timeElapses(5, TimeUnit.SECONDS);
        assertEquals(1, executionCounter.get());
        assertEquals(1, tasks.size());
        transientException.set(false);
        scheduler.timeElapses(1, TimeUnit.SECONDS);
        assertEquals(2, executionCounter.get());
        assertEquals(0, tasks.size());
    }

    @Test
    public void createLocalTaskNonTransientException() {
        nonTransientException.set(true);
        testSubject.createTask("DummyHandler",
                               new UserInfo("user", Collections.emptySet()),
                               Duration.ofSeconds(5));
        assertEquals(1, tasks.size());
        scheduler.timeElapses(5, TimeUnit.SECONDS);
        assertEquals(1, executionCounter.get());
        assertEquals(1, tasks.size());
        scheduler.timeElapses(1, TimeUnit.SECONDS);
        assertEquals(1, executionCounter.get());
        assertEquals(1, tasks.size());
    }

    @Test
    public void cancel() {
        String taskId = testSubject.createTask("DummyHandler",
                                               new TaskPayload("Dummy", "DummyPayload".getBytes()),
                                               scheduler.clock().millis() + TimeUnit.MINUTES.toMillis(1));
        assertEquals(1, tasks.size());
        assertEquals(2, scheduler.tasks());
        scheduler.timeElapses(5, TimeUnit.SECONDS);
        assertEquals(1, tasks.size());
        testSubject.cancel(taskId);
        assertEquals(0, tasks.size());
    }
}
