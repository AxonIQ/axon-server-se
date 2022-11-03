package io.axoniq.axonserver.commandprocesing.imp;

import io.axoniq.axonserver.commandprocessing.spi.CapacityException;
import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandHandlerUnsubscribedInterceptor;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.NonNull;

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

public class QueuedCommandDispatcher implements CommandDispatcher, CommandHandlerUnsubscribedInterceptor {

    private static final Logger logger = LoggerFactory.getLogger(QueuedCommandDispatcher.class);

    private final Map<String, CommandQueue> commandQueueMap = new ConcurrentHashMap<>();
    private final Scheduler executor;
    private final Function<CommandHandler, Optional<String>> queueNameProvider;
    private final int softLimit;
    private final int hardLimit;

    private final long defaultTimeout;
    private final MeterRegistry meterRegistry;


    public QueuedCommandDispatcher(Scheduler executor,
                                   Function<CommandHandler,
                                           Optional<String>> queueNameProvider,
                                   int softLimit,
                                   long defaultTimeout,
                                   MeterRegistry meterRegistry
    ) {
        this.executor = executor;
        this.queueNameProvider = queueNameProvider;
        this.softLimit = softLimit;
        this.hardLimit = (int) (softLimit * 1.1);
        this.defaultTimeout = defaultTimeout;
        this.meterRegistry = meterRegistry;
        executor.schedulePeriodically(this::timeout, 1, 1, TimeUnit.MINUTES);
    }

    @Override
    public Mono<CommandResult> dispatch(CommandHandlerSubscription handler, Command commandRequest) {
        return Mono.fromCallable(() -> commandQueueMap.computeIfAbsent(queueNameProvider.apply(handler.commandHandler())
                                                                                        .orElseThrow(() -> new RuntimeException(
                                                                                                "cannot determine queue name for handler")),
                                                                       CommandQueue::new))
                   .flatMap(q -> q.enqueue(new CommandAndHandler(commandRequest, handler)));
    }

    @Override
    public void request(String clientId, long count) {
        commandQueueMap.computeIfAbsent(clientId, CommandQueue::new)
                       .request(count);
    }

    @Override
    public Mono<Void> onCommandHandlerUnsubscribed(CommandHandler commandHandler) {
        return Mono.fromRunnable(() -> {
            String queueName = queueNameProvider.apply(commandHandler).orElse(null);
            if (queueName == null) {
                return;
            }
            CommandQueue queue = commandQueueMap.get(queueName);
            if (queue != null) {
                queue.cancel(commandHandler.context(), commandHandler.commandName());
            }
        });
    }

    public void timeout() {
        commandQueueMap.values().forEach(CommandQueue::timeout);
    }

    private class CommandQueue {

        private final Sinks.Many<CommandAndHandler> processor;
        private final String queueName;
        private final AtomicReference<Subscription> clientSubscription = new AtomicReference<>();
        private final Map<String, Sinks.One<CommandResult>> resultMap = new ConcurrentHashMap<>();
        private final PriorityBlockingQueue<CommandAndHandler> queue = new PriorityBlockingQueue<>(100,
                                                                                                   Comparator.comparingLong(
                                                                                                                     CommandAndHandler::priority)
                                                                                                             .thenComparingLong(
                                                                                                                     CommandAndHandler::timeout));

        private final AtomicReference<Runnable> onClosed = new AtomicReference<>();
        private final Gauge gauge;

        public CommandQueue(String queueName) {
            this.queueName = queueName;
            processor = Sinks.many()
                             .unicast()
                             .onBackpressureBuffer(queue,
                                                   () -> logger.warn(
                                                           "CommandQueue executor has terminated and will no longer execute commands."));
            processor.asFlux()
                     .publishOn(executor)
                     .subscribe(clientSubscription());
            gauge = Gauge.builder("commands.queued", queueName, q -> queue.size())
                         .tags(Tags.of("QUEUE", queueName))
                         .register(meterRegistry);
        }

        private BaseSubscriber<CommandAndHandler> clientSubscription() {
            return new BaseSubscriber<>() {

                @Override
                protected void hookOnSubscribe(@NonNull Subscription subscription) {
                    CommandQueue.this.clientSubscription.set(subscription);
                }

                @Override
                protected void hookOnNext(@NonNull CommandAndHandler cmd) {
                    cmd.dispatch()
                       .subscribe(result -> signalSuccess(cmd.id(), result),
                                  e -> signalError(cmd.id(), e));
                }
            };
        }

        private void signalSuccess(String id, CommandResult cr) {
            Sinks.One<CommandResult> resultMono = resultMap.remove(id);
            if (resultMono != null) {
                if (cr != null) {
                    resultMono.tryEmitValue(cr);
                } else {
                    resultMono.tryEmitEmpty();
                }
            }
        }

        private void signalError(String id, Throwable e) {
            Sinks.One<CommandResult> resultMono = resultMap.remove(id);
            if (resultMono != null) {
                resultMono.tryEmitError(e);
            }
        }

        public Mono<CommandResult> enqueue(CommandAndHandler commandRequest) {
            return Mono.defer(() -> {
                int size = queue.size();
                if (logger.isDebugEnabled()) {
                    logger.debug("{}: Enqueue: {}, queueSize: {}", queueName, commandRequest.id(), size);
                }
                if (size >= hardLimit) {
                    logger.warn(
                            "Reached hard limit on queue {} of size {}, priority of item failed to be added {}, hard limit {}.",
                            queueName,
                            size,
                            commandRequest.priority,
                            hardLimit);
                    return Mono.error(new CapacityException(
                            "Failed to add request to queue " + queueName + " as hard limit was reached."));
                }
                if (commandRequest.priority <= 0 && size >= softLimit) {
                    logger.warn(
                            "Reached soft limit on queue size {} of size {}, priority of item failed to be added {}, soft limit {}.",
                            queueName,
                            size,
                            commandRequest.priority,
                            softLimit);
                    return Mono.error(new CapacityException("Failed to add request to queue " + queueName));
                }
                Sinks.One<CommandResult> sink = Sinks.one();
                resultMap.put(commandRequest.id(), sink);

                processor.emitNext(commandRequest, (signalType, emitResult) ->
                        emitResult.equals(Sinks.EmitResult.FAIL_NON_SERIALIZED));

                return sink.asMono();
            });
        }

        public int size() {
            return queue.size();
        }

        public void request(long count) {
            clientSubscription.get().request(count);
        }

        public void cancel(String context, String commandName) {
            for (Iterator<CommandAndHandler> it = queue.iterator(); it.hasNext(); ) {
                CommandAndHandler commandAndHandler = it.next();
                if (commandAndHandler.handler.commandHandler().commandName().equals(commandName) &&
                        commandAndHandler.handler.commandHandler().context().equals(context)) {
                    it.remove();
                    signalError(commandAndHandler.commandRequest.id(), new RequestDequeuedException());
                }
            }
            Runnable onClosedHandler = onClosed.getAndSet(null);
            if (onClosedHandler != null) {
                onClosedHandler.run();
            }
            meterRegistry.remove(gauge);
        }

        public void timeout() {
            for (Iterator<CommandAndHandler> commandIterator = queue.iterator();
                 commandIterator.hasNext(); ) {
                CommandAndHandler command = commandIterator.next();
                if (command.timeout() < System.currentTimeMillis()) {
                    commandIterator.remove();
                    Sinks.One<CommandResult> s = resultMap.remove(command.commandRequest.id());
                    s.tryEmitError(new TimeoutException());
                }
            }
        }
    }

    private class CommandAndHandler {

        private final Command commandRequest;
        private final CommandHandlerSubscription handler;
        private final long priority;

        private final long timeout;

        public CommandAndHandler(Command commandRequest, CommandHandlerSubscription handler) {
            this.commandRequest = commandRequest;
            this.handler = handler;
            priority = commandRequest.metadata().metadataValue(Command.PRIORITY, 0L);
            timeout = commandRequest.metadata().metadataValue(Command.TIMEOUT,
                                                              System.currentTimeMillis() + defaultTimeout);
        }

        public long priority() {
            return priority;
        }

        public String id() {
            return commandRequest.id();
        }

        public Mono<CommandResult> dispatch() {
            return handler.dispatch(commandRequest);
        }

        public long timeout() {
            return timeout;
        }
    }

    @Override
    public Health health() {
        AtomicReference<Status> status = new AtomicReference<>(Status.UP);

        Map<String, String> details = commandQueueMap
                .entrySet()
                .stream()
                .map(q -> new AbstractMap.SimpleEntry<>(String.format("%s.waitingCommands", q.getKey()), q.getValue().size()))
                .peek(q -> {
                    if (q.getValue() > 10) {
                        status.set(Status.WARN);
                    }
                })
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, q->q.getValue().toString()));

        return new Health() {
            @Override
            public Status status() {
                return status.get();
            }

            @Override
            public Map<String, String> details() {
                return details;
            }
        };
    }
}
