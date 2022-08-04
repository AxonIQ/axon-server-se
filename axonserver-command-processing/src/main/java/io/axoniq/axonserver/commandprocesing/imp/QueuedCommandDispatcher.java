package io.axoniq.axonserver.commandprocesing.imp;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandHandlerUnsubscribedInterceptor;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class QueuedCommandDispatcher implements CommandDispatcher, CommandHandlerUnsubscribedInterceptor {

    private final static Logger logger = LoggerFactory.getLogger(QueuedCommandDispatcher.class);

    private final Map<String, CommandQueue> commandQueueMap = new ConcurrentHashMap<>();
    private final Scheduler executor;
    private final Function<CommandHandler, Optional<String>> queueNameProvider;

    public QueuedCommandDispatcher(Scheduler executor, Function<CommandHandler, Optional<String>> queueNameProvider) {
        this.executor = executor;
        this.queueNameProvider = queueNameProvider;
    }

    @Override
    public Mono<CommandResult> dispatch(CommandHandlerSubscription handler, Command commandRequest) {
        return Mono.fromCallable(() -> commandQueueMap.computeIfAbsent(queueNameProvider.apply(handler.commandHandler())
                                                                                        .orElseThrow(() -> new RuntimeException(
                                                                                                "cannot determine queue name for handler")),
                                                                       CommandQueue::new))
                   .flatMap(q -> q.enqueue(new CommandAndHandler(commandRequest, handler)));
    }

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

    private class CommandQueue {

        private final Sinks.Many<CommandAndHandler> processor;
        private final String queueName;
        private AtomicReference<Subscription> clientSubscription = new AtomicReference<>();
        private final Map<String, Sinks.One<CommandResult>> resultMap = new ConcurrentHashMap<>();
        private final PriorityBlockingQueue<CommandAndHandler> queue = new PriorityBlockingQueue<>(100,
                                                                                                   Comparator.comparingLong(
                                                                                                           CommandAndHandler::priority));

        public CommandQueue(String queueName) {
            this.queueName = queueName;
            processor = Sinks.many().unicast()
                             .onBackpressureBuffer(queue,
                                                   () -> logger.warn(
                                                           "CommandQueue executor has terminated and will no longer execute commands."));
            processor.asFlux()
                     .limitRate(1)
                     .concatMap(cmd -> cmd.dispatch()
                                          .doOnSuccess(result -> signalSuccess(cmd.id(), result))
                                          .doOnError(e -> signalError(cmd.id(), e)), 0)
                     .subscribeOn(executor)
                     .subscribe(clientSubscription());
        }

        private BaseSubscriber<CommandResult> clientSubscription() {
            return new BaseSubscriber<>() {

                @Override
                protected void hookOnSubscribe(Subscription subscription) {
                    CommandQueue.this.clientSubscription.set(subscription);
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
            logger.debug("{}: Enqueue: {}, queueSize: {}", queueName, commandRequest.id(), queue.size());
            Sinks.One<CommandResult> sink = Sinks.one();
            resultMap.put(commandRequest.id(), sink);
            processor.emitNext(commandRequest, (signalType, emitResult) -> {
                logger.warn("Failed to emit command: {}", signalType);
                return false;
            });
            return sink.asMono();
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
                    Sinks.One<CommandResult> sink = resultMap.remove(commandAndHandler.commandRequest.id());
                    sink.tryEmitError(new RequestDequeuedException());
                }
            }
        }
    }

    private static class CommandAndHandler {

        private final Command commandRequest;
        private final CommandHandlerSubscription handler;
        //todo duration how long can it be enqueued?
        private final long priority;

        public CommandAndHandler(Command commandRequest, CommandHandlerSubscription handler) {
            this.commandRequest = commandRequest;
            this.handler = handler;
            priority = commandRequest.metadata().metadataValue(Command.PRIORITY, 0L);
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
    }
}
