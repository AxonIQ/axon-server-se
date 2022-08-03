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
                                                                       id -> new CommandQueue()))
                   .flatMap(q -> q.enqueue(new CommandAndHandler(commandRequest, handler)));
    }

    public void request(String clientId, long count) {
        commandQueueMap.computeIfAbsent(clientId, id -> new CommandQueue())
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
        private Subscription subscription;
        private final Map<String, Sinks.One<CommandResult>> resultMap = new ConcurrentHashMap<>();
        private final PriorityBlockingQueue<CommandAndHandler> queue = new PriorityBlockingQueue<>(100,
                                                                                                   Comparator.comparingInt(
                                                                                                           CommandAndHandler::priority)) {
            @Override
            public boolean offer(CommandAndHandler o) {
                logger.debug("Size: {}: Offer: {}", this.size(), o.id());
                boolean result = super.offer(o);
                logger.debug("Size: {}: Result: {}", this.size(), result);
                return result;
            }
        };

        CommandQueue() {
            processor = Sinks.many().unicast().onBackpressureBuffer(queue);
            processor.asFlux()
                     .subscribeOn(executor)
                     .subscribe(new BaseSubscriber<>() {

                         @Override
                         protected void hookOnSubscribe(Subscription subscription) {
                             CommandQueue.this.subscription = subscription;
                         }

                         @Override
                         protected void hookOnNext(CommandAndHandler value) {
                             logger.warn("Dispatch: {}", value.id());
                             value.handler.dispatch(value.commandRequest).subscribe(commandResult -> {
                                 Sinks.One<CommandResult> resultMono = resultMap.remove(value.id());
                                 if (resultMono != null) {
                                     resultMono.tryEmitValue(commandResult);
                                 }
                             });
                         }
                     });
        }

        public Mono<CommandResult> enqueue(CommandAndHandler commandRequest) {
            logger.debug("Enqueue: {}", commandRequest.id());
            Sinks.One<CommandResult> sink = Sinks.one();
            resultMap.put(commandRequest.id(), sink);
            processor.emitNext(commandRequest, (signalType, emitResult) -> {
                logger.warn("Failed to emit command: {}", signalType);
                return false;
            });
            return sink.asMono();
        }

        public void request(long count) {
            subscription.request(count);
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

        public CommandAndHandler(Command commandRequest, CommandHandlerSubscription handler) {

            this.commandRequest = commandRequest;
            this.handler = handler;
        }

        public int priority() {
            return 0;
        }

        public String id() {
            return commandRequest.id();
        }
    }
}
