package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.DispatchEvents;
import io.axoniq.axonserver.SubscriptionEvents;
import io.axoniq.axonserver.TopologyEvents.CommandHandlerDisconnected;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandProviderInbound;
import io.axoniq.axonserver.grpc.command.CommandProviderOutbound;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.grpc.command.CommandServiceGrpc;
import io.axoniq.axonserver.message.command.CommandDispatcher;
import io.axoniq.axonserver.message.command.DirectCommandHandler;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import javax.annotation.PreDestroy;

/**
 * Author: marc
 */
@Service("CommandService")
public class CommandService extends CommandServiceGrpc.CommandServiceImplBase implements AxonServerClientService {

    private final CommandDispatcher commandDispatcher;
    private final ContextProvider contextProvider;
    private final ApplicationEventPublisher eventPublisher;
    private final Logger logger = LoggerFactory.getLogger(CommandService.class);

    @Value("${axoniq.axonserver.command-threads:0}")
    private final int processingThreads = 1;
    private final Set<GrpcFlowControlledDispatcherListener> dispatcherListenerSet = new CopyOnWriteArraySet<>();

    public CommandService(CommandDispatcher commandDispatcher,
                          ContextProvider contextProvider,
                          ApplicationEventPublisher eventPublisher
    ) {
        this.commandDispatcher = commandDispatcher;
        this.contextProvider = contextProvider;
        this.eventPublisher = eventPublisher;
    }

    @PreDestroy
    public void cleanup() {
        dispatcherListenerSet.forEach(GrpcFlowControlledDispatcherListener::cancel);
        dispatcherListenerSet.clear();
    }

    @Override
    public StreamObserver<CommandProviderOutbound> openStream(StreamObserver<CommandProviderInbound> responseObserver) {
        String context = contextProvider.getContext();
        SendingStreamObserver<CommandProviderInbound> wrappedResponseObserver = new SendingStreamObserver<>(
                responseObserver);
        return new ReceivingStreamObserver<CommandProviderOutbound>(logger) {
            private volatile String client;
            private volatile GrpcCommandDispatcherListener listener;

            @Override
            protected void consume(CommandProviderOutbound commandFromSubscriber) {
                switch (commandFromSubscriber.getRequestCase()) {
                    case SUBSCRIBE:
                        if (this.client == null) {
                            client = commandFromSubscriber.getSubscribe().getClientId();
                        }
                        eventPublisher.publishEvent(new SubscriptionEvents.SubscribeCommand(context,
                                                                                            commandFromSubscriber
                                                                                                    .getSubscribe(),
                                                                                            new DirectCommandHandler(
                                                                                                    wrappedResponseObserver,
                                                                                                    client,
                                                                                                    commandFromSubscriber
                                                                                                            .getSubscribe()
                                                                                                            .getComponentName())));
                        break;
                    case UNSUBSCRIBE:
                        if (client != null) {
                            eventPublisher.publishEvent(new SubscriptionEvents.UnsubscribeCommand(context,
                                                                                                  commandFromSubscriber
                                                                                                          .getUnsubscribe(),
                                                                                                  false));
                        }
                        break;
                    case FLOW_CONTROL:
                        if (this.listener == null) {
                            listener = new GrpcCommandDispatcherListener(commandDispatcher.getCommandQueues(),
                                                                         commandFromSubscriber.getFlowControl()
                                                                                              .getClientId(),
                                                                         wrappedResponseObserver, processingThreads);
                            dispatcherListenerSet.add(listener);
                        }
                        listener.addPermits(commandFromSubscriber.getFlowControl().getPermits());
                        break;
                    case COMMAND_RESPONSE:
                        commandDispatcher.handleResponse(commandFromSubscriber.getCommandResponse(),false);
                        break;

                    case REQUEST_NOT_SET:
                        break;
                }
            }

            @Override
            protected String sender() {
                return client;
            }

            @Override
            public void onError(Throwable cause) {
                logger.warn("{}: Error on connection from subscriber - {}", client, cause.getMessage());
                cleanup();
            }

            private void cleanup() {
                eventPublisher.publishEvent(new CommandHandlerDisconnected(context, client));
                if (listener != null) {
                    listener.cancel();
                    dispatcherListenerSet.remove(listener);
                }
            }

            @Override
            public void onCompleted() {
                logger.debug("{}: Connection to subscriber closed by subscriber", client);
                cleanup();
                try {
                    responseObserver.onCompleted();
                } catch( RuntimeException cause) {
                    logger.warn("{}: Error completing connection to subscriber - {}", client, cause.getMessage());
                }
            }
        };
    }

    @Override
    public void dispatch(Command request, StreamObserver<CommandResponse> responseObserver) {
        if( logger.isTraceEnabled()) logger.trace("{}: Received command: {}", request.getClientId(), request);
        commandDispatcher.on(new DispatchEvents.DispatchCommand(contextProvider.getContext(),
                                                                       request,
                                                                       commandResponse -> safeReply(request.getClientId(),
                                                                                                    commandResponse,
                                                                                                    responseObserver),
                                                                       false));
    }

    private void safeReply(String clientId, CommandResponse commandResponse, StreamObserver<CommandResponse> responseObserver) {
        try {
            responseObserver.onNext(commandResponse);
            responseObserver.onCompleted();
        } catch (RuntimeException ex) {
            logger.warn("Response to client {} failed", clientId, ex);
        }
    }
}
