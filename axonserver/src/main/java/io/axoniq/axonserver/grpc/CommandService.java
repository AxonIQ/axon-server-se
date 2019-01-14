package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.applicationevents.SubscriptionEvents;
import io.axoniq.axonserver.applicationevents.TopologyEvents.CommandHandlerDisconnected;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandProviderOutbound;
import io.axoniq.axonserver.grpc.command.CommandServiceGrpc;
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.message.command.CommandDispatcher;
import io.axoniq.axonserver.message.command.CommandHandler;
import io.axoniq.axonserver.message.command.DirectCommandHandler;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import javax.annotation.PreDestroy;

import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;

/**
 * Author: marc
 */
@Service("CommandService")
public class CommandService implements AxonServerClientService {

    public static final MethodDescriptor<Command, SerializedCommandResponse> METHOD_DISPATCH =
            CommandServiceGrpc.getDispatchMethod().toBuilder(ProtoUtils.marshaller(Command.getDefaultInstance()),
                    ProtoUtils.marshaller(SerializedCommandResponse.getDefaultInstance()))
                          .build();

    public static final MethodDescriptor<CommandProviderOutbound, SerializedCommandProviderInbound> METHOD_OPEN_STREAM =
            CommandServiceGrpc.getOpenStreamMethod().toBuilder(ProtoUtils.marshaller(CommandProviderOutbound.getDefaultInstance()),
                                                               ProtoUtils.marshaller(SerializedCommandProviderInbound.getDefaultInstance()))
                .build();


    private final CommandDispatcher commandDispatcher;
    private final ContextProvider contextProvider;
    private final ApplicationEventPublisher eventPublisher;
    private final Logger logger = LoggerFactory.getLogger(CommandService.class);

    @Value("${axoniq.axonserver.command-threads:1}")
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
    public final ServerServiceDefinition bindService() {
        return ServerServiceDefinition.builder(CommandServiceGrpc.SERVICE_NAME)
                                              .addMethod(
                                                      METHOD_OPEN_STREAM,
                                                      asyncBidiStreamingCall(this::openStream))
                                              .addMethod(
                                                      METHOD_DISPATCH,
                                                      asyncUnaryCall(this::dispatch))
                                              .build();
    }


    public StreamObserver<CommandProviderOutbound> openStream(StreamObserver<SerializedCommandProviderInbound> responseObserver) {
        String context = contextProvider.getContext();
        SendingStreamObserver<SerializedCommandProviderInbound> wrappedResponseObserver = new SendingStreamObserver<>(
                responseObserver);
        return new ReceivingStreamObserver<CommandProviderOutbound>(logger) {
            private volatile String client;
            private volatile GrpcCommandDispatcherListener listener;
            private volatile CommandHandler commandHandler;

            @Override
            protected void consume(CommandProviderOutbound commandFromSubscriber) {
                switch (commandFromSubscriber.getRequestCase()) {
                    case SUBSCRIBE:
                        checkInitClient(commandFromSubscriber.getSubscribe().getClientId(), commandFromSubscriber.getSubscribe().getComponentName());
                        eventPublisher.publishEvent(new SubscriptionEvents.SubscribeCommand(context,
                                                                                            commandFromSubscriber
                                                                                                    .getSubscribe()
                                                                                            , commandHandler));
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
                                                                         new ClientIdentification(context, commandFromSubscriber.getFlowControl()
                                                                                                                                .getClientId()).toString(),
                                                                         wrappedResponseObserver, processingThreads);
                            dispatcherListenerSet.add(listener);
                        }
                        listener.addPermits(commandFromSubscriber.getFlowControl().getPermits());
                        break;
                    case COMMAND_RESPONSE:
                        commandDispatcher.handleResponse(new SerializedCommandResponse(commandFromSubscriber.getCommandResponse()),false);
                        break;

                    case REQUEST_NOT_SET:
                        break;
                }
            }

            private void checkInitClient(String clientId, String component) {
                if( this.client == null) {
                    client = clientId;
                    commandHandler = new DirectCommandHandler(
                            wrappedResponseObserver, new ClientIdentification(context, client), component);
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
                if( client != null) {
                    eventPublisher.publishEvent(new CommandHandlerDisconnected(context, client));
                }
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

    public void dispatch(Command command, StreamObserver<SerializedCommandResponse> responseObserver) {
        SerializedCommand request = new SerializedCommand(command);
        String clientId = command.getClientId();
        if( logger.isTraceEnabled()) logger.trace("{}: Received command: {}", clientId, request);
        commandDispatcher.dispatch(contextProvider.getContext(), request, commandResponse -> safeReply(clientId,
                                                                                                    commandResponse,
                                                                                                    responseObserver),
                                                                       false);
    }

    private void safeReply(String clientId, SerializedCommandResponse commandResponse, StreamObserver<SerializedCommandResponse> responseObserver) {
        try {
            responseObserver.onNext(commandResponse);
            responseObserver.onCompleted();
        } catch (RuntimeException ex) {
            logger.warn("Response to client {} failed", clientId, ex);
        }
    }
}
