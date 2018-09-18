package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandProviderInbound;
import io.axoniq.axonserver.grpc.command.CommandProviderOutbound;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoUtils;
import org.springframework.stereotype.Component;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;

/**
 * Author: marc
 */
@Component
public class AxonServerCommandService implements AxonServerClientService {
    public static final String SERVICE_NAME = "io.axoniq.axonhub.grpc.CommandService";
    // Static method descriptors that strictly reflect the proto.
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final MethodDescriptor<CommandProviderOutbound, CommandProviderInbound> METHOD_OPEN_STREAM =
                MethodDescriptor.newBuilder(ProtoUtils.marshaller(CommandProviderOutbound.getDefaultInstance()),
                                            ProtoUtils.marshaller(CommandProviderInbound.getDefaultInstance()))
                        .setFullMethodName( generateFullMethodName(
                                SERVICE_NAME, "OpenStream"))
                        .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                        .build();

    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final MethodDescriptor<Command, CommandResponse> METHOD_DISPATCH =
            MethodDescriptor.newBuilder(ProtoUtils.marshaller(Command.getDefaultInstance()),
                                                                  ProtoUtils.marshaller(CommandResponse.getDefaultInstance()))
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Dispatch"))
                            .setType(MethodDescriptor.MethodType.UNARY)
                            .build();

    private final CommandService commandService;

    public AxonServerCommandService(CommandService commandService) {
        this.commandService = commandService;
    }

    @Override
    public ServerServiceDefinition bindService() {
        return ServerServiceDefinition.builder(SERVICE_NAME)
                                              .addMethod(
                                                      METHOD_OPEN_STREAM,
                                                      asyncBidiStreamingCall(commandService::openStream))
                                              .addMethod(
                                                      METHOD_DISPATCH,
                                                      asyncUnaryCall(commandService::dispatch))
                                              .build();
    }
}
