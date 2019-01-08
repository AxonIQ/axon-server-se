package io.axoniq.axonserver.grpc.axonhub;

import io.axoniq.axonserver.grpc.AxonServerClientService;
import io.axoniq.axonserver.grpc.CommandService;
import io.axoniq.axonserver.grpc.SerializedCommandProviderInbound;
import io.axoniq.axonserver.grpc.SerializedCommandResponse;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandProviderOutbound;
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
public class AxonHubCommandService implements AxonServerClientService {
    public static final String SERVICE_NAME = "io.axoniq.axonhub.grpc.CommandService";
    // Static method descriptors that strictly reflect the proto.
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final MethodDescriptor<CommandProviderOutbound, SerializedCommandProviderInbound> METHOD_OPEN_STREAM =
                MethodDescriptor.newBuilder(ProtoUtils.marshaller(CommandProviderOutbound.getDefaultInstance()),
                                            ProtoUtils.marshaller(SerializedCommandProviderInbound.getDefaultInstance()))
                        .setFullMethodName( generateFullMethodName(
                                SERVICE_NAME, "OpenStream"))
                        .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                        .build();

    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final MethodDescriptor<Command, SerializedCommandResponse> METHOD_DISPATCH =
            MethodDescriptor.newBuilder(ProtoUtils.marshaller(Command.getDefaultInstance()),
                                        ProtoUtils.marshaller(SerializedCommandResponse.getDefaultInstance()))
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Dispatch"))
                            .setType(MethodDescriptor.MethodType.UNARY)
                            .build();

    private final CommandService commandService;

    public AxonHubCommandService(CommandService commandService) {
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
