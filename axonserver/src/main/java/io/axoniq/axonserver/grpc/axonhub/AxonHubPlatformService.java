package io.axoniq.axonserver.grpc;

import io.axoniq.platform.grpc.PlatformServiceGrpc;
import org.springframework.stereotype.Component;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;

/**
 * Author: marc
 */
@Component
public class AxonServerPlatformService implements AxonServerClientService {

    private static volatile io.grpc.ServiceDescriptor serviceDescriptor;
    public static final String SERVICE_NAME = "io.axoniq.axonserver.grpc.control.PlatformService";

    private final PlatformService platformService;


    public static final io.grpc.MethodDescriptor<io.axoniq.platform.grpc.ClientIdentification,
            io.axoniq.platform.grpc.PlatformInfo> METHOD_GET_PLATFORM_SERVER =
            io.grpc.MethodDescriptor.create(
                    io.grpc.MethodDescriptor.MethodType.UNARY,
                    generateFullMethodName(
                            SERVICE_NAME, "GetPlatformServer"),
                    io.grpc.protobuf.ProtoUtils.marshaller(io.axoniq.platform.grpc.ClientIdentification.getDefaultInstance()),
                    io.grpc.protobuf.ProtoUtils.marshaller(io.axoniq.platform.grpc.PlatformInfo.getDefaultInstance()));
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<io.axoniq.platform.grpc.PlatformInboundInstruction,
            io.axoniq.platform.grpc.PlatformOutboundInstruction> METHOD_OPEN_STREAM =
            io.grpc.MethodDescriptor.create(
                    io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING,
                    generateFullMethodName(
                            SERVICE_NAME, "OpenStream"),
                    io.grpc.protobuf.ProtoUtils.marshaller(io.axoniq.platform.grpc.PlatformInboundInstruction.getDefaultInstance()),
                    io.grpc.protobuf.ProtoUtils.marshaller(io.axoniq.platform.grpc.PlatformOutboundInstruction.getDefaultInstance()));

    public AxonServerPlatformService(PlatformService platformService) {
        this.platformService = platformService;
    }

    private static final class PlatformServiceDescriptorSupplier implements io.grpc.protobuf.ProtoFileDescriptorSupplier {
        @java.lang.Override
        public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
            return io.axoniq.platform.grpc.PlatformApi.getDescriptor();
        }
    }


    public static io.grpc.ServiceDescriptor getServiceDescriptor() {
        io.grpc.ServiceDescriptor result = serviceDescriptor;
        if (result == null) {
            synchronized (PlatformServiceGrpc.class) {
                result = serviceDescriptor;
                if (result == null) {
                    serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
                                                                          .setSchemaDescriptor(new PlatformServiceDescriptorSupplier())
                                                                          .addMethod(METHOD_GET_PLATFORM_SERVER)
                                                                          .addMethod(METHOD_OPEN_STREAM)
                                                                          .build();
                }
            }
        }
        return result;
    }


    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
        return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
                                              .addMethod(
                                                      METHOD_GET_PLATFORM_SERVER,
                                                      asyncUnaryCall(platformService::getPlatformServer))
                                              .addMethod(
                                                      METHOD_OPEN_STREAM,
                                                      asyncBidiStreamingCall(platformService::openStream))
                                              .build();
    }

}
