package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.message.event.EventDispatcher;
import io.axoniq.axonserver.message.event.InputStreamMarshaller;
import io.grpc.ServerServiceDefinition;
import org.springframework.stereotype.Component;

import java.io.InputStream;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.*;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;

/**
 * Author: marc
 */
@Component
public class AxonServerEventService implements AxonServerClientService {
    public static final String SERVICE_NAME = "io.axoniq.axonserver.grpc.event.EventStore";

    public static final io.grpc.MethodDescriptor<io.axoniq.axondb.Event,
            io.axoniq.axondb.grpc.Confirmation> METHOD_APPEND_EVENT =
            io.grpc.MethodDescriptor.create(
                    io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING,
                    generateFullMethodName(
                            SERVICE_NAME, "AppendEvent"),
                    io.grpc.protobuf.ProtoUtils.marshaller(io.axoniq.axondb.Event.getDefaultInstance()),
                    io.grpc.protobuf.ProtoUtils.marshaller(io.axoniq.axondb.grpc.Confirmation.getDefaultInstance()));
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<io.axoniq.axondb.Event,
            io.axoniq.axondb.grpc.Confirmation> METHOD_APPEND_SNAPSHOT =
            io.grpc.MethodDescriptor.create(
                    io.grpc.MethodDescriptor.MethodType.UNARY,
                    generateFullMethodName(
                            SERVICE_NAME, "AppendSnapshot"),
                    io.grpc.protobuf.ProtoUtils.marshaller(io.axoniq.axondb.Event.getDefaultInstance()),
                    io.grpc.protobuf.ProtoUtils.marshaller(io.axoniq.axondb.grpc.Confirmation.getDefaultInstance()));
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<io.axoniq.axondb.grpc.GetAggregateEventsRequest,InputStream> METHOD_LIST_AGGREGATE_EVENTS =
            io.grpc.MethodDescriptor.create(
                    io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING,
                    generateFullMethodName(
                            SERVICE_NAME, "ListAggregateEvents"),
                    io.grpc.protobuf.ProtoUtils.marshaller(io.axoniq.axondb.grpc.GetAggregateEventsRequest.getDefaultInstance()),
                    InputStreamMarshaller.inputStreamMarshaller());
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<io.axoniq.axondb.grpc.GetEventsRequest,InputStream> METHOD_LIST_EVENTS =
            io.grpc.MethodDescriptor.create(
                    io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING,
                    generateFullMethodName(
                            SERVICE_NAME, "ListEvents"),
                    io.grpc.protobuf.ProtoUtils.marshaller(io.axoniq.axondb.grpc.GetEventsRequest.getDefaultInstance()),
                    InputStreamMarshaller.inputStreamMarshaller());
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<io.axoniq.axondb.grpc.ReadHighestSequenceNrRequest,
            io.axoniq.axondb.grpc.ReadHighestSequenceNrResponse> METHOD_READ_HIGHEST_SEQUENCE_NR =
            io.grpc.MethodDescriptor.create(
                    io.grpc.MethodDescriptor.MethodType.UNARY,
                    generateFullMethodName(
                            SERVICE_NAME, "ReadHighestSequenceNr"),
                    io.grpc.protobuf.ProtoUtils.marshaller(io.axoniq.axondb.grpc.ReadHighestSequenceNrRequest.getDefaultInstance()),
                    io.grpc.protobuf.ProtoUtils.marshaller(io.axoniq.axondb.grpc.ReadHighestSequenceNrResponse.getDefaultInstance()));
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<io.axoniq.axondb.grpc.QueryEventsRequest,
            io.axoniq.axondb.grpc.QueryEventsResponse> METHOD_QUERY_EVENTS =
            io.grpc.MethodDescriptor.create(
                    io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING,
                    generateFullMethodName(
                            SERVICE_NAME, "QueryEvents"),
                    io.grpc.protobuf.ProtoUtils.marshaller(io.axoniq.axondb.grpc.QueryEventsRequest.getDefaultInstance()),
                    io.grpc.protobuf.ProtoUtils.marshaller(io.axoniq.axondb.grpc.QueryEventsResponse.getDefaultInstance()));
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<io.axoniq.axondb.grpc.GetFirstTokenRequest,
            io.axoniq.axondb.grpc.TrackingToken> METHOD_GET_FIRST_TOKEN =
            io.grpc.MethodDescriptor.create(
                    io.grpc.MethodDescriptor.MethodType.UNARY,
                    generateFullMethodName(
                            SERVICE_NAME, "GetFirstToken"),
                    io.grpc.protobuf.ProtoUtils.marshaller(io.axoniq.axondb.grpc.GetFirstTokenRequest.getDefaultInstance()),
                    io.grpc.protobuf.ProtoUtils.marshaller(io.axoniq.axondb.grpc.TrackingToken.getDefaultInstance()));
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<io.axoniq.axondb.grpc.GetLastTokenRequest,
            io.axoniq.axondb.grpc.TrackingToken> METHOD_GET_LAST_TOKEN =
            io.grpc.MethodDescriptor.create(
                    io.grpc.MethodDescriptor.MethodType.UNARY,
                    generateFullMethodName(
                            SERVICE_NAME, "GetLastToken"),
                    io.grpc.protobuf.ProtoUtils.marshaller(io.axoniq.axondb.grpc.GetLastTokenRequest.getDefaultInstance()),
                    io.grpc.protobuf.ProtoUtils.marshaller(io.axoniq.axondb.grpc.TrackingToken.getDefaultInstance()));
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<io.axoniq.axondb.grpc.GetTokenAtRequest,
            io.axoniq.axondb.grpc.TrackingToken> METHOD_GET_TOKEN_AT =
            io.grpc.MethodDescriptor.create(
                    io.grpc.MethodDescriptor.MethodType.UNARY,
                    generateFullMethodName(
                            SERVICE_NAME, "GetTokenAt"),
                    io.grpc.protobuf.ProtoUtils.marshaller(io.axoniq.axondb.grpc.GetTokenAtRequest.getDefaultInstance()),
                    io.grpc.protobuf.ProtoUtils.marshaller(io.axoniq.axondb.grpc.TrackingToken.getDefaultInstance()));


    private final EventDispatcher eventDispatcher;

    public AxonServerEventService(EventDispatcher eventDispatcher) {
        this.eventDispatcher = eventDispatcher;
    }

    @Override
    public ServerServiceDefinition bindService() {
        return io.grpc.ServerServiceDefinition.builder(SERVICE_NAME)
                                              .addMethod(
                                                      METHOD_APPEND_EVENT,
                                                      asyncClientStreamingCall( eventDispatcher::appendEvent))
                                              .addMethod(
                                                      METHOD_APPEND_SNAPSHOT,
                                                      asyncUnaryCall(
                                                              eventDispatcher::appendSnapshot))
                                              .addMethod(
                                                      METHOD_LIST_AGGREGATE_EVENTS,
                                                      asyncServerStreamingCall(eventDispatcher::listAggregateEvents))
                                              .addMethod(
                                                      METHOD_LIST_EVENTS,
                                                      asyncBidiStreamingCall(eventDispatcher::listEvents))
                                              .addMethod(
                                                      METHOD_READ_HIGHEST_SEQUENCE_NR,
                                                      asyncUnaryCall(eventDispatcher::readHighestSequenceNr))
                                              .addMethod(
                                                      METHOD_GET_FIRST_TOKEN,
                                                      asyncUnaryCall(eventDispatcher::getFirstToken))
                                              .addMethod(
                                                      METHOD_GET_LAST_TOKEN,
                                                      asyncUnaryCall(eventDispatcher::getLastToken))
                                              .addMethod(
                                                      METHOD_GET_TOKEN_AT,
                                                      asyncUnaryCall(eventDispatcher::getTokenAt))
                                              .addMethod(
                                                      METHOD_QUERY_EVENTS,
                                                      asyncBidiStreamingCall(eventDispatcher::queryEvents))
                                              .build();

    }
}
