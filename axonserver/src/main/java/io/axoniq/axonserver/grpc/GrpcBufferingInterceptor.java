/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */
package io.axoniq.axonserver.grpc;

import io.grpc.*;

import javax.annotation.Nullable;

public class GrpcBufferingInterceptor implements ClientInterceptor, ServerInterceptor {
    private final int additionalBuffer;

    public GrpcBufferingInterceptor(int additionalBuffer) {this.additionalBuffer = additionalBuffer;}

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        ClientCall<ReqT, RespT> call = next.newCall(method, callOptions);
        if (additionalBuffer <= 0 || method.getType().serverSendsOneMessage()) {
            return call;
        }
        return new AdditionalMessageRequestingCall<>(call, additionalBuffer);

    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        ServerCall.Listener<ReqT> listener = next.startCall(call, headers);
        if (additionalBuffer > 0 && !call.getMethodDescriptor().getType().clientSendsOneMessage()) {
            call.request(additionalBuffer);
        }
        return listener;
    }

    private static class AdditionalMessageRequestingCall<ReqT, RespT> extends ClientCall<ReqT, RespT> {
        private final ClientCall<ReqT, RespT> call;
        private final int additionalBuffer;

        public AdditionalMessageRequestingCall(ClientCall<ReqT, RespT> call,
                                               int additionalBuffer) {this.call = call;
            this.additionalBuffer = additionalBuffer;
        }

        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
            call.start(responseListener, headers);
            call.request(additionalBuffer);
        }

        @Override
        public void request(int numMessages) {
            call.request(numMessages);
        }

        @Override
        public void cancel(@Nullable String message, @Nullable Throwable cause) {
            call.cancel(message, cause);
        }

        @Override
        public void halfClose() {
            call.halfClose();
        }

        @Override
        public void sendMessage(ReqT message) {
            call.sendMessage(message);
        }
    }
}
