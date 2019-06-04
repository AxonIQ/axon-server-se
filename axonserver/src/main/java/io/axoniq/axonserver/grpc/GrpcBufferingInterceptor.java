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

/**
 * Interceptor that immediately requests a number of messages from the connected component, to increase the flow of
 * messages.
 * <p>
 * As an additional message is requested by default each time a message is received, setting an initial request amount
 * will allow that number of messages to be "in transit" before the server stops sending more.
 */
public class GrpcBufferingInterceptor implements ClientInterceptor, ServerInterceptor {
    private final int additionalBuffer;

    /**
     * Initialize the interceptor to ask for {@code additionalBuffer} amount of messages from the server.
     *
     * @param additionalBuffer The number of messages the server may send before waiting for permits to be renewed
     */
    public GrpcBufferingInterceptor(int additionalBuffer) {
        this.additionalBuffer = additionalBuffer;
    }

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

    private static class AdditionalMessageRequestingCall<ReqT, RespT> extends ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT> {

        private final int additionalBuffer;

        public AdditionalMessageRequestingCall(ClientCall<ReqT, RespT> call, int additionalBuffer) {
            super(call);
            this.additionalBuffer = additionalBuffer;
        }

        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
            super.start(responseListener, headers);
            request(additionalBuffer);
        }
    }
}
