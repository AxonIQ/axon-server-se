/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.AxonServerAccessController;
import io.grpc.Attributes;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static io.grpc.Status.Code.PERMISSION_DENIED;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Marc Gathier
 */
@RunWith(MockitoJUnitRunner.class)
public class AuthenticationInterceptorTest {
    private AuthenticationInterceptor testSubject;

    @Mock
    private AxonServerAccessController accessController;

    @Mock
    private ServerCallHandler<String, String> handler;

    @Mock
    private ServerCall<String, String> call;

    @Mock
    private MethodDescriptor.Marshaller<String> marshaller;

    private MethodDescriptor<String, String> path1;
    private MethodDescriptor<String, String> path2;

    private ArgumentCaptor<Status> status;
    private ArgumentCaptor<Metadata> trailers;
    private Metadata metadata = new Metadata();


    @Before
    public void setup() {
        when(accessController.allowed(eq("path1"), any(), eq("1234"))).thenReturn(false);
        when(accessController.allowed(eq("path2"), any(), eq("1234"))).thenReturn(true);
        when(call.getAttributes()).thenReturn(Attributes.EMPTY);
        testSubject = new AuthenticationInterceptor(accessController);

        status = ArgumentCaptor.forClass(Status.class);
        trailers = ArgumentCaptor.forClass(Metadata.class);
        path1 =
                MethodDescriptor.<String, String>newBuilder().setFullMethodName("path1")
                        .setType(MethodDescriptor.MethodType.CLIENT_STREAMING)
                        .setRequestMarshaller(marshaller)
                        .setResponseMarshaller(marshaller)
                        .build();
        path2 =
                MethodDescriptor.<String, String>newBuilder().setFullMethodName("path2")
                        .setType(MethodDescriptor.MethodType.CLIENT_STREAMING)
                        .setRequestMarshaller(marshaller)
                        .setResponseMarshaller(marshaller)
                        .build();
    }

    @Test
    public void noToken() {
        when(call.getMethodDescriptor()).thenReturn(path1);
        when(call.getAttributes()).thenReturn(Attributes.EMPTY);
        testSubject.interceptCall(call, metadata, handler);
        verify(call).close(status.capture(), trailers.capture());
        assertEquals(PERMISSION_DENIED, status.getValue().getCode());
        assertEquals("AXONIQ-1000", trailers.getValue().get(GrpcMetadataKeys.ERROR_CODE_KEY));
        verify(handler, times(0)).startCall(any(), any());
    }

    @Test
    public void invalidToken() {
        metadata.put(GrpcMetadataKeys.TOKEN_KEY, "1234");
        when(call.getMethodDescriptor()).thenReturn(path1);

        testSubject.interceptCall(call, metadata, handler);

        verify(call).close(status.capture(), trailers.capture());
        assertEquals(PERMISSION_DENIED, status.getValue().getCode());
        assertEquals("AXONIQ-1001", trailers.getValue().get(GrpcMetadataKeys.ERROR_CODE_KEY));
        verify(handler, times(0)).startCall(any(), any());
    }

    @Test
    public void validToken() {
        metadata.put(GrpcMetadataKeys.TOKEN_KEY, "1234");
        when(call.getMethodDescriptor()).thenReturn(path2);

        testSubject.interceptCall(call, metadata, handler);

        verify(handler, times(1)).startCall(any(), any());
    }

}
