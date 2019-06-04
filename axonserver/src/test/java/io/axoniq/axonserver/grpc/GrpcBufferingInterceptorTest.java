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
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.InputStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

public class GrpcBufferingInterceptorTest {

    private CallOptions mockCallOptions;
    private Channel mockChannel;
    private ClientCall.Listener<Object> mockResponseListener;
    private ClientCall<Object, Object> mockCall;
    private ServerCall<Object, Object> mockServerCall;
    private Metadata mockMetaData;
    private ServerCallHandler<Object, Object> mockServerCallHandler;
    private ServerCall.Listener<Object> mockListener;

    @Before
    public void setUp() throws Exception {
        mockServerCall = mock(ServerCall.class);
        mockMetaData = InternalMetadata.newMetadata();
        mockCallOptions = CallOptions.DEFAULT;
        mockChannel = mock(Channel.class);
        mockResponseListener = mock(ClientCall.Listener.class);
        mockCall = mock(ClientCall.class);
        mockServerCallHandler = mock(ServerCallHandler.class);
        mockListener = mock(ServerCall.Listener.class);
        when(mockChannel.newCall(any(), any())).thenReturn(mockCall);
        when(mockServerCallHandler.startCall(any(), any())).thenReturn(mockListener);

    }

    @Test
    public void testInterceptClientCall_BiDiStreaming() {
        MethodDescriptor<Object, Object> method = buildMethod(MethodDescriptor.MethodType.BIDI_STREAMING);

        GrpcBufferingInterceptor testSubject = new GrpcBufferingInterceptor(1000);
        ClientCall<Object, Object> actual = testSubject.interceptCall(method, mockCallOptions, mockChannel);

        verify(mockChannel).newCall(method, mockCallOptions);

        actual.start(mockResponseListener, null);

        verify(mockCall).request(1000);
    }

    @Test
    public void testInterceptClientCall_ServerStreaming() {
        MethodDescriptor<Object, Object> method = buildMethod(MethodDescriptor.MethodType.SERVER_STREAMING);

        GrpcBufferingInterceptor testSubject = new GrpcBufferingInterceptor(1000);
        ClientCall<Object, Object> actual = testSubject.interceptCall(method, mockCallOptions, mockChannel);

        verify(mockChannel).newCall(method, mockCallOptions);

        actual.start(mockResponseListener, null);

        verify(mockCall).request(1000);
    }

    @Test
    public void testInterceptClientCall_ClientStreaming() {
        MethodDescriptor<Object, Object> method = buildMethod(MethodDescriptor.MethodType.CLIENT_STREAMING);

        GrpcBufferingInterceptor testSubject = new GrpcBufferingInterceptor(1000);
        ClientCall<Object, Object> actual = testSubject.interceptCall(method, mockCallOptions, mockChannel);

        verify(mockChannel).newCall(method, mockCallOptions);

        actual.start(mockResponseListener, null);

        verify(mockCall, never()).request(anyInt());
    }

    @Test
    public void testInterceptClientCall_NoStreaming() {
        MethodDescriptor<Object, Object> method = buildMethod(MethodDescriptor.MethodType.UNARY);

        GrpcBufferingInterceptor testSubject = new GrpcBufferingInterceptor(1000);
        ClientCall<Object, Object> actual = testSubject.interceptCall(method, mockCallOptions, mockChannel);

        verify(mockChannel).newCall(method, mockCallOptions);

        actual.start(mockResponseListener, null);

        verify(mockCall, never()).request(anyInt());
    }

    @Test
    public void testInterceptClientCall_ZeroBuffer() {
        MethodDescriptor<Object, Object> method = buildMethod(MethodDescriptor.MethodType.BIDI_STREAMING);

        GrpcBufferingInterceptor testSubject = new GrpcBufferingInterceptor(0);
        ClientCall<Object, Object> actual = testSubject.interceptCall(method, mockCallOptions, mockChannel);

        verify(mockChannel).newCall(method, mockCallOptions);

        actual.start(mockResponseListener, null);

        verify(mockCall, never()).request(anyInt());
    }

    @Test
    public void testInterceptServerCall_BiDiStreaming() {
        MethodDescriptor<Object, Object> method = buildMethod(MethodDescriptor.MethodType.BIDI_STREAMING);
        when(mockServerCall.getMethodDescriptor()).thenReturn(method);

        GrpcBufferingInterceptor testSubject = new GrpcBufferingInterceptor(1000);
        testSubject.interceptCall(mockServerCall, mockMetaData, mockServerCallHandler);

        InOrder inOrder = inOrder(mockServerCallHandler, mockServerCall);
        inOrder.verify(mockServerCallHandler).startCall(mockServerCall, mockMetaData);
        inOrder.verify(mockServerCall).request(1000);
    }

    @Test
    public void testInterceptServerCall_ClientStreaming() {
        MethodDescriptor<Object, Object> method = buildMethod(MethodDescriptor.MethodType.CLIENT_STREAMING);
        when(mockServerCall.getMethodDescriptor()).thenReturn(method);

        GrpcBufferingInterceptor testSubject = new GrpcBufferingInterceptor(1000);
        testSubject.interceptCall(mockServerCall, mockMetaData, mockServerCallHandler);

        InOrder inOrder = inOrder(mockServerCallHandler, mockServerCall);
        inOrder.verify(mockServerCallHandler).startCall(mockServerCall, mockMetaData);
        inOrder.verify(mockServerCall).request(1000);
    }

    @Test
    public void testInterceptServerCall_ServerStreaming() {
        MethodDescriptor<Object, Object> method = buildMethod(MethodDescriptor.MethodType.SERVER_STREAMING);
        when(mockServerCall.getMethodDescriptor()).thenReturn(method);

        GrpcBufferingInterceptor testSubject = new GrpcBufferingInterceptor(1000);
        testSubject.interceptCall(mockServerCall, mockMetaData, mockServerCallHandler);

        verify(mockServerCallHandler).startCall(mockServerCall, mockMetaData);
        verify(mockServerCall, never()).request(anyInt());
    }

    @Test
    public void testInterceptServerCall_NoStreaming() {
        MethodDescriptor<Object, Object> method = buildMethod(MethodDescriptor.MethodType.UNARY);
        when(mockServerCall.getMethodDescriptor()).thenReturn(method);

        GrpcBufferingInterceptor testSubject = new GrpcBufferingInterceptor(1000);
        testSubject.interceptCall(mockServerCall, mockMetaData, mockServerCallHandler);

        verify(mockServerCallHandler).startCall(mockServerCall, mockMetaData);
        verify(mockServerCall, never()).request(anyInt());
    }

    @Test
    public void testInterceptServerCall_ZeroBuffer() {
        MethodDescriptor<Object, Object> method = buildMethod(MethodDescriptor.MethodType.BIDI_STREAMING);
        when(mockServerCall.getMethodDescriptor()).thenReturn(method);

        GrpcBufferingInterceptor testSubject = new GrpcBufferingInterceptor(0);
        testSubject.interceptCall(mockServerCall, mockMetaData, mockServerCallHandler);

        verify(mockServerCallHandler).startCall(mockServerCall, mockMetaData);
        verify(mockServerCall, never()).request(anyInt());
    }

    private MethodDescriptor<Object, Object> buildMethod(MethodDescriptor.MethodType type) {
        return MethodDescriptor.newBuilder()
                               .setFullMethodName("test")
                               .setType(type)
                               .setRequestMarshaller(new StubMarshaller())
                               .setResponseMarshaller(new StubMarshaller())
                               .build();
    }

    private static class StubMarshaller implements MethodDescriptor.Marshaller<Object> {
        @Override
        public InputStream stream(Object value) {
            return null;
        }

        @Override
        public Object parse(InputStream stream) {
            return null;
        }
    }
}
