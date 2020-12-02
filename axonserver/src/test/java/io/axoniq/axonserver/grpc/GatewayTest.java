/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.AxonServerAccessController;
import io.axoniq.axonserver.AxonServerStandardAccessController;
import io.axoniq.axonserver.LicenseAccessController;
import io.axoniq.axonserver.config.AccessControlConfiguration;
import io.axoniq.axonserver.config.GrpcContextAuthenticationProvider;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.config.SslConfiguration;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.control.NodeInfo;
import io.axoniq.axonserver.grpc.control.PlatformInfo;
import io.axoniq.axonserver.grpc.control.PlatformServiceGrpc;
import io.axoniq.axonserver.topology.Topology;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.*;
import org.springframework.security.core.Authentication;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class GatewayTest {

    private Gateway testSubject;
    private AxonServerAccessController accessController;
    private MessagingPlatformConfiguration routingConfiguration;
    private LicenseAccessController licenseAccessController = mock(LicenseAccessController.class);


    @Before
    public void setUp() {
        accessController = mock(AxonServerStandardAccessController.class);
        routingConfiguration = new MessagingPlatformConfiguration(null);
        routingConfiguration.setPort(7023);
        routingConfiguration.setAccesscontrol(new AccessControlConfiguration());
        routingConfiguration.getAccesscontrol().setEnabled(true);
        routingConfiguration.setName("JUnit");
        when(licenseAccessController.allowed()).thenReturn(true);
    }

    @After
    public void tearDown() {
        if (testSubject.isRunning()) {
            testSubject.stop();
        }
    }


    @Test
    public void stopWithCallback() {
        testSubject = new Gateway(routingConfiguration, Collections.emptyList(),
                                  accessController, licenseAccessController);

        AtomicBoolean stopped = new AtomicBoolean(false);
        testSubject.start();
        testSubject.stop(() -> stopped.set(true));
        assertTrue(stopped.get());
        assertFalse(testSubject.isRunning());
    }

    @Test
    public void start() {
        testSubject = new Gateway(routingConfiguration, Collections.emptyList(),
                                  accessController, licenseAccessController);

        testSubject.start();
        assertTrue(testSubject.isRunning());
        testSubject.stop();
        assertFalse(testSubject.isRunning());
    }

    @Test(expected = RuntimeException.class)
    public void startWithSslIncompleteConfiguration() {
        routingConfiguration.setSsl(new SslConfiguration());
        routingConfiguration.getSsl().setEnabled(true);
        testSubject = new Gateway(routingConfiguration, Collections.emptyList(),
                                  accessController, licenseAccessController);

        testSubject.start();
    }

    @Test
    public void startWithSsl() {
        routingConfiguration.setSsl(new SslConfiguration());
        routingConfiguration.getSsl().setEnabled(true);
        routingConfiguration.getSsl().setCertChainFile("../resources/sample.crt");
        routingConfiguration.getSsl().setPrivateKeyFile("../resources/sample.pem");
        testSubject = new Gateway(routingConfiguration, Collections.emptyList(),
                                  accessController, licenseAccessController);
        assertTrue(testSubject.isAutoStartup());
        testSubject.start();
        assertTrue(testSubject.isRunning());
        testSubject.stop();
    }

    @Test
    public void testAccessControlInterceptor() {
        accessController = new AxonServerAccessController() {
            @Override
            public boolean allowed(String fullMethodName, String context, String token) {
                return "1234".equals(token) && Topology.DEFAULT_CONTEXT.equals(context);
            }

            @Override
            public boolean isRoleBasedAuthentication() {
                return false;
            }

            @Override
            public Authentication authentication(String context, String token) {
                return GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL;
            }
        };

        AxonServerClientService dummyPlatformService = new DummyPlatformService();
        testSubject = new Gateway(routingConfiguration,
                                  Collections.singletonList(dummyPlatformService),
                                  accessController, licenseAccessController);
        testSubject.start();

        Channel channel = NettyChannelBuilder.forAddress("localhost", routingConfiguration.getPort()).usePlaintext()
                                             .build();
        try {
            PlatformServiceGrpc.newBlockingStub(channel).getPlatformServer(ClientIdentification
                                                                                   .newBuilder()
                                                                                   .build());
            fail("Expected exception");
        } catch (StatusRuntimeException sre) {
            assertEquals(ErrorCode.AUTHENTICATION_TOKEN_MISSING.getCode(),
                         sre.getTrailers().get(GrpcMetadataKeys.ERROR_CODE_KEY));
        }

        try {
            PlatformServiceGrpc.newBlockingStub(channel)
                               .withInterceptors(getClientInterceptor(GrpcMetadataKeys.TOKEN_KEY, "2345"))
                               .withInterceptors(getClientInterceptor(GrpcMetadataKeys.CONTEXT_MD_KEY, Topology.DEFAULT_CONTEXT))
                               .getPlatformServer(ClientIdentification.newBuilder().build());
            fail("Expected exception");
        } catch (StatusRuntimeException sre) {
            assertEquals(ErrorCode.AUTHENTICATION_INVALID_TOKEN.getCode(),
                         sre.getTrailers().get(GrpcMetadataKeys.ERROR_CODE_KEY));
        }

        PlatformInfo result = PlatformServiceGrpc.newBlockingStub(channel)
                                                 .withInterceptors(getClientInterceptor(GrpcMetadataKeys.TOKEN_KEY,
                                                                                        "1234"))
                                                 .withInterceptors(getClientInterceptor(GrpcMetadataKeys.CONTEXT_MD_KEY,
                                                                                        Topology.DEFAULT_CONTEXT))
                                                 .getPlatformServer(ClientIdentification.newBuilder().build());
        assertEquals(Topology.DEFAULT_CONTEXT, result.getPrimary().getNodeName());
    }

    @Test
    public void testSmallMaxMessageSize() {
        int size = 100;
        routingConfiguration.setMaxMessageSize(size);
        routingConfiguration.getAccesscontrol().setEnabled(false);
        AxonServerClientService dummyPlatformService = new DummyPlatformService();
        testSubject = new Gateway(routingConfiguration,
                                  Collections.singletonList(dummyPlatformService),
                                  accessController, licenseAccessController);
        testSubject.start();

        String aLongName = generateString(size);
        Channel channel = NettyChannelBuilder.forAddress("localhost", routingConfiguration.getPort()).usePlaintext()
                                             .build();
        PlatformServiceGrpc.PlatformServiceBlockingStub stub = PlatformServiceGrpc.newBlockingStub(channel);

        try {
            stub.getPlatformServer(ClientIdentification.newBuilder().setClientId(aLongName).build());
            fail("Should fail on too long");
        } catch (StatusRuntimeException sre) {
            assertTrue(Status.Code.CANCELLED.equals(sre.getStatus().getCode()) ||
                               Status.Code.INTERNAL.equals(sre.getStatus().getCode()));
        }
    }

    @Test
    public void testLargeMaxMessageSize() {
        int size = 1024 * 1024 * 10;
        routingConfiguration.setMaxMessageSize(size);
        routingConfiguration.getAccesscontrol().setEnabled(false);
        AxonServerClientService dummyPlatformService = new DummyPlatformService();
        testSubject = new Gateway(routingConfiguration,
                                  Collections.singletonList(dummyPlatformService),
                                  accessController, licenseAccessController);
        testSubject.start();

        String aLongName = generateString(size);
        Channel channel = NettyChannelBuilder.forAddress("localhost", routingConfiguration.getPort()).usePlaintext()
                                             .build();
        PlatformServiceGrpc.PlatformServiceBlockingStub stub = PlatformServiceGrpc.newBlockingStub(channel);

        try {
            stub.getPlatformServer(ClientIdentification.newBuilder().setClientId(aLongName).build());
            fail("Should fail on too long");
        } catch (StatusRuntimeException sre) {
            assertEquals(Status.Code.CANCELLED, sre.getStatus().getCode());
        }
        String aShorterName = generateString(size - 100);
        stub.getPlatformServer(ClientIdentification.newBuilder().setClientId(aShorterName).build());
    }

    private String generateString(int length) {
        StringBuilder stringBuilder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            stringBuilder.append("A");
        }
        return stringBuilder.toString();
    }

    private ClientInterceptor getClientInterceptor(Metadata.Key<String> tokenKey, String token) {
        return new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                    MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions,
                    Channel channel) {
                return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(channel.newCall(methodDescriptor,
                                                                                                        callOptions)) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        headers.put(tokenKey, token);
                        super.start(responseListener, headers);
                    }
                };
            }
        };
    }


    private static class DummyPlatformService extends PlatformServiceGrpc.PlatformServiceImplBase
            implements AxonServerClientService {

        private ContextProvider contextProvider = new DefaultContextProvider();

        @Override
        public void getPlatformServer(ClientIdentification request, StreamObserver<PlatformInfo> responseObserver) {
            responseObserver.onNext(PlatformInfo.newBuilder().setPrimary(NodeInfo.newBuilder().setNodeName(contextProvider.getContext()))
                                                .build());
            responseObserver.onCompleted();
        }
    }
}
