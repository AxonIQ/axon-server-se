package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.Context;
import io.axoniq.axonserver.grpc.internal.ContextApplication;
import io.axoniq.axonserver.grpc.internal.ContextConfiguration;
import io.axoniq.axonserver.grpc.internal.ContextEntry;
import io.axoniq.axonserver.grpc.internal.ContextLoadBalanceStrategy;
import io.axoniq.axonserver.grpc.internal.ContextName;
import io.axoniq.axonserver.grpc.internal.ContextProcessorLBStrategy;
import io.axoniq.axonserver.grpc.internal.ContextUpdateConfirmation;
import io.axoniq.axonserver.grpc.internal.ContextUser;
import io.axoniq.axonserver.grpc.internal.DeleteContextRequest;
import io.axoniq.axonserver.grpc.internal.LoadBalanceStrategy;
import io.axoniq.axonserver.grpc.internal.NodeContext;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import io.axoniq.axonserver.grpc.internal.RaftGroupServiceGrpc;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.*;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Marc Gathier
 */
public class RemoteRaftGroupServiceTest {

    private static final String CONTEXT = "TEST";
    private static final String NODE = "NODE";
    private static final int port = 27777;
    public static final int TIMEOUT = 5;
    private RemoteRaftGroupService testSubject;
    private static Server dummyServer;

    @BeforeClass
    public static void startServer() throws IOException {
        dummyServer = ServerBuilder.forPort(port)
                                   .addService(new DummyRaftGroupServiceGrpc())
                                   .build();
        dummyServer.start();
    }

    @AfterClass
    public static void stopServer() throws InterruptedException {
        if (dummyServer != null && !dummyServer.isShutdown()) {
            dummyServer.shutdownNow().awaitTermination();
        }
    }

    @Before
    public void setUp() {
        Channel channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
        testSubject = new RemoteRaftGroupService(RaftGroupServiceGrpc.newStub(channel));
    }

    @Test
    public void stepDown() {
        testSubject.stepDown(CONTEXT);
    }

    @Test
    public void addNodeToContext() throws InterruptedException, ExecutionException, TimeoutException {
        ContextUpdateConfirmation result = testSubject.addNodeToContext(CONTEXT, Node.getDefaultInstance()).get(TIMEOUT,
                                                                                                                TimeUnit.SECONDS);
    }

    @Test
    public void deleteNode() throws InterruptedException, ExecutionException, TimeoutException {
        ContextUpdateConfirmation result = testSubject.deleteNode(CONTEXT, NODE).get(TIMEOUT, TimeUnit.SECONDS);
    }

    @Test
    public void updateApplication() throws InterruptedException, ExecutionException, TimeoutException {
        Void result = testSubject.updateApplication(ContextApplication.getDefaultInstance()).get(TIMEOUT,
                                                                                                 TimeUnit.SECONDS);
    }

    @Test
    public void appendEntry() throws InterruptedException, ExecutionException, TimeoutException {
        Void result = testSubject.appendEntry(CONTEXT, "Entry", "Entry".getBytes()).get(TIMEOUT, TimeUnit.SECONDS);
        System.out.println(result);
    }

    @Test
    public void getStatus() throws InterruptedException, ExecutionException, TimeoutException {
        Void result = testSubject.getStatus(c -> System.out.println(c)).get(TIMEOUT, TimeUnit.SECONDS);
    }

    @Test
    public void initContext() throws InterruptedException, ExecutionException, TimeoutException {
        ContextConfiguration result = testSubject.initContext(CONTEXT,
                                                              Collections.singletonList(Node.getDefaultInstance())).get(
                TIMEOUT,
                TimeUnit.SECONDS);
    }

    @Test
    public void updateLoadBalancingStrategy() throws InterruptedException, ExecutionException, TimeoutException {
        Void result = testSubject.updateLoadBalancingStrategy(CONTEXT, LoadBalanceStrategy.getDefaultInstance()).get(
                TIMEOUT,
                TimeUnit.SECONDS);
    }

    @Test
    public void updateProcessorLoadBalancing() throws InterruptedException, ExecutionException, TimeoutException {
        Void result = testSubject.updateProcessorLoadBalancing(CONTEXT, ProcessorLBStrategy.getDefaultInstance()).get(
                TIMEOUT,
                TimeUnit.SECONDS);
    }

    @Test
    public void deleteLoadBalancingStrategy() throws InterruptedException, ExecutionException, TimeoutException {
        Void result = testSubject.deleteLoadBalancingStrategy(CONTEXT, LoadBalanceStrategy.getDefaultInstance()).get(
                TIMEOUT,
                TimeUnit.SECONDS);
    }

    @Test
    public void deleteContext() throws InterruptedException, ExecutionException, TimeoutException {
        Void result = testSubject.deleteContext(CONTEXT, false).get(TIMEOUT, TimeUnit.SECONDS);
    }

    @Test
    public void configuration() throws InterruptedException, ExecutionException, TimeoutException {
        ContextConfiguration result = testSubject.configuration(CONTEXT).get(TIMEOUT, TimeUnit.SECONDS);
    }

    @Test
    public void transferLeadership() throws InterruptedException, ExecutionException, TimeoutException {
        Void result = testSubject.transferLeadership(CONTEXT).get(TIMEOUT, TimeUnit.SECONDS);
    }

    @Test
    public void deleteApplication() throws InterruptedException, ExecutionException, TimeoutException {
        Void result = testSubject.deleteApplication(ContextApplication.getDefaultInstance()).get(TIMEOUT,
                                                                                                 TimeUnit.SECONDS);
    }

    @Test
    public void updateUser() throws InterruptedException, ExecutionException, TimeoutException {
        Void result = testSubject.updateUser(ContextUser.getDefaultInstance()).get(TIMEOUT, TimeUnit.SECONDS);
    }

    @Test
    public void deleteUser() throws InterruptedException, ExecutionException, TimeoutException {
        Void result = testSubject.deleteUser(ContextUser.getDefaultInstance()).get(TIMEOUT, TimeUnit.SECONDS);
    }

    @Test
    public void prepareDeleteNodeFromContext() throws InterruptedException, ExecutionException, TimeoutException {
        Void result = testSubject.prepareDeleteNodeFromContext(CONTEXT, NODE).get(TIMEOUT, TimeUnit.SECONDS);
    }

    private static class DummyRaftGroupServiceGrpc extends RaftGroupServiceGrpc.RaftGroupServiceImplBase {

        @Override
        public void initContext(Context request, StreamObserver<ContextConfiguration> responseObserver) {
            responseObserver.onNext(ContextConfiguration.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void deleteContext(DeleteContextRequest request, StreamObserver<InstructionAck> responseObserver) {
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void addServer(Context request, StreamObserver<ContextUpdateConfirmation> responseObserver) {
            responseObserver.onNext(ContextUpdateConfirmation.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void removeServer(Context request, StreamObserver<ContextUpdateConfirmation> responseObserver) {
            responseObserver.onNext(ContextUpdateConfirmation.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void mergeAppAuthorization(ContextApplication request, StreamObserver<InstructionAck> responseObserver) {
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void deleteAppAuthorization(ContextApplication request,
                                           StreamObserver<InstructionAck> responseObserver) {
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void mergeUserAuthorization(ContextUser request, StreamObserver<InstructionAck> responseObserver) {
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void deleteUserAuthorization(ContextUser request, StreamObserver<InstructionAck> responseObserver) {
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void mergeLoadBalanceStrategy(ContextLoadBalanceStrategy request,
                                             StreamObserver<InstructionAck> responseObserver) {
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void deleteLoadBalanceStrategy(ContextLoadBalanceStrategy request,
                                              StreamObserver<InstructionAck> responseObserver) {
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void mergeProcessorLBStrategy(ContextProcessorLBStrategy request,
                                             StreamObserver<InstructionAck> responseObserver) {
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void deleteProcessorLBStrategy(ContextProcessorLBStrategy request,
                                              StreamObserver<InstructionAck> responseObserver) {
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void getStatus(Context request, StreamObserver<Context> responseObserver) {
            responseObserver.onNext(Context.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void appendEntry(ContextEntry request, StreamObserver<InstructionAck> responseObserver) {
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void configuration(ContextName request, StreamObserver<ContextConfiguration> responseObserver) {
            responseObserver.onNext(ContextConfiguration.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void transferLeadership(ContextName request, StreamObserver<InstructionAck> responseObserver) {
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void preDeleteNodeFromContext(NodeContext request, StreamObserver<InstructionAck> responseObserver) {
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        }
    }
}