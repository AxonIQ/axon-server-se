package io.axoniq.axonserver.enterprise.replication.group;

import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.NodeReplicationGroup;
import io.axoniq.axonserver.grpc.internal.RaftGroupServiceGrpc;
import io.axoniq.axonserver.grpc.internal.ReplicationGroup;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupConfiguration;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupEntry;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupName;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupUpdateConfirmation;
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
    public void addServer() throws InterruptedException, ExecutionException, TimeoutException {
        ReplicationGroupUpdateConfirmation result = testSubject.addServer(CONTEXT,
                                                                          Node.getDefaultInstance())
                                                               .get(TIMEOUT,
                                                                    TimeUnit.SECONDS);
    }

    @Test
    public void deleteServer() throws InterruptedException, ExecutionException, TimeoutException {
        ReplicationGroupUpdateConfirmation result = testSubject.deleteServer(CONTEXT, NODE).get(TIMEOUT,
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
    public void initReplicationGroup() throws InterruptedException, ExecutionException, TimeoutException {
        ReplicationGroupConfiguration result = testSubject.initReplicationGroup(CONTEXT,
                                                                                Collections
                                                                                        .singletonList(Node.getDefaultInstance()))
                                                          .get(
                                                                  TIMEOUT,
                                                                  TimeUnit.SECONDS);
    }

    @Test
    public void configuration() throws InterruptedException, ExecutionException, TimeoutException {
        ReplicationGroupConfiguration result = testSubject.configuration(CONTEXT).get(TIMEOUT, TimeUnit.SECONDS);
    }

    @Test
    public void transferLeadership() throws InterruptedException, ExecutionException, TimeoutException {
        Void result = testSubject.transferLeadership(CONTEXT).get(TIMEOUT, TimeUnit.SECONDS);
    }

    @Test
    public void prepareDeleteNodeFromReplicationGroup()
            throws InterruptedException, ExecutionException, TimeoutException {
        Void result = testSubject.prepareDeleteNodeFromReplicationGroup(CONTEXT, NODE).get(TIMEOUT, TimeUnit.SECONDS);
    }

    private static class DummyRaftGroupServiceGrpc extends RaftGroupServiceGrpc.RaftGroupServiceImplBase {

        @Override
        public void initReplicationGroup(ReplicationGroup request,
                                         StreamObserver<ReplicationGroupConfiguration> responseObserver) {
            responseObserver.onNext(ReplicationGroupConfiguration.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void addServer(ReplicationGroup request,
                              StreamObserver<ReplicationGroupUpdateConfirmation> responseObserver) {
            responseObserver.onNext(ReplicationGroupUpdateConfirmation.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void removeServer(ReplicationGroup request,
                                 StreamObserver<ReplicationGroupUpdateConfirmation> responseObserver) {
            responseObserver.onNext(ReplicationGroupUpdateConfirmation.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void getStatus(ReplicationGroup request, StreamObserver<ReplicationGroup> responseObserver) {
            responseObserver.onNext(ReplicationGroup.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void appendEntry(ReplicationGroupEntry request, StreamObserver<InstructionAck> responseObserver) {
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void configuration(ReplicationGroupName request,
                                  StreamObserver<ReplicationGroupConfiguration> responseObserver) {
            responseObserver.onNext(ReplicationGroupConfiguration.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void transferLeadership(ReplicationGroupName request, StreamObserver<InstructionAck> responseObserver) {
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void preDeleteNodeFromReplicationGroup(NodeReplicationGroup request,
                                                      StreamObserver<InstructionAck> responseObserver) {
            responseObserver.onNext(InstructionAck.newBuilder().build());
            responseObserver.onCompleted();
        }
    }
}