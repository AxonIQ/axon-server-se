package io.axoniq.axonserver.saas;

import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.axonserver.grpc.internal.ApplicationContextRole;
import io.axoniq.axonserver.grpc.internal.Context;
import io.axoniq.axonserver.grpc.internal.saas.SaasAdminServiceGrpc;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Marc Gathier
 */
public class SaasAdminServiceITCase {

    private SaasAdminServiceGrpc.SaasAdminServiceStub testSubject;

    @Before
    public void setUp() throws Exception {
        System.out.printf("test.internal.grpc.port=%s%n", System.getProperty("test.internal.grpc.port"));
        int port = Integer.parseInt(System.getProperty("test.internal.grpc.port", "8224"));
        Channel channel = ManagedChannelBuilder.forAddress("localhost", port)
                                               .usePlaintext()
                                               .build();
        testSubject = SaasAdminServiceGrpc.newStub(channel);
    }

    @Test
    public void createContext() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<InstructionAck> completableFuture = new CompletableFuture<>();
        Context request = Context.newBuilder()
                                 .setName("demo2")
                                 .putMetaData("nodes", "1")
                                 .putMetaData("organizationId", "MyOrganisation")
                                 .build();
        testSubject.createContext(request, new FutureStreamObserver<>(completableFuture));
        completableFuture.get(5, TimeUnit.MINUTES);
    }

    @Test
    public void createApplication() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Application> completableFuture = new CompletableFuture<>();
        Application request = Application.newBuilder()
                                         .setName("demoApp")
                                         .putMetaData("organizationId", "MyOrganisation")
                                         .addRolesPerContext(ApplicationContextRole.newBuilder()
                                                                                   .setContext("demo2")
                                                                                   .addRoles("USE_CONTEXT")
                                                                                   .build())
                                         .build();
        testSubject.createApplication(request, new FutureStreamObserver<>(completableFuture));
        completableFuture.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void deleteApplication() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<InstructionAck> completableFuture = new CompletableFuture<>();
        testSubject.deleteApplication(Application.newBuilder().setName("demoApp").build(),
                                      new FutureStreamObserver<>(completableFuture));
        completableFuture.get(5, TimeUnit.SECONDS);
    }

    private static class FutureStreamObserver<T> implements StreamObserver<T> {

        private final CompletableFuture<T> completableFuture;

        public FutureStreamObserver(CompletableFuture<T> completableFuture) {
            this.completableFuture = completableFuture;
        }

        @Override
        public void onNext(T instructionAck) {
            completableFuture.complete(instructionAck);
        }

        @Override
        public void onError(Throwable throwable) {
            completableFuture.completeExceptionally(throwable);
        }

        @Override
        public void onCompleted() {

        }
    }
}