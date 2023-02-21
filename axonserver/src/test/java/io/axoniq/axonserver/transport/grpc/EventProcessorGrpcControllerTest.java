package io.axoniq.axonserver.transport.grpc;

import io.axoniq.axonserver.AxonServerAccessController;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessor;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorAdminService;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorId;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorInstance;
import io.axoniq.axonserver.admin.eventprocessor.api.Result;
import io.axoniq.axonserver.api.Authentication;
import io.axoniq.axonserver.component.processor.balancing.LoadBalancingStrategy;
import io.axoniq.axonserver.config.GrpcContextAuthenticationProvider;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.admin.AdminActionResult;
import io.axoniq.axonserver.grpc.admin.EventProcessorIdentifier;
import io.axoniq.axonserver.grpc.admin.MoveSegment;
import io.axoniq.axonserver.test.FakeStreamObserver;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.assertj.core.util.Arrays;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EventProcessorGrpcControllerTest {

    public static final String CONNECTION_CONTEXT = "context";
    private EventProcessorGrpcController testSubject;
    private boolean result = true;
    private boolean accepted;

    private boolean error;

    private org.springframework.security.core.Authentication authentication = GrpcContextAuthenticationProvider.ADMIN_PRINCIPAL;

    private final Set<String> allowedContexts = new HashSet<>();


    @Before
    public void setUp() throws Exception {
        EventProcessorAdminService adminService = new EventProcessorAdminService() {
            @NotNull
            @Override
            public Flux<String> clientsBy(@NotNull EventProcessorId identifier,
                                          @NotNull Authentication authentication) {
                return Flux.fromArray(Arrays.array("client1", "client2"));
            }

            @NotNull
            @Override
            public Flux<EventProcessor> eventProcessorsByComponent(@NotNull String component,
                                                                   @NotNull Authentication authentication) {
                return Flux.just(eventProcessor());
            }

            @NotNull
            @Override
            public Flux<EventProcessor> eventProcessors(@NotNull Authentication authentication) {
                return Flux.just(eventProcessor());
            }

            @NotNull
            @Override
            public Mono<Result> pause(@NotNull EventProcessorId identifier, @NotNull Authentication authentication) {
                if (error) return Mono.error(new MessagingPlatformException(ErrorCode.EVENT_PROCESSOR_NOT_FOUND, "processor not found"));
                return Mono.just(result());
            }

            @NotNull
            @Override
            public Mono<Result> start(@NotNull EventProcessorId identifier, @NotNull Authentication authentication) {
                if (error) return Mono.error(new MessagingPlatformException(ErrorCode.EVENT_PROCESSOR_NOT_FOUND, "processor not found"));
                return Mono.just(result());
            }

            @NotNull
            @Override
            public Mono<Result> split(@NotNull EventProcessorId identifier, @NotNull Authentication authentication) {
                if (error) return Mono.error(new MessagingPlatformException(ErrorCode.EVENT_PROCESSOR_NOT_FOUND, "processor not found"));
                return Mono.just(result());
            }

            @NotNull
            @Override
            public Mono<Result> merge(@NotNull EventProcessorId identifier, @NotNull Authentication authentication) {
                if (error) return Mono.error(new MessagingPlatformException(ErrorCode.EVENT_PROCESSOR_NOT_FOUND, "processor not found"));
                return Mono.just(result());
            }

            @NotNull
            @Override
            public Mono<Result> move(@NotNull EventProcessorId identifier, int segment, @NotNull String target,
                                     @NotNull Authentication authentication) {
                if (error) return Mono.error(new MessagingPlatformException(ErrorCode.EVENT_PROCESSOR_NOT_FOUND, "processor not found"));
                return Mono.just(result());
            }

            @NotNull
            @Override
            public Mono<Void> loadBalance(@NotNull EventProcessorId identifier, @NotNull String strategy,
                                          @NotNull Authentication authentication) {
                if (error) return Mono.error(new MessagingPlatformException(ErrorCode.EVENT_PROCESSOR_NOT_FOUND, "processor not found"));
                return Mono.empty();
            }

            @NotNull
            @Override
            public Mono<Void> setAutoLoadBalanceStrategy(@NotNull EventProcessorId identifier, @NotNull String strategy,
                                                         @NotNull Authentication authentication) {
                return Mono.empty();
            }

            @NotNull
            @Override
            public Iterable<LoadBalancingStrategy> getBalancingStrategies(@NotNull Authentication authentication) {
                return Collections.emptySet();
            }
        };
        testSubject = new EventProcessorGrpcController(adminService, () -> authentication, () -> CONNECTION_CONTEXT,
                                                       accessController());
    }

    private AxonServerAccessController accessController() {
        return new AxonServerAccessController() {
            @Override
            public boolean allowed(String fullMethodName, String context, String token) {
                return false;
            }

            @Override
            public boolean allowed(String fullMethodName, String context,
                                   org.springframework.security.core.Authentication authentication) {
                return allowedContexts.contains(context);
            }

            @Override
            public org.springframework.security.core.Authentication authentication(String context, String token) {
                return null;
            }
        };
    }

    private Result result() {
        return new Result() {
            @Override
            public boolean isSuccess() {
                return result;
            }

            @Override
            public boolean isAccepted() {
                return accepted;
            }
        };
    }

    private EventProcessor eventProcessor() {
        EventProcessorId id = new EventProcessorId() {
            @NotNull
            @Override
            public String name() {
                return "name";
            }

            @NotNull
            @Override
            public String tokenStoreIdentifier() {
                return "tokenStoreIdentifier";
            }

            @NotNull
            @Override
            public String context() {
                return CONNECTION_CONTEXT;
            }
        };
        return new EventProcessor() {
            @NotNull
            @Override
            public EventProcessorId id() {
                return id;
            }

            @Override
            public boolean isStreaming() {
                return false;
            }

            @NotNull
            @Override
            public String mode() {
                return "Subscribing";
            }

            @NotNull
            @Override
            public Iterable<EventProcessorInstance> instances() {
                return Collections.emptyList();
            }

            @Nullable
            @Override
            public String loadBalancingStrategyName() {
                return null;
            }
        };
    }

    @Test
    public void pauseEventProcessor() {
        allowedContexts.add(CONNECTION_CONTEXT);
        FakeStreamObserver<AdminActionResult> responseObserver = new FakeStreamObserver<>();
        testSubject.pauseEventProcessor(eventProcessorIdentifier(), responseObserver);
        assertEquals(1, responseObserver.values().size());

        AdminActionResult result = responseObserver.values().get(0);
        assertEquals(io.axoniq.axonserver.grpc.admin.Result.SUCCESS, result.getResult());
        assertTrue(responseObserver.errors().isEmpty());
    }

    @Test
    public void pauseEventProcessorOtherContext() {
        allowedContexts.add("OTHER");
        FakeStreamObserver<AdminActionResult> responseObserver = new FakeStreamObserver<>();
        testSubject.pauseEventProcessor(eventProcessorIdentifier("OTHER"), responseObserver);
        assertEquals(1, responseObserver.values().size());

        AdminActionResult result = responseObserver.values().get(0);
        assertEquals(io.axoniq.axonserver.grpc.admin.Result.SUCCESS, result.getResult());
        assertTrue(responseObserver.errors().isEmpty());
    }

    @Test
    public void pauseEventProcessorOtherContextNoAccess() {
        allowedContexts.add(CONNECTION_CONTEXT);
        FakeStreamObserver<AdminActionResult> responseObserver = new FakeStreamObserver<>();
        testSubject.pauseEventProcessor(eventProcessorIdentifier("OTHER"), responseObserver);
        assertTrue(responseObserver.values().isEmpty());
        assertEquals(1, responseObserver.errors().size());

        Throwable error = responseObserver.errors().get(0);
        assertTrue(error instanceof StatusRuntimeException);
        assertEquals(Status.PERMISSION_DENIED.getCode(), Status.fromThrowable(error).getCode());
    }

    @Test
    public void pauseEventProcessorNoAccessControl() {
        authentication = GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL;
        FakeStreamObserver<AdminActionResult> responseObserver = new FakeStreamObserver<>();
        testSubject.pauseEventProcessor(eventProcessorIdentifier(), responseObserver);
        assertEquals(1, responseObserver.values().size());

        AdminActionResult result = responseObserver.values().get(0);
        assertEquals(io.axoniq.axonserver.grpc.admin.Result.SUCCESS, result.getResult());
        assertTrue(responseObserver.errors().isEmpty());
    }


    @Test
    public void startEventProcessor() {
        allowedContexts.add(CONNECTION_CONTEXT);
        FakeStreamObserver<AdminActionResult> responseObserver = new FakeStreamObserver<>();
        testSubject.startEventProcessor(eventProcessorIdentifier(), responseObserver);
        assertEquals(1, responseObserver.values().size());

        AdminActionResult result = responseObserver.values().get(0);
        assertEquals(io.axoniq.axonserver.grpc.admin.Result.SUCCESS, result.getResult());
        assertTrue(responseObserver.errors().isEmpty());
    }

    @Test
    public void startEventProcessorOtherContext() {
        allowedContexts.add("OTHER");
        FakeStreamObserver<AdminActionResult> responseObserver = new FakeStreamObserver<>();
        testSubject.startEventProcessor(eventProcessorIdentifier("OTHER"), responseObserver);
        assertEquals(1, responseObserver.values().size());

        AdminActionResult result = responseObserver.values().get(0);
        assertEquals(io.axoniq.axonserver.grpc.admin.Result.SUCCESS, result.getResult());
        assertTrue(responseObserver.errors().isEmpty());
    }

    @Test
    public void startEventProcessorOtherContextNoAccess() {
        allowedContexts.add(CONNECTION_CONTEXT);
        FakeStreamObserver<AdminActionResult> responseObserver = new FakeStreamObserver<>();
        testSubject.startEventProcessor(eventProcessorIdentifier("OTHER"), responseObserver);
        assertTrue(responseObserver.values().isEmpty());
        assertEquals(1, responseObserver.errors().size());

        Throwable error = responseObserver.errors().get(0);
        assertTrue(error instanceof StatusRuntimeException);
        assertEquals(Status.PERMISSION_DENIED.getCode(), Status.fromThrowable(error).getCode());
    }

    @Test
    public void startEventProcessorNoAccessControl() {
        authentication = GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL;
        FakeStreamObserver<AdminActionResult> responseObserver = new FakeStreamObserver<>();
        testSubject.startEventProcessor(eventProcessorIdentifier(), responseObserver);
        assertEquals(1, responseObserver.values().size());

        AdminActionResult result = responseObserver.values().get(0);
        assertEquals(io.axoniq.axonserver.grpc.admin.Result.SUCCESS, result.getResult());
        assertTrue(responseObserver.errors().isEmpty());
    }

    @Test
    public void splitEventProcessor() {
        allowedContexts.add(CONNECTION_CONTEXT);
        FakeStreamObserver<AdminActionResult> responseObserver = new FakeStreamObserver<>();
        testSubject.splitEventProcessor(eventProcessorIdentifier(), responseObserver);
        assertEquals(1, responseObserver.values().size());

        AdminActionResult result = responseObserver.values().get(0);
        assertEquals(io.axoniq.axonserver.grpc.admin.Result.SUCCESS, result.getResult());
        assertTrue(responseObserver.errors().isEmpty());
    }

    @Test
    public void splitEventProcessorOtherContext() {
        allowedContexts.add("OTHER");
        FakeStreamObserver<AdminActionResult> responseObserver = new FakeStreamObserver<>();
        testSubject.splitEventProcessor(eventProcessorIdentifier("OTHER"), responseObserver);
        assertEquals(1, responseObserver.values().size());

        AdminActionResult result = responseObserver.values().get(0);
        assertEquals(io.axoniq.axonserver.grpc.admin.Result.SUCCESS, result.getResult());
        assertTrue(responseObserver.errors().isEmpty());
    }

    @Test
    public void splitEventProcessorOtherContextNoAccess() {
        allowedContexts.add(CONNECTION_CONTEXT);
        FakeStreamObserver<AdminActionResult> responseObserver = new FakeStreamObserver<>();
        testSubject.splitEventProcessor(eventProcessorIdentifier("OTHER"), responseObserver);
        assertTrue(responseObserver.values().isEmpty());
        assertEquals(1, responseObserver.errors().size());

        Throwable error = responseObserver.errors().get(0);
        assertTrue(error instanceof StatusRuntimeException);
        assertEquals(Status.PERMISSION_DENIED.getCode(), Status.fromThrowable(error).getCode());
    }

    @Test
    public void splitEventProcessorNoAccessControl() {
        authentication = GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL;
        FakeStreamObserver<AdminActionResult> responseObserver = new FakeStreamObserver<>();
        testSubject.splitEventProcessor(eventProcessorIdentifier(), responseObserver);
        assertEquals(1, responseObserver.values().size());

        AdminActionResult result = responseObserver.values().get(0);
        assertEquals(io.axoniq.axonserver.grpc.admin.Result.SUCCESS, result.getResult());
        assertTrue(responseObserver.errors().isEmpty());
    }


    @Test
    public void mergeEventProcessor() {
        allowedContexts.add(CONNECTION_CONTEXT);
        FakeStreamObserver<AdminActionResult> responseObserver = new FakeStreamObserver<>();
        testSubject.mergeEventProcessor(eventProcessorIdentifier(), responseObserver);
        assertEquals(1, responseObserver.values().size());

        AdminActionResult result = responseObserver.values().get(0);
        assertEquals(io.axoniq.axonserver.grpc.admin.Result.SUCCESS, result.getResult());
        assertTrue(responseObserver.errors().isEmpty());
    }

    @Test
    public void mergeEventProcessorOtherContext() {
        allowedContexts.add("OTHER");
        FakeStreamObserver<AdminActionResult> responseObserver = new FakeStreamObserver<>();
        testSubject.mergeEventProcessor(eventProcessorIdentifier("OTHER"), responseObserver);
        assertEquals(1, responseObserver.values().size());

        AdminActionResult result = responseObserver.values().get(0);
        assertEquals(io.axoniq.axonserver.grpc.admin.Result.SUCCESS, result.getResult());
        assertTrue(responseObserver.errors().isEmpty());
    }

    @Test
    public void mergeEventProcessorOtherContextNoAccess() {
        allowedContexts.add(CONNECTION_CONTEXT);
        FakeStreamObserver<AdminActionResult> responseObserver = new FakeStreamObserver<>();
        testSubject.mergeEventProcessor(eventProcessorIdentifier("OTHER"), responseObserver);
        assertTrue(responseObserver.values().isEmpty());
        assertEquals(1, responseObserver.errors().size());

        Throwable error = responseObserver.errors().get(0);
        assertTrue(error instanceof StatusRuntimeException);
        assertEquals(Status.PERMISSION_DENIED.getCode(), Status.fromThrowable(error).getCode());
    }

    @Test
    public void mergeEventProcessorNoAccessControl() {
        authentication = GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL;
        FakeStreamObserver<AdminActionResult> responseObserver = new FakeStreamObserver<>();
        testSubject.mergeEventProcessor(eventProcessorIdentifier(), responseObserver);
        assertEquals(1, responseObserver.values().size());

        AdminActionResult result = responseObserver.values().get(0);
        assertEquals(io.axoniq.axonserver.grpc.admin.Result.SUCCESS, result.getResult());
        assertTrue(responseObserver.errors().isEmpty());
    }

    @Test
    public void moveEventProcessorSegment() {
        allowedContexts.add(CONNECTION_CONTEXT);
        FakeStreamObserver<AdminActionResult> responseObserver = new FakeStreamObserver<>();
        testSubject.moveEventProcessorSegment(move(eventProcessorIdentifier()), responseObserver);
        assertEquals(1, responseObserver.values().size());

        AdminActionResult result = responseObserver.values().get(0);
        assertEquals(io.axoniq.axonserver.grpc.admin.Result.SUCCESS, result.getResult());
        assertTrue(responseObserver.errors().isEmpty());
    }

    @Test
    public void moveEventProcessorOtherContext() {
        allowedContexts.add("OTHER");
        FakeStreamObserver<AdminActionResult> responseObserver = new FakeStreamObserver<>();
        testSubject.moveEventProcessorSegment(move(eventProcessorIdentifier("OTHER")), responseObserver);
        assertEquals(1, responseObserver.values().size());

        AdminActionResult result = responseObserver.values().get(0);
        assertEquals(io.axoniq.axonserver.grpc.admin.Result.SUCCESS, result.getResult());
        assertTrue(responseObserver.errors().isEmpty());
    }

    @Test
    public void moveEventProcessorOtherContextNoAccess() {
        allowedContexts.add(CONNECTION_CONTEXT);
        FakeStreamObserver<AdminActionResult> responseObserver = new FakeStreamObserver<>();
        testSubject.moveEventProcessorSegment(move(eventProcessorIdentifier("OTHER")), responseObserver);
        assertTrue(responseObserver.values().isEmpty());
        assertEquals(1, responseObserver.errors().size());

        Throwable error = responseObserver.errors().get(0);
        assertTrue(error instanceof StatusRuntimeException);
        assertEquals(Status.PERMISSION_DENIED.getCode(), Status.fromThrowable(error).getCode());
    }

    @Test
    public void moveEventProcessorNoAccessControl() {
        authentication = GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL;
        FakeStreamObserver<AdminActionResult> responseObserver = new FakeStreamObserver<>();
        testSubject.moveEventProcessorSegment(move(eventProcessorIdentifier()), responseObserver);
        assertEquals(1, responseObserver.values().size());

        AdminActionResult result = responseObserver.values().get(0);
        assertEquals(io.axoniq.axonserver.grpc.admin.Result.SUCCESS, result.getResult());
        assertTrue(responseObserver.errors().isEmpty());
    }

    @NotNull
    private EventProcessorIdentifier eventProcessorIdentifier() {
        return EventProcessorIdentifier.getDefaultInstance();
    }
    @NotNull
    private EventProcessorIdentifier eventProcessorIdentifier(String context) {
        return EventProcessorIdentifier.newBuilder().setContextName(context).build();
    }

    private MoveSegment move(EventProcessorIdentifier ev) {
        return MoveSegment.newBuilder().setEventProcessor(ev).build();
    }


}