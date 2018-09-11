package io.axoniq.axonserver.grpc;

import io.axoniq.axonhub.Command;
import io.axoniq.axonhub.CommandResponse;
import io.axoniq.axonhub.CommandSubscription;
import io.axoniq.axonhub.grpc.CommandProviderInbound;
import io.axoniq.axonhub.grpc.CommandProviderOutbound;
import io.axoniq.axonhub.grpc.FlowControl;
import io.axoniq.axonserver.DispatchEvents;
import io.axoniq.axonserver.SubscriptionEvents;
import io.axoniq.axonserver.message.FlowControlQueues;
import io.axoniq.axonserver.message.command.CommandDispatcher;
import io.axoniq.axonserver.message.command.WrappedCommand;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.CountingStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.*;
import org.springframework.context.ApplicationEventPublisher;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Author: marc
 */
public class CommandServiceTest {
    private CommandService testSubject;
    private FlowControlQueues<WrappedCommand> commandQueue;
    private ApplicationEventPublisher eventPublisher;
    private CommandDispatcher commandDispatcher;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        commandDispatcher = mock(CommandDispatcher.class);
        commandQueue = new FlowControlQueues<>();
        eventPublisher = mock(ApplicationEventPublisher.class);

        when(commandDispatcher.getCommandQueues()).thenReturn(commandQueue);
        //when(commandDispatcher.redispatch(any(WrappedCommand.class))).thenReturn("test");
        testSubject = new CommandService(commandDispatcher, () -> Topology.DEFAULT_CONTEXT, eventPublisher);
    }

    @Test
    public void flowControl() throws Exception {
        CountingStreamObserver<CommandProviderInbound> countingStreamObserver  = new CountingStreamObserver<>();
        StreamObserver<CommandProviderOutbound> requestStream = testSubject.openStream(countingStreamObserver);
        requestStream.onNext(CommandProviderOutbound.newBuilder().setFlowControl(FlowControl.newBuilder().setPermits(1).setClientName("name").build()).build());
        Thread.sleep(150);
        assertEquals(1, commandQueue.getSegments().size());
        commandQueue.put("name", new WrappedCommand(Topology.DEFAULT_CONTEXT, Command.newBuilder().build()));
        Thread.sleep(50);
        assertEquals(1, countingStreamObserver.count);
    }

    @Test
    public void subscribe() {
        StreamObserver<CommandProviderOutbound> requestStream = testSubject.openStream(new CountingStreamObserver<>());
        requestStream.onNext(CommandProviderOutbound.newBuilder()
                .setSubscribe(CommandSubscription.newBuilder().setClientName("name").setComponentName("component").setCommand("command"))
                .build());
        verify(eventPublisher).publishEvent(isA(SubscriptionEvents.SubscribeCommand.class));
    }
    @Test
    public void unsubscribe() {
        StreamObserver<CommandProviderOutbound> requestStream = testSubject.openStream(new CountingStreamObserver<>());
        requestStream.onNext(CommandProviderOutbound.newBuilder()
                .setUnsubscribe(CommandSubscription.newBuilder().setClientName("name").setComponentName("component").setCommand("command"))
                .build());
        verify(eventPublisher, times(0)).publishEvent(isA(SubscriptionEvents.UnsubscribeCommand.class));
    }
    @Test
    public void unsubscribeAfterSubscribe() {
        StreamObserver<CommandProviderOutbound> requestStream = testSubject.openStream(new CountingStreamObserver<>());
        requestStream.onNext(CommandProviderOutbound.newBuilder()
                .setSubscribe(CommandSubscription.newBuilder().setClientName("name").setComponentName("component").setCommand("command"))
                .build());
        requestStream.onNext(CommandProviderOutbound.newBuilder()
                .setUnsubscribe(CommandSubscription.newBuilder().setClientName("name").setComponentName("component").setCommand("command"))
                .build());
        verify(eventPublisher).publishEvent(isA(SubscriptionEvents.UnsubscribeCommand.class));
    }

    @Test
    public void cancelAfterSubscribe() {
        StreamObserver<CommandProviderOutbound> requestStream = testSubject.openStream(new CountingStreamObserver<>());
        requestStream.onNext(CommandProviderOutbound.newBuilder()
                .setSubscribe(CommandSubscription.newBuilder().setClientName("name").setComponentName("component").setCommand("command"))
                .build());
        requestStream.onError(new RuntimeException("failed"));
    }

    @Test
    public void cancelBeforeSubscribe() {
        StreamObserver<CommandProviderOutbound> requestStream = testSubject.openStream(new CountingStreamObserver<>());
        requestStream.onError(new RuntimeException("failed"));
    }

    @Test
    public void close() {
        StreamObserver<CommandProviderOutbound> requestStream = testSubject.openStream(new CountingStreamObserver<>());
        requestStream.onNext(CommandProviderOutbound.newBuilder().setFlowControl(FlowControl.newBuilder().setPermits(1).setClientName("name").build()).build());
        requestStream.onCompleted();
    }

    @Test
    public void dispatch() {
        doAnswer(invocationOnMock -> {
            DispatchEvents.DispatchCommand dispatchCommand = (DispatchEvents.DispatchCommand) invocationOnMock.getArguments()[0];
            dispatchCommand.getResponseObserver().accept(CommandResponse.newBuilder().build());
            return null;
        }).when(commandDispatcher).on(isA(DispatchEvents.DispatchCommand.class));
        CountingStreamObserver<CommandResponse> responseObserver = new CountingStreamObserver<>();
        testSubject.dispatch(Command.newBuilder().build(), responseObserver);
        assertEquals(1, responseObserver.count);
    }

}