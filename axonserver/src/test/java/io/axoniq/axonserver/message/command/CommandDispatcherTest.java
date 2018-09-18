package io.axoniq.axonserver.message.command;

import com.google.common.collect.Sets;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.grpc.command.CommandSubscription;
import io.axoniq.axonserver.ProcessingInstructionHelper;
import io.axoniq.axonserver.SubscriptionEvents;
import io.axoniq.axonserver.TopologyEvents;
import io.axoniq.axonserver.grpc.command.CommandProviderInbound;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.CountingStreamObserver;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Author: marc
 */
@RunWith(MockitoJUnitRunner.class)
public class CommandDispatcherTest {
    private CommandDispatcher commandDispatcher;
    private CommandMetricsRegistry metricsRegistry;
    @Mock
    private CommandCache commandCache;
    @Mock
    private CommandRegistrationCache registrations;

    @Before
    public void setup() {
        metricsRegistry = new CommandMetricsRegistry(new SimpleMeterRegistry(),
                                                     new DefaultMetricCollector());
        commandDispatcher = new CommandDispatcher(registrations, commandCache, metricsRegistry);
        ConcurrentMap<CommandHandler, Set<CommandRegistrationCache.RegistrationEntry>> dummyRegistrations = new ConcurrentHashMap<>();
        Set<CommandRegistrationCache.RegistrationEntry> commands =
                Sets.newHashSet(new CommandRegistrationCache.RegistrationEntry(Topology.DEFAULT_CONTEXT, "Command"));
        dummyRegistrations.put(new DirectCommandHandler(new CountingStreamObserver<>(), "client", "component"),
                commands);
        when( registrations.getAll()).thenReturn(dummyRegistrations);
    }

    @Test
    public void registerCommandHandler()  {
        CountingStreamObserver<CommandProviderInbound> countingStreamObserver = new CountingStreamObserver<>();
        CommandHandler commandHandler = new DirectCommandHandler(countingStreamObserver, "client", "component");
        CommandSubscription subscribeRequest = CommandSubscription.newBuilder().setCommand("command").setClientName("client").setMessageId("1234")
                .build();

        commandDispatcher.on(new SubscriptionEvents.SubscribeCommand(Topology.DEFAULT_CONTEXT, subscribeRequest, commandHandler));

        assertEquals(1, countingStreamObserver.count);
        assertEquals("1234", countingStreamObserver.responseList.get(0).getConfirmation().getMessageId());

        CommandSubscription unsubscribeRequest = CommandSubscription.newBuilder().setCommand("command").setClientName("client").setMessageId("1235")
                .build();
        when( registrations.remove(any(), any(), any())).thenReturn(commandHandler);
        commandDispatcher.on(new SubscriptionEvents.UnsubscribeCommand(Topology.DEFAULT_CONTEXT, unsubscribeRequest, false));

        assertEquals(2, countingStreamObserver.count);
        assertEquals("1235", countingStreamObserver.responseList.get(1).getConfirmation().getMessageId());
    }

    @Test
    public void unregisterCommandHandler()  {
        when(registrations.getCommandsFor(anyObject())).thenReturn(Sets.newHashSet(new CommandRegistrationCache.RegistrationEntry(Topology.DEFAULT_CONTEXT, "One")));
        commandDispatcher.on(new TopologyEvents.ApplicationDisconnected(null, null, "client"));
    }

    @Test
    public void dispatch()  {
        CountingStreamObserver<CommandResponse> responseObserver = new CountingStreamObserver<>();
        Command request = Command.newBuilder()
                .addProcessingInstructions(ProcessingInstructionHelper.routingKey("1234"))
                .setName("Command")
                .setMessageIdentifier("12")
                .build();
        CountingStreamObserver<CommandProviderInbound> commandProviderInbound = new CountingStreamObserver<>();
        DirectCommandHandler result = new DirectCommandHandler(commandProviderInbound, "client", "component");
        when(registrations.getNode(eq(Topology.DEFAULT_CONTEXT), anyObject(), anyObject())).thenReturn(result);

        commandDispatcher.dispatch(Topology.DEFAULT_CONTEXT, request, response -> {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }, false);
        assertEquals(1, commandDispatcher.getCommandQueues().getSegments().get("client").size());
        assertEquals(0, responseObserver.count);
        Mockito.verify(commandCache, times(1)).put(eq("12"), anyObject());

    }
    @Test
    public void dispatchNotFound() {
        CountingStreamObserver<CommandResponse> responseObserver = new CountingStreamObserver<>();
        Command request = Command.newBuilder()
                .addProcessingInstructions(ProcessingInstructionHelper.routingKey("1234"))
                .setName("Command")
                .setMessageIdentifier("12")
                .build();
        when(registrations.getNode(any(), anyObject(), anyObject())).thenReturn(null);

        commandDispatcher.dispatch(Topology.DEFAULT_CONTEXT, request, response -> {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }, false);
        assertEquals(1, responseObserver.count);
        assertNotEquals("", responseObserver.responseList.get(0).getErrorCode());
        Mockito.verify(commandCache, times(0)).put(eq("12"), anyObject());

    }


    @Test
    public void subscribe() {
        CountingStreamObserver<CommandProviderInbound> countingStreamObserver = new CountingStreamObserver<>();
        CommandSubscription subscribeRequest = CommandSubscription.newBuilder().setCommand("command").setClientName("client").setMessageId("1236")
                .build();
        CommandHandler handler = new DirectCommandHandler(countingStreamObserver, "client", "component");
        commandDispatcher.on(new SubscriptionEvents.SubscribeCommand(Topology.DEFAULT_CONTEXT, subscribeRequest, handler));
        assertEquals(1, countingStreamObserver.count);
        assertEquals("1236", countingStreamObserver.responseList.get(0).getConfirmation().getMessageId());

        Mockito.verify(registrations, Mockito.times(1)).add(eq(Topology.DEFAULT_CONTEXT), eq("command"), anyObject());
    }

    @Test
    public void unsubscribe() {
        CountingStreamObserver<CommandProviderInbound> countingStreamObserver = new CountingStreamObserver<>();
        CommandSubscription unsubscribeRequest = CommandSubscription.newBuilder().setCommand("command").setClientName("client").setMessageId("1235")
                .build();
        when(registrations.remove(any(), any(), any())).thenReturn(new DirectCommandHandler(countingStreamObserver, "client", "component"));
        commandDispatcher.on(new SubscriptionEvents.UnsubscribeCommand(Topology.DEFAULT_CONTEXT, unsubscribeRequest, false));
        assertEquals(1, countingStreamObserver.count);
        assertEquals("1235", countingStreamObserver.responseList.get(0).getConfirmation().getMessageId());

        Mockito.verify(registrations, Mockito.times(1)).remove(eq(Topology.DEFAULT_CONTEXT), eq("command"), anyObject());
    }

    @Test
    public void dispatchProxied() throws Exception {
        CountingStreamObserver<CommandResponse> responseObserver = new CountingStreamObserver<>();
        Command request = Command.newBuilder()
                .addProcessingInstructions(ProcessingInstructionHelper.targetClient("client"))
                .setName("Command")
                .setMessageIdentifier("12")
                .build();
        CountingStreamObserver<CommandProviderInbound> commandProviderInbound = new CountingStreamObserver<>();
        DirectCommandHandler result = new DirectCommandHandler(commandProviderInbound, "client", "component");
        when(registrations.findByClientAndCommand(eq("client"), anyObject())).thenReturn(result);
        String context = ProcessingInstructionHelper.context(request.getProcessingInstructionsList());

        commandDispatcher.dispatch(context, request, responseObserver::onNext, true);
        assertEquals(1, commandDispatcher.getCommandQueues().getSegments().get("client").size());
        assertEquals("12", commandDispatcher.getCommandQueues().take("client").command().getMessageIdentifier());
        assertEquals(0, responseObserver.count);
        Mockito.verify(commandCache, times(1)).put(eq("12"), anyObject());
    }

    @Test
    public void dispatchProxiedClientNotFound()  {
        CountingStreamObserver<CommandResponse> responseObserver = new CountingStreamObserver<>();
        Command request = Command.newBuilder()
                .addProcessingInstructions(ProcessingInstructionHelper.routingKey("1234"))
                .setName("Command")
                .setMessageIdentifier("12")
                .build();
        when(registrations.findByClientAndCommand(eq("1234"), anyObject())).thenReturn(null);

        commandDispatcher.dispatch(null, request, responseObserver::onNext, true);
        assertEquals(1, responseObserver.count);
        Mockito.verify(commandCache, times(0)).put(eq("12"), anyObject());
    }

    @Test
    public void handleResponse() {
        AtomicBoolean responseHandled = new AtomicBoolean(false);
        CommandInformation commandInformation = new CommandInformation("TheCommand", (r) -> responseHandled.set(true),
                                                                       "Client", "Component");
        when(commandCache.remove(any(String.class))).thenReturn(commandInformation);

        commandDispatcher.handleResponse(CommandResponse.newBuilder().build(), false);
        assertTrue(responseHandled.get());
        assertEquals(1, metricsRegistry.commandMetric("TheCommand", "Client", "Component").getCount());

    }
}