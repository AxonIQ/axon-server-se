package io.axoniq.axonserver.cluster.coordinator;

import io.axoniq.axonhub.internal.grpc.ConnectorCommand;
import io.axoniq.axonhub.internal.grpc.NodeContext;
import org.junit.*;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * Created by Sara Pellegrini on 27/08/2018.
 * sara.pellegrini@gmail.com
 */
public class ConfirmationSourceTest {


    @Test
    public void testConfirmCandidate() {
        ConfirmationSource testSubject = new ConfirmationSource(() -> "SSS", command -> {}, context -> null);
        AtomicBoolean result = new AtomicBoolean();
        NodeContext request = NodeContext.newBuilder().setContext("context").setNodeName("AAA").build();
        testSubject.on(new RequestToBeCoordinatorReceived(request, result::set));
        assertTrue(result.get());
    }

    @Test
    public void testRejectCandidate() {
        ConfirmationSource testSubject = new ConfirmationSource(() -> "SSS", command -> {}, context -> null);
        AtomicBoolean result = new AtomicBoolean();
        NodeContext request = NodeContext.newBuilder().setContext("context").setNodeName("ZZZ").build();
        testSubject.on(new RequestToBeCoordinatorReceived(request, result::set));
        assertFalse(result.get());
    }

    @Test
    public void testCoordinatorAlreadyAppointed() {
        AtomicReference<ConnectorCommand> connectorCommand = new AtomicReference<>();
        AtomicBoolean result = new AtomicBoolean();
        ConfirmationSource testSubject = new ConfirmationSource(() -> "SSS", connectorCommand::set, context -> "CCC");
        NodeContext request = NodeContext.newBuilder().setContext("context").setNodeName("AAA").build();
        testSubject.on(new RequestToBeCoordinatorReceived(request, result::set));
        assertFalse(result.get());
        NodeContext confirmation = connectorCommand.get().getCoordinatorConfirmation();
        assertEquals("CCC", confirmation.getNodeName());
    }
}