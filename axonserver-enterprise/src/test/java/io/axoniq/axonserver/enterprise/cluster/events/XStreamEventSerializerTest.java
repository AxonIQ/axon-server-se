package io.axoniq.axonserver.enterprise.cluster.events;

import io.axoniq.axonserver.enterprise.cluster.events.serializer.XStreamEventSerializer;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import org.junit.*;

import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.TEN;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link XStreamEventSerializer}.
 *
 * @author Sara Pellegrini
 */
public class XStreamEventSerializerTest {

    private XStreamEventSerializer testSubject = new XStreamEventSerializer();

    @Test
    public void serialize() {
        TestInternalEvent nestedObject = new TestInternalEvent("nested string", TEN, null);
        TestInternalEvent object = new TestInternalEvent("a string", ONE, nestedObject);

        ConnectorCommand serializedObject = testSubject.serialize(object);
        Object deserializedObject = testSubject.deserialize(serializedObject);
        assertEquals(object, deserializedObject);
    }
}