package io.axoniq.axonserver.enterprise.cluster.events;

import io.axoniq.axonserver.applicationevents.AxonServerEvent;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * @author Sara Pellegrini
 */
public class TestInternalEvent implements AxonServerEvent {

    private String string;
    private BigDecimal bigDecimal;
    private TestInternalEvent nestedObject;

    public TestInternalEvent(String string, BigDecimal bigDecimal,
                             TestInternalEvent nestedObject) {
        this.string = string;
        this.bigDecimal = bigDecimal;
        this.nestedObject = nestedObject;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TestInternalEvent that = (TestInternalEvent) o;
        return Objects.equals(string, that.string) &&
                Objects.equals(bigDecimal, that.bigDecimal) &&
                Objects.equals(nestedObject, that.nestedObject);
    }

    @Override
    public int hashCode() {
        return Objects.hash(string, bigDecimal, nestedObject);
    }
}
