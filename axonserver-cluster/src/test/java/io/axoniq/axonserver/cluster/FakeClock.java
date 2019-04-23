package io.axoniq.axonserver.cluster;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

/**
 * @author Marc Gathier
 */
public class FakeClock extends Clock {
    private Instant instant;

    public FakeClock(Instant instant) {
        this.instant = instant;
    }
    @Override
    public ZoneId getZone() {
        return null;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return null;
    }

    @Override
    public Instant instant() {
        return instant;
    }

    public void plusMillis(long millis) {
        instant = instant.plusMillis(millis);
        
    }
}
