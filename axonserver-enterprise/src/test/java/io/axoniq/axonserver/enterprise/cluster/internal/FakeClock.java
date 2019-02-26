package io.axoniq.axonserver.enterprise.cluster.internal;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

/**
 * @author Marc Gathier
 */
public class FakeClock extends Clock {
    private long timeInMillis = System.currentTimeMillis();
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
        return Instant.ofEpochMilli(timeInMillis);
    }

    public void add(long time, TimeUnit timeUnit) {
        timeInMillis += timeUnit.toMillis(time);
    }
}
