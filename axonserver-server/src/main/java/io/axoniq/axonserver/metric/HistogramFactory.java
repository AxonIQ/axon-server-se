package io.axoniq.axonserver.metric;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Created by Sara Pellegrini on 12/04/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class HistogramFactory implements MetricRegistry.MetricSupplier<Histogram> {

    private final int minutes;

    public HistogramFactory(@Value("${axoniq.axonserver.metrics-interval}") int minutes) {
        this.minutes = minutes;
    }

    @Override
    public Histogram newMetric() {
        return new Histogram( new SlidingTimeWindowArrayReservoir(minutes, TimeUnit.MINUTES));
    }
}
