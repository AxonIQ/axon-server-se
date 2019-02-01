package io.axoniq.axonserver.migration;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.commons.io.FileUtils;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author Marc Gathier
 */
@Component
public class MetricReporter {

    private final MeterRegistry metricRegistry;
//    private final ConsoleReporter consoleReporter;
//    private final CsvReporter csvReporter;

    public MetricReporter(MeterRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        File dir = new File("metrics_reports");
        try {
            FileUtils.deleteDirectory(dir);
        } catch (IOException e) {
            throw new RuntimeException("couldn't delete current metrics_reports directory", e);
        }
        dir.mkdirs();

//        csvReporter = CsvReporter.forRegistry(metricRegistry)
//                .scheduleOn(Executors.newSingleThreadScheduledExecutor())
//                .formatFor(Locale.US)
//                .convertRatesTo(TimeUnit.SECONDS)
//                .convertDurationsTo(TimeUnit.MILLISECONDS)
//                .build(dir);
//
//        consoleReporter = ConsoleReporter.forRegistry(metricRegistry)
//                .scheduleOn(Executors.newSingleThreadScheduledExecutor())
//                .convertRatesTo(TimeUnit.SECONDS)
//                .convertDurationsTo(TimeUnit.MILLISECONDS)
//                .build();
    }

//    public MetricRegistry getMetricRegistry() {
//        return metricRegistry;
//    }

    public void beginReporting() {
//        csvReporter.start(10, 10, TimeUnit.SECONDS);
    }

    public void endReporting() {
//        csvReporter.stop();
//        csvReporter.report();
//        consoleReporter.report();
    }

}
