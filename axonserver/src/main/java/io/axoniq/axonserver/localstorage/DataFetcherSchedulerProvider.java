package io.axoniq.axonserver.localstorage;

import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * @author Stefan Dragisic
 * @author Sara Pellegrini
 * @since 4.5.3
 */

public class DataFetcherSchedulerProvider implements Supplier<ExecutorService> {

    private static ExecutorService dataFetcher;

    public static void setDataFetcher(ExecutorService dataFetcher) {
        DataFetcherSchedulerProvider.dataFetcher = dataFetcher;
    }

    @Override
    public ExecutorService get() {
        if (dataFetcher == null) {
            DataFetcherSchedulerProvider.setDataFetcher(Executors.newFixedThreadPool(24,
                                                                                     new CustomizableThreadFactory(
                                                                                             "data-fetcher-")));
        }
        return dataFetcher;
    }
}
