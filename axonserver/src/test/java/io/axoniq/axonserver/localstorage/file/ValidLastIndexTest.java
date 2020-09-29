package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.transformation.DefaultEventTransformerFactory;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.test.TestUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;

import java.io.File;
import java.net.UnknownHostException;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class ValidLastIndexTest {

    private PrimaryEventStore testSubject;

    File sampleEventStoreFolder = new File(TestUtils
                                                   .fixPathOnWindows(InputStreamEventStore.class
                                                                             .getResource(
                                                                                     "/event-store-with-last-segment-index")
                                                                             .getFile()));

    @Before
    public void init() {
        MeterFactory meterFactory = new MeterFactory(new SimpleMeterRegistry(), new DefaultMetricCollector());

        StorageProperties storageProperties = new StorageProperties(new SystemInfoProvider() {
            @Override
            public String getHostName() throws UnknownHostException {
                return null;
            }
        });
        storageProperties.setStorage(sampleEventStoreFolder.getAbsolutePath());

        IndexManager indexManager = new StandardIndexManager("default",
                                                             storageProperties,
                                                             EventType.EVENT,
                                                             meterFactory);
        InputStreamEventStore secondaryEventStore = new InputStreamEventStore(new EventTypeContext("default",
                                                                                                   EventType.EVENT),
                                                                              indexManager,
                                                                              new DefaultEventTransformerFactory(),
                                                                              storageProperties,
                                                                              meterFactory);
        testSubject = new PrimaryEventStore(new EventTypeContext("default", EventType.EVENT),
                                            indexManager,
                                            new DefaultEventTransformerFactory(),
                                            storageProperties,
                                            secondaryEventStore,
                                            meterFactory);
    }

    @Test
    public void startWithNewSegment() {
        testSubject.init(true);
        assertTrue(new File(sampleEventStoreFolder.getAbsolutePath() + "/default/00000000000000000462.events")
                           .exists());
    }
}
