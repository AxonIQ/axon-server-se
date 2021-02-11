package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.config.FileSystemMonitor;
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
import java.time.Instant;

import static org.junit.Assert.*;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.ArgumentMatchers.any;

/**
 * @author Marc Gathier
 */
public class TokenAtTest {

    private static PrimaryEventStore testSubject;
    private final static FileSystemMonitor fileSystemMonitor = mock(FileSystemMonitor.class);

    @BeforeClass
    public static void init() {

        File sampleEventStoreFolder = new File(TestUtils
                                                       .fixPathOnWindows(InputStreamEventStore.class
                                                                                 .getResource(
                                                                                         "/token-at")
                                                                                 .getFile()));

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

        doNothing().when(fileSystemMonitor).registerPath(any());

        testSubject = new PrimaryEventStore(new EventTypeContext("default", EventType.EVENT),
                                            indexManager,
                                            new DefaultEventTransformerFactory(),
                                            storageProperties,
                                            secondaryEventStore,
                                            meterFactory, fileSystemMonitor);
        testSubject.init(true);
    }

    @Test
    public void tokenAtBeforeFirst() {
        assertEquals(0, testSubject.getTokenAt(0));
    }

    @Test
    public void tokenAtSecondSegment() {
        assertEquals(834, testSubject.getTokenAt(278));
    }

    @Test
    public void tokenAtActiveSegment() {
        assertEquals(2000, testSubject.getTokenAt(4062));
    }

    @Test
    public void tokenAtAfterLast() {
        assertEquals(2486, testSubject.getTokenAt(Instant.now().toEpochMilli()));
    }
}
