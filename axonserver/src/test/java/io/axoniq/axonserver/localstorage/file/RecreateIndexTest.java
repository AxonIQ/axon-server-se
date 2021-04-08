package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.refactoring.metric.DefaultMetricCollector;
import io.axoniq.axonserver.refactoring.metric.MeterFactory;
import io.axoniq.axonserver.refactoring.store.EventType;
import io.axoniq.axonserver.refactoring.store.EventTypeContext;
import io.axoniq.axonserver.refactoring.store.engine.file.IndexManager;
import io.axoniq.axonserver.refactoring.store.engine.file.InputStreamEventStore;
import io.axoniq.axonserver.refactoring.store.engine.file.PrimaryEventStore;
import io.axoniq.axonserver.refactoring.store.engine.file.StandardIndexManager;
import io.axoniq.axonserver.refactoring.store.engine.file.StorageProperties;
import io.axoniq.axonserver.refactoring.store.transformation.DefaultEventTransformerFactory;
import io.axoniq.axonserver.refactoring.transport.rest.actuator.FileSystemMonitor;
import io.axoniq.axonserver.test.TestUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;

import java.io.File;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class RecreateIndexTest {

    private PrimaryEventStore testSubject;
    private final FileSystemMonitor fileSystemMonitor = mock(FileSystemMonitor.class);

    @Before
    public void init() {

        File sampleEventStoreFolder = new File(TestUtils
                                                       .fixPathOnWindows(InputStreamEventStore.class
                                                                                 .getResource(
                                                                                         "/event-store-without-index")
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

        doNothing().when(fileSystemMonitor).registerPath(any(), any());

        testSubject = new PrimaryEventStore(new EventTypeContext("default", EventType.EVENT),
                                            indexManager,
                                            new DefaultEventTransformerFactory(),
                                            storageProperties,
                                            secondaryEventStore,
                                            meterFactory, fileSystemMonitor);
    }

    @Test
    public void recreateIndex() {
        testSubject.init(true);

        List<String> files = testSubject.getBackupFilenames(-1).collect(Collectors.toList());
        assertEquals(6, files.size());

        AtomicInteger events = new AtomicInteger();
        testSubject.processEventsPerAggregate("Aggregate-1", 0, Long.MAX_VALUE, 0,
                                              e -> events.incrementAndGet());

        assertEquals(14, events.get());
    }
}
