package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.refactoring.metric.DefaultMetricCollector;
import io.axoniq.axonserver.refactoring.metric.MeterFactory;
import io.axoniq.axonserver.refactoring.store.EventType;
import io.axoniq.axonserver.refactoring.store.EventTypeContext;
import io.axoniq.axonserver.refactoring.store.SerializedEvent;
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
import java.util.Optional;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class MultipleSnapshotSegments {

    private PrimaryEventStore testSubject;

    private FileSystemMonitor fileSystemMonitor = mock(FileSystemMonitor.class);

    File sampleEventStoreFolder = new File(TestUtils
                                                   .fixPathOnWindows(InputStreamEventStore.class
                                                                             .getResource("/multiple-snapshot-segments")
                                                                             .getFile()));

    @Before
    public void init() {
        MeterFactory meterFactory = new MeterFactory(new SimpleMeterRegistry(), new DefaultMetricCollector());

        doNothing().when(fileSystemMonitor).registerPath(any(), any());

        StorageProperties storageProperties = new StorageProperties(new SystemInfoProvider() {
            @Override
            public String getHostName() throws UnknownHostException {
                return null;
            }
        }, ".snapshots", ".sindex", ".sbloom", ".snindex", ".sxref").withStorage(sampleEventStoreFolder
                                                                                         .getAbsolutePath());

        IndexManager indexManager = new StandardIndexManager("default",
                                                             storageProperties,
                                                             EventType.SNAPSHOT,
                                                             meterFactory);
        InputStreamEventStore secondaryEventStore = new InputStreamEventStore(new EventTypeContext("default",
                                                                                                   EventType.SNAPSHOT),
                                                                              indexManager,
                                                                              new DefaultEventTransformerFactory(),
                                                                              storageProperties,
                                                                              meterFactory);
        testSubject = new PrimaryEventStore(new EventTypeContext("default", EventType.SNAPSHOT),
                                            indexManager,
                                            new DefaultEventTransformerFactory(),
                                            storageProperties,
                                            secondaryEventStore,
                                            meterFactory, fileSystemMonitor);
        testSubject.init(true);
    }

    @Test
    public void readSnapshotInFirstSegment() {
        testSubject.init(true);
        Optional<SerializedEvent> snapshot = testSubject.getLastEvent(
                "Aggregate-325",
                0, Long.MAX_VALUE);
        assertTrue(snapshot.isPresent());
        assertEquals(1, snapshot.get().getAggregateSequenceNumber());
    }

    @Test
    public void readSnapshotInLastSegment() {
        testSubject.init(true);
        Optional<SerializedEvent> snapshot = testSubject.getLastEvent(
                "Aggregate-360",
                0, Long.MAX_VALUE);
        assertTrue(snapshot.isPresent());
        assertEquals(1, snapshot.get().getAggregateSequenceNumber());
    }
}
