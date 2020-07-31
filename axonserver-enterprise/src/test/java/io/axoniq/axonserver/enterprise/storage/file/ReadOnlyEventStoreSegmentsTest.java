package io.axoniq.axonserver.enterprise.storage.file;

import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.enterprise.storage.multitier.MultiTierInformationProvider;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonserver.localstorage.file.EventInformation;
import io.axoniq.axonserver.localstorage.file.EventIterator;
import io.axoniq.axonserver.localstorage.file.EventSource;
import io.axoniq.axonserver.localstorage.file.IndexManager;
import io.axoniq.axonserver.localstorage.file.InputStreamEventStore;
import io.axoniq.axonserver.localstorage.file.StandardIndexManager;
import io.axoniq.axonserver.localstorage.transformation.DefaultEventTransformerFactory;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.metric.MetricName;
import io.axoniq.axonserver.test.TestUtils;
import io.axoniq.axonserver.topology.Topology;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;

import static io.axoniq.axonserver.test.AssertUtils.assertWithin;
import static junit.framework.TestCase.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class ReadOnlyEventStoreSegmentsTest {

    private ReadOnlyEventStoreSegments testSubject;
    private EmbeddedDBProperties embeddedDBProperties = new EmbeddedDBProperties(new SystemInfoProvider() {
    });

    @Before
    public void setUp() throws IOException {
        embeddedDBProperties.getEvent().setStorage(TestUtils
                                                           .fixPathOnWindows(InputStreamEventStore.class
                                                                                     .getResource("/data").getFile()));
        embeddedDBProperties.getEvent().setUseMmapIndex(false);
        String context = Topology.DEFAULT_CONTEXT;
        MeterFactory meterFactory = new MeterFactory(new SimpleMeterRegistry(), new DefaultMetricCollector());
        IndexManager indexManager = new StandardIndexManager(context, embeddedDBProperties.getEvent(), EventType.EVENT,
                                                             meterFactory);
        EventTransformerFactory eventTransformerFactory = new DefaultEventTransformerFactory();
        MultiTierInformationProvider multiTierInformationProvider = mock(MultiTierInformationProvider.class);
        when(multiTierInformationProvider.isMultiTier(anyString())).thenReturn(true);
        when(multiTierInformationProvider.tier(anyString())).thenReturn(0);
        when(multiTierInformationProvider.safeToken(anyString(), any(MetricName.class))).thenReturn(15L);
        embeddedDBProperties.getEvent().setRetentionTime(new Duration[]{Duration.ofMillis(10),
                Duration.ofMillis(1000)});
        testSubject = new ReadOnlyEventStoreSegments(new EventTypeContext(context, EventType.EVENT),
                                                     indexManager,
                                                     eventTransformerFactory,
                                                     embeddedDBProperties.getEvent(),
                                                     multiTierInformationProvider, meterFactory, 20);
        testSubject.init(false);
    }

    @After
    public void clean() {
        testSubject.close(false);
    }


    @Test
    public void getEventSource() {
        EventSource eventSource = testSubject.getEventSource(0).get();
        try (EventIterator iterator = eventSource.createEventIterator(0, 5)) {
            assertTrue(iterator.hasNext());
            EventInformation next = iterator.next();
            assertEquals(5, next.getToken());
            while (iterator.hasNext()) {
                next = iterator.next();
            }
            assertEquals(13, next.getToken());
        }
    }

    @Test
    public void getSegments() {
        SortedSet<Long> segments = testSubject.getSegments();
        assertTrue(segments.contains(0L));
        assertTrue(segments.contains(14L));
        assertEquals(14, (long) segments.first());
    }

    @Test
    public void handover() throws InterruptedException, IOException {
        File datafile = embeddedDBProperties.getEvent().dataFile(Topology.DEFAULT_CONTEXT, 0L);
        File copy = new File(datafile.getAbsolutePath() + ".temp");
        copy(datafile, copy);
        testSubject.handover(20L, () -> System.out.println("Done"));
        assertWithin(5, TimeUnit.SECONDS, () -> assertFalse(datafile.exists()));
        copy.renameTo(datafile);
    }

    private void copy(File datafile, File copy) throws IOException {
        try (FileInputStream fis = new FileInputStream(datafile); FileOutputStream fos = new FileOutputStream(copy)) {
            byte[] buffer = new byte[5000];
            int r = fis.read(buffer);
            while (r > 0) {
                fos.write(buffer, 0, r);
                r = fis.read(buffer);
            }
        }
    }
}