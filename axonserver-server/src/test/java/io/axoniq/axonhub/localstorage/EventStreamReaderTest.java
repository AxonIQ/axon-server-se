package io.axoniq.axonhub.localstorage;

import org.junit.*;
import org.junit.rules.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.axoniq.axonhub.util.AssertUtils.assertWithin;
import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class EventStreamReaderTest {
    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();
    private static TestStorageContainer testStorageContainer;

    private EventStreamReader testSubject;

    @BeforeClass
    public static void init() throws Exception {
        testStorageContainer = new TestStorageContainer(tempFolder.getRoot());
        testStorageContainer.createDummyEvents(1000, 100);
    }

    @Before
    public void setUp() {
        testSubject = new EventStreamReader(testStorageContainer.getDatafileManagerChain(),
                                            testStorageContainer.getEventWriter());

    }

    @Test
    public void readEventsFromStart() throws InterruptedException {
        AtomicLong counter = new AtomicLong();
        EventStreamController controller = testSubject.createController(eventWithToken -> {
            counter.incrementAndGet();
        }, Throwable::printStackTrace);

        controller.update(0, 100);
        assertWithin(1000, TimeUnit.MILLISECONDS, () -> assertEquals(100, counter.get()));

        controller.update(0, 100);
        assertWithin(1000, TimeUnit.MILLISECONDS, () -> assertEquals(200, counter.get()));
    }

    @Test
    public void readEventsFromEnd() throws InterruptedException {
        AtomicLong counter = new AtomicLong();
        EventStreamController controller = testSubject.createController(eventWithToken -> {
            counter.incrementAndGet();
        }, Throwable::printStackTrace);

        controller.update(testStorageContainer.getEventWriter().getLastToken()-1, 100);
        assertWithin(1000, TimeUnit.MILLISECONDS, () -> assertEquals(2, counter.get()));
    }

    @Test
    public void readEventsWhileWriting() throws InterruptedException {
        AtomicLong counter = new AtomicLong();
        EventStreamController controller = testSubject.createController(eventWithToken -> counter.incrementAndGet(),
                                                                        Throwable::printStackTrace);

        controller.update(95000, 10000);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        Future<?> task = executor.submit(() -> {
            testStorageContainer.createDummyEvents(5000, 1, "live-");
        });

        assertWithin(5000, TimeUnit.MILLISECONDS, () -> assertEquals(10000, counter.get()));
        if( ! task.isDone()) task.cancel(true);
    }


}