package io.axoniq.axonserver.filestorage.impl;

import io.axoniq.axonserver.filestorage.FileStoreEntry;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.reactivestreams.Subscription;
import org.springframework.data.util.CloseableIterator;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.util.annotation.NonNull;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * @author Marc Gathier
 */
public class BaseAppendOnlyFileStoreTest {
    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    private BaseAppendOnlyFileStore baseFileStore;
    private final StorageProperties storageProperties = new StorageProperties();

    @Before
    public void setUp() throws Exception {
        storageProperties.setStorage(tempFolder.newFolder(UUID.randomUUID().toString()));
        storageProperties.setSegmentSize(2_000);
        baseFileStore = new BaseAppendOnlyFileStore(storageProperties, "test");
        baseFileStore.open(false).block();
    }

    @After
    public void tearDown()  {
        try {
            baseFileStore.delete();
        } catch (Exception ignored) {

        }
    }

    @Test
    public void append() {
        long index = append("Hello, World!", 0);
        assertEquals(0, index);
        FileStoreEntry entry = baseFileStore.lastEntry();
        assertEquals("Hello, World!", new String(entry.bytes()));
    }

    @Test
    public void appendLargeEntry() {
        byte[] bytes = new byte[(int)storageProperties.getSegmentSize() + 10];
        Arrays.fill(bytes, (byte) 'a');
        Long index = baseFileStore.append(entry(bytes, 10)).block(Duration.ofSeconds(1));
        assertNotNull(index);
        assertEquals(0, index.longValue());
        File dataFile = storageProperties.dataFile(0);
        assertEquals(bytes.length + 18, dataFile.length());
        append("Hello, World!", 0);
    }

    @Test
    public void appendMultiple() {
        Sinks.Many<FileStoreEntry> publisher = Sinks.many().unicast().onBackpressureBuffer();
        Mono<Long> result = baseFileStore.append(publisher.asFlux());
        publisher.tryEmitNext(entry("one".getBytes(), 1)).orThrow();
        publisher.tryEmitNext(entry("two".getBytes(), 1)).orThrow();
        publisher.tryEmitNext(entry("three".getBytes(), 1)).orThrow();
        publisher.tryEmitNext(entry("four".getBytes(), 1)).orThrow();
        publisher.tryEmitNext(entry("five".getBytes(), 1)).orThrow();
        publisher.tryEmitComplete().orThrow();
        Long index = result.block(Duration.ofSeconds(1));
        assertNotNull(index);
        assertEquals(0, index.longValue());
        FileStoreEntry entry = baseFileStore.lastEntry();
        assertEquals("five", new String(entry.bytes()));
        index = append("six", 2);
        assertEquals(5, index.longValue());

    }
    @Test
    public void appendMultipleReopenAndRead() {
        Sinks.Many<FileStoreEntry> publisher = Sinks.many().unicast().onBackpressureBuffer();
        Mono<Long> result = baseFileStore.append(publisher.asFlux());
        publisher.tryEmitNext(entry("one".getBytes(), 1)).orThrow();
        publisher.tryEmitNext(entry("two".getBytes(), 1)).orThrow();
        publisher.tryEmitNext(entry("three".getBytes(), 1)).orThrow();
        publisher.tryEmitNext(entry("four".getBytes(), 1)).orThrow();
        publisher.tryEmitNext(entry("five".getBytes(), 1)).orThrow();
        publisher.tryEmitComplete().orThrow();
        Long index = result.block(Duration.ofSeconds(1));
        assertNotNull(index);
        assertEquals(0, index.longValue());
        index = append("six", 2);
        assertEquals(5, index.longValue());
        baseFileStore.close().block();
        baseFileStore = new BaseAppendOnlyFileStore(storageProperties, "test");
        baseFileStore.open(false).block();
        try (CloseableIterator<FileStoreEntry> it = baseFileStore.iterator(0)) {
            while( it.hasNext()) {
                System.out.println(it.next());
            }
        }
        FileStoreEntry entry = baseFileStore.read(0).block();
        assertNotNull(entry);
        assertEquals("one", new String(entry.bytes()));
    }

    private long append(String text, int version) {
        Long index = baseFileStore.append(entry(text.getBytes(), version)).block(Duration.ofSeconds(1));
        assertNotNull(index);
        return index;
    }

    private FileStoreEntry entry(byte[] data, int version) {
        return new FileStoreEntry() {
            @Override
            public byte[] bytes() {
                return data;
            }

            @Override
            public byte version() {
                return (byte)version;
            }
        };
    }

    @Test
    public void read() {
        append("First entry", 9);
        append("Second entry", 0);
        append("Third entry", 0);
        FileStoreEntry entry = baseFileStore.read(0).block(Duration.ofSeconds(1));
        assertNotNull(entry);
        assertEquals("First entry", new String(entry.bytes()));
        assertEquals(9, entry.version());
        entry = baseFileStore.read(2).block(Duration.ofSeconds(1));
        assertNotNull(entry);
        assertEquals("Third entry", new String(entry.bytes()));
        assertEquals(0, entry.version());
    }

    @Test
    public void reset() {
        append("First entry", 9);
        append("Second entry", 0);
        append("Third entry", 0);
        baseFileStore.reset(0L).block();
        List<FileStoreEntry> entries = baseFileStore.stream(0).collect(Collectors.toList()).block();
        assertNotNull(entries);
        assertEquals(1, entries.size());
    }

    @Test
    public void resetCancelsStreams() throws InterruptedException {
        append("First entry", 9);
        append("Second entry", 0);
        append("Third entry", 0);
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        baseFileStore.stream(0)
                     .subscribeOn(Schedulers.boundedElastic())
                     .subscribe(new BaseSubscriber<FileStoreEntry>() {
            Subscription subscription;
            @Override
            protected void hookOnSubscribe(@NonNull Subscription subscription) {
                this.subscription = subscription;
                subscription.request(1);
            }

            @Override
            protected void hookOnNext(@NonNull FileStoreEntry value) {
                try {
                    Thread.sleep(1_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                subscription.request(1);
            }

            @Override
            protected void hookOnError(@NonNull Throwable throwable) {
                completableFuture.completeExceptionally(throwable);
            }

            @Override
            protected void hookOnComplete() {
                completableFuture.complete(null);
            }
        });
        baseFileStore.reset(0L).block();
        try {
            completableFuture.get(5, TimeUnit.SECONDS);
            fail("Unexpected complete");
        } catch (ExecutionException e) {
            e.printStackTrace();
            assertEquals(IllegalStateException.class, e.getCause().getClass());
        } catch (TimeoutException e) {
            fail("Unexpected timeout");
        }
    }

    @Test
    public void resetMultipleSegments() {
        for (int i = 0; i < 1000; i++) {
            append("An entry" + i, 9);
        }

        baseFileStore.reset(11L).block();
        List<FileStoreEntry> entries = baseFileStore.stream(0)
                                                    .collect(Collectors.toList())
                                                    .block();
        assertNotNull(entries);
        assertEquals(12, entries.size());
    }

    @Test
    public void stream() {
        append("First entry", 9);
        append("Second entry", 0);
        append("Third entry", 0);
        List<FileStoreEntry> entries = baseFileStore.stream(1)
                                                    .collect(Collectors.toList())
                                                    .block(Duration.ofSeconds(1));
        assertNotNull(entries);
        assertEquals(2, entries.size());
    }

    @Test
    public void streamSublist() {
        append("First entry", 9);
        append("Second entry", 0);
        append("Third entry", 0);
        List<FileStoreEntry> entries = baseFileStore.stream(0, 1).collect(Collectors.toList()).block(Duration.ofSeconds(1));
        assertNotNull(entries);
        assertEquals(2, entries.size());
        entries = baseFileStore.stream(1, 1000).collect(Collectors.toList()).block(Duration.ofSeconds(1));
        assertNotNull(entries);
        assertEquals(2, entries.size());
        entries = baseFileStore.stream(2, 1).collect(Collectors.toList()).block(Duration.ofSeconds(1));
        assertNotNull(entries);
        assertEquals(0, entries.size());
    }

}