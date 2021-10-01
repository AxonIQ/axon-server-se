package io.axoniq.axonserver.filestorage.impl;

import io.axoniq.axonserver.filestorage.FileStoreEntry;
import org.junit.*;
import org.junit.rules.*;
import org.springframework.data.util.CloseableIterator;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 * @since
 */
public class BaseFileStoreTest {
    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    private BaseFileStore baseFileStore;
    private final StorageProperties storageProperties = new StorageProperties();


    @Before
    public void setUp() throws Exception {
        storageProperties.setStorage(tempFolder.newFolder(UUID.randomUUID().toString()));
        System.out.println(storageProperties.getStorage());
        baseFileStore = new BaseFileStore(storageProperties, "test");
        baseFileStore.open(false);
    }

    @After
    public void tearDown()  {
        for (File file : storageProperties.getStorage().listFiles()) {
            System.out.printf("%s: size = %d%n", file.getName(), file.length());
        }
        baseFileStore.close();
    }

    @Test
    public void append() {
        long index = append("Hello, World!", 0);
        assertEquals(0, index);
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
//        try (CloseableIterator<FileStoreEntry> it = baseFileStore.iterator(0)) {
//            while( it.hasNext()) {
//                System.out.println(it.next());
//            }
//        }
        baseFileStore.close();
        baseFileStore = new BaseFileStore(storageProperties, "test");
        baseFileStore.open(false);
        try (CloseableIterator<FileStoreEntry> it = baseFileStore.iterator(0)) {
            while( it.hasNext()) {
                System.out.println(it.next());
            }
        }
        FileStoreEntry entry = baseFileStore.read(0).block();
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
    public void stream() {
        append("First entry", 9);
        append("Second entry", 0);
        append("Third entry", 0);
        List<FileStoreEntry> entries = baseFileStore.stream(1).collect(Collectors.toList()).block(Duration.ofSeconds(1));
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