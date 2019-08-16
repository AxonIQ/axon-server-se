package io.axoniq.axonserver.cluster.replication.file;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.cluster.TermIndex;
import io.axoniq.axonserver.cluster.replication.EntryIterator;
import io.axoniq.axonserver.grpc.cluster.Config;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import org.junit.*;
import org.junit.rules.*;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import static junit.framework.TestCase.*;

/**
 * @author Marc Gathier
 */
public class PrimaryLogEntryStoreTest {

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();
    private PrimaryLogEntryStore testSubject;

    @Before
    public void setup() {
        String context= "default";
        LogEntryTransformerFactory eventTransformerFactory = (version, flags, storageProperties) -> new LogEntryTransformer() {
            @Override
            public byte[] readLogEntry(byte[] bytes) {
                return reverse(bytes);
            }

            @Override
            public byte[] transform(byte[] bytes) {
                return reverse(bytes);
            }
        };
        StorageProperties storageOptions = new StorageProperties();
        storageOptions.setSegmentSize(1024*1024);
        storageOptions.setLogStorageFolder(tempFolder.getRoot().getAbsolutePath() + "/" + UUID.randomUUID().toString());


        IndexManager indexManager = new IndexManager(storageOptions, context);
        testSubject = new PrimaryLogEntryStore(context, indexManager, eventTransformerFactory, storageOptions);
        testSubject.next = new SecondaryLogEntryStore(context, indexManager, eventTransformerFactory, storageOptions);
        testSubject.initSegments(Long.MAX_VALUE);
    }

    private byte[] reverse(byte[] bytes) {
        byte[] result = new byte[bytes.length];
        for( int i = 0; i < bytes.length; i++) {
            result[i] = bytes[bytes.length - i - 1];
        }
        return result;
    }

    @After
    public void complete() {
        System.out.println( tempFolder.getRoot());
        testSubject.cleanup(0);
    }

    @Test
    public void writeSerializedObject() throws InterruptedException, ExecutionException, TimeoutException {
        SerializedObject so = SerializedObject.newBuilder().setType("Demo").setData(ByteString.copyFromUtf8("Hello, world")).build();
        long event = testSubject.write(1, Entry.DataCase.SERIALIZEDOBJECT.getNumber(), so.toByteArray()).get(1, TimeUnit.SECONDS);
        assertEquals(1, event);
        Entry entry = testSubject.getEntry(1);
        assertTrue(entry.hasSerializedObject());
        assertEquals(1, entry.getTerm());
        assertEquals(1, entry.getIndex());
        assertEquals("Demo", entry.getSerializedObject().getType());
        assertEquals("Hello, world", entry.getSerializedObject().getData().toStringUtf8());
    }

    @Ignore("Config object not defined yet")
    @Test
    public void writeConfig() throws InterruptedException, ExecutionException, TimeoutException {
        Config so = Config.getDefaultInstance();
        long event = testSubject.write(1, Entry.DataCase.NEWCONFIGURATION.getNumber(), so.toByteArray()).get(1, TimeUnit.SECONDS);
        assertEquals(1, event);
        Entry entry = testSubject.getEntry(1);
        assertTrue(entry.hasNewConfiguration());
        assertEquals(1, entry.getTerm());
        assertEquals(1, entry.getIndex());
    }


    @Test
    public void writeMultipleObjects() throws InterruptedException, ExecutionException, TimeoutException {
        SerializedObject so = SerializedObject.newBuilder().setType("Demo").setData(ByteString.copyFromUtf8("Hello, world")).build();
        CompletableFuture[] futures = new CompletableFuture[50000];
        IntStream.range(0,futures.length).parallel().forEach(i ->
            futures[i] = testSubject.write(1, Entry.DataCase.SERIALIZEDOBJECT.getNumber(), so.toByteArray()));

        CompletableFuture.allOf(futures).get(1, TimeUnit.SECONDS);
        Thread.sleep(3000);
        assertEquals(futures.length, testSubject.getLastToken());
        Entry entry = testSubject.getEntry(100);
        assertTrue(entry.hasSerializedObject());
        assertEquals(1, entry.getTerm());
        assertEquals(100, entry.getIndex());
    }

    @Ignore("NIO has problem cleaning buffers on windows")
    @Test
    public void clear() throws InterruptedException, ExecutionException, TimeoutException {
        SerializedObject so = SerializedObject.newBuilder()
                                              .setType("Demo")
                                              .setData(ByteString.copyFromUtf8("Hello, world"))
                                              .build();
        CompletableFuture[] futures = new CompletableFuture[50000];
        IntStream.range(0, futures.length)
                 .parallel()
                 .forEach(i -> futures[i] = testSubject
                         .write(1, Entry.DataCase.SERIALIZEDOBJECT.getNumber(), so.toByteArray()));

        CompletableFuture.allOf(futures).get(1, TimeUnit.SECONDS);
        Thread.sleep(3000);
        testSubject.clear(0);
        assertEquals(0, testSubject.getSegments().size());
    }

    @Test
    public void rollback() throws InterruptedException, ExecutionException, TimeoutException {
        SerializedObject so = SerializedObject.newBuilder().setType("Demo").setData(ByteString.copyFromUtf8("Hello, world")).build();
        CompletableFuture[] futures = new CompletableFuture[50000];
        IntStream.range(0,futures.length).parallel().forEach(i ->
                                                                     futures[i] = testSubject.write(1, Entry.DataCase.SERIALIZEDOBJECT.getNumber(), so.toByteArray()));

        CompletableFuture.allOf(futures).get(1, TimeUnit.SECONDS);
        assertEquals(futures.length, testSubject.getLastToken());
        testSubject.rollback(4000);
        assertEquals(4000, testSubject.getLastToken());
        CompletableFuture[] futures2 = new CompletableFuture[500];
        IntStream.range(0,futures2.length).parallel().forEach(i ->
                                                                      futures2[i] = testSubject.write(1, Entry.DataCase.SERIALIZEDOBJECT.getNumber(), so.toByteArray()));

        CompletableFuture.allOf(futures2).get(1, TimeUnit.SECONDS);
        testSubject.rollback(4200);
        assertEquals(4200, testSubject.getLastToken());
    }

    @Test
    public void createIterator() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture[] futures = new CompletableFuture[10];
        IntStream.range(0,futures.length).forEach(i -> {
            SerializedObject so = SerializedObject.newBuilder().setType("Demo").setData(ByteString.copyFromUtf8(
                    randomString(5000))).build();
            futures[i] = testSubject.write(i + 1, Entry.DataCase.SERIALIZEDOBJECT.getNumber(), so.toByteArray());
        });

        CompletableFuture.allOf(futures).get(1, TimeUnit.SECONDS);
        assertEquals(futures.length, testSubject.getLastToken());
        SegmentEntryIterator iterator = testSubject.getIterator(2);
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());
        Entry next = iterator.next();

        assertEquals(2, next.getTerm());
        assertEquals(2, next.getIndex());
        while(iterator.hasNext()) {
            next = iterator.next();
        }
        assertEquals(futures.length, next.getIndex());
    }

    private String randomString(int maxLength) {
        String characters = "abcdefghijklmnopqrstuvwxyz";
        int length = ThreadLocalRandom.current().nextInt(maxLength)+1;
        StringBuilder stringBuilder = new StringBuilder();
        for( int i = 0; i< length; i++) {
            stringBuilder.append(characters.charAt(ThreadLocalRandom.current().nextInt(characters.length())));
        }
        return stringBuilder.toString();
    }

    @Test
    public void createIteratorMultipleSegments() throws InterruptedException, ExecutionException, TimeoutException {
        SerializedObject so = SerializedObject.newBuilder().setType("Demo").setData(ByteString.copyFromUtf8(randomString(4000))).build();
        CompletableFuture[] futures = new CompletableFuture[3000];
        IntStream.range(0,futures.length).forEach(i ->
                                                                     futures[i] = testSubject.write(i+1, Entry.DataCase.SERIALIZEDOBJECT.getNumber(), so.toByteArray()));

        CompletableFuture.allOf(futures).get(1, TimeUnit.SECONDS);
        assertEquals(futures.length, testSubject.getLastToken());
        int lastBefore = 1000;
        int count = 0;
        EntryIterator iterator = testSubject.getEntryIterator(lastBefore+1);
        Entry last = null;
        TermIndex prev = null;
            while (iterator.hasNext()) {
                last = iterator.next();
                prev = iterator.previous();
                assertEquals(last.getIndex(), prev.getIndex()+1);
                count++;
            }
        assertEquals(futures.length, count+lastBefore);
        assertEquals(futures.length, last.getIndex());
    }

}