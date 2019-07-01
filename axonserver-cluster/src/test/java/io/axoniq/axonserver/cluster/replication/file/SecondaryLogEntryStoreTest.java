package io.axoniq.axonserver.cluster.replication.file;

import org.junit.*;
import org.junit.rules.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link SecondaryLogEntryStore}
 *
 * @author Sara Pellegrini
 * @since 4.1.6
 */
public class SecondaryLogEntryStoreTest {

    public static TemporaryFolder tempFolder = new TemporaryFolder();

    private SecondaryLogEntryStore testSubject;
    private StorageProperties storageOptions;
    private String context;
    private String contextLogFolder;

    @Before
    public void setup() throws IOException {
        tempFolder.create();
        context = "default";
        LogEntryTransformerFactory entryTransformerFactory = (version, flags, storageProperties) -> new LogEntryTransformer() {
            @Override
            public byte[] readLogEntry(byte[] bytes) {
                return bytes;
            }

            @Override
            public byte[] transform(byte[] bytes) {
                return bytes;
            }
        };
        storageOptions = new StorageProperties();
        storageOptions.setSegmentSize(1024 * 1024);
        storageOptions.setLogStorageFolder(tempFolder.getRoot().getAbsolutePath() + "/" + UUID.randomUUID().toString());


        IndexManager indexManager = new IndexManager(storageOptions, context) {
            @Override
            public boolean validIndex(long segment) {
                return true;
            }
        };
        testSubject = new SecondaryLogEntryStore(context, indexManager, entryTransformerFactory, storageOptions) {
            @Override
            public long getSegmentFor(long token) {
                return super.getSegmentFor(token);
            }
        };

        testSubject.initSegments(Long.MAX_VALUE);
        String logSample = "secondaryLogEntryStoreTest/00000000000000000001.log";
        InputStream logFile = this.getClass().getClassLoader().getResourceAsStream(logSample);
        contextLogFolder = storageOptions.getLogStorageFolder() + "/" + context;
        Files.copy(logFile, Paths.get(contextLogFolder, "00000000000000019664.log"), REPLACE_EXISTING);
        Files.copy(logFile, Paths.get(contextLogFolder, "00000000000000019835.log"), REPLACE_EXISTING);
        Files.copy(logFile, Paths.get(contextLogFolder, "00000000000000020026.log"), REPLACE_EXISTING);

        testSubject.initSegments(Long.MAX_VALUE);
    }

    @Test
    public void clearOlderThanLastAppliedEntryIsTheFirst() throws IOException {

        testSubject.clearOlderThan(0, TimeUnit.SECONDS, () -> 19_664L);
        File[] files = new File(contextLogFolder).listFiles();
        assertEquals(3, files.length);
    }

    @Test
    public void clearOlderThanLastAppliedEntryIsBeforeTheFirst() throws IOException {
        testSubject.clearOlderThan(0, TimeUnit.SECONDS, () -> 16_993L);
        File[] files = new File(contextLogFolder).listFiles();
        assertEquals(3, files.length);
    }

    @Test
    public void clearOlderThanLastAppliedEntryInTheMiddle() throws IOException {
        testSubject.clearOlderThan(0, TimeUnit.SECONDS, () -> 20_000L);
        File[] files = new File(contextLogFolder).listFiles();
        assertEquals(2, files.length);
    }


    @Test
    public void clearOlderThanLastAppliedEntryIsTheLast() throws IOException {
        testSubject.initSegments(Long.MAX_VALUE);
        testSubject.clearOlderThan(0, TimeUnit.SECONDS, () -> 20_026L);
        File[] files = new File(contextLogFolder).listFiles();
        assertEquals(1, files.length);
    }

    @Test
    public void clearOlderThanLastAppliedEntryIsAfterTheLast() throws IOException {
        testSubject.clearOlderThan(0, TimeUnit.SECONDS, () -> 20_027L);
        File[] files = new File(contextLogFolder).listFiles();
        assertEquals(1, files.length);
    }

    @After
    public void tearDown() throws Exception {
        tempFolder.delete();
    }
}