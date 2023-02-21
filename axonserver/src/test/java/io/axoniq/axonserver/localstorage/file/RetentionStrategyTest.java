package io.axoniq.axonserver.localstorage.file;

/**
 * @author Stefan Dragisic
 */

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author Stefan Dragisic
 */
public class RetentionStrategyTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testTimeBasedRetentionStrategy() throws IOException {
        String propertyName = "retentionTime";
        Map<String, String> contextMetaData = new HashMap<>();
        contextMetaData.put(propertyName, "PT1M");
        NavigableMap<Long, Integer> segments = new TreeMap<>();

        segments.put(100L, 1);
        segments.put(200L, 1); //200L is older in this test

        Function<FileVersion, File> dataFileResolver = fileVersion -> {
            try {
                File file = tempFolder.newFile(fileVersion.segment() + ".events");
                Instant timeModified = Instant.now().minusSeconds(fileVersion.segment() * 60);
                file.setLastModified(timeModified.toEpochMilli());
                return file;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };

        StorageTier.TimeBasedRetentionStrategy strategy = new StorageTier.TimeBasedRetentionStrategy(propertyName, contextMetaData, segments, dataFileResolver);
        assertEquals(Long.valueOf(200L), strategy.findNextSegment());
    }

    @Test
    public void testSizeBasedRetentionStrategy() throws IOException {
        String propertyName = "retentionSize";
        Map<String, String> contextMetaData = new HashMap<>();
        contextMetaData.put(propertyName, "1048576"); //retention size = 1MB
        NavigableMap<Long, Integer> segments =  new ConcurrentSkipListMap<>(Comparator.reverseOrder());
        segments.put(200L, 1);
        segments.put(100L, 1);

        File file200 = tempFolder.newFile("200.events");
        Path tempFile200 = file200.toPath();
        try (RandomAccessFile raf = new RandomAccessFile(tempFile200.toFile(), "rw")) {
            raf.setLength(1048576 * 2); //both files are 2MB
        }

        File file100 = tempFolder.newFile("100.events");
        Path tempFile100 = file100.toPath();
        try (RandomAccessFile raf = new RandomAccessFile(tempFile100.toFile(), "rw")) {
            raf.setLength(1048576 * 2); //both files are 2MB
        }

        Function<FileVersion, File> dataFileResolver = fileVersion -> {
            if (fileVersion.segment() == 200L) {
                return tempFile200.toFile();
            } else {
                return tempFile100.toFile();
            }
        };

        StorageTier.SizeBasedRetentionStrategy strategy = new StorageTier.SizeBasedRetentionStrategy(propertyName, contextMetaData, segments, dataFileResolver);
        assertEquals(Long.valueOf(100), strategy.findNextSegment());
        assertEquals(Long.valueOf(200), strategy.findNextSegment());
        assertNull(strategy.findNextSegment());
    }

}

