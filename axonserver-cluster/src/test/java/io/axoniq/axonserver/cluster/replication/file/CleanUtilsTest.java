package io.axoniq.axonserver.cluster.replication.file;

import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

/**
 * @author Marc Gathier
 */
public class CleanUtilsTest {

    @Test
    public void cleanDirectBuffer() throws IOException {
        Assume.assumeTrue(System.getProperty("os.name").startsWith("Windows"));
        MappedByteBuffer buffer;
        File file = File.createTempFile("sample", ".tmp");

        try (FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel()) {
            buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 100);
        }
        assertFalse(file.delete());
        CleanUtils.cleanDirectBuffer(buffer, true, 0);
        assertTrue(file.delete());
    }
}