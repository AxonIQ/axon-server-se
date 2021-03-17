package io.axoniq.axonserver.config;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.actuate.autoconfigure.system.DiskSpaceHealthIndicatorProperties;
import org.springframework.boot.actuate.health.Health;
import org.springframework.util.unit.DataSize;

import java.io.File;
import java.nio.file.FileSystem;
import java.nio.file.Path;

import static org.mockito.Mockito.*;

/**
 * @author Stefan Dragisic
 */
public class FileSystemMonitorTest {

    public static final String STORE_EVENTS_UNIX = "/store/events";
    public static final String STORE_NAME = "store";
    public static final String STORE_EVENTS_OSX = "/store/store";
    public static final String STORE_EVENTS_WIN = "C:\\store\\events";
    FileSystemMonitor testSubject;

    DiskSpaceHealthIndicatorProperties diskSpaceHealthProperties = mock(DiskSpaceHealthIndicatorProperties.class);

    MeterRegistry meterRegistry = mock(MeterRegistry.class);

    @Before
    public void setUp() throws Exception {
        testSubject = new FileSystemMonitor(diskSpaceHealthProperties,meterRegistry);
    }

    //***************** NO FILE SYSTEM FOUND ********************
    @Test
    public void whenFileSystemNotMountedThenStatusIsDown() {
        final DataSize THRESHOLD = DataSize.ofMegabytes(2);

        FileSystem mockFileSystem = mock(FileSystem.class);
        Path pathMock = mock(Path.class);

        when(mockFileSystem.getPath(STORE_EVENTS_UNIX)).thenReturn(pathMock);
        when(pathMock.toString()).thenReturn(STORE_EVENTS_UNIX);

        Path path = mockFileSystem.getPath(STORE_EVENTS_UNIX);

        testSubject.registerPath(STORE_NAME, path);

        when(diskSpaceHealthProperties.getThreshold()).thenReturn(THRESHOLD);
        when(diskSpaceHealthProperties.getPath()).thenReturn(new File(STORE_EVENTS_UNIX));

        Health.Builder builder = mock(Health.Builder.class);
        testSubject.doHealthCheck(builder);

        verify(builder).down();
    }

    //************************ UNIX ************************

    @Test
    public void unixWhenSizeAboveThresholdThenStatusIsUp() {
        final DataSize DISK_SIZE = DataSize.ofMegabytes(2);
        final DataSize THRESHOLD = DataSize.ofMegabytes(1);

        FileSystem fileSystem = Jimfs.newFileSystem(
                Configuration.unix()
                .toBuilder()
                .setBlockSize(1)
                .setMaxSize(DISK_SIZE.toBytes())
                .build()
                );
        Path path = fileSystem.getPath(STORE_EVENTS_UNIX);

        testSubject.registerPath(STORE_NAME, path);

        when(diskSpaceHealthProperties.getThreshold()).thenReturn(THRESHOLD);
        when(diskSpaceHealthProperties.getPath()).thenReturn(new File(path.toString()));

        Health.Builder builder = mock(Health.Builder.class);
        testSubject.doHealthCheck(builder);

        verify(builder).up();
    }

    @Test
    public void unixWhenSizeBelowThresholdThenStatusIsWarn() {
        final DataSize DISK_SIZE = DataSize.ofMegabytes(1);
        final DataSize THRESHOLD = DataSize.ofMegabytes(2);

        FileSystem fileSystem = Jimfs.newFileSystem(
                Configuration.unix()
                        .toBuilder()
                        .setBlockSize(1)
                        .setMaxSize(DISK_SIZE.toBytes())
                        .build()
        );
        Path path = fileSystem.getPath(STORE_EVENTS_UNIX);

        testSubject.registerPath(STORE_NAME, path);

        when(diskSpaceHealthProperties.getThreshold()).thenReturn(THRESHOLD);
        when(diskSpaceHealthProperties.getPath()).thenReturn(new File(path.toString()));

        Health.Builder builder = mock(Health.Builder.class);
        testSubject.doHealthCheck(builder);

        verify(builder).status(HealthStatus.WARN_STATUS);
    }


    //************************ OSX ************************
    @Test
    public void osXWhenSizeAboveThresholdThenStatusIsUp() {
        final DataSize DISK_SIZE = DataSize.ofMegabytes(2);
        final DataSize THRESHOLD = DataSize.ofMegabytes(1);

        FileSystem fileSystem = Jimfs.newFileSystem(
                Configuration.osX()
                        .toBuilder()
                        .setBlockSize(1)
                        .setMaxSize(DISK_SIZE.toBytes())
                        .build()
        );
        Path path = fileSystem.getPath(STORE_EVENTS_OSX);

        testSubject.registerPath(STORE_NAME, path);

        when(diskSpaceHealthProperties.getThreshold()).thenReturn(THRESHOLD);
        when(diskSpaceHealthProperties.getPath()).thenReturn(new File(path.toString()));

        Health.Builder builder = mock(Health.Builder.class);
        testSubject.doHealthCheck(builder);

        verify(builder).up();
    }

    @Test
    public void osXWhenSizeBelowThresholdThenStatusIsWarn() {
        final DataSize DISK_SIZE = DataSize.ofMegabytes(1);
        final DataSize THRESHOLD = DataSize.ofMegabytes(2);

        FileSystem fileSystem = Jimfs.newFileSystem(
                Configuration.osX()
                        .toBuilder()
                        .setBlockSize(1)
                        .setMaxSize(DISK_SIZE.toBytes())
                        .build()
        );
        Path path = fileSystem.getPath(STORE_EVENTS_OSX);

        testSubject.registerPath(STORE_NAME, path);

        when(diskSpaceHealthProperties.getThreshold()).thenReturn(THRESHOLD);
        when(diskSpaceHealthProperties.getPath()).thenReturn(new File(path.toString()));

        Health.Builder builder = mock(Health.Builder.class);
        testSubject.doHealthCheck(builder);

        verify(builder).status(HealthStatus.WARN_STATUS);
    }

    //*********************** WINDOWS ***********************
    @Test
    public void windowsWhenSizeAboveThresholdThenStatusIsUp() {
        final DataSize DISK_SIZE = DataSize.ofMegabytes(2);
        final DataSize THRESHOLD = DataSize.ofMegabytes(1);

        FileSystem fileSystem = Jimfs.newFileSystem(
                Configuration.windows()
                        .toBuilder()
                        .setBlockSize(1)
                        .setMaxSize(DISK_SIZE.toBytes())
                        .build()
        );
        Path path = fileSystem.getPath(STORE_EVENTS_WIN);

        testSubject.registerPath(STORE_NAME, path);

        when(diskSpaceHealthProperties.getThreshold()).thenReturn(THRESHOLD);
        when(diskSpaceHealthProperties.getPath()).thenReturn(new File(path.toString()));

        Health.Builder builder = mock(Health.Builder.class);
        testSubject.doHealthCheck(builder);

        verify(builder).up();
    }

    @Test
    public void windowsWhenSizeBelowThresholdThenStatusIsWarn() {
        final DataSize DISK_SIZE = DataSize.ofMegabytes(1);
        final DataSize THRESHOLD = DataSize.ofMegabytes(2);

        FileSystem fileSystem = Jimfs.newFileSystem(
                Configuration.windows()
                        .toBuilder()
                        .setBlockSize(1)
                        .setMaxSize(DISK_SIZE.toBytes())
                        .build()
        );
        Path path = fileSystem.getPath(STORE_EVENTS_WIN);

        testSubject.registerPath(STORE_NAME, path);

        when(diskSpaceHealthProperties.getThreshold()).thenReturn(THRESHOLD);
        when(diskSpaceHealthProperties.getPath()).thenReturn(new File(path.toString()));

        Health.Builder builder = mock(Health.Builder.class);
        testSubject.doHealthCheck(builder);

        verify(builder).status(HealthStatus.WARN_STATUS);
    }

}