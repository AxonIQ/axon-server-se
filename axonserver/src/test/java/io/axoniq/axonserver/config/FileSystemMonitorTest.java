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

        when(mockFileSystem.getPath("/store/events")).thenReturn(pathMock);
        when(pathMock.toString()).thenReturn("/store/events");

        Path path = mockFileSystem.getPath("/store/events");

        testSubject.registerPath(path);

        when(diskSpaceHealthProperties.getThreshold()).thenReturn(THRESHOLD);
        when(diskSpaceHealthProperties.getPath()).thenReturn(new File("/store/events"));

        Health.Builder builder = mock(Health.Builder.class);
        testSubject.doHealthCheck(builder);

        verify(builder).down();
        verify(builder).withDetail(".path", "/store/events");
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
        Path path = fileSystem.getPath("/store/events");

        testSubject.registerPath(path);

        when(diskSpaceHealthProperties.getThreshold()).thenReturn(THRESHOLD);
        when(diskSpaceHealthProperties.getPath()).thenReturn(new File(path.toString()));

        Health.Builder builder = mock(Health.Builder.class);
        testSubject.doHealthCheck(builder);

        verify(builder).up();
        verify(builder).withDetail("/" + ".total", DISK_SIZE.toBytes());
        verify(builder).withDetail("/" +".free", DISK_SIZE.toBytes());
        verify(builder).withDetail(".threshold", THRESHOLD.toBytes());
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
        Path path = fileSystem.getPath("/store/events");

        testSubject.registerPath(path);

        when(diskSpaceHealthProperties.getThreshold()).thenReturn(THRESHOLD);
        when(diskSpaceHealthProperties.getPath()).thenReturn(new File(path.toString()));

        Health.Builder builder = mock(Health.Builder.class);
        testSubject.doHealthCheck(builder);

        verify(builder).status(HealthStatus.WARN_STATUS);
        verify(builder).withDetail("/" + ".total", DISK_SIZE.toBytes());
        verify(builder).withDetail("/" +".free", DISK_SIZE.toBytes());
        verify(builder).withDetail(".threshold", THRESHOLD.toBytes());
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
        Path path = fileSystem.getPath("/store/store");

        testSubject.registerPath(path);

        when(diskSpaceHealthProperties.getThreshold()).thenReturn(THRESHOLD);
        when(diskSpaceHealthProperties.getPath()).thenReturn(new File(path.toString()));

        Health.Builder builder = mock(Health.Builder.class);
        testSubject.doHealthCheck(builder);

        verify(builder).up();
        verify(builder).withDetail("/" + ".total", DISK_SIZE.toBytes());
        verify(builder).withDetail("/" +".free", DISK_SIZE.toBytes());
        verify(builder).withDetail(".threshold", THRESHOLD.toBytes());
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
        Path path = fileSystem.getPath("/store/store");

        testSubject.registerPath(path);

        when(diskSpaceHealthProperties.getThreshold()).thenReturn(THRESHOLD);
        when(diskSpaceHealthProperties.getPath()).thenReturn(new File(path.toString()));

        Health.Builder builder = mock(Health.Builder.class);
        testSubject.doHealthCheck(builder);

        verify(builder).status(HealthStatus.WARN_STATUS);
        verify(builder).withDetail("/" + ".total", DISK_SIZE.toBytes());
        verify(builder).withDetail("/" +".free", DISK_SIZE.toBytes());
        verify(builder).withDetail(".threshold", THRESHOLD.toBytes());
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
        Path path = fileSystem.getPath("C:\\store\\events");

        testSubject.registerPath(path);

        when(diskSpaceHealthProperties.getThreshold()).thenReturn(THRESHOLD);
        when(diskSpaceHealthProperties.getPath()).thenReturn(new File(path.toString()));

        Health.Builder builder = mock(Health.Builder.class);
        testSubject.doHealthCheck(builder);

        verify(builder).up();
        verify(builder).withDetail("C:\\" + ".total", DISK_SIZE.toBytes());
        verify(builder).withDetail("C:\\" +".free", DISK_SIZE.toBytes());
        verify(builder).withDetail(".threshold", THRESHOLD.toBytes());
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
        Path path = fileSystem.getPath("C:\\store\\events");

        testSubject.registerPath(path);

        when(diskSpaceHealthProperties.getThreshold()).thenReturn(THRESHOLD);
        when(diskSpaceHealthProperties.getPath()).thenReturn(new File(path.toString()));

        Health.Builder builder = mock(Health.Builder.class);
        testSubject.doHealthCheck(builder);

        verify(builder).status(HealthStatus.WARN_STATUS);
        verify(builder).withDetail("C:\\" + ".total", DISK_SIZE.toBytes());
        verify(builder).withDetail("C:\\" +".free", DISK_SIZE.toBytes());
        verify(builder).withDetail(".threshold", THRESHOLD.toBytes());
    }

}