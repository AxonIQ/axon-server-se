package io.axoniq.axonserver.cluster.replication.file;

import io.axoniq.axonserver.cluster.util.AxonThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Utility to support cleaning MemoryMapped files on Windows. When running on Windows MemoryMappedFiles retain lock on file, even when closed. Need to call clean
 * to remove the lock explicitely, otherwise it depends on the OS cleaning the memory segment.
 * Original code from Tomcat to clean buffer: org.apache.tomcat.util.buf.ByteBufferUtils
 *
 * @author Marc Gathier
 */
public class CleanUtils {
    private static final Method cleanerMethod;
    private static final Method cleanMethod;
    private static final Logger logger = LoggerFactory.getLogger(CleanUtils.class);
    private static final ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor(new AxonThreadFactory("fileCleaner"));

    static {
        ByteBuffer tempBuffer = ByteBuffer.allocateDirect(0);
        Method cleanerMethodLocal;
        Method cleanMethodLocal;

        try {
            cleanerMethodLocal = tempBuffer.getClass().getMethod("cleaner");
            cleanerMethodLocal.setAccessible(true);
            Object cleanerObject = cleanerMethodLocal.invoke(tempBuffer);
            if (cleanerObject instanceof Runnable) {
                cleanMethodLocal = Runnable.class.getMethod("run");
            } else {
                cleanMethodLocal = cleanerObject.getClass().getMethod("clean");
            }

            cleanMethodLocal.invoke(cleanerObject);
        } catch (IllegalAccessException methodException) {
            logger.warn("Unable to configure clean method for MemoryMappedFiles. If you are running on Windows with a Java version 9 or higher start Axon Server with --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED");
            cleanerMethodLocal = null;
            cleanMethodLocal = null;
        } catch (IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException  methodException) {
            logger.warn("Unable to configure clean method for MemoryMappedFiles", methodException);
            cleanerMethodLocal = null;
            cleanMethodLocal = null;
        }

        cleanerMethod = cleanerMethodLocal;
        cleanMethod = cleanMethodLocal;
    }
    private CleanUtils() {
    }

    public static void cleanDirectBuffer(ByteBuffer buf, boolean allowed, long delay) {
        if (allowed && cleanMethod != null && buf != null) {
            if (delay <= 0) {
                doCleanup(buf);
            } else {
                try {
                    cleanupExecutor.schedule(() -> doCleanup(buf), delay, TimeUnit.SECONDS);
                } catch( Exception ignore) {
                    //may be as executor is shutdown
                }
            }
        }
    }

    private static void doCleanup(ByteBuffer buf) {
        try {
            cleanMethod.invoke(cleanerMethod.invoke(buf));
        } catch (IllegalArgumentException | InvocationTargetException | SecurityException | IllegalAccessException exception) {
            logger.warn("Clean failed", exception);
        }
    }
}
