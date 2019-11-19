package io.axoniq.axonserver.cluster.replication.file;

import io.axoniq.axonserver.cluster.util.AxonThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Utility to support cleaning MemoryMapped files on Windows. When running on Windows MemoryMappedFiles retain lock on file, even when closed. Need to call clean
 * to remove the lock explicitely, otherwise it depends on the OS cleaning the memory segment.
 *
 * @author Marc Gathier
 * @since 4.1
 */
public class CleanUtils {
    private static final Logger logger = LoggerFactory.getLogger(CleanUtils.class);
    private static final ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor(new AxonThreadFactory("fileCleaner"));
    private static final boolean java8 = System.getProperty("java.version").startsWith("1.8");

    private static boolean cleanOldsJDK(final ByteBuffer buffer) {
        return AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> {
            Boolean success = Boolean.FALSE;
            try {
                Method getCleanerMethod = buffer.getClass().getMethod("cleaner", (Class[]) null);
                if (!getCleanerMethod.isAccessible()) {
                    getCleanerMethod.setAccessible(true);
                }
                Object cleaner = getCleanerMethod.invoke(buffer, (Object[]) null);
                Method clean = cleaner.getClass().getMethod("clean", (Class[]) null);
                clean.invoke(cleaner, (Object[]) null);
                success = Boolean.TRUE;
            } catch (Exception ex) {
                logger.warn("Clean failed", ex);
            }
            return success;
        });
    }

    private static boolean cleanJavaWithModules(final java.nio.ByteBuffer buffer) {
        return AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> {
            Boolean success = Boolean.FALSE;
            try {
                final Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
                final Field theUnsafeField = unsafeClass.getDeclaredField("theUnsafe");
                theUnsafeField.setAccessible(true);
                final Object theUnsafe = theUnsafeField.get(null);
                final Method invokeCleanerMethod = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
                invokeCleanerMethod.invoke(theUnsafe, buffer);
                success = Boolean.TRUE;
            } catch (Exception ex) {
                logger.warn("Clean failed", ex);
            }
            return success;
        });
    }

    private CleanUtils() {
    }

    public static void cleanDirectBuffer(ByteBuffer buf, boolean allowed, long delay) {
        if (allowed && buf != null) {
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
        if (java8) {
            cleanOldsJDK(buf);
        } else {
            cleanJavaWithModules(buf);
        }
    }
}
