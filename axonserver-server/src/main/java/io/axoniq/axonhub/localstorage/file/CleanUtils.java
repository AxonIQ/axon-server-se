package io.axoniq.axonhub.localstorage.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Author: marc
 * Copied code from Tomcat to clean buffer: org.apache.tomcat.util.buf.ByteBufferUtils
 */
public class CleanUtils {
    private static final Method cleanerMethod;
    private static final Method cleanMethod;
    private static final Logger logger = LoggerFactory.getLogger(CleanUtils.class);
    private static final ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor();

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
        } catch (IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException | IllegalAccessException methodException) {
            logger.warn("byteBufferUtils.cleaner", methodException);
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
                cleanupExecutor.schedule(() -> {
                    doCleanup(buf);
                }, delay, TimeUnit.SECONDS);
            }
        }
    }

    private static void doCleanup(ByteBuffer buf) {
        try {
            cleanMethod.invoke(cleanerMethod.invoke(buf));
        } catch (IllegalArgumentException | InvocationTargetException | SecurityException | IllegalAccessException ignored) {
            logger.warn("Clean failed", ignored);
        }
    }
}
