package io.axoniq.axonserver.localstorage.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

/**
 * Author: marc
 * Copied code from Tomcat to clean buffer: org.apache.tomcat.util.buf.ByteBufferUtils
 */
public class CleanUtils {
    private static final Method cleanerMethod;
    private static final Method cleanMethod;
    private static final Logger logger = LoggerFactory.getLogger(CleanUtils.class);
    private static final ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor(new CustomizableThreadFactory("fileCleaner") {
        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = super.newThread(runnable);
            thread.setDaemon(true);
            return thread;
        }
    });

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

    public static void cleanDirectBuffer(ByteBuffer buf, BooleanSupplier allowed, long delay) {
        if (cleanMethod != null && buf != null) {
            if (delay <= 0) {
                doCleanup(allowed, buf);
            } else {
                try {
                    cleanupExecutor.schedule(() -> doCleanup(allowed, buf), delay, TimeUnit.SECONDS);
                } catch( Exception ignore) {
                    //may be as executor is shutdown
                }
            }
        }
    }

    private static void doCleanup(BooleanSupplier allowed, ByteBuffer buf) {
        if( ! allowed.getAsBoolean()) return;
        try {
            cleanMethod.invoke(cleanerMethod.invoke(buf));
        } catch (IllegalArgumentException | InvocationTargetException | SecurityException | IllegalAccessException exception) {
            logger.warn("Clean failed", exception);
        }
    }
}
