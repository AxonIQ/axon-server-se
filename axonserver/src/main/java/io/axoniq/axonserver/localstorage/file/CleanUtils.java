/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * @author Marc Gathier
 * @since 4.0
 */
public class CleanUtils {
    private static final Logger logger = LoggerFactory.getLogger(CleanUtils.class);
    private static final ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor(new CustomizableThreadFactory("fileCleaner") {
        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = super.newThread(runnable);
            thread.setDaemon(true);
            return thread;
        }
    });
    private static final int RETRIES = 5;
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

    public static void cleanDirectBuffer(ByteBuffer buf, BooleanSupplier allowed, long delay, String file) {
        if (buf != null) {
            if (delay <= 0) {
                doCleanup(allowed, buf, 10, file, RETRIES);
            } else {
                try {
                    cleanupExecutor.schedule(() -> doCleanup(allowed, buf, delay, file, RETRIES), delay, TimeUnit.SECONDS);
                } catch( Exception ignore) {
                    //may be as executor is shutdown
                }
            }
        }
    }

    private static void doCleanup(BooleanSupplier allowed, ByteBuffer buf, long delay, String file, int retries) {
        if( ! allowed.getAsBoolean()) {
            if( retries > 0) {
                logger.debug("Memory mapped buffer not cleared for {}, retry after {} seconds", file, delay);
                cleanupExecutor.schedule(() -> doCleanup(allowed, buf, delay, file, retries-1), delay, TimeUnit.SECONDS);
            } else {
                logger.debug("Memory mapped buffer not cleared for {}, giving up", file);
            }
            return;
        }
        try {
            if (java8) {
                cleanOldsJDK(buf);
            } else {
                cleanJavaWithModules(buf);
            }
            logger.debug("Memory mapped buffer cleared for {}", file);
        } catch (Exception exception) {
            logger.warn("Clean failed", exception);
        }
    }
}
