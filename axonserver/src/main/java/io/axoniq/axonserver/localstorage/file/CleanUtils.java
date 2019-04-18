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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * @author Marc Gathier
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
    private static final int RETRIES = 5;

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
            if( logger.isDebugEnabled()) {
                logger.warn("Unable to configure clean method. If you are running on Windows with a Java version 9 or higher start Axon Server with --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED", methodException);
            } else {
                logger.warn("Unable to configure clean method. If you are running on Windows with a Java version 9 or higher start Axon Server with --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED");
            }
            cleanerMethodLocal = null;
            cleanMethodLocal = null;
        }

        cleanerMethod = cleanerMethodLocal;
        cleanMethod = cleanMethodLocal;
    }
    private CleanUtils() {
    }

    public static void cleanDirectBuffer(ByteBuffer buf, BooleanSupplier allowed, long delay, String file) {
        if (cleanMethod != null && buf != null) {
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
            cleanMethod.invoke(cleanerMethod.invoke(buf));
            logger.debug("Memory mapped buffer cleared for {}", file);
        } catch (IllegalArgumentException | InvocationTargetException | SecurityException | IllegalAccessException exception) {
            logger.warn("Clean failed", exception);
        }
    }
}
