/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * @author Marc Gathier
 */
public class FileUtils {

    public static final Logger logger = LoggerFactory.getLogger(FileUtils.class);

    private FileUtils() {

    }

    public static void checkCreateDirectory(File events) {
        if (events.exists() && !events.isDirectory()) {
            throw new MessagingPlatformException(ErrorCode.DIRECTORY_CREATION_FAILED,
                                                 "Could not setup directory " + events.getAbsolutePath());
        }
        if (!events.exists() && !events.mkdirs()) {
            throw new MessagingPlatformException(ErrorCode.DIRECTORY_CREATION_FAILED,
                                                 "Could not setup directory " + events.getAbsolutePath());
        }
    }

    public static String[] getFilesWithSuffix(File events, String suffix) {
        String[] eventFiles = events.list((dir, name) -> name.endsWith(suffix));
        if (eventFiles == null) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR,
                                                 "Could not list files in " + events.getAbsolutePath());
        }
        return eventFiles;
    }

    public static FileVersion process(String name) {
        String baseName = name.substring(0, name.indexOf('.'));
        int separator = baseName.indexOf('_');
        if (separator < 0) {
            return new FileVersion(Long.parseLong(baseName), 0);
        }
        return new FileVersion(Long.parseLong(baseName.substring(0, separator)),
                               Integer.parseInt(baseName.substring(separator + 1)));
    }

    public static boolean delete(File file) {
        if (!file.exists()) {
            return true;
        }
        logger.debug("Delete file {}", file.getAbsolutePath());

        try {
            Files.delete(file.toPath());
        } catch (IOException e) {
            logger.warn("Failed to delete: {}", file, e);
            return false;
        }
        return true;
    }

    public static Mono<Void> rename(File source, File target) {
        return Mono.<Void>create(sink -> {
            try {
                Files.move(source.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
                sink.success();
            } catch (Exception e) {
                sink.error(e);
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    public static String name(File file) {
        try {
            return file
                    .getCanonicalFile()
                    .getAbsolutePath();
        } catch (IOException e) {
            return file.getAbsolutePath();
        }
    }

}
