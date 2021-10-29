/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.filestorage.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public class FileUtils {
    public static final Logger logger = LoggerFactory.getLogger(FileUtils.class);
    private FileUtils() {
    }

    public static void checkCreateDirectory(File events) {
        if (events.exists() && !events.isDirectory()) {
            throw new FileStoreException(FileStoreErrorCode.DIRECTORY_CREATION_FAILED,
                                                 "Could not setup directory " + events.getAbsolutePath());
        }
        if (!events.exists() && !events.mkdirs()) {
            throw new FileStoreException(FileStoreErrorCode.DIRECTORY_CREATION_FAILED,
                                                 "Could not setup directory " + events.getAbsolutePath());
        }
    }

    public static String[] getFilesWithSuffix(File events, String suffix) {
        String[] eventFiles = events.list((dir, name) -> name.endsWith(suffix));
        if (eventFiles == null) {
            throw new FileStoreException(FileStoreErrorCode.DATAFILE_READ_ERROR,
                                                 "Could not list files in " + events.getAbsolutePath());
        }
        return eventFiles;
    }

    public static boolean delete(File file) {
        if( ! file.exists()) return true;
        logger.debug("Delete file {}", file.getAbsolutePath());

        try {
            Files.delete(file.toPath());
        } catch (IOException e) {
            logger.warn("Failed to delete: {}", file, e);
            return false;
        }
        return true;
    }
}
