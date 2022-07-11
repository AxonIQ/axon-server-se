/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;

import java.io.File;

public class UpgradeUtils {

    public static void fixInvalidFileName(long segment, StorageProperties storageProperties, String context) {
        File dataFile = storageProperties.oldDataFile(context, segment);
        if (dataFile.exists()) {
            if (!dataFile.renameTo(storageProperties.dataFile(context, segment))) {
                throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR,
                                                     renameMessage(dataFile,
                                                                   storageProperties.dataFile(context, segment)));
            }
            File indexFile = storageProperties.oldIndex(context, segment);
            if (indexFile.exists() && !indexFile.renameTo(storageProperties.index(context, segment))) {
                throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR,
                                                     renameMessage(indexFile,
                                                                   storageProperties.index(context, segment)));
            }
            File bloomFile = storageProperties.oldBloomFilter(context, segment);
            if (bloomFile.exists() && !bloomFile.renameTo(storageProperties.bloomFilter(context, segment))) {
                throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR,
                                                     renameMessage(bloomFile,
                                                                   storageProperties.bloomFilter(context, segment)));
            }
        }
    }

    private static String renameMessage(File from, File to) {
        return "Could not rename " + from.getAbsolutePath() + " to " + to.getAbsolutePath();
    }
}
