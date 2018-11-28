package io.axoniq.axonserver.cluster.replication.file;

import io.axoniq.axonserver.cluster.exception.ErrorCode;
import io.axoniq.axonserver.cluster.exception.LogException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * Author: marc
 */
public class FileUtils {
    public static final Logger logger = LoggerFactory.getLogger(FileUtils.class);
    private FileUtils() {

    }

    static void checkCreateDirectory(File events) {
        if( events.exists() && ! events.isDirectory()) {
            throw new LogException(ErrorCode.DIRECTORY_CREATION_FAILED, "Could not setup directory " + events.getAbsolutePath());
        }
        if( !events.exists() && ! events.mkdirs()) {
            throw new LogException(ErrorCode.DIRECTORY_CREATION_FAILED, "Could not setup directory " + events.getAbsolutePath());
        }
    }

    static String[] getFilesWithSuffix(File events, String suffix) {
        String[] eventFiles = events.list((dir, name) -> name.endsWith(suffix));
        if( eventFiles == null) {
            throw new LogException(ErrorCode.DATAFILE_READ_ERROR, "Could not list files in " + events.getAbsolutePath());
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
