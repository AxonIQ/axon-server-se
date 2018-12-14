package io.axoniq.axonserver.localstorage.file;

/**
 * Author: marc
 */
public class VersionUtils {
    public static boolean hasIndexField(byte version) {
        return version > 1;
    }

}
