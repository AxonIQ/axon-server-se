package io.axoniq.axonserver.localstorage.file;

/**
 * @author Stefan Dragisic
 */

import java.nio.channels.FileChannel;
import java.util.function.Supplier;

public interface Segment {


    Supplier<FileChannel> contentProvider();

    long id();
}
