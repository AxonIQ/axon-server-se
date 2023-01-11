package io.axoniq.axonserver.localstorage.file;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

/**
 * @author Marc Gathier
 */
public class FileVersion implements Comparable<FileVersion>{
    private final long segment;
    private final int version;

    public FileVersion(long segment, int version) {
        this.segment = segment;
        this.version = version;
    }

    public long segment() {
        return segment;
    }

    public int segmentVersion() {
        return version;
    }

    @Override
    public int compareTo(@NotNull FileVersion o) {
        int compare = Long.compare(segment, o.segment);
        if ( compare == 0) return Integer.compare(version, o.version);
        return compare;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileVersion that = (FileVersion) o;
        return segment == that.segment && version == that.version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(segment, version);
    }

    @Override
    public String toString() {
        return segment + "/" + version;
    }
}
