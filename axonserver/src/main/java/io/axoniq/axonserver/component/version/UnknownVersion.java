package io.axoniq.axonserver.component.version;

/**
 * @author Sara Pellegrini
 * @since 4.2.3
 */
public class UnknownVersion implements Version {

    @Override
    public String name() {
        return "UNKNOWN";
    }

    @Override
    public int major() {
        throw new UnsupportedOperationException("This version is UNKNOWN.");
    }

    @Override
    public int minor() {
        throw new UnsupportedOperationException("This version is UNKNOWN.");
    }

    @Override
    public int patch() {
        throw new UnsupportedOperationException("This version is UNKNOWN.");
    }

    @Override
    public boolean match(Version version) {
        throw new UnsupportedOperationException("This version is UNKNOWN.");
    }

    @Override
    public boolean greaterOrEqualThan(String versionClass) {
        throw new UnsupportedOperationException("This version is UNKNOWN.");
    }
}
