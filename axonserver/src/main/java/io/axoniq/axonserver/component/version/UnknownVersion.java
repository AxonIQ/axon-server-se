package io.axoniq.axonserver.component.version;

/**
 * Implementation of {@link Version} useful when the version is UNKNOWN.
 *
 * @author Sara Pellegrini
 * @since 4.2.3
 */
public class UnknownVersion implements Version {

    /**
     * {@inheritDoc}
     *
     * @return UNKNOWN
     */
    @Override
    public String name() {
        return "UNKNOWN";
    }

    /**
     * {@inheritDoc}
     * @return nothing, as the version is unknown
     * @throws UnsupportedOperationException
     */
    @Override
    public int major() {
        throw new UnsupportedOperationException("This version is UNKNOWN.");
    }

    /**
     * {@inheritDoc}
     * @return nothing, as the version is unknown
     * @throws UnsupportedOperationException
     */
    @Override
    public int minor() {
        throw new UnsupportedOperationException("This version is UNKNOWN.");
    }

    /**
     * {@inheritDoc}
     * @return nothing, as the version is unknown
     * @throws UnsupportedOperationException
     */
    @Override
    public int patch() {
        throw new UnsupportedOperationException("This version is UNKNOWN.");
    }

    /**
     * {@inheritDoc}
     * @return nothing, as the version is unknown it is impossible to know if two version are the same
     * @throws UnsupportedOperationException
     */
    @Override
    public boolean match(Version version) {
        throw new UnsupportedOperationException("This version is UNKNOWN.");
    }

    /**
     * {@inheritDoc}
     * @return nothing, as the version is unknown it is impossible to know if one version is greater than another one
     * @throws UnsupportedOperationException
     */
    @Override
    public boolean greaterOrEqualThan(Version version) {
        throw new UnsupportedOperationException("This version is UNKNOWN.");
    }
}
