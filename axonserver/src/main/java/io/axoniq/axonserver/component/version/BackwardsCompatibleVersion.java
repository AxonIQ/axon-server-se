package io.axoniq.axonserver.component.version;

import static java.lang.Integer.parseInt;

/**
 * Implementation of {@link Version} useful in case of parallel release branches.
 * Indeed when more that one release branch coexist, it is possible to say if a version is greater than another version
 * only if the to versions derives from the same ancestor. In all the other case it is impossible to compare two
 * versions, and {@link BackwardsCompatibleVersion#greaterOrEqualThan(Version)} returns false.
 *
 * @author Sara Pellegrini
 * @since 4.2.3
 */
public class BackwardsCompatibleVersion implements Version {

    private final String name;

    /**
     * Builds a {@link BackwardsCompatibleVersion} with the specified version name.
     *
     * @param name the version name
     */
    public BackwardsCompatibleVersion(String name) {
        this.name = name;
    }

    /**
     * {@inheritDoc}
     *
     * @return the version name
     */
    @Override
    public String name() {
        return name;
    }

    /**
     * {@inheritDoc}
     * </p>
     * This implementation consider that a version is greater than another one, when the last specified number is
     * greater or equal than the corresponding value in the other version, while all the previous version numbers
     * are equal.
     * In other words, to make an example, 4.1.3 is greater than 4.1.2 but not than 4.3, because it belongs to another
     * release branch.
     *
     * @param version the other version to compare with
     * @return {@code true} if this version can be considered greater or equal than the specified one, {@code false} otherwise
     */
    @Override
    public boolean greaterOrEqualThan(Version version) {
        String[] split = version.name().split(DOT_REGEX);
        if (split.length < 1 || split.length > 3) {
            throw new IllegalArgumentException("Version class format is not compliant");
        }

        if (split.length == 3) {
            return parseInt(split[0]) == this.major() &&
                    parseInt(split[1]) == this.minor() &&
                    parseInt(split[2]) <= this.patch();
        }

        if (split.length == 2) {
            return parseInt(split[0]) == this.major() &&
                    parseInt(split[1]) <= this.minor();
        }

        return parseInt(split[0]) <= this.major();
    }
}
