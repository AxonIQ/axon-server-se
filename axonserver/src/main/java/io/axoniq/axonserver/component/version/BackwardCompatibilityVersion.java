package io.axoniq.axonserver.component.version;

import javax.annotation.Nonnull;

import static java.lang.Integer.parseInt;

/**
 * @author Sara Pellegrini
 * @since 4.2.2
 */
public class BackwardCompatibilityVersion implements Version {

    @Nonnull
    private final String name;

    public BackwardCompatibilityVersion(@Nonnull String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean greaterOrEqualThan(String versionClass) {
        String[] split = versionClass.split(DOT_REGEX);
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
