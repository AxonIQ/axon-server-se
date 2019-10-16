package io.axoniq.axonserver.component.version;

import static java.lang.Integer.parseInt;

/**
 * @author Sara Pellegrini
 * @since 4.2.1
 */
public interface Version {

    String DOT_REGEX = "\\.";
    String DASH_REGEX = "-";

    String name();

    default int major() {
        return parseInt(name().split(DASH_REGEX)[0].split(DOT_REGEX)[0]);
    }

    default int minor() {
        String[] split = name().split(DASH_REGEX)[0].split(DOT_REGEX);
        return (split.length >= 2) ? parseInt(split[1]) : 0;
    }

    default int patch() {
        String[] split = name().split(DASH_REGEX)[0].split(DOT_REGEX);
        return (split.length >= 3) ? parseInt(split[2]) : 0;
    }

    default boolean match(Version version) {
        return name().equals(version.name());
    }

    boolean greaterOrEqualThan(String versionClass);
}
