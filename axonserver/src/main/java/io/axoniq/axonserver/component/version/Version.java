/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.version;

import static java.lang.Integer.parseInt;

/**
 * Represents an artifact version.
 *
 * @author Sara Pellegrini
 * @since 4.2.3
 */
public interface Version {

    /**
     * Regex representing a dot.
     */
    String DOT_REGEX = "\\.";

    /**
     * Regex representing a dash.
     */
    String DASH_REGEX = "-";

    /**
     * Returns the full version name, that can be composed by a major number, a minor number and optionally a patch
     * number, all separated by dots. The full name can also contain a tag at the end, separated from the previous part
     * by a dash.
     *
     * @return the version name
     */
    String name();

    /**
     * Returns the major number of the version
     *
     * @return the major number
     */
    default int major() {
        try {
            return parseInt(name().split(DASH_REGEX)[0].split(DOT_REGEX)[0]);
        } catch (NumberFormatException e) {
            throw new IllegalStateException("The version name is not consistent with version pattern.");
        }
    }

    /**
     * Returns the minor number of the version
     * @return the minor number
     */
    default int minor() {
        String[] split = name().split(DASH_REGEX)[0].split(DOT_REGEX);
        return (split.length >= 2) ? parseInt(split[1]) : 0;
    }

    /**
     * Returns the patch number of the version
     * @return the patch number
     */
    default int patch() {
        String[] split = name().split(DASH_REGEX)[0].split(DOT_REGEX);
        return (split.length >= 3) ? parseInt(split[2]) : 0;
    }

    /**
     * Returns {@code true} if the two objects represent the same version.
     * @param version the second version
     * @return {@code true} if this instance represent the same version of the argument, {@link false} otherwise
     */
    default boolean match(Version version) {
        return name().equals(version.name());
    }

    /**
     * Returns {@code true} if this version can be considered greater or equal than the specified one.
     *
     * @param version the other version to compare with
     * @return {@code true} if this version can be considered greater or equal than the specified one, {@code false}
     * otherwise
     */
    boolean greaterOrEqualThan(Version version);

    default boolean greaterOrEqualThan(Iterable<Version> versions) {
        for (Version supportedVersion : versions) {
            try {
                if (this.greaterOrEqualThan(supportedVersion)) {
                    return true;
                }
            } catch (UnsupportedOperationException e1) {
                return false;
            }
        }
        return false;
    }
}
