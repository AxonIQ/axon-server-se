/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

import java.time.LocalDate;
import java.util.Collections;
import java.util.List;

/**
 * @author Marc Gathier
 */
public interface FeatureChecker {

    default boolean isBasic() {
        return true;
    }

    default boolean isEnterprise() {
        return false;
    }

    default boolean isBigData() {
        return false;
    }

    default boolean isScale() {
        return false;
    }

    default boolean isSecurity() {
        return false;
    }

    default List<String> getFeatureList() {
        return Collections.emptyList();
    }

    default LocalDate getExpiryDate() {
        return null;
    }

    default String getEdition() {
        return "Standard Edition";
    }

    default String getLicensee() {
        return null;
    }

    default int getMaxClusterSize() {
        return 1;
    }

    default int getMaxContexts() {
        return 1;
    }
}
