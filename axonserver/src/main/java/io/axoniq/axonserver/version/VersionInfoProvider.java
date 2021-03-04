/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.version;

import javax.annotation.Nonnull;

/**
 * Provides information on the current version of the product.
 *
 * @author Marc Gathier
 * @since 4.2.4
 */
public interface VersionInfoProvider {

    /**
     * Returns the version information.
     *
     * @return the product name and version
     */
    @Nonnull
    VersionInfo get();
}
