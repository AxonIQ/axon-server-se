/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

/**
 * @author Marc Gathier
 */
public interface ExtensionPackageRepository extends JpaRepository<ExtensionPackage, Long> {

    Optional<ExtensionPackage> findByExtensionAndVersion(String extension, String version);
}
