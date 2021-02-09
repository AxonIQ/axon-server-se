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

import java.util.List;
import java.util.Optional;

/**
 * @author Marc Gathier
 */
public interface ExtensionStatusRepository extends JpaRepository<ExtensionStatus, Long> {

    Optional<ExtensionStatus> findByContextAndExtension(String context, ExtensionPackage extension);

    Optional<ExtensionStatus> findByContextAndExtension_ExtensionAndActive(String context, String extension,
                                                                           boolean active);

    List<ExtensionStatus> findAllByContextIn(List<String> context);

    List<ExtensionStatus> findAllByExtension(ExtensionPackage extension);
}