/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.logging.AuditLog;
import io.swagger.v3.oas.annotations.Parameter;
import org.slf4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingClass;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.security.Principal;
import java.sql.SQLException;

/**
 * Rest Controller responsible to provide the endpoint for ControlDB backup.
 * This component is registered only in the standard version of Axon Server because
 * Axon Server Enterprise requires a specific implementation for controlDB backup.
 *
 * @author Sara Pellegrini
 * @since 4.1
 */
@ConditionalOnMissingClass("io.axoniq.axonserver.rest.ClusterBackupInfoRestController")
@RestController
public class BackupControlDBRestController {

    private static final Logger auditLog = AuditLog.getLogger();

    private final BackupInfoRestController backupInfoRestController;

    public BackupControlDBRestController(BackupInfoRestController backupInfoRestController) {
        this.backupInfoRestController = backupInfoRestController;
    }

    @PostMapping("/createControlDbBackup")
    public String createControlDbBackup(@Parameter(hidden = true) Principal principal) throws SQLException {
        auditLog.info("[{}] Request for a backup of the controlDB.", AuditLog.username(principal));

        return backupInfoRestController.createControlDbBackup();
    }

}
