/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingClass;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

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

    private final BackupInfoRestController backupInfoRestController;

    public BackupControlDBRestController(BackupInfoRestController backupInfoRestController) {
        this.backupInfoRestController = backupInfoRestController;
    }

    @PostMapping("/createControlDbBackup")
    public String createControlDbBackup() throws SQLException {
        return backupInfoRestController.createControlDbBackup();
    }

}
