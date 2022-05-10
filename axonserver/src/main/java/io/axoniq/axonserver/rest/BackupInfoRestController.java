/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.logging.AuditLog;
import io.axoniq.axonserver.topology.Topology;
import io.swagger.v3.oas.annotations.Parameter;
import org.slf4j.Logger;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.io.File;
import java.security.Principal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static io.axoniq.axonserver.util.StringUtils.sanitize;

/**
 * REST Controller to retrieve files for backup and create backup of controldb.
 *
 * @author Marc Gathier
 * @since 4.0
 */
@RestController
@RequestMapping("/v1/backup")
public class BackupInfoRestController {

    private static final Logger auditLog = AuditLog.getLogger();

    private final DataSource dataSource;
    private final LocalEventStore localEventStore;
    private final String controlDbBackupLocation;

    public BackupInfoRestController(DataSource dataSource,
                                    MessagingPlatformConfiguration messagingPlatformConfiguration,
                                    LocalEventStore localEventStore) {
        this.dataSource = dataSource;
        this.localEventStore = localEventStore;
        this.controlDbBackupLocation = messagingPlatformConfiguration.getControldbBackupLocation();
    }

    @GetMapping("/filenames")
    @Deprecated
    public List<String> getFilenames(
            @RequestParam(value = "context", defaultValue = Topology.DEFAULT_CONTEXT) String context,
            @RequestParam(value = "type") String type,
            @RequestParam(value = "lastSegmentBackedUp", required = false, defaultValue = "-1") long lastSegmentBackedUp,
            @Parameter(hidden = true) Principal principal) {
        if (auditLog.isInfoEnabled()) {
            auditLog.info("[{}] Request for event store backup filenames. Context=\"{}\", type=\"{}\"",
                          AuditLog.username(principal),
                          sanitize(context),
                          sanitize(type));
        }

        return localEventStore
                .getBackupFilenames(context, EventType.valueOf(type), lastSegmentBackedUp, false)
                .collect(Collectors.toList());
    }

    @GetMapping("/eventstore")
    public EventStoreBackupInfo eventStoreFilenames(
            @RequestParam(value = "targetContext", defaultValue = Topology.DEFAULT_CONTEXT) String context,
            @RequestParam(value = "type") String type,
            @RequestParam(value = "lastClosedSegmentBackedUp", required = false, defaultValue = "-1") long lastSegmentBackedUp,
            @Parameter(hidden = true) Principal principal) {
        if (auditLog.isInfoEnabled()) {
            auditLog.info("[{}] Request for event store backup filenames. Context=\"{}\", type=\"{}\"",
                          AuditLog.username(principal),
                          sanitize(context),
                          sanitize(type));
        }

        return new EventStoreBackupInfo(localEventStore.getFirstCompletedSegment(context, EventType.valueOf(type)),
                                                                             localEventStore
                                                                                     .getBackupFilenames(context, EventType.valueOf(type), lastSegmentBackedUp, true)
                                                                                     .collect(Collectors.toList()));
    }

    /**
     * Makes a safe export of controlDB and returns the location of the the full path to the export file.
     *
     * @return the full path to the export file
     *
     * @throws SQLException if a database access error occurs
     */
    public String createControlDbBackup() throws SQLException {
        File path = new File(controlDbBackupLocation);
        if (!path.exists() && !path.mkdirs()) {
            throw new MessagingPlatformException(ErrorCode.OTHER,
                                                 "Failed to create directory: " + path.getAbsolutePath());
        }
        File file = new File(path.getAbsolutePath() + "/controldb" + System.currentTimeMillis() + ".zip");
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(
                    "BACKUP TO '" + file.getAbsolutePath() + "'")) {
                preparedStatement.execute();
            }
        }
        return file.getAbsolutePath();
    }

    private class EventStoreBackupInfo {

        private final long lastClosedSegment;
        private final List<String> filenames;

        public EventStoreBackupInfo(long lastClosedSegment, List<String> filenames) {
            this.lastClosedSegment = lastClosedSegment;
            this.filenames = filenames;
        }

        public long getLastClosedSegment() {
            return lastClosedSegment;
        }

        public List<String> getFilenames() {
            return filenames;
        }
    }
}
