package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.cluster.replication.file.FileSegmentLogEntryStore;
import io.axoniq.axonserver.enterprise.cluster.GrpcRaftController;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static io.axoniq.axonserver.topology.Topology.DEFAULT_CONTEXT;

/**
 * Rest Controller responsible to expose backup endpoints for log entry files and controlDB.
 *
 * @author Sara Pellegrini
 * @since 4.1
 */
@RestController
@RequestMapping("/v1/backup")
public class ClusterBackupInfoRestController {

    private final BackupInfoRestController backupInfoRestController;
    private final GrpcRaftController grpcRaftController;

    public ClusterBackupInfoRestController(BackupInfoRestController backupInfoRestController,
                                           GrpcRaftController grpcRaftController) {
        this.backupInfoRestController = backupInfoRestController;
        this.grpcRaftController = grpcRaftController;
    }

    /**
     * Returns the list of log entry files to backup.
     *
     * @param context of log entry files
     * @return the list of the log entry files that should be backed up
     */
    @GetMapping("/log/filenames")
    public List<String> getFilenames(@RequestParam(value = "context", defaultValue = DEFAULT_CONTEXT) String context) {
        LogEntryStore logEntryStore = grpcRaftController.getRaftGroup(context).localLogEntryStore();
        return logEntryStore.getBackupFilenames().collect(Collectors.toList());
    }

    /**
     * Makes a safe export of controlDB and returns the location of the the full path to the export file.
     * The execution of this method guarantees that the log cleaning procedure is not triggered in the next hour,
     * to extend the temporal windows suitable to perform a consistent backup of log entry files.
     *
     * @return the full path to the export file
     *
     * @throws SQLException if a database access error occurs
     */
    @PostMapping("/createControlDbBackup")
    public String createControlDbBackup() throws SQLException {
        try {
            return backupInfoRestController.createControlDbBackup();
        } finally {
            grpcRaftController.getMyContexts()
                              .forEach(c -> grpcRaftController.getRaftGroup(c).localNode().restartLogCleaning());
        }
    }
}
