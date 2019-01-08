package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.cluster.replication.file.FileSegmentLogEntryStore;
import io.axoniq.axonserver.enterprise.cluster.GrpcRaftController;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static io.axoniq.axonserver.topology.Topology.DEFAULT_CONTEXT;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
@RestController
@RequestMapping("/v1/backup")
public class BackupLogFilesRestController {

    private final GrpcRaftController grpcRaftController;

    public BackupLogFilesRestController(GrpcRaftController grpcRaftController) {
        this.grpcRaftController = grpcRaftController;
    }

    @GetMapping("/log/filenames")
    public List<String> getFilenames(@RequestParam(value = "context", defaultValue = DEFAULT_CONTEXT) String context) {
        LogEntryStore logEntryStore = grpcRaftController.getRaftGroup(context).localLogEntryStore();
        if (logEntryStore instanceof FileSegmentLogEntryStore) {
            return ((FileSegmentLogEntryStore)logEntryStore).getBackupFilenames().collect(Collectors.toList());
        }
        return Collections.emptyList();

    }

}
