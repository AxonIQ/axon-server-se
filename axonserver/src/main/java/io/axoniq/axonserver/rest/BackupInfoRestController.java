package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.topology.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import javax.sql.DataSource;

@RestController
@RequestMapping("/v1/backup")
public class BackupInfoRestController {
    private final Logger log = LoggerFactory.getLogger(BackupInfoRestController.class);

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
    public List<String> getFilenames(
            @RequestParam(value = "context", defaultValue = Topology.DEFAULT_CONTEXT) String context,
            @RequestParam(value = "type") String type,
            @RequestParam(value = "lastSegmentBackedUp", required = false, defaultValue = "-1") long lastSegmentBackedUp
    ) {
        return localEventStore
                .getBackupFilenames(context, EventType.valueOf(type), lastSegmentBackedUp)
                .collect(Collectors.toList());
    }

    @PostMapping("/createControlDbBackup")
    public String createControlDbBackup() throws SQLException {
        File path = new File(controlDbBackupLocation);
        if( ! path.exists() && ! path.mkdirs()) {
            throw new MessagingPlatformException(ErrorCode.OTHER, "Failed to create directory: " + path.getAbsolutePath());
        }
        File file = new File(path.getAbsolutePath() + "/controldb" + System.currentTimeMillis() + ".zip");
        try(Connection connection = dataSource.getConnection()) {
            try(PreparedStatement preparedStatement = connection.prepareStatement("BACKUP TO '" + file.getAbsolutePath() + "'")) {
                preparedStatement.execute();
            }
        }
        return file.getAbsolutePath();
    }
}
