package io.axoniq.axonserver.access.application;

import io.axoniq.axonserver.LifecycleController;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.UUID;
import javax.annotation.PostConstruct;

/**
 * Default implementation for {@link SystemTokenProvider}. Tries reading system token from file specified in property
 * "systemtokenfile",
 * if property not set it will generate a property and store that in tokenDir directory (in file .token).
 * Aborts Axon Server when it is not able to get the system token (and access control is enabled).
 *
 * @author Marc Gathier
 * @since 4.3
 */
@Component
public class DefaultSystemTokenProvider implements SystemTokenProvider {

    private final Logger logger = LoggerFactory.getLogger(DefaultSystemTokenProvider.class);

    private final LifecycleController lifecycleController;
    private final String systemTokenDir;
    private final String systemTokenFile;
    private final boolean accessControlEnabled;

    private final String[] systemToken = new String[1];

    /**
     * Constructor for testing purposes
     *
     * @param lifecycleController  provides the abort operation in case of error
     * @param systemTokenDir       location for the generated token file
     * @param systemTokenFile      location of the specified token file
     * @param accessControlEnabled indicates if access control is enabled
     */
    public DefaultSystemTokenProvider(LifecycleController lifecycleController,
                                      String systemTokenDir,
                                      String systemTokenFile,
                                      boolean accessControlEnabled) {
        this.lifecycleController = lifecycleController;
        this.systemTokenDir = systemTokenDir;
        this.systemTokenFile = systemTokenFile;
        this.accessControlEnabled = accessControlEnabled;
    }

    /**
     * Autowired constructor.
     *
     * @param messagingPlatformConfiguration Axon Server configuration
     * @param systemTokenDir                 location for the generated token file
     * @param systemTokenFile                location of the specified token file
     * @param lifecycleController            provides the abort operation in case of error
     */
    @Autowired
    public DefaultSystemTokenProvider(
            MessagingPlatformConfiguration messagingPlatformConfiguration,
            @Value("${axoniq.axonserver.accesscontrol.token-dir:security}") String systemTokenDir,
            @Value("${axoniq.axonserver.accesscontrol.systemtokenfile:#{null}}") String systemTokenFile,
            LifecycleController lifecycleController) {
        this(lifecycleController, systemTokenDir, systemTokenFile,
             messagingPlatformConfiguration.getAccesscontrol().isEnabled());
    }


    @PostConstruct
    void generateSystemToken() {
        if (!accessControlEnabled) {
            return;
        }
        try {
            if (systemTokenFile != null) {
                readFromFile();
            } else {
                File securityDir = new File(systemTokenDir);
                if (!securityDir.exists()) {
                    if (!securityDir.mkdirs()) {
                        throw new IOException(String.format("Cannot create directory for system token: %s",
                                                            securityDir));
                    }
                } else {
                    if (!securityDir.isDirectory()) {
                        throw new IOException(String.format("System token directory is not a directory: %s",
                                                            securityDir));
                    }
                }

                try (FileWriter tokenFile = new FileWriter(new File(
                        securityDir.getAbsolutePath() + File.separator + ".token"))) {
                    systemToken[0] = UUID.randomUUID().toString();
                    tokenFile.write(systemToken[0]);
                    tokenFile.write('\n');
                }
            }
        } catch (IOException ioException) {
            logger.error("Initialization of system token failed - {}", ioException.getMessage());
            lifecycleController.abort();
        }
    }

    private void readFromFile() throws IOException {
        File systemTokenFile0 = new File(systemTokenFile);
        if (!systemTokenFile0.exists() || !systemTokenFile0.canRead()) {
            throw new IOException(String.format("Cannot read file for system token in: %s", systemTokenFile));
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(systemTokenFile0))) {
            String line = reader.readLine();
            while (line != null && notToken(line)) {
                line = reader.readLine();
            }
            if (line == null) {
                throw new IOException(String.format("Could not read token in: %s", systemTokenFile));
            }
            systemToken[0] = line;
        }
    }

    private boolean notToken(String line) {
        return line.length() == 0 || line.startsWith("#");
    }

    @Override
    public String get() {
        return systemToken[0];
    }
}
