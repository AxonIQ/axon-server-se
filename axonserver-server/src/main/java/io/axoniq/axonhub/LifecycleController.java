package io.axoniq.axonhub;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * Author: marc
 */
@Component
public class LifecycleController {
    private static final String EVENT_STORE_SERVER_PID = "AxonIQ.pid";
    private final Logger logger = LoggerFactory.getLogger(LifecycleController.class);
    private boolean cleanShutdown=false;
    private final Path path;
    private final ApplicationContext applicationContext;

    public LifecycleController(ApplicationContext applicationContext) {
        this.path = Paths.get(EVENT_STORE_SERVER_PID);
        this.applicationContext = applicationContext;
    }

    @PostConstruct
    public void checkAndCreatePidFile() throws IOException {
        if (! path.toFile().exists()) {
            cleanShutdown = true;
        } else {
            Files.delete(path);
        }

        String jvmName = ManagementFactory.getRuntimeMXBean().getName();

        try(OutputStream is = Files.newOutputStream(path, StandardOpenOption.CREATE) ) {
            is.write(jvmName.getBytes(Charset.forName("UTF-8")));
        }
    }

    @PreDestroy
    public void removePidFile() {
        try {
            if( cleanShutdown) Files.delete(path);
        } catch (IOException e) {
            logger.info("Failed to remove {}", path);
        }
    }

    public boolean isCleanShutdown() {
        return cleanShutdown;
    }

    public void setCleanShutdown() {
        this.cleanShutdown = true;
    }

    Path getPath() {
        return path;
    }

    public void abort() {
        SpringApplication.exit(applicationContext, () -> {
            System.exit(99);
            return 99;
        });
    }

    public void configurationError(String parameter) {
        logger.error("Cannot start AxonDB: Missing required configuration parameter: {}", parameter);
        SpringApplication.exit(applicationContext, () -> {
            System.exit(1);
            return 1;
        });
    }
    public void licenseError(String parameter) {
        logger.error(parameter);
        SpringApplication.exit(applicationContext, () -> {
            System.exit(1);
            return 1;
        });
    }

}
