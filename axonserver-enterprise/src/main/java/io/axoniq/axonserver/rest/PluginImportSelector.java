package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.connector.EventConnector;
import io.axoniq.axonserver.connector.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.ImportSelector;
import org.springframework.core.type.AnnotationMetadata;

import java.util.HashSet;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * @author Marc Gathier
 */
public class PluginImportSelector implements ImportSelector {
    private Logger logger = LoggerFactory.getLogger(PluginImportSelector.class);
    @Override
    public String[] selectImports(AnnotationMetadata annotationMetadata) {
        logger.debug("Selecting imports");
        Set<String> classes = new HashSet<>();
        ServiceLoader.load(Page.class, Thread.currentThread().getContextClassLoader()).forEach(e -> {
            logger.info("Adding page {}", e.getClass().getName());
            classes.add(e.getClass().getName());
        });

        ServiceLoader.load(EventConnector.class, Thread.currentThread().getContextClassLoader()).forEach(e -> {
            logger.info("Adding connector {}", e.getClass().getName());
            classes.add(e.getClass().getName());
        });

        return classes.toArray(new String[classes.size()]);
    }
}
