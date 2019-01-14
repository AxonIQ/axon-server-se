package io.axoniq.axonserver.message.command;

import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Marc Gathier
 */
@Component
public class CommandCache extends ConcurrentHashMap<String, CommandInformation> {
}
