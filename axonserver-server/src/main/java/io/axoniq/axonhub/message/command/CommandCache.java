package io.axoniq.axonhub.message.command;

import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Author: marc
 */
@Component("CommandCache")
public class CommandCache extends ConcurrentHashMap<String, CommandInformation> {
}
