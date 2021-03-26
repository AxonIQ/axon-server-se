package io.axoniq.axonserver.localstorage.file;

/**
 * This exception is thrown when it's impossible to open the segment, likely because it has been moved to secondary node.
 *
 * @author Sara Pellegrini
 * @author Stefan Dragisic
 * @since 4.5
 */
public class EventSourceNotFoundException extends Exception {

}
