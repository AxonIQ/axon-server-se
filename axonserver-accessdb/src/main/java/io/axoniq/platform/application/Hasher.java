package io.axoniq.platform.application;

/**
 * @author Marc Gathier
 */
public interface Hasher {
    String hash(String token);

    boolean checkpw(String token, String hashedToken);
}
