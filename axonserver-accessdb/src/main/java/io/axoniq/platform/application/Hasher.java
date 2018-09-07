package io.axoniq.platform.application;

/**
 * Created by marc on 7/14/2017.
 */
public interface Hasher {
    String hash(String token);

    boolean checkpw(String token, String hashedToken);
}
