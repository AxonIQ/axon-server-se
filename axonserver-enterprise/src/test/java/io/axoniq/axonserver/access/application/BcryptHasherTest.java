package io.axoniq.axonserver.access.application;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class BcryptHasherTest {
    @Test
    public void checkpw() throws Exception {
        BcryptHasher hasher = new BcryptHasher();
        String hashed = hasher.hash("password");
        System.out.println(hashed);
        assertTrue(hasher.checkpw("password", hashed));
    }
    @Test
    public void checkInvalid() throws Exception {
        BcryptHasher hasher = new BcryptHasher();
        assertFalse(hasher.checkpw("password", "aaaaaaaaaaaaaaaa"));
    }

}
