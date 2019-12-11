package io.axoniq.axonserver.extension.encryption;


import org.junit.*;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class EncryptingEventTransformerTest {

    private EncryptingEventTransformer testSubject = new EncryptingEventTransformer("context1");

    @Test
    public void testEncryptAndDecrypt() {
        byte[] encrypted = testSubject.toStorage("demo".getBytes());
        byte[] decrypted = testSubject.fromStorage(encrypted);

        assertEquals("demo", new String(decrypted));
    }
}