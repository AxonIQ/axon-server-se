package io.axoniq.axonserver.extension.encryption;

import io.axoniq.axonserver.localstorage.transformation.EventTransformer;

/**
 * Simple event transformer to simulate encryption of data. This implementation just writes the context specific key
 * and then the reversed bytes.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public class EncryptingEventTransformer implements EventTransformer {

    private final byte[] key;

    /**
     * Constructor for transformer.
     *
     * @param key the specific key fot the context
     */
    public EncryptingEventTransformer(String key) {
        this.key = key.getBytes();
    }

    /**
     * Transforms the stored data to unencrypted data
     * @param eventBytes bytes as stored
     * @return the unencrypted data
     */
    @Override
    public byte[] fromStorage(byte[] eventBytes) {
        byte[] decrypted = new byte[eventBytes.length - key.length];

        for (int i = 0; i < decrypted.length; i++) {
            decrypted[i] = eventBytes[eventBytes.length - i - 1];
        }

        return decrypted;
    }

    /**
     * Transforms original data to encrypted data.
     * @param bytes the original data
     * @return the encrypted data
     */
    @Override
    public byte[] toStorage(byte[] bytes) {
        byte[] encrypted = new byte[bytes.length + key.length];

        for (int i = 0; i < key.length; i++) {
            encrypted[i] = key[i];
        }

        for (int i = 0; i < bytes.length; i++) {
            encrypted[encrypted.length - 1 - i] = bytes[i];
        }

        return encrypted;
    }
}
