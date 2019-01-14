package io.axoniq.platform.application;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingClass;
import org.springframework.stereotype.Component;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author Marc Gathier
 */
@Component
@ConditionalOnMissingClass("org.mindrot.jbcrypt.BCrypt")
public class ShaHasher implements Hasher {

    private static final String PREFIX = "{SHA-1}";

    @Override
    public String hash(String token) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-1");
            byte[] bytes = digest.digest(token.getBytes());
            return PREFIX + DatatypeConverter.printHexBinary(bytes);
        } catch (NoSuchAlgorithmException e) {
            return token;
        }
    }

    @Override
    public boolean checkpw(String candidate, String hashedToken) {
        if( hashedToken == null || ! hashedToken.startsWith(PREFIX)) return false;

        return hash(candidate).equals(hashedToken);
    }

}
