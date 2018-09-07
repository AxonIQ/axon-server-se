package io.axoniq.platform.application;

import org.mindrot.jbcrypt.BCrypt;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.stereotype.Component;

/**
 * Created by marc on 7/14/2017.
 */
@Component
@ConditionalOnClass(BCrypt.class)
public class BcryptHasher implements Hasher {
    private static final String PREFIX = "{BCrypt}";
    @Override
    public String hash(String token) {
        return PREFIX + BCrypt.hashpw(token, BCrypt.gensalt());
    }

    @Override
    public boolean checkpw(String candidate, String hashedToken) {
        if( hashedToken == null || ! hashedToken.startsWith(PREFIX)) return false;
        return BCrypt.checkpw(candidate, hashedToken.substring(PREFIX.length()));
    }
}
