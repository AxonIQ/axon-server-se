package io.axoniq.axonhub.licensing;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Reads a license property file and in this process also validates the signature on it.
 *
 * @author Frans van Buul
 */
public class LicensePropertyReader {

    private static final String PUBKEY =
            "MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEA0xyNKA5aL42X7eXy1zwe\n" +
            "9V6do3TYhrH7smWa6RBtCkhQ2holCEalrdiEX3LQoyhPmvV8lqWrDc9JHuYheWQL\n" +
            "pQXKB84sb9DCCWZWTPV0OZpe8nyotgmwBYohvEzTGLLRrAp+pM/J+/IVSiMyiP5E\n" +
            "Kf6ODcRWQH/us+4x4IsjTZC+o0HsYjSXG62Bo7pXFXLKjUqA3rpTyT1v3Yafgp4C\n" +
            "78wHa/fqKCE562B2IEEhxWdsJl//wOsk/I8bYH+YoZtceGpRJlkMjK3t/KOExU61\n" +
            "ae5NJruyXbqRBWOtrcBb37b2cgykqaZlCwQczsZwl8Pglm1Yl0t8lTdTM+wxLErI\n" +
            "AbxYE50UtvMLCaIG8lqT9W28UQgOr+RPdkEwUWYNeWWH2R0Kukva9loB+LBDe/Ce\n" +
            "YhRvh41KpekJhU0NYjymCizNFohQ0rUDtt8p+i/IpIxfWBtgJODOrP2tbr8necX8\n" +
            "X5oMyN4H/ar6favdWCHXi9FtTrHv1lchisXn3R9/obJptkxyZc8yvWuEBhXBFJ6H\n" +
            "ydOPNdbiWIH9TptZ2vaQrSFyaPR5yCoG/kyZ6o7TQE8lK6MrULiJNB/6ZKujri5x\n" +
            "LovNJrtY/w69qVkC/8lIJhwJMSJKySeUYBhOjVN4f7vVEVYncYx8HJU2utQ1j6+e\n" +
            "9T0pQ8CjhkOpmcTcaaMmU0UCAwEAAQ==";


    Properties readLicenseProperties() {
        String licenseFile = System.getProperty("license");
        if(licenseFile == null) {
            licenseFile = "axoniq.license";
            File file = new File(licenseFile);
            if( ! file.exists()) return null;
        }
        Properties licenseProperties = load(licenseFile);
        validate(licenseProperties);
        return licenseProperties;
    }

    private Properties load(String licenseFile) {
        File file = new File(licenseFile);
        Properties licenseProperties = new Properties();
        try {
            licenseProperties.load(new FileInputStream(file));
        } catch(IOException ex) {
            throw LicenseException.unableToRead(file);
        }
        return licenseProperties;
    }

    private void validate(Properties licenseProperties) {
        try {
            KeyFactory rsaKeyFactory = KeyFactory.getInstance("RSA");
            byte[] pubKeyBytes = Base64.getMimeDecoder().decode(PUBKEY);
            X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(pubKeyBytes);
            PublicKey pubKey = rsaKeyFactory.generatePublic(pubKeySpec);
            Signature rsaVerify = Signature.getInstance("SHA256withRSA");
            rsaVerify.initVerify(pubKey);
            List<String> propnames = new ArrayList<>(licenseProperties.stringPropertyNames());
            Collections.sort(propnames);
            if(!propnames.remove("signature")) throw LicenseException.wrongSignature("signature field missing");
            for(String propname : propnames) {
                rsaVerify.update(propname.getBytes(StandardCharsets.UTF_8));
                rsaVerify.update(licenseProperties.getProperty(propname, "").getBytes(StandardCharsets.UTF_8));
            }
            boolean verifies = rsaVerify.verify(Base64.getDecoder().decode(licenseProperties.getProperty("signature")));
            if(!verifies) {
                throw LicenseException.wrongSignature("signature invalid");
            }
        } catch (SignatureException ex) {
            throw LicenseException.wrongSignature("SignatureException: " + ex.getMessage());
        } catch (NoSuchAlgorithmException | InvalidKeySpecException | InvalidKeyException ex) {
            throw new Error("This should never happen", ex);
        }
    }

}
