package io.axoniq.axonserver.enterprise.storage.file;

import org.springframework.stereotype.Component;

@Component
public class LicenseManager {

    private final String licenseDirectory = ".";
    private final String licenseSuffix = ".license";

    private void addOrUpdate(byte[] license) {

    }

}
