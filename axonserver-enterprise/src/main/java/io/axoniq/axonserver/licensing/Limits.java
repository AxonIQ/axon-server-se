package io.axoniq.axonserver.licensing;

import io.axoniq.axonserver.features.Feature;
import io.axoniq.axonserver.features.FeatureChecker;
import io.axoniq.axonserver.features.FeatureStatus;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Author: marc
 */
@Component
@Primary
public class Limits implements FeatureChecker  {
    public int getMaxClusterSize() {
        if( Feature.CLUSTERING.enabled(this)) return LicenseConfiguration.getInstance().getClusterNodes();
        return 1;
    }

    @Override
    public boolean isBasic() {
        return true;
    }

    public boolean isEnterprise() {
        return LicenseConfiguration.isEnterprise();
    }

    @Override
    public boolean isBigData() {
        return false;
    }

    @Override
    public boolean isScale() {
        return false;
    }

    @Override
    public boolean isSecurity() {
        return false;
    }

    public int getMaxContexts() {
        return LicenseConfiguration.getInstance().getContexts();
    }

    public List<FeatureStatus> getFeatureList() {
        return Arrays.stream(Feature.values()).map(f -> new FeatureStatus(f, f.enabled(this))).collect(Collectors.toList());
    }

    @Override
    public LocalDate getExpiryDate() {
        return LicenseConfiguration.getInstance().getExpiryDate();
    }

    @Override
    public String getEdition() {
        return LicenseConfiguration.getInstance().getEdition().name();
    }

    @Override
    public String getLicensee() {
        return LicenseConfiguration.getInstance().getLicensee();
    }
}
