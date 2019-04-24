package io.axoniq.axonserver.licensing;

import io.axoniq.axonserver.config.FeatureChecker;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
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

    public List<String> getFeatureList() {
        return Arrays.stream(Feature.values())
                     .filter(f -> f.enabled(this))
                     .map(Enum::name)
                     .collect(Collectors.toList());
    }

    @Override
    public LocalDate getExpiryDate() {
        return LicenseConfiguration.getInstance().getExpiryDate();
    }

    @Override
    public String getEdition() {
        return LicenseConfiguration.getInstance().getEdition().displayName();
    }

    @Override
    public String getLicensee() {
        return LicenseConfiguration.getInstance().getLicensee();
    }
}
