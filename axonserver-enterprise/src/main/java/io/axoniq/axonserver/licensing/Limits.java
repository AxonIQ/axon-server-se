package io.axoniq.axonserver.licensing;

import io.axoniq.axonserver.config.FeatureChecker;
import io.axoniq.axonserver.util.StringUtils;
import io.netty.util.internal.StringUtil;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 */
@Component
@Primary
@DependsOn({"licenseManager"})
public class Limits implements FeatureChecker  {

    private final LicenseManager licenseManager;

    public Limits(LicenseManager licenseManager) {
        this.licenseManager = licenseManager;
    }

    public int getMaxClusterSize() {
        try {
            if( Feature.CLUSTERING.enabled(this)) {
                return licenseManager.getLicenseConfiguration().getClusterNodes();
            } else {
                return 1;
            }
        } catch (Exception e) {
            return 1;
        }
    }

    @Override
    public boolean isBasic() {
        return true;
    }

    public boolean isEnterprise() {
        try {
            return licenseManager.getLicenseConfiguration().isEnterprise();
        } catch (Exception e) {
            return false;
        }
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
        try {
            return licenseManager.getLicenseConfiguration().getContexts();
        }
        catch (Exception e) {
           return 1;
        }
    }

    public List<String> getFeatureList() {
        try {
            return Arrays.stream(Feature.values())
                    .filter(f -> f.enabled(this))
                    .map(Enum::name)
                    .collect(Collectors.toList());
        } catch (Exception e){
            return Collections.emptyList();
        }
    }

    @Override
    public LocalDate getExpiryDate() {
        try {
            return licenseManager.getLicenseConfiguration().getExpiryDate();
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public String getEdition() {
        try {
            return licenseManager.getLicenseConfiguration().getEdition().displayName();
        } catch (Exception e) {
            return LicenseConfiguration.Edition.Standard.displayName();
        }
    }

    @Override
    public String getLicensee() {
        try {
            return licenseManager.getLicenseConfiguration().getLicensee();
        } catch (Exception e) {
            return StringUtil.EMPTY_STRING;
        }
    }
}
