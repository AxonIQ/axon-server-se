package io.axoniq.axonserver.licensing;

import io.axoniq.axonserver.rest.Feature;
import io.axoniq.axonserver.rest.FeatureStatus;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Author: marc
 */
@Component("Limits")
@Primary
public class Limits {
    public boolean isClusterAllowed() {
        return Feature.CLUSTERING.enabled(this);
    }
    public boolean isClusterAutobalancingEnabled() {
        return Feature.CONNECTION_BALANCING.enabled(this);
    }

    public int getMaxClusterSize() {
        if( Feature.CLUSTERING.enabled(this)) return LicenseConfiguration.getInstance().getClusterNodes();
        return 1;
    }

    public boolean isDeveloper() {
        return LicenseConfiguration.isDeveloper();
    }

    public boolean isEnterprise() {
        return LicenseConfiguration.isEnterprise();
    }

    public boolean isAddContextAllowed() {
        return Feature.MULTI_CONTEXT.enabled(this);
    }

    public int getMaxContexts() {
        if( ! Feature.MULTI_CONTEXT.enabled(this)) return 1;
        return LicenseConfiguration.getInstance().getContexts();
    }

    public List<FeatureStatus> getFeatureList() {
        return Arrays.stream(Feature.values()).map(f -> new FeatureStatus(f, f.enabled(this))).collect(Collectors.toList());
    }
}
