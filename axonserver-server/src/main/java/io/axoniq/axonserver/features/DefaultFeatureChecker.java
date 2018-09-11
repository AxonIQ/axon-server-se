package io.axoniq.axonserver.features;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Author: marc
 */
public class DefaultFeatureChecker implements FeatureChecker {

    @Override
    public boolean isBasic() {
        return true;
    }

    @Override
    public boolean isEnterprise() {
        return false;
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

    @Override
    public List<FeatureStatus> getFeatureList() {
        return Arrays.stream(Feature.values()).map(f -> new FeatureStatus(f, f.enabled(this))).collect(Collectors.toList());
    }
}
