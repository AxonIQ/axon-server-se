package io.axoniq.axonserver.features;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Author: marc
 */
public interface FeatureChecker {

    default boolean isBasic() {
        return true;
    }

    default boolean isEnterprise() {
        return false;
    }

    default boolean isBigData() {
        return false;
    }

    default boolean isScale() {
        return false;
    }

    default boolean isSecurity() {
        return false;
    }

    default List<FeatureStatus> getFeatureList() {
        return Arrays.stream(Feature.values())
                     .map(feature -> new FeatureStatus(feature, feature.enabled(this)))
                     .collect(Collectors.toList());
    }

    default LocalDate getExpiryDate() {
        return null;
    }

    default String getEdition() {
        return "Free edition";
    }

    default String getLicensee() {
        return null;
    }

    default int getMaxClusterSize() {
        return 1;
    }

    default int getMaxContexts() {
        return 1;
    }
}
