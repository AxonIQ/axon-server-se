package io.axoniq.axonserver.config;

import java.time.LocalDate;
import java.util.Collections;
import java.util.List;

/**
 * @author Marc Gathier
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

    default List<String> getFeatureList() {
        return Collections.emptyList();
    }

    default LocalDate getExpiryDate() {
        return null;
    }

    default String getEdition() {
        return "Standard edition";
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
