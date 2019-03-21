package io.axoniq.axonserver.licensing;

import io.axoniq.axonserver.config.FeatureChecker;

import java.util.function.Function;

/**
 * @author Marc Gathier
 */
public enum Feature {
    // Basic
    FILESYSTEM_STORAGE_ENGINE(FeatureChecker::isBasic),
    MANUAL_TRACKING_PROCESSOR_SCALING_BALANCING( FeatureChecker::isBasic),
    AD_HOC_QUERIES( FeatureChecker::isBasic),
    APPLICATION_OVERVIEW( FeatureChecker::isBasic),
    BASIC_APP_MONITORING( FeatureChecker::isBasic),
    SSL_SUPPORT( FeatureChecker::isBasic),
    IN_APP_CONNECTORS( FeatureChecker::isBasic),
    UNLIMITED_STORAGE( FeatureChecker::isBasic),
    // Enterprise
    CLUSTERING(FeatureChecker::isEnterprise),
    ADVANCED_MONITORING(FeatureChecker::isEnterprise),
    APP_AUTHENTICATION(FeatureChecker::isEnterprise),
    MEMORY_MAPPED_FILE_STORAGE(FeatureChecker::isEnterprise),
    CONNECTION_BALANCING(FeatureChecker::isEnterprise),
    DETAILED_STATISTICS(FeatureChecker::isEnterprise),
    AUTOMATIC_TRACKING_PROCESSOR_SCALING_BALANCING(FeatureChecker::isEnterprise),
    MULTI_CONTEXT(FeatureChecker::isEnterprise),
    THIRD_PARTY_CONNECTORS(FeatureChecker::isEnterprise),
    // Big Data
    SHARDING(FeatureChecker::isBigData),
    MULTI_TIER_STORAGE(FeatureChecker::isBigData),
    READABLE_REPLICA(FeatureChecker::isBigData),
    DATA_COMPRESSION(FeatureChecker::isBigData),
    QOS_MESSAGES(FeatureChecker::isBigData),
    // Scale
    LOCATION_AWARENESS(FeatureChecker::isScale),
    MULTI_DATACENTER_SUPPORT(FeatureChecker::isScale),
    // Security/Complience
    PRIVACY_GUARD(FeatureChecker::isSecurity),
    RDBMS_STORAGE(FeatureChecker::isSecurity),
    DATA_MIGRATION_TOOLS(FeatureChecker::isSecurity),
    ENCRYPTION_AT_REST(FeatureChecker::isSecurity),
    AUTOMATED_BACKUPS(FeatureChecker::isSecurity);

    private final Function<FeatureChecker, Boolean> checkerFunction;

    Feature() {
        this( l -> false);
    }
    Feature(Function<FeatureChecker, Boolean> checkerFunction) {
        this.checkerFunction = checkerFunction;
    }

    public boolean enabled(FeatureChecker limits) {
        return checkerFunction.apply(limits);
    }
}

