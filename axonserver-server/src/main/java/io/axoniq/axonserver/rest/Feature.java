package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.licensing.Limits;

import java.util.function.Function;

/**
 * Author: marc
 */
public enum Feature {
    // Basic
    FILESYSTEM_STORAGE_ENGINE( limits -> true),
    MANUAL_TRACKING_PROCESSOR_SCALING_BALANCING( limits -> true),
    AD_HOC_QUERIES( limits -> true),
    APPLICATION_OVERVIEW( limits -> true),
    BASIC_APP_MONITORING( limits -> true),
    SSL_SUPPORT( limits -> true),
    IN_APP_CONNECTORS( limits -> true),
    UNLIMITED_STORAGE( limits -> true),
    // Enterprise
    CLUSTERING(Limits::isEnterprise),
    ADVANCED_MONITORING(Limits::isEnterprise),
    APP_AUTHENTICATION(Limits::isEnterprise),
    MEMORY_MAPPED_FILE_STORAGE(Limits::isEnterprise),
    CONNECTION_BALANCING(Limits::isEnterprise),
    DETAILED_STATISTICS(Limits::isEnterprise),
    AUTOMATIC_TRACKING_PROCESSOR_SCALING_BALANCING(Limits::isEnterprise),
    MULTI_CONTEXT(Limits::isEnterprise),
    THIRD_PARTY_CONNECTORS(Limits::isEnterprise),
    // Big Data
    SHARDING,
    MULTI_TIER_STORAGE,
    READABLE_REPLICA,
    DATA_COMPRESSION,
    QOS_MESSAGES,
    // Scale
    LOCATION_AWARENESS,
    MULTI_DATACENTER_SUPPORT,
    // Security/Complience
    PRIVACY_GUARD,
    RDBMS_STORAGE,
    DATA_MIGRATION_TOOLS,
    ENCRYPTION_AT_REST,
    AUTOMATED_BACKUPS;

    private final Function<Limits, Boolean> checkerFunction;

    Feature() {
        this( l -> false);
    }
    Feature(Function<Limits, Boolean> checkerFunction) {
        this.checkerFunction = checkerFunction;
    }

    public boolean enabled(Limits limits) {
        return checkerFunction.apply(limits);
    }
}

