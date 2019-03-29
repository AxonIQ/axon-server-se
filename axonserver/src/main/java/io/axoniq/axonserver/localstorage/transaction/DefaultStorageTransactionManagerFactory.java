/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.transaction;

import io.axoniq.axonserver.localstorage.EventStorageEngine;

/**
 * @author Marc Gathier
 */
public class DefaultStorageTransactionManagerFactory implements StorageTransactionManagerFactory {

    @Override
    public StorageTransactionManager createTransactionManager(EventStorageEngine eventStore) {
        return new SingleInstanceTransactionManager(eventStore);
    }
}
