/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver;

import org.springframework.stereotype.Component;

/**
 * Default implementation for Axon Server SE.
 * Doesnt check for license and allows all requests.
 *
 * @author Stefan Dragisic
 * @since 4.4
 */
@Component
public class AxonServerStandardLicenseAccessController implements LicenseAccessController {

    @Override
    public boolean allowed() {
        return true;
    }

}
