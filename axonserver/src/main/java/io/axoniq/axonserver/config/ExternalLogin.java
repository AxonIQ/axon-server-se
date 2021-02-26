/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

/**
 * Definition of an external login method and the URL to redirect to from the login page.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public class ExternalLogin {

    private final String image;
    private final String url;
    private final String name;

    public ExternalLogin(String image, String url, String name) {
        this.image = image;
        this.url = url;
        this.name = name;
    }

    /**
     * @return font-awesome style class for the image to display
     */
    public String getImage() {
        return image;
    }

    /**
     * @return URL to redirect from login page
     */
    public String getUrl() {
        return url;
    }

    /**
     * @return name of the external login provider
     */
    public String getName() {
        return name;
    }
}
