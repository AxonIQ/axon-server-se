/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.svg.element;

import io.axoniq.axonserver.rest.svg.Element;
import io.axoniq.axonserver.rest.svg.Printable;

/**
 * Created by Sara Pellegrini on 27/04/2018.
 * sara.pellegrini@gmail.com
 */
public interface Box extends Printable, Element {

    Rectangle rectangle();

    void connectTo(Box connected, String lineStyle);

}
