/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.serializer;

import java.util.Map;

/**
 * Created by Sara Pellegrini on 21/03/2018.
 * sara.pellegrini@gmail.com
 */
public interface Media {

    Media with(String property, String value);

    Media with(String property, Number value);

    Media with(String property, Boolean value);

    Media with(String property, Printable value);

    Media with(String property, Iterable<? extends Printable> values);

    Media withStrings(String property, Iterable<String> values);

    String toString();

    void with(String property, Map<String, String> map);
}
