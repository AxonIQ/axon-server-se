/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.serializer;

import org.junit.*;

import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Created by Sara Pellegrini on 21/03/2018.
 * sara.pellegrini@gmail.com
 */
public class GsonMediaTest {

    @Test
    public void testProperties(){
        String json = new GsonMedia().with("number", 3)
                                  .with("string", "aString")
                                  .with("boolean", true)
                                  .toString();
        assertEquals("{\"number\":3,\"string\":\"aString\",\"boolean\":true}", json);
    }

    @Test
    public void testObjects(){
        Printable object = media -> media.with("innerObjectProperty", "value");
        String json = new GsonMedia().with("innerObject", object).toString();
        assertEquals("{\"innerObject\":{\"innerObjectProperty\":\"value\"}}",json);
    }

    @Test
    public void testCollections(){
        Printable object = media -> media.with("property", "value");
        Collection<Printable> mediaCollection = asList(object, object);
        String json = new GsonMedia().with("collection", mediaCollection).toString();
        assertEquals("{\"collection\":[{\"property\":\"value\"},{\"property\":\"value\"}]}",json);
    }

    @Test
    public void testEmpty(){
        String json = new GsonMedia().toString();
        assertEquals("{}", json);
    }

}