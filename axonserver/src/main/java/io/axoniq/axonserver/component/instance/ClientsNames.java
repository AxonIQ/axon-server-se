/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.instance;

import java.util.Iterator;
import javax.annotation.Nonnull;

import static java.util.stream.StreamSupport.stream;

/**
 * Created by Sara Pellegrini on 16/05/2018.
 * sara.pellegrini@gmail.com
 */
public class ClientsNames implements Iterable<String> {

    private final Iterable<Client> clients;

    public ClientsNames(Iterable<Client> clients) {
        this.clients = clients;
    }

    @Override @Nonnull
    public Iterator<String> iterator() {
        return stream(clients.spliterator(),false).map(Client::name).iterator();
    }
}
