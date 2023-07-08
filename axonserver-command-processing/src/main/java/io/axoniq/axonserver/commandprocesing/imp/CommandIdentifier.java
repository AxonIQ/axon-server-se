/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.commandprocesing.imp;

import java.util.Objects;

class CommandIdentifier {

    private final String name;
    private final String context;

    public CommandIdentifier(String name, String context) {
        this.name = name;
        this.context = context;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CommandIdentifier that = (CommandIdentifier) o;
        return name.equals(that.name) && context.equals(that.context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, context);
    }
}
