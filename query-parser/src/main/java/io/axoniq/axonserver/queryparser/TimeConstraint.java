/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.queryparser;

import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 * @since 4.0
 */
public class TimeConstraint extends BaseQueryElement {
    @Override
    public String toString() {
        return children.get(0) + " " +
                String.join(" ", children.stream().skip(1).map(Object::toString).collect(Collectors.toList()));
    }

    @Override
    public List<String> getIdentifiers() {
        return Collections.emptyList();
    }

    @Override
    public String operator() {
        return String.valueOf(children.get(0));
    }

    @Override
    public List<? extends QueryElement> getParameters() {
        return children.subList(1, children.size());
    }

    public Optional<String> alias() {
        return Optional.empty();
    }

    public long getStart() {
        double count = 0.0;
        Identifier identifier;
        if( children.size() == 2) {
            count = 1.0;
            identifier = (Identifier)get(1);
        } else {
            Numeric numeric = (Numeric) get(1);
            identifier = (Identifier)get(2);
            count = Double.parseDouble(numeric.getLiteral());
        }
        Calendar now = Calendar.getInstance();
        switch( identifier.getIdentifier().toLowerCase()) {
            case "second":
                now.add(Calendar.SECOND, (int) -count);
                break;
            case "minute":
                now.add(Calendar.SECOND, (int) -(count*60));
                break;
            case "hour":
                now.add(Calendar.SECOND, (int) -(count*60*60));
                break;
            case "day":
                now.add(Calendar.SECOND, (int) -(count*60*60*24));
                break;
            case "week":
                now.add(Calendar.DAY_OF_MONTH, (int) (-7 * count));
                break;
            case "month":
                now.add(Calendar.MONTH, (int) -count);
                break;
            case "year":
                now.add(Calendar.YEAR, (int) -count);
                break;
        }
        return now.getTimeInMillis();
    }
}
