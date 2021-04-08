/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.client.processor.balancing.jpa;

import io.axoniq.axonserver.refactoring.transport.rest.serializer.Media;
import io.axoniq.axonserver.refactoring.transport.rest.serializer.Printable;

import java.util.Map;
import javax.annotation.Nonnull;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

/**
 * Created by Sara Pellegrini on 14/08/2018.
 * sara.pellegrini@gmail.com
 */
@Entity
public class LoadBalancingStrategy implements Printable {

    @Id
    @GeneratedValue
    private Long id;

    @Column(unique = true)
    private String name;

    private String label;

    private String factoryBean;

    @SuppressWarnings("unused")
    public LoadBalancingStrategy() {
    }

    public LoadBalancingStrategy(Map<String, String> body) {
        this(body.get("name"), body.get("label"), body.get("factoryBean"));
    }

    public LoadBalancingStrategy(@Nonnull String name, @Nonnull String label, @Nonnull String factoryBean) {
        this.name = name;
        this.label = label;
        this.factoryBean = factoryBean;
    }

    public String name() {
        return name;
    }

    public String label() {
        return label;
    }

    public String factoryBean() {
        return factoryBean;
    }

    @Override
    public void printOn(Media media) {
        media.with("name", name)
             .with("label", label)
             .with("factoryBean", factoryBean);
    }
}
