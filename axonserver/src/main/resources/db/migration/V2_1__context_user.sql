/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

create table jpa_context_user
(
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    context  varchar(255) not null,
    username varchar(255) not null,
    password varchar(255)
);

create sequence jpa_context_user_seq increment by 50;

create table jpa_context_user_roles
(
    jpa_context_user_id BIGINT references jpa_context_user (id),
    roles               varchar(255) not null,
    primary key (jpa_context_user_id, roles)
);
