/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

create table event_store_transformations (
    transformation_id varchar(255) not null primary key,
    context varchar(255) not null,
    first_token bigint,
    last_token bigint,
    status int
);