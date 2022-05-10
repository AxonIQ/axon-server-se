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
    status int,
    version int,
    description varchar(4000),
    date_applied date,
    applied_by varchar(255),
    last_sequence long
);

create table event_store_transformation_progress (
    transformation_id varchar(255) not null primary key,
    last_sequence_applied bigint,
    applied bool
);

