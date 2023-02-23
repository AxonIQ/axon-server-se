/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

create table et_event_store_transformation
(
    transformation_id varchar(255) not null primary key,
    context           varchar(255) not null,
    status            varchar(255) not null,
    version           int,
    description       varchar(4000),
    date_applied      date,
    applier           varchar(255),
    last_sequence     long,
    last_event_token  long
);

create table et_local_event_store_transformation
(
    transformation_id     varchar(255) not null primary key,
    last_sequence_applied bigint,
    applied               bool
);

create table et_event_store_state
(
    context                  varchar(255) not null primary key,
    state                    varchar(255),
    in_progress_operation_id varchar(255)
);