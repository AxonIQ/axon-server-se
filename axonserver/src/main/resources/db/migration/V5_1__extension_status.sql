/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

create table plugin_package
(
    id       bigint AUTO_INCREMENT primary key,
    name     varchar(200) not null,
    version  varchar(200) not null,
    filename varchar(200) not null
);


create table context_plugin_status
(
    id bigint AUTO_INCREMENT primary key,
    plugin_package_id bigint       not null references plugin_package (id),
    context           varchar(200) not null,
    configuration     clob,
    active            boolean
);

create table admin_context_plugin_status
(
    id bigint AUTO_INCREMENT primary key,
    plugin_package_id bigint       not null references plugin_package (id),
    context           varchar(200) not null,
    configuration     clob,
    active            boolean
);


insert into PATHS_TO_FUNCTIONS
values ('GET:/v1/plugins', 'LIST_PLUGINS');

insert into PATHS_TO_FUNCTIONS
values ('GET:/v1/plugins/configuration', 'GET_PLUGIN_CONFIGURATION');

insert into PATHS_TO_FUNCTIONS
values ('DELETE:/v1/plugins', 'DELETE_PLUGIN');

insert into PATHS_TO_FUNCTIONS
values ('POST:/v1/plugins', 'ADD_PLUGIN');

insert into PATHS_TO_FUNCTIONS
values ('DELETE:/v1/plugins/context', 'UNREGISTER_PLUGIN');

insert into PATHS_TO_FUNCTIONS
values ('POST:/v1/plugins/status', 'ACTIVATE_PLUGIN');

insert into PATHS_TO_FUNCTIONS
values ('POST:/v1/plugins/configuration', 'CONFIGURE_PLUGIN');

insert into FUNCTION_ROLES(function, role)
values ('LIST_PLUGINS', 'ADMIN');

insert into FUNCTION_ROLES(function, role)
values ('GET_PLUGIN_CONFIGURATION', 'ADMIN');

insert into FUNCTION_ROLES(function, role)
values ('DELETE_PLUGIN', 'ADMIN');

insert into FUNCTION_ROLES(function, role)
values ('ADD_PLUGIN', 'ADMIN');

insert into FUNCTION_ROLES(function, role)
values ('UNREGISTER_PLUGIN', 'ADMIN');

insert into FUNCTION_ROLES(function, role)
values ('ACTIVATE_PLUGIN', 'ADMIN');

insert into FUNCTION_ROLES(function, role)
values ('CONFIGURE_PLUGIN', 'ADMIN');