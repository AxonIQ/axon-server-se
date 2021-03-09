/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

insert into roles
values ('VIEW_CONFIGURATION', 'Read only access to configuration data');

insert into FUNCTION_ROLES(FUNCTION, ROLE)
values ('LIST_APPS', 'VIEW_CONFIGURATION');
insert into FUNCTION_ROLES(FUNCTION, ROLE)
values ('LIST_CONTEXTS', 'VIEW_CONFIGURATION');
insert into FUNCTION_ROLES(FUNCTION, ROLE)
values ('LIST_NODES', 'VIEW_CONFIGURATION');
insert into FUNCTION_ROLES(FUNCTION, ROLE)
values ('LIST_USERS', 'VIEW_CONFIGURATION');
insert into FUNCTION_ROLES(FUNCTION, ROLE)
values ('LIST_REPLICATION_GROUPS', 'VIEW_CONFIGURATION');
insert into FUNCTION_ROLES(FUNCTION, ROLE)
values ('LIST_PLUGINS', 'VIEW_CONFIGURATION');
