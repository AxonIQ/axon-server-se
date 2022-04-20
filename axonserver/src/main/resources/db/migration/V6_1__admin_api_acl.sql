/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.ContextAdminService/CreateContext', 'CREATE_CONTEXT');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.ContextAdminService/UpdateContextProperties', 'UPDATE_CONTEXT_PROPERTIES');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.ContextAdminService/DeleteContext', 'DELETE_CONTEXT');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.ContextAdminService/GetContext', 'LIST_CONTEXTS');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.ContextAdminService/GetContexts', 'LIST_CONTEXTS');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.ContextAdminService/SubscribeContextUpdates', 'LIST_CONTEXTS');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.ReplicationGroupAdminService/GetNodes', 'LIST_NODES');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.ReplicationGroupAdminService/CreateReplicationGroup',
        'CREATE_REPLICATION_GROUP');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.ReplicationGroupAdminService/GetReplicationGroups', 'LIST_REPLICATION_GROUPS');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.ReplicationGroupAdminService/GetReplicationGroup', 'LIST_REPLICATION_GROUPS');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.ReplicationGroupAdminService/DeleteReplicationGroup',
        'DELETE_REPLICATION_GROUP');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.ReplicationGroupAdminService/AddNodeToReplicationGroup',
        'ADD_NODE_TO_REPLICATION_GROUP');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.ReplicationGroupAdminService/RemoveNodeFromReplicationGroup',
        'DELETE_NODE_FROM_REPLICATION_GROUP');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.ApplicationAdminService/CreateOrUpdateApplication', 'CREATE_APP');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.ApplicationAdminService/DeleteApplication', 'DELETE_APP');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.ApplicationAdminService/GetApplication', 'LIST_APPS');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.ApplicationAdminService/GetApplications', 'LIST_APPS');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.ApplicationAdminService/RefreshToken', 'RENEW_APP_TOKEN');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.UserAdminService/CreateOrUpdateUser', 'MERGE_USER');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.UserAdminService/DeleteUser', 'DELETE_USER');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.UserAdminService/GetUsers', 'LIST_USERS');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.EventProcessorAdminService/PauseEventProcessor', 'PAUSE_EVENT_PROCESSOR');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.EventProcessorAdminService/StartEventProcessor', 'START_EVENT_PROCESSOR');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.EventProcessorAdminService/SplitEventProcessor',
        'SPLIT_EVENT_PROCESSOR_SEGMENTS');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.EventProcessorAdminService/MergeEventProcessor',
        'MERGE_EVENT_PROCESSOR_SEGMENTS');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.EventProcessorAdminService/MoveEventProcessorSegment',
        'MOVE_EVENT_PROCESSOR_SEGMENT');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.EventProcessorAdminService/GetAllEventProcessors', 'GET_EVENT_PROCESSORS');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.EventProcessorAdminService/GetEventProcessorsByComponent',
        'GET_EVENT_PROCESSORS');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.EventProcessorAdminService/LoadBalanceProcessor', 'REBALANCE_PROCESSOR');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.EventProcessorAdminService/SetAutoLoadBalanceStrategy',
        'SET_EVENT_PROCESSOR_STRATEGY');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.admin.EventProcessorAdminService/GetBalancingStrategies',
        'GET_EVENT_PROCESSORS_STRATEGIES');

insert into PATHS_TO_FUNCTIONS
values ('PATCH:/v1/context/.*',
        'UPDATE_CONTEXT_PROPERTIES');

insert into FUNCTION_ROLES(function, role)
values ('GET_EVENT_PROCESSORS', 'CONTEXT_ADMIN');

insert into FUNCTION_ROLES(function, role)
values ('GET_EVENT_PROCESSORS_STRATEGIES', 'CONTEXT_ADMIN');

insert into FUNCTION_ROLES(function, role)
values ('GET_EVENT_PROCESSOR_STRATEGY', 'CONTEXT_ADMIN');

insert into FUNCTION_ROLES(function, role)
values ('GET_EVENT_PROCESSORS', 'ADMIN');

insert into FUNCTION_ROLES(function, role)
values ('GET_EVENT_PROCESSORS_STRATEGIES', 'ADMIN');

insert into FUNCTION_ROLES(function, role)
values ('GET_EVENT_PROCESSOR_STRATEGY', 'ADMIN');

insert into FUNCTION_ROLES(function, role)
values ('GET_EVENT_PROCESSORS', 'USE_CONTEXT');

insert into FUNCTION_ROLES(function, role)
values ('GET_EVENT_PROCESSORS_STRATEGIES', 'USE_CONTEXT');

insert into FUNCTION_ROLES(function, role)
values ('GET_EVENT_PROCESSOR_STRATEGY', 'USE_CONTEXT');

insert into FUNCTION_ROLES(function, role)
values ('UPDATE_CONTEXT_PROPERTIES', 'ADMIN');

