/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

insert into roles (role, description)
values ('TRANSFORM', 'Transform events.');
insert into roles (role, description)
values ('TRANSFORM_ADMIN', 'Applies transformation.');

insert into function_roles (function, role)
values ('TRANSFORM', 'TRANSFORM');
insert into function_roles (function, role)
values ('LIST_TRANSFORMATIONS', 'TRANSFORM');
insert into function_roles (function, role)
values ('APPLY_TRANSFORMATION', 'TRANSFORM_ADMIN');

insert into paths_to_functions (path, function)
values ('io.axoniq.axonserver.grpc.event.EventTransformationService/Transformations', 'LIST_TRANSFORMATIONS');
insert into paths_to_functions (path, function)
values ('io.axoniq.axonserver.grpc.event.EventTransformationService/StartTransformation', 'TRANSFORM');
insert into paths_to_functions (path, function)
values ('io.axoniq.axonserver.grpc.event.EventTransformationService/TransformEvents', 'TRANSFORM');
insert into paths_to_functions (path, function)
values ('io.axoniq.axonserver.grpc.event.EventTransformationService/CancelTransformation', 'TRANSFORM');
insert into paths_to_functions (path, function)
values ('io.axoniq.axonserver.grpc.event.EventTransformationService/ApplyTransformation', 'APPLY_TRANSFORMATION');
insert into paths_to_functions (path, function)
values ('io.axoniq.axonserver.grpc.event.EventTransformationService/Compact', 'TRANSFORM');

insert into paths_to_functions (path, function)
values ('GET:/v1/transformations/', 'LIST_TRANSFORMATIONS');
insert into paths_to_functions (path, function)
values ('POST:/v1/transformations/', 'TRANSFORM');
insert into paths_to_functions (path, function)
values ('PATCH:/v1/transformations/[^/]*/event/.*', 'TRANSFORM');
insert into paths_to_functions (path, function)
values ('PATCH:/v1/transformations/[^/]*/apply/', 'APPLY_TRANSFORMATION');
insert into paths_to_functions (path, function)
values ('PATCH:/v1/transformations/[^/]*/cancel/.*', 'TRANSFORM');
insert into paths_to_functions (path, function)
values ('POST:/v1/eventstore/compact/', 'TRANSFORM');