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

delete from PATHS_TO_FUNCTIONS where PATH like 'io.axoniq.axonserver.grpc.admin.ContextAdminService/GetContext';

delete from PATHS_TO_FUNCTIONS where PATH like 'io.axoniq.axonserver.grpc.admin.ContextAdminService/GetContexts';

delete from PATHS_TO_FUNCTIONS where PATH like 'io.axoniq.axonserver.grpc.admin.ContextAdminService/SubscribeContextUpdates';