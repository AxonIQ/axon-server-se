create table if not exists task
(
    task_id        varchar(200) not null primary key,
    context        varchar(200) not null,
    data           blob,
    type           varchar(200),
    status         INTEGER      not null,
    retry_interval BIGINT,
    task_executor  varchar(200) not null,
    timestamp      BIGINT       not null
);

alter table task
    add (
        message varchar(255)
        );


insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.event.EventScheduler/ScheduleEvent', 'SCHEDULE_EVENT');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.event.EventScheduler/RescheduleEvent', 'RESCHEDULE_EVENT');

insert into PATHS_TO_FUNCTIONS
values ('io.axoniq.axonserver.grpc.event.EventScheduler/CancelScheduledEvent', 'CANCEL_SCHEDULED_EVENT');

insert into FUNCTION_ROLES(function, role)
values ('SCHEDULE_EVENT', 'PUBLISH_EVENTS');

insert into FUNCTION_ROLES(function, role)
values ('SCHEDULE_EVENT', 'WRITE');

insert into FUNCTION_ROLES(function, role)
values ('SCHEDULE_EVENT', 'USE_CONTEXT');

insert into FUNCTION_ROLES(function, role)
values ('RESCHEDULE_EVENT', 'PUBLISH_EVENTS');

insert into FUNCTION_ROLES(function, role)
values ('RESCHEDULE_EVENT', 'WRITE');

insert into FUNCTION_ROLES(function, role)
values ('RESCHEDULE_EVENT', 'USE_CONTEXT');

insert into FUNCTION_ROLES(function, role)
values ('CANCEL_SCHEDULED_EVENT', 'PUBLISH_EVENTS');

insert into FUNCTION_ROLES(function, role)
values ('CANCEL_SCHEDULED_EVENT', 'WRITE');

insert into FUNCTION_ROLES(function, role)
values ('CANCEL_SCHEDULED_EVENT', 'USE_CONTEXT');
