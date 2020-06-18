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

