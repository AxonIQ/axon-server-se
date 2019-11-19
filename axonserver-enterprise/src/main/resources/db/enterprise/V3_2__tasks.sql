create table task
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

alter table JPA_RAFT_GROUP_NODE
    add (
        pending_delete boolean
        );

alter table CONTEXT_CLUSTER_NODE
    add (
        pending_delete boolean
        );

update JPA_RAFT_GROUP_NODE
set pending_delete = false;

update CONTEXT_CLUSTER_NODE
set pending_delete = false;
