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
