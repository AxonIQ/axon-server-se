alter table JPA_RAFT_GROUP_NODE
    add if not exists
        ROLE integer
;

alter table CONTEXT_CLUSTER_NODE
    add if not exists
        ROLE integer
;