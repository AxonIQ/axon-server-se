create table jpa_raft_state (
  group_id varchar(255) not null primary key,
  commit_index BIGINT,
  commit_term BIGINT,
  current_term BIGINT,
  last_applied_index BIGINT,
  last_applied_term BIGINT,
  voted_for varchar(255)
);


create table jpa_raft_group_node (
  group_id varchar(255) not null,
  node_id varchar(255) not null,
  host varchar(255) not null,
  port INTEGER not null,
  primary key (group_id, node_id)
);

