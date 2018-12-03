create table jpa_raft_state (
  group_id varchar(255) not null primary key,
  commit_index BIGINT,
  current_term BIGINT,
  last_applied BIGINT,
  voted_for varchar(255)
);


