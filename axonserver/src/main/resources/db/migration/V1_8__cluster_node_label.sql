ALTER TABLE context_cluster_node ADD (
  cluster_node_label VARCHAR(255)
  );

update context_cluster_node set cluster_node_label = cluster_node_name;

alter table jpa_raft_group_node add (
  node_name VARCHAR(255)
  );

update jpa_raft_group_node set node_name = node_id;
