ALTER TABLE context_cluster_node ADD (
  cluster_node_label VARCHAR(255)
  );

update context_cluster_node set cluster_node_label = cluster_node_name;
