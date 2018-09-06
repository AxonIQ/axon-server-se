alter table cluster_node add constraint cluster_node_uq1 unique (internal_host_name, grpc_internal_port);
