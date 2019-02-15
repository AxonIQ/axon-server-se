--- delete default context only when this is a clean start
delete from context where name = 'default' and not exists(select 1 from context_cluster_node);