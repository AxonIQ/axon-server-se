-- add _admin context to all nodes if there is already a node connected to a context (database created with AxonServer 4.0)
insert into context(name)
  select distinct '_admin'
  from context_cluster_node;


insert into context_cluster_node( context_name, cluster_node_name, storage, messaging)
  select '_admin', name, false, false
  from cluster_node
  where exists(select 1 from context_cluster_node);

update context_cluster_node
  set cluster_node_label = cluster_node_name;

insert into jpa_raft_group_node (group_id, node_id, host, port, node_name)
  select context_cluster_node.context_name, cluster_node.name, cluster_node.internal_host_name, cluster_node.grpc_internal_port, cluster_node.name
  from cluster_node, context_cluster_node
  where cluster_node.name = context_cluster_node.cluster_node_name;


insert into jpa_context_application (context, hashed_token, name, token_prefix)
  select  application_context.context, application.hashed_token, application.name, application.token_prefix
  from application_context, application
  where application_context.application_id = application.id;


insert into jpa_context_application_roles (jpa_context_application_id, roles)
  select distinct jpa_context_application.id, application_context_role.role
  from jpa_context_application, application_context, application_context_role, application
  where application_context_role.application_context_id = application_context.id
    and application.name = jpa_context_application.name
    and application.id = application_context.application_id
    and application_context.context = jpa_context_application.context;

drop table application_role;
