alter table CONTEXT_CLUSTER_NODE rename to CONTEXT_CLUSTER_NODE_OLD;
alter table CONTEXT rename to CONTEXT_OLD;

create table context (name varchar(255) not null , primary key (name));

create table context_cluster_node (
  context_name varchar(255) not null REFERENCES context(name),
  cluster_node_name varchar(255) not null REFERENCES cluster_node(name),
  storage boolean not null,
  messaging boolean not null,
  PRIMARY KEY (context_name, cluster_node_name)
);

insert into context(name) select context from context_old;

insert into context_cluster_node(context_name, cluster_node_name, storage, messaging)
  select context_old.context, cluster_node_name, true, true
  from CONTEXT_CLUSTER_NODE_OLD, CONTEXT_OLD
  where CONTEXT_CLUSTER_NODE_OLD.context_id = CONTEXT_OLD.id;

drop table CONTEXT_CLUSTER_NODE_OLD;

drop table CONTEXT_OLD;
