update adm_replication_group_member
set role = 0
where role is null;

update rg_member
set role = 0
where role is null;