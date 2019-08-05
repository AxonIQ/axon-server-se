insert into ROLES
values ('USE_CONTEXT', 'All actions on a context');

insert into FUNCTION_ROLES(function, role)
values ('LIST_BACKUP_FILENAMES', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('LIST_BACKUP_LOGFILES', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('LIST_QUERIES', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('LOCAL_GET_LAST_EVENT', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('LOCAL_GET_LAST_SNAPSHOT', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('MERGE_EVENT_PROCESSOR_SEGMENTS', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('MOVE_EVENT_PROCESSOR_SEGMENT', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('PAUSE_EVENT_PROCESSOR', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('RAFT_CLEAN_LOG', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('RAFT_GET_STATUS', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('RAFT_LIST_APPLICATIONS', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('RAFT_LIST_CONTEXT_MEMBERS', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('RAFT_START_CONTEXT', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('RAFT_STEPDOWN', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('RAFT_STOP_CONTEXT', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('RECONNECT_CLIENT', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('SET_EVENT_PROCESSOR_STRATEGY', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('SPLIT_EVENT_PROCESSOR_SEGMENTS', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('START_EVENT_PROCESSOR', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('REBALANCE_PROCESSOR', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('AUTO_REBALANCE_PROCESSOR', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('DISPATCH_COMMAND', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('DISPATCH_QUERY', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('DISPATCH_SUBSCRIPTION_QUERY', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('GET_COMMANDS_COUNT', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('GET_COMMANDS_QUEUE', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('APPEND_EVENT', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('APPEND_SNAPSHOT', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('GET_FIRST_TOKEN', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('GET_LAST_TOKEN', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('GET_TOKEN_AT', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('LIST_EVENTS', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('LIST_SNAPSHOTS', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('READ_HIGHEST_SEQNR', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('SEARCH_EVENTS', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('HANDLE_COMMANDS', 'USE_CONTEXT');
insert into FUNCTION_ROLES(function, role)
values ('HANDLE_QUERIES', 'USE_CONTEXT');
