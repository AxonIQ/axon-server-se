insert into PATHS_TO_FUNCTIONS
values ('POST:/v1/cluster/upload-license', 'UPLOAD_LICENSE');

insert into PATHS_TO_FUNCTIONS
values ('GET:/v1/cluster/download-template', 'DOWNLOAD_TEMPLATE');

insert into PATHS_TO_FUNCTIONS
values ('GET:/v1/tasks', 'LIST_TASKS');

insert into PATHS_TO_FUNCTIONS
values ('DELETE:/v1/tasks/.*', 'DELETE_TASK');

insert into FUNCTION_ROLES(function, role)
values ('UPLOAD_LICENSE', 'ADMIN');

insert into FUNCTION_ROLES(function, role)
values ('LIST_TASKS', 'ADMIN');

insert into FUNCTION_ROLES(function, role)
values ('DELETE_TASK', 'ADMIN');

insert into FUNCTION_ROLES(function, role)
values ('DOWNLOAD_TEMPLATE', 'ADMIN');
