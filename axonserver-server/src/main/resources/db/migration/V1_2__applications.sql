delete from path_mapping where path in ('GET:/v1/applications','GET:/v1/applications/*');

insert into path_mapping( path, role) values ('GET:/v1/applications', 'ADMIN');
insert into path_mapping( path, role) values ('GET:/v1/applications/*', 'ADMIN');
