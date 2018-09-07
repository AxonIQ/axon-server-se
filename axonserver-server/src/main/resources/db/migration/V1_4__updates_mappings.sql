delete from path_mapping where path like 'io.axoniq.axonhubrpc%';

insert into path_mapping( path, role) values ('io.axoniq.axonserverver.grpc.CommandService/OpenStream', 'WRITE');
insert into path_mapping( path, role) values ('io.axoniq.axonserverver.grpc.CommandService/Dispatch', 'WRITE');

insert into path_mapping( path, role) values ('io.axoniq.axonserverver.grpc.QueryService/OpenStream', 'READ');
insert into path_mapping( path, role) values ('io.axoniq.axonserverver.grpc.QueryService/Query', 'READ');

insert into path_mapping( path, role) values ('io.axoniq.axonserverver.grpc.InstructionService/OpenStream', 'READ');

