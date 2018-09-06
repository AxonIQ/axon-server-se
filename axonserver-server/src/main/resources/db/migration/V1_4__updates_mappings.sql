delete from path_mapping where path like 'io.axoniq.axonhubrpc%';

insert into path_mapping( path, role) values ('io.axoniq.axonhub.grpc.CommandService/OpenStream', 'WRITE');
insert into path_mapping( path, role) values ('io.axoniq.axonhub.grpc.CommandService/Dispatch', 'WRITE');

insert into path_mapping( path, role) values ('io.axoniq.axonhub.grpc.QueryService/OpenStream', 'READ');
insert into path_mapping( path, role) values ('io.axoniq.axonhub.grpc.QueryService/Query', 'READ');

insert into path_mapping( path, role) values ('io.axoniq.axonhub.grpc.InstructionService/OpenStream', 'READ');

