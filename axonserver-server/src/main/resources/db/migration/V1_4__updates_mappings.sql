delete from path_mapping where path like 'io.axoniq.axonhub.grpc%';

insert into path_mapping( path, role) values ('io.axoniq.axonserver.grpc.command.CommandService/OpenStream', 'WRITE');
insert into path_mapping( path, role) values ('io.axoniq.axonserver.grpc.command.CommandService/Dispatch', 'WRITE');

insert into path_mapping( path, role) values ('io.axoniq.axonserver.grpc.query.QueryService/OpenStream', 'READ');
insert into path_mapping( path, role) values ('io.axoniq.axonserver.grpc.query.QueryService/Query', 'READ');

insert into path_mapping( path, role) values ('io.axoniq.axonhub.grpc.InstructionService/OpenStream', 'READ');

