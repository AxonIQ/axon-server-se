insert into path_mapping( path, role) values ('io.axoniq.axondb.grpc.EventStore/GetFirstToken', 'READ');
insert into path_mapping( path, role) values ('io.axoniq.axondb.grpc.EventStore/GetLastToken', 'READ');
insert into path_mapping( path, role) values ('io.axoniq.axondb.grpc.EventStore/GetTokenAt', 'READ');

insert into path_mapping( path, role) values ('io.axoniq.axonserverver.grpc.QueryService/Subscription', 'READ');
