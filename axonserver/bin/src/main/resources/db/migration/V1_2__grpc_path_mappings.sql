insert into path_mapping (path, role) values ('io.axoniq.axonserver.grpc.event.EventStore/ListEvents', 'READ');
insert into path_mapping (path, role) values ('io.axoniq.axonserver.grpc.event.EventStore/AppendEvent', 'WRITE');
insert into path_mapping (path, role) values ('io.axoniq.axonserver.grpc.event.EventStore/AppendSnapshot', 'WRITE');
insert into path_mapping (path, role) values ('io.axoniq.axonserver.grpc.event.EventStore/ListAggregateEvents', 'READ');
insert into path_mapping (path, role) values ('io.axoniq.axonserver.grpc.event.EventStore/ReadHighestSequenceNr', 'READ');
insert into path_mapping( path, role) values ('io.axoniq.axonserver.grpc.event.EventStore/GetFirstToken', 'READ');
insert into path_mapping( path, role) values ('io.axoniq.axonserver.grpc.event.EventStore/GetLastToken', 'READ');
insert into path_mapping( path, role) values ('io.axoniq.axonserver.grpc.event.EventStore/GetTokenAt', 'READ');

insert into path_mapping (path, role) values ('io.axoniq.axonserver.grpc.command.CommandService/OpenStream', 'WRITE');
insert into path_mapping (path, role) values ('io.axoniq.axonserver.grpc.command.CommandService/Dispatch', 'WRITE');

insert into path_mapping (path, role) values ('io.axoniq.axonserver.grpc.query.QueryService/OpenStream', 'READ');
insert into path_mapping (path, role) values ('io.axoniq.axonserver.grpc.query.QueryService/Query', 'READ');
insert into path_mapping( path, role) values ('io.axoniq.axonserver.grpc.query.QueryService/Subscription', 'READ');

insert into path_mapping (path, role) values ('io.axoniq.axonserver.grpc.control.PlatformService/GetPlatformServer', 'READ');
insert into path_mapping (path, role) values ('io.axoniq.axonserver.grpc.control.PlatformService/OpenStream', 'READ');

-- old axonhub grpc names

insert into path_mapping (path, role) values ('io.axoniq.axondb.grpc.EventStore/ListEvents', 'READ');
insert into path_mapping (path, role) values ('io.axoniq.axondb.grpc.EventStore/AppendEvent', 'WRITE');
insert into path_mapping (path, role) values ('io.axoniq.axondb.grpc.EventStore/AppendSnapshot', 'WRITE');
insert into path_mapping (path, role) values ('io.axoniq.axondb.grpc.EventStore/ListAggregateEvents', 'READ');
insert into path_mapping (path, role) values ('io.axoniq.axondb.grpc.EventStore/ReadHighestSequenceNr', 'READ');
insert into path_mapping( path, role) values ('io.axoniq.axondb.grpc.EventStore/GetFirstToken', 'READ');
insert into path_mapping( path, role) values ('io.axoniq.axondb.grpc.EventStore/GetLastToken', 'READ');
insert into path_mapping( path, role) values ('io.axoniq.axondb.grpc.EventStore/GetTokenAt', 'READ');

insert into path_mapping (path, role) values ('io.axoniq.axonhub.grpc.CommandService/OpenStream', 'WRITE');
insert into path_mapping (path, role) values ('io.axoniq.axonhub.grpc.CommandService/Dispatch', 'WRITE');

insert into path_mapping (path, role) values ('io.axoniq.axonhub.grpc.QueryService/OpenStream', 'READ');
insert into path_mapping (path, role) values ('io.axoniq.axonhub.grpc.QueryService/Query', 'READ');
insert into path_mapping( path, role) values ('io.axoniq.axonhub.grpc.QueryService/Subscription', 'READ');

insert into path_mapping (path, role) values ('io.axoniq.platform.grpc.PlatformService/GetPlatformServer', 'READ');
insert into path_mapping (path, role) values ('io.axoniq.platform.grpc.PlatformService/OpenStream', 'READ');
