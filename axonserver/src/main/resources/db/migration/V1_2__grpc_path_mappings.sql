insert into path_mapping (path, role) values ('io.axoniq.axonserver.grpc.event.EventStorageEngine/ListEvents', 'READ');
insert into path_mapping (path, role) values ('io.axoniq.axonserver.grpc.event.EventStorageEngine/AppendEvent', 'WRITE');
insert into path_mapping (path, role) values ('io.axoniq.axonserver.grpc.event.EventStorageEngine/AppendSnapshot', 'WRITE');
insert into path_mapping (path, role) values ('io.axoniq.axonserver.grpc.event.EventStorageEngine/ListAggregateEvents', 'READ');
insert into path_mapping (path, role) values ('io.axoniq.axonserver.grpc.event.EventStorageEngine/ReadHighestSequenceNr', 'READ');
insert into path_mapping( path, role) values ('io.axoniq.axonserver.grpc.event.EventStorageEngine/GetFirstToken', 'READ');
insert into path_mapping( path, role) values ('io.axoniq.axonserver.grpc.event.EventStorageEngine/GetLastToken', 'READ');
insert into path_mapping( path, role) values ('io.axoniq.axonserver.grpc.event.EventStorageEngine/GetTokenAt', 'READ');

insert into path_mapping (path, role) values ('io.axoniq.axonserver.grpc.command.CommandService/OpenStream', 'WRITE');
insert into path_mapping (path, role) values ('io.axoniq.axonserver.grpc.command.CommandService/Dispatch', 'WRITE');

insert into path_mapping (path, role) values ('io.axoniq.axonserver.grpc.query.QueryService/OpenStream', 'READ');
insert into path_mapping (path, role) values ('io.axoniq.axonserver.grpc.query.QueryService/Query', 'READ');
insert into path_mapping( path, role) values ('io.axoniq.axonserver.grpc.query.QueryService/Subscription', 'READ');

insert into path_mapping (path, role) values ('io.axoniq.axonserver.grpc.control.PlatformService/GetPlatformServer', 'READ');
insert into path_mapping (path, role) values ('io.axoniq.axonserver.grpc.control.PlatformService/OpenStream', 'READ');

-- old axonhub grpc names

insert into path_mapping (path, role) values ('io.axoniq.axondb.grpc.EventStorageEngine/ListEvents', 'READ');
insert into path_mapping (path, role) values ('io.axoniq.axondb.grpc.EventStorageEngine/AppendEvent', 'WRITE');
insert into path_mapping (path, role) values ('io.axoniq.axondb.grpc.EventStorageEngine/AppendSnapshot', 'WRITE');
insert into path_mapping (path, role) values ('io.axoniq.axondb.grpc.EventStorageEngine/ListAggregateEvents', 'READ');
insert into path_mapping (path, role) values ('io.axoniq.axondb.grpc.EventStorageEngine/ReadHighestSequenceNr', 'READ');
insert into path_mapping( path, role) values ('io.axoniq.axondb.grpc.EventStorageEngine/GetFirstToken', 'READ');
insert into path_mapping( path, role) values ('io.axoniq.axondb.grpc.EventStorageEngine/GetLastToken', 'READ');
insert into path_mapping( path, role) values ('io.axoniq.axondb.grpc.EventStorageEngine/GetTokenAt', 'READ');

insert into path_mapping (path, role) values ('io.axoniq.axonhub.grpc.CommandService/OpenStream', 'WRITE');
insert into path_mapping (path, role) values ('io.axoniq.axonhub.grpc.CommandService/Dispatch', 'WRITE');

insert into path_mapping (path, role) values ('io.axoniq.axonhub.grpc.QueryService/OpenStream', 'READ');
insert into path_mapping (path, role) values ('io.axoniq.axonhub.grpc.QueryService/Query', 'READ');
insert into path_mapping( path, role) values ('io.axoniq.axonhub.grpc.QueryService/Subscription', 'READ');

insert into path_mapping (path, role) values ('io.axoniq.platform.grpc.PlatformService/GetPlatformServer', 'READ');
insert into path_mapping (path, role) values ('io.axoniq.platform.grpc.PlatformService/OpenStream', 'READ');
