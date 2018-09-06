create table safepoint (context varchar(255) not null, type varchar(255) not null, token bigint not null, primary key (context, type));
