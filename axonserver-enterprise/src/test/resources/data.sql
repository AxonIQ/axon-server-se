insert into application(id, description, name, hashed_token) values( 1000, 'TEST', 'Test', 'AAAA')
insert into application(id, description, name, hashed_token) values( 2000, 'TestApplication for Delete', 'Delete', 'BBBB')

INSERT INTO role (name) VALUES ('TEST_APPLICATION_ROLE')
INSERT INTO role_types (role_name, types) VALUES ('TEST_APPLICATION_ROLE', 'APPLICATION')
INSERT INTO role (name) VALUES ('TEST_APPLICATION_ROLE_2')
INSERT INTO role_types (role_name, types) VALUES ('TEST_APPLICATION_ROLE_2', 'APPLICATION')

INSERT INTO role (name) VALUES ('TEST_USER_ROLE')
INSERT INTO role_types (role_name, types) VALUES ('TEST_USER_ROLE', 'USER')

create sequence hibernate_sequence start with 10 increment by 1