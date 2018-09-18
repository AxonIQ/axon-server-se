CREATE  TABLE role (
  name VARCHAR(45) NOT NULL ,
  PRIMARY KEY (name));

CREATE TABLE role_types (
  role_name varchar(45) NOT NULL,
  types varchar(45) NOT NULL,
  CONSTRAINT fk_rolename FOREIGN KEY (role_name) REFERENCES role (name));

INSERT INTO role (name) VALUES ('ADMIN');
INSERT INTO role_types (role_name, types) VALUES ('ADMIN', 'USER');
INSERT INTO role_types (role_name, types) VALUES ('ADMIN', 'APPLICATION');

INSERT INTO role (name) VALUES ('WRITE');
INSERT INTO role_types (role_name, types) VALUES ('WRITE', 'APPLICATION');

INSERT INTO role (name) VALUES ('READ');
INSERT INTO role_types (role_name, types) VALUES ('READ', 'APPLICATION');

INSERT INTO role (name) VALUES ('USER');
INSERT INTO role_types (role_name, types) VALUES ('USER', 'USER');

insert into path_mapping( path, role) values ('GET:/v1/roles/*', 'ADMIN');
