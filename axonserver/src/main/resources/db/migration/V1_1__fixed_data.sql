INSERT INTO context (name)
VALUES ('default');

INSERT INTO role (name)
VALUES ('ADMIN');
INSERT INTO role_types (role_name, types)
VALUES ('ADMIN', 'USER');
INSERT INTO role_types (role_name, types)
VALUES ('ADMIN', 'APPLICATION');

INSERT INTO role (name)
VALUES ('WRITE');
INSERT INTO role_types (role_name, types)
VALUES ('WRITE', 'APPLICATION');

INSERT INTO role (name)
VALUES ('READ');
INSERT INTO role_types (role_name, types)
VALUES ('READ', 'APPLICATION');
INSERT INTO role_types (role_name, types)
VALUES ('READ', 'USER');

INSERT INTO LOAD_BALANCING_STRATEGY (FACTORY_BEAN, LABEL, NAME)
VALUES ('NoLoadBalance', 'Default (disabled)', 'default');
INSERT INTO LOAD_BALANCING_STRATEGY (FACTORY_BEAN, LABEL, NAME)
VALUES ('ThreadNumberBalancingStrategy', 'Thread Number', 'threadNumber');