CREATE SCHEMA fed;

CREATE TABLE fed.federal_salaries (
    name        varchar,
    grade       varchar(2),
    pay_plan    varchar(2),
    salary      integer,
    bonus       integer,
    agency      varchar,
    location    varchar,
    occupation  varchar,
    fiscal_year integer,
    id          serial,
    CONSTRAINT serial_id PRIMARY KEY(id)
);