CREATE TABLE federal_salaries (
    id          serial,
    name        varchar(40),
    grade       integer,
    pay_plan    varchar(2),
    salary      integer,
    bonus       integer,
    agency      varchar(50),
    location    varchar(50),
    occupation  varchar(50),
    fiscal_year integer,
    CONSTRAINT serial_id PRIMARY KEY(id)
);