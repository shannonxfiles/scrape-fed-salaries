CREATE TABLE federal_salaries (
    name        varchar(40),
    grade       varchar(2),
    pay_plan    varchar(2),
    salary      integer,
    bonus       integer,
    agency      varchar(50),
    location    varchar(50),
    occupation  varchar(50),
    fiscal_year integer,
    id          serial,
    CONSTRAINT serial_id PRIMARY KEY(id)
);