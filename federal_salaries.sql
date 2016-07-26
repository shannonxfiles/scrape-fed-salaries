CREATE TABLE federal_salaries (
    id          serial,
    name        varchar(40),
    grade       varchar(5),
    pay_plan    integer,
    salary      integer,
    bonus       integer,
    agency      varchar(50),
    location    varchar(50),
    occupation  varchar(50),
    fiscal_year integer(4),
    CONSTRAINT serial_id PRIMARY KEY(id)
);