use test.public;

-- create two tables w/ (unenforced) constraints
create or replace table dept (
	dept_id     integer     PRIMARY KEY,
	name        string      NOT NULL UNIQUE,
	location    string      NULL);

create or replace table emp (
	emp_id      integer     PRIMARY KEY,
	name        string      NOT NULL UNIQUE,
	job         string		DEFAULT 'SALESMAN',
    education   string,
	mgr_id      integer     NULL FOREIGN KEY REFERENCES emp(emp_id),
	hire_date   date,
	salary      float       DEFAULT 0 NOT NULL,
	commission  float		DEFAULT NULL,
    status      string,
    gender      string,
	dept_id     integer     NOT NULL,
    FOREIGN KEY (dept_id)   REFERENCES dept(dept_id));

-- show metadata & relationships
SHOW DATABASES;
SHOW SCHEMAS IN DATABASE test;

SHOW TABLES IN SCHEMA test.public;
SHOW COLUMNS IN SCHEMA test.public;

SHOW UNIQUE KEYS IN SCHEMA test.public;
SHOW PRIMARY KEYS IN SCHEMA test.public;
SHOW IMPORTED KEYS IN SCHEMA test.public;
