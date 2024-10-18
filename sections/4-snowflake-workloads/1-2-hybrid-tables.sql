-- hybrid table (OLTP+OLAP-optimized)
-- only in a paid Snowflake account! (now in PuPr, in some AWS regions only)
use test.public;

create or replace hybrid table projh (
	proj_id     integer     PRIMARY KEY,
	name        string      NOT NULL UNIQUE,
	start_date  date        NOT NULL,
    end_date    date        NULL);

create or replace index projh_dates (start_date, end_date) on projh;

insert into projh values
    (1,    'Cleanup Data',          '1980-12-05',   '1981-01-09'    ),
    (2,    'ETL Pipeline',          '1981-01-09',   '1981-04-02'    ),
    (3,    'Data Preprocessing',    '1981-04-02',   '1981-06-08'    ),
    (4,    'Create Dashboard',      '1981-06-09',   '1981-07-22'    ),
    (5,    'ML Kickoff',            '1981-08-28',   '1981-09-11'    ),
    (6,    'Model Training',        '1981-09-28',   '1982-12-10'    ),
    (7,    'Model Deployment',      '1982-12-11',   null            );

-- these will fail!
insert into projh values (1, 'Cleanup Data 2', '1980-12-05', '1981-01-09');
insert into projh values (10, 'Cleanup Data', '1980-12-05', '1981-01-09');

-- cleanup resource (to avoid costs!)
drop table projh;
