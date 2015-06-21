# RamSQL

## Why a non-persistent SQL engine

The idead behing RamSQL is testing.
I don't want to bother with a persistant DBMS, setup the database, manage credentials and clean tables...
But I do want to test my queries, my constraints and run a full test suite in no time. 

### Unit testing

- Full isolation between tests
- No setup (either file or databases)
- Good performance

### Stress testing

- File system full error with configurable maximum database size
- Random configurable slow queries
- Random deconnection

### Developement

When starting a new project, enjoy a clean database at each reboot of your application.

### Production

Not advised


## Roadmap


## Todo

- UPDATE
- PRIMARY KEY

## Done

- CREATE
- INSERT
- SELECT
- WHERE
- DELETE
- COUNT
- AUTO INCREMENT
