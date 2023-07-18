## Setup
- Requires Python 3.11+
- Install Make
- Install Docker
- Create and activate virtual environment
```
make create-venv
. .venv/bin/activate
```

## Test
### Setup
Create a `.env` file in the `/tests` directory. Tests expect a schema to be present in database built by the docker-compose. `SCHEMA1` should be set to the pre-configured schema and `SCHEMA2` will be the schema that is created by the export/import test.

`### .env ###`
```
DB_HOST=localhost
DATABASE=ORCLPDB1
ADMIN_USER=SYSTEM
ORACLE_PWD=manager
PARALLEL=2
SCHEMA1=HR
SCHEMA2=HR2
SCHEMA1_TBLSPC=users
SCHEMA2_TBLSPC=users2
```

### Run tests
```
make test
```

## Build
```
make build
```
