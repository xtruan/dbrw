# dbrw

Postgres DataBase Reader and Writer utilities.

## Installation

```bash
pip install dbrw
```

## Basic Usage


```bash
export DBRW_PGHOST=localhost
export DBRW_PGPORT=5432
export DBRW_PGDBNAME=postgres
export DBRW_PGUSER=postgres
export DBRW_PGPASSWORD=postgres
export DBRW_PGPOOLSIZE=10
```

```python
from dbrw import DbSession, DbUtilities

session: DbSession = DbSession()
db: DbUtilities = session.get_db()

db.execute("SELECT * FROM mytable;")
```
