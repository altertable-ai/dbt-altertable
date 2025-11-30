# dbt-altertable

A dbt adapter for Altertable.

## Installation

Install the package using pip:

```bash
pip install dbt-altertable
```

Or install from source:

```bash
git clone <repository-url>
cd dbt-altertable
pip install .
```

## Configuration

Add the following to your `profiles.yml`:

```yaml
your_profile_name:
  target: dev
  outputs:
    dev:
      type: altertable
      username: your_username
      password: your_password
      database: your_database
      schema: your_schema
```

### Credentials Fields

- **username** (required): Your Altertable username
- **password** (required): Your Altertable password
- **database** (required): Target database name
- **schema** (required): Target schema name

## SQL Dialect

dbt models should use **DuckDB-compatible SQL syntax**. Altertable uses DuckDB as its query engine, so you can leverage all DuckDB SQL features and functions in your models.

For DuckDB SQL reference, see: https://duckdb.org/docs/sql/introduction

## Credits

This adapter is built upon the excellent work of the [dbt-duckdb](https://github.com/duckdb/dbt-duckdb) project.
