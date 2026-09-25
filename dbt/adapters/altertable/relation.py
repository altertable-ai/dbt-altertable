from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation


@dataclass(frozen=True, eq=False, repr=False)
class AltertableRelation(BaseRelation):
    # DuckDB accepts a subquery in FROM without an alias. The BaseRelation
    # default (True) makes `dbt build --empty` emit `_dbt_limit_subq_<name>`
    # immediately before an alias the model already wrote.
    require_alias: bool = False
