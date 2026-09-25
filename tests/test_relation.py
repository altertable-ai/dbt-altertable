from __future__ import annotations

from dbt.adapters.altertable.impl import AltertableAdapter


def test_empty_ref_omits_generated_subquery_alias() -> None:
    """dbt build --empty wraps each ref() in a limit-0 subquery.

    DuckDB does not require an alias on that subquery. If one is inserted,
    it collides with the alias the model already wrote (`ref('integ_left') src`).
    """
    relation = AltertableAdapter.Relation.create(
        database="memory",
        schema="analytics",
        identifier="integ_left",
        limit=0,
    )

    assert str(relation) == (
        '(select * from "memory"."analytics"."integ_left" where false limit 0)'
    )
