# Changelog

## [0.2.6](https://github.com/altertable-ai/dbt-altertable/compare/dbt-altertable-v0.2.5...dbt-altertable-v0.2.6) (2026-07-30)


### Bug Fixes

* **incremental:** support delete-insert runs ([#35](https://github.com/altertable-ai/dbt-altertable/issues/35)) ([a093529](https://github.com/altertable-ai/dbt-altertable/commit/a09352924a014a72ec74f41fab75fc036740ee0e))

## [0.2.5](https://github.com/altertable-ai/dbt-altertable/compare/dbt-altertable-v0.2.4...dbt-altertable-v0.2.5) (2026-07-25)


### Performance Improvements

* **connections:** isolate Flight sessions per dbt connection ([#33](https://github.com/altertable-ai/dbt-altertable/issues/33)) ([37787a6](https://github.com/altertable-ai/dbt-altertable/commit/37787a6271698911eecda935e5278ecc44074400))

## [0.2.4](https://github.com/altertable-ai/dbt-altertable/compare/dbt-altertable-v0.2.3...dbt-altertable-v0.2.4) (2026-05-16)


### Features

* add MetricFlow compatibility patch for dbt-altertable ([#26](https://github.com/altertable-ai/dbt-altertable/issues/26)) ([8cad37f](https://github.com/altertable-ai/dbt-altertable/commit/8cad37f7312b66456707d9a328e072f328930b28))


### Bug Fixes

* reuse shared client across threads ([#30](https://github.com/altertable-ai/dbt-altertable/issues/30)) ([9bb3d76](https://github.com/altertable-ai/dbt-altertable/commit/9bb3d763ea77b25151570c43f465c09e9b59af09))
* **seeds:** default to INSERT batches and gate COPY fast path ([#29](https://github.com/altertable-ai/dbt-altertable/issues/29)) ([7f87082](https://github.com/altertable-ai/dbt-altertable/commit/7f87082471ac8a999db611ac23db80d96351afb6))

## [0.2.3](https://github.com/altertable-ai/dbt-altertable/compare/dbt-altertable-v0.2.2...dbt-altertable-v0.2.3) (2026-04-28)


### Bug Fixes

* **persist_docs:** fail gracefully when trying to add a comment on view columns ([#22](https://github.com/altertable-ai/dbt-altertable/issues/22)) ([e0b3c5c](https://github.com/altertable-ai/dbt-altertable/commit/e0b3c5cc01d549670cc34965f00f9ed0f116ba8a))

## [0.2.2](https://github.com/altertable-ai/dbt-altertable/compare/dbt-altertable-v0.2.1...dbt-altertable-v0.2.2) (2026-04-27)


### Features

* expose all user-allowed catalogs in each dbt session ([#14](https://github.com/altertable-ai/dbt-altertable/issues/14)) ([4c7b701](https://github.com/altertable-ai/dbt-altertable/commit/4c7b701e37a7dc50c69c9f00a8232ef3517d75eb))
* make everything ready for release ([#2](https://github.com/altertable-ai/dbt-altertable/issues/2)) ([0d09027](https://github.com/altertable-ai/dbt-altertable/commit/0d090274ff4d6c1ab9544cdacc75a611d2d73f63))


### Bug Fixes

* connect without binding a schema ([#11](https://github.com/altertable-ai/dbt-altertable/issues/11)) ([9cbc523](https://github.com/altertable-ai/dbt-altertable/commit/9cbc523c2520f1b00a0c2f403216c633875ca70d))
* **macros:** omit CASCADE from drop statements for DuckLake ([#15](https://github.com/altertable-ai/dbt-altertable/issues/15)) ([bcf065d](https://github.com/altertable-ai/dbt-altertable/commit/bcf065d6d4b5a7e5ba62c7660a127160289711d6))
* persisted docs and integration tests ([#17](https://github.com/altertable-ai/dbt-altertable/issues/17)) ([54415c3](https://github.com/altertable-ai/dbt-altertable/commit/54415c337cb9a8040ed0f6719428b36dbffc27df))


### Performance Improvements

* **connections:** fetch Arrow natively and lazy-materialize rows ([#7](https://github.com/altertable-ai/dbt-altertable/issues/7)) ([ff615ca](https://github.com/altertable-ai/dbt-altertable/commit/ff615ca3b12e962d4d4b8f45599144632a292abc))

## [0.2.1](https://github.com/altertable-ai/dbt-altertable/compare/v0.2.0...v0.2.1) (2026-04-27)


### Bug Fixes

* persisted docs and integration tests ([#17](https://github.com/altertable-ai/dbt-altertable/issues/17)) ([54415c3](https://github.com/altertable-ai/dbt-altertable/commit/54415c337cb9a8040ed0f6719428b36dbffc27df))

## [0.2.0](https://github.com/altertable-ai/dbt-altertable/compare/v0.1.0...v0.2.0) (2026-04-23)


### Features

* expose all user-allowed catalogs in each dbt session ([#14](https://github.com/altertable-ai/dbt-altertable/issues/14)) ([4c7b701](https://github.com/altertable-ai/dbt-altertable/commit/4c7b701e37a7dc50c69c9f00a8232ef3517d75eb))


### Bug Fixes

* connect without binding a schema ([#11](https://github.com/altertable-ai/dbt-altertable/issues/11)) ([9cbc523](https://github.com/altertable-ai/dbt-altertable/commit/9cbc523c2520f1b00a0c2f403216c633875ca70d))
* **macros:** omit CASCADE from drop statements for DuckLake ([#15](https://github.com/altertable-ai/dbt-altertable/issues/15)) ([bcf065d](https://github.com/altertable-ai/dbt-altertable/commit/bcf065d6d4b5a7e5ba62c7660a127160289711d6))


### Performance Improvements

* **connections:** fetch Arrow natively and lazy-materialize rows ([#7](https://github.com/altertable-ai/dbt-altertable/issues/7)) ([ff615ca](https://github.com/altertable-ai/dbt-altertable/commit/ff615ca3b12e962d4d4b8f45599144632a292abc))

## 0.1.0 (2025-11-30)


### Features

* make everything ready for release ([#2](https://github.com/altertable-ai/dbt-altertable/issues/2)) ([0d09027](https://github.com/altertable-ai/dbt-altertable/commit/0d090274ff4d6c1ab9544cdacc75a611d2d73f63))
