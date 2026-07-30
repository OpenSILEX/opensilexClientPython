# CHANGELOG


## Unreleased

### Bug Fixes

- Fix 404 handling in `exists_variable_ctx()` — now properly catches `ApiException` with status 404 and falls back to name-based search instead of logging ERROR

- Fix `test_import_run.py` mock to distinguish between `existing=1` and `created=1` stats

- Fix `test_find_or_create_unit_creation_failure` — mock now properly simulates URI not found before creation failure

- Convert pyproject.toml to Poetry 2.0 format (move metadata from [project] to [tool.poetry])

### Features

- **logging**: Reduce terminal noise during CSV import — console handler set to `WARNING` level, file handler remains at `DEBUG` for full details

- **variables**: Add `ComponentResolutionStats` dataclass to track created/existing/failed component counts during resolution

- **variables**: Add component resolution summary log line: `Components summary: created=N, existing=N, failed=N`

- **variables**: `find_or_create_component()` now returns `(uri, stats)` tuple for component resolution tracking

### Documentation

- Remove Quick Start section and icons from README
  ([`9cc7c06`](https://gitlab.com/OpenSILEX/data-analysis-visualisation/opensilex-clients/opensilex-ws-python-client/-/commit/9cc7c06821e40fcfcd811a6b6315876b257dd772))

- Translate YAML comments to English for international users
  ([`c6af7bb`](https://gitlab.com/OpenSILEX/data-analysis-visualisation/opensilex-clients/opensilex-ws-python-client/-/commit/c6af7bb91a743fa19f3f026fcc86125cad65bd0d))

- Update example notebook
  ([`5bdca47`](https://gitlab.com/OpenSILEX/data-analysis-visualisation/opensilex-clients/opensilex-ws-python-client/-/commit/5bdca477d639e613fce24a91c467a4b20f207a38))

- Update notebook and regenerate documentation
  ([`5014e9f`](https://gitlab.com/OpenSILEX/data-analysis-visualisation/opensilex-clients/opensilex-ws-python-client/-/commit/5014e9f46b251a2eaa3dd72dc6c532f624c895b5))

### Features

- Add a configuration file for credentials and update the example of variable import.
  ([`5dec072`](https://gitlab.com/OpenSILEX/data-analysis-visualisation/opensilex-clients/opensilex-ws-python-client/-/commit/5dec0720c3b53b3af7d7ecc2dc2d9bdc76dfc0f5))

- Add example configuration and CSV files for variable import
  ([`fce74f4`](https://gitlab.com/OpenSILEX/data-analysis-visualisation/opensilex-clients/opensilex-ws-python-client/-/commit/fce74f44a50cc8269e45ada9458c103a1df18be4))

- Add issue
  ([`4f2dfca`](https://gitlab.com/OpenSILEX/data-analysis-visualisation/opensilex-clients/opensilex-ws-python-client/-/commit/4f2dfcab75c4fc1e868a873c37338fac02c9765c))

- Ajouter un fichier CHANGELOG pour la version 2.0.0
  ([`72b5f56`](https://gitlab.com/OpenSILEX/data-analysis-visualisation/opensilex-clients/opensilex-ws-python-client/-/commit/72b5f5682ba38b47118d8df2a14947386b0a250a))

- Allow to download config files
  ([`59401a4`](https://gitlab.com/OpenSILEX/data-analysis-visualisation/opensilex-clients/opensilex-ws-python-client/-/commit/59401a45820f8804708c166a7ca32bb71d8f2452))

- **logging**: Ajouter la gestion structurée des logs et remplacer les print
  ([`11c6421`](https://gitlab.com/OpenSILEX/data-analysis-visualisation/opensilex-clients/opensilex-ws-python-client/-/commit/11c6421be6cb863dde792a9bda12059787e8da8b))

- **variables**: Améliorer la gestion de l'importation des variables et des groupes
  ([`b74f1b8`](https://gitlab.com/OpenSILEX/data-analysis-visualisation/opensilex-clients/opensilex-ws-python-client/-/commit/b74f1b8517f7130b77cfcec08a67ac169ac0b368))

- **variables**: Extraire la logique d'expansion des namespaces
  ([`f14693b`](https://gitlab.com/OpenSILEX/data-analysis-visualisation/opensilex-clients/opensilex-ws-python-client/-/commit/f14693b26d0fb98f3ba07f907dc9896589e8b620))

### Refactoring

- **variables**: Améliorer l'expérience utilisateur lors de l'importation
  ([`19b11b4`](https://gitlab.com/OpenSILEX/data-analysis-visualisation/opensilex-clients/opensilex-ws-python-client/-/commit/19b11b41525da2876b4889571ef63252b96d0670))

- **variables**: Restructurer la gestion des groupes et l'importation des variables
  ([`e0af420`](https://gitlab.com/OpenSILEX/data-analysis-visualisation/opensilex-clients/opensilex-ws-python-client/-/commit/e0af420201ed1e4b8ae3fae07e784ae0e3862388))
