Changelog
=========


v2.0.889 (2021-10-25)
---------------------
- Test(datatype, timetz): ensure tz is always utc. [Brooke White]
- Fix(datatype, timetz): always use binary transfer format. [Brooke
  White]
- Tests(conftest): configure unit tests to run without config.ini.
  [Brooke White]
- Fix named paramstyle in readme (#60) [Ash Berlin-Taylor]


v2.0.888 (2021-09-27)
---------------------

Fix
~~~
- Data manipulation issues in saml_credentials_provider (#57)
  [kylemcleland-fanduel]

  * issue-55

  * issue-56

  * fix: whitespace issue

  * fix: list manipulation

Other
~~~~~
- Docs(readme, datatypes): add varbyte support. [Brooke White]
- Test(datatype, varbyte): send receive varbyte. [Brooke White]
- Fix(datatype, varbyte): support send and receive hex/text varbyte.
  [Brooke White]
- Chore: apply lint. [Brooke White]
- Chore(deps): update requests requirement (#44) [dependabot[bot]]


v2.0.887 (2021-09-14)
---------------------
- Fix(cursor, metadata-queries): support for super, varbyte datatype.
  [Brooke White]
- Test(datatype, varbyte): ensure support. [Brooke White]
- Feat(datatype): support varbyte. [Brooke White]


v2.0.886 (2021-08-30)
---------------------
- Test(datatype, system-tables): enable test for pg_proc. [Brooke White]
- Fix(JwtCredentialsProvider, refresh): use derived user from JWT token
  as RoleSessionName. [Brooke White]
- Test(Cursor, callproc): ensure support. [Brooke White]
- Feat(Cursor, callproc): support db-api method callproc() [Brooke
  White]


v2.0.885 (2021-08-16)
---------------------
- Test(iam, set_iam_credentials): ensure force_lowercase is used.
  [Brooke White]
- Fix(iam, set_iam_credentials): utilize force_lowercase connection opt.
  [Brooke White]
- Fix(iam, set_iam_credentials): consider precedence when setting
  db_user. [Brooke White]


v2.0.884 (2021-08-02)
---------------------
- Refactor(iam-helper): allow IAM keys and profile when auth_profile is
  used. [Brooke White]
- Test(connection, auth-profile): auth via redshift authentication
  profile. [Brooke White]
- Build(dependency): add packaging. [Brooke White]
- Docs(tutorials, 001): redshift auth profile. [Brooke White]
- Feat(connection, auth-profile): support Redshift authentication
  profile. [Brooke White]
- Docs(connection-parameters): add endpoint_url, auth_profile. [Brooke
  White]
- Docs(connection-parameters): reflect change to use all optional
  params. [Brooke White]
- Refactor(docs): alphabetize connection parameters. [Brooke White]
- Refactor(redshift-property): kwargs check. [Brooke White]
- Refactor(Connection): alphabetize setting definitions. [Brooke White]
- Refactor(test, iam_helper): rework for optional connection param
  change. [Brooke White]
- Refactor(Connection): make all parameters optional. [Brooke White]
- Docs(connection): add iam_disable_cache. [Brooke White]
- Test(IamHelper, iam_disable_cache): validate fetch from server when
  cache disable. [Brooke White]
- Feat(connection, iam_disable_cache): add new connection option.
  [Brooke White]
- Chore: apply linting. [Brooke White]
- Test(credentials-holder, is_expired): get current datetime inside
  test. [Brooke White]
- Fix(credentials-holder): check credentials expiration in localtime.
  [ivica.kolenkas]
- Chore(code-style): 'black-ifies' `test_credentials_holder`
  [ivica.kolenkas]
- Fix(tests): adds 'testpaths' config key. [ivica.kolenkas]

  according to https://docs.pytest.org/en/6.2.x/customize.html#setup-cfg it should
  be a path to where tests are. without it running `pytest test/unit` fails
- Chore(code-style): 'black-ifies' code. [ivica.kolenkas]


v2.0.883 (2021-07-19)
---------------------
- Docs(Connection, handle_DATA_ROW): add doc-string. [Brooke White]
- Docs(tutorials, BrowserAzureCredentialsProvider): fix bug in tutorial.
  [Brooke White]
- Docs: update doc-strings, fix typo. [Brooke White]
- Docs(readme): update connection params table, pandas usage. [Brooke
  White]
- Test(idp, browser-azure): display close window message after auth with
  IdP. [Brooke White]
- Fix(idp, browser): display close window message after auth with IdP.
  [Brooke White]


v2.0.882 (2021-06-23)
---------------------
- Refactor(redshift-property, ssl_insecure): use ssl_insecure rather
  than sslInsecure in internal code. [Brooke White]
- Refactor(logging): add additional debug stmts. [Brooke White]
- Docs(tutorials): init tutorials. [Brooke White]
- Refactor(tests, idp): don't pass host, port, db password to idp
  fixtures. [Brooke White]
- Fix(connection, iam-auth): host, port connect() params override
  describe_cluster response. [Brooke White]


v2.0.881 (2021-05-19)
---------------------
- Test(connection, client_protocol_invalid_logs): disable flaky test.
  [Brooke White]
- Test(datatype, geometryhex): ensure support for all protocols. [Brooke
  White]
- Feat(datatype, geometryhex): support geometryhex. [Brooke White]
- Test(connection, transfer-protocol): ensure system table queries
  execute. [Brooke White]
- Chore(deps): update scramp requirement (#35) [dependabot[bot]]
- Chore: update dependabot.yml prefix. [Brooke White]
- Chore: fix dependabot.yml prefix. [Brooke White]
- Chore: init dependabot.yml. [Brooke White]


v2.0.880 (2021-05-10)
---------------------
- Test(connection, transfer-protocol): modify conversion functions when
  server version < client requests. [Brooke White]
- Fix(connection, transfer-protocol): modify conversion functions when
  server version < client requests. [Brooke White]


v2.0.879 (2021-05-10)
---------------------
- Refactor(cursor, setinputsize): use named arguments. [Brooke White]
- Refactor(test, connection): use mock py_types, pg_types. [Brooke
  White]
- Refactor(Connection, type_utils): add py_types, pg_types to Connection
  class, declare typecode constants at package level. [Brooke White]
- Docs(contributing): update PR guidelines. [Brooke White]
- Test(hooks): disable writing test failures to file. [Brooke White]
- Refactor(connection, client-protocol): log when there is a mismatch in
  requested vs granted protocol. [Brooke White]
- Docs(transfer-protocol): modifying transfer protocol via connect
  method. [Brooke White]
- Fix(datatype): support regproc in binary format. [Brooke White]
- Feat(connection, transfer-protocol): enable binary transfer protocol
  by default. [Brooke White]
- Test(connection, datatype): datatype tests for supported transfer
  protocols. [Brooke White]
- Test(connection, transfer-protocol): manual protocol performance
  comparison. [Brooke White]
- Feat(connection, transfer-protocol): support binary transfer protocol.
  [Brooke White]
- Fix(iam_helper): user, password are not required with credential
  provider. [Brooke White]
- Perf(cursor, merge_socket_read): enable by default. [Brooke White]
- Docs(pull_request_template): update PR checklist. [Brooke White]


v2.0.878 (2021-04-19)
---------------------

Fix
~~~
- Running code in windows environment (#30) [HuaHsin Lu]

  * running code in windows environment

  I was getting file not found error, as it turned out the path to the redshift-ca-bundle.crt was incorrectly constructed on MS Windows platform

  * Revert "running code in windows environment"

  This reverts commit efcf32bf7bc97fc7657a05d1e5a7fcdc20c6a17c.

  * docs: running code in MS Windows

Other
~~~~~
- Chore: apply pre-commit. [Brooke White]
- Test(idp, JwtCredentialsProvider): derive db_user from jwt response.
  [Brooke White]
- Fix(idp, JwtCredentialsProvider): derive db_user from jwt token.
  [Brooke White]
- Test(connection, startup): support new startup message properties.
  [Brooke White]
- Fix(connection, startup): support new startup message properties.
  [Brooke White]
- Dev(pre-commit): bump hook versions. [Brooke White]
- Fix(idp, ping): utilize sessions for retrieving SAML (#29) [Dalton
  Conley, Dalton Conley]


v2.0.877 (2021-03-29)
---------------------
- Fix(dependency): bump requests, beautifulsoup4, pytz. [Brooke White]
- Docs: add docstrings for methods directly used methods. [Brooke White]
- Docs(installation): clarify installation methods. [Brooke White]
- Fix(docs): remove pesky \u200e from usage examples. [Brooke White]
- Refactor(IdP): log error response of SAML request. [Brooke White]
- Chore: add dev dependency docutils. [Brooke White]
- Docs(datatype): supported Amazon Redshift datatypes and Python
  datatype counterpart. [Brooke White]
- Docs(cursor, paramstyle): add explanation and examples. [Brooke White]
- Fix(docs, connection-parameters): replace section header. [Brooke
  White]


v2.0.876 (2021-03-10)
---------------------
- Fix(dependency): bump max boto3, botocore version. [Brooke White]
- Chore(dependency): bump lxml>=4.6.2. [Brooke White]


v2.0.875 (2021-03-08)
---------------------
- Fix(requirements): invalid version specifiers. [Brooke White]
- Docs(autocommit): Provide autocommit usage example. [Brooke White]
- Test(auth): add manual authentication tests. [Brooke White]
- Test(iam_helper): update test cases without user and password. [Brooke
  White]
- Fix(credentials): user, password are not required when using AWS
  credentials or AWS profile. [Brooke White]


v2.0.874 (2021-02-15)
---------------------
- Docs(connection): update connection parameters. [Brooke White]
- Refactor(tests): modify config for use with CI. [Brooke White]
- Test(IdP, JwtCredentialsProvider): JWT SSO IdP support. [Brooke White]
- Feat(IdP, JwtCredentialsProvider): support JWT SSO IdP. [Brooke White]
- Test(idp): cached temporary AWS credentials used if present and valid.
  [Brooke White]
- Feat(idp): cache temporary aws credentials to reduce calls to AWS API.
  [Brooke White]
- Refactor(adfs_credentials_provider): explicitly specify lxml as parser
  for server response. [Brooke White]
- Refactor(iam_helper): simplify validation of sslmode connection
  parameter. [Brooke White]
- Fix(connection): require cluster_identifier when IAM is enabled.
  [Brooke White]
- Test(IdP): dynamic loading and use of external IdP plugin. [Brooke
  White]
- Feat(IdP): support dynamically loaded IdP plugins. [Brooke White]


v2.0.873 (2021-01-25)
---------------------
- Docs(auth): IAM credential authentication. [Brooke White]
- Test(auth): IAM credential authentication. [Brooke White]
- Feat(auth): support IAM credential authentication. [Brooke White]
- Test(datatype): mark for manual execution. [Brooke White]
- Docs: add conda installation instructions, shields. [Brooke White]
- Test(datatype): handle geometry. [Brooke White]
- Feat(datatype): support geometry. [Brooke White]
- Refctor(mypy): correct type errors. [Brooke White]
- Chore: add mypy pre-commit hook. [Brooke White]
- Fix(test, datatype): Fix datatype test table generation. [Brooke
  White]
- Revert "chore: init traffic action" [Brooke White]

  This reverts commit e73cf79799a6c04a6eaefbac77314b87388d77d0.
- Revert "chore: add manual trigger to traffic workflow" [Brooke White]

  This reverts commit 4209b703dbdf93f40310a2a05846d1e1fa1afcaf.
- Revert "chore: update traffic analyzer display name" [Brooke White]

  This reverts commit 77a9c1de1f6ce54266ff67bc6aa894d3b4910240.
- Chore: update traffic analyzer display name. [Brooke White]
- Chore: add manual trigger to traffic workflow. [Brooke White]
- Chore: init traffic action. [Brooke White]


v2.0.872 (2020-12-14)
---------------------
- Fix(test,datatype): setup/teardown sql script path. [Brooke White]
- Chore(test): ignore InsecureRequestWarning. [Brooke White]
- Test(idp): integration and unit tests for adfs, azure, okta. [Brooke
  White]
- Test(datashare): skip datashare tests for now. [Brooke White]
- Test(datatype): handle data from server. [Brooke White]
- Feat(datatype): support timetz. [Brooke White]
- Feat(datatype): support super. [Brooke White]
- Feat(connection): add ssl_insecure parameter default to True. [Brooke
  White]
- Test(datashare): connection args, cursor metadata methods. [Brooke
  White]
- Feat(connection): add datashare support, disabled by default. [Brooke
  White]
- Docs: remove log args from connection parameter, explain logging
  usage. [Brooke White]
- Fix(logging): remove logging configuration from connect(), suppress
  log stderr output by default. [Brooke White]
- Perf(imports): use lazy-imports for idp plugins. [Brooke White]
- Chore: correct mypy errors. [Brooke White]
- Feat(Connection): add initializer parameter for server protocol
  version, default to extended metadata. [Brooke White]


v2.0.711 (2020-11-25)
---------------------
- Refactor(cursor): use prepared statements in class methods. [Brooke
  White]


v2.0.659 (2020-11-23)
---------------------
- Refactor(integer-datetime): remove datatype conversion functions for
  when server option integer_datetime is disabled. [Brooke White]
- Refactor: clean up setup.py. [Brooke White]
- Docs: update installation instructions to reflect pandas, numpy being
  optional. [Brooke White]
- Test(cursor): skips test using numpy, pandas if not available. [Brooke
  White]
- Refactor(cursor): check pandas, numpy available before use. [Brooke
  White]
- Build: make numpy, pandas optional dependencies unify dependecy list
  in requirements txt. [Brooke White]
- Test(datatype): remove unsupported tests for timetz, time. [Brooke
  White]
- Test(auth): test invalid db_groups vals result in server error.
  [Brooke White]
- Fix(auth): include DbGroups when getting temp credentials from boto.
  [Brooke White]
- Refactor(cursor): extend fetch_dataframe error handling. [Brooke
  White]
- Doc: Fix errors in connection parameter table. Add missing parameters
  to table (#12) [Brooke White]


v2.0.405 (2020-11-05)
---------------------
- Doc: re-word project description (#8) [Brooke White]


v2.0.399 (2020-11-05)
---------------------
- Chore: Add license, usage files and include in whl (#7) [Brooke White]


v2.0.393 (2020-11-05)
---------------------
- Build: set long_description_format_type set to x-rst (#6) [Brooke
  White]


v2.0.389 (2020-11-05)
---------------------
- Build: include README in built dist (#5) [Brooke White]


v2.0.384 (2020-11-04)
---------------------
- Chore: bump boto3, botocore min versions to 1.16.8, 1.19.8 resp (#4)
  [Brooke White]
- Chore: add readme hyperlink to DBAPI2.0 (#3) [Brooke White]
- Chore: init repository. [Brooke White]
- Docs: Add installation instructions and example usage to README.
  [Brooke White]
- Update THIRD_PARTY_LICENSES. [iggarish]
- Update PULL_REQUEST_TEMPLATE.md. [iggarish]
- Create ISSUE_TEMPLATE.md. [iggarish]
- Create THIRD_PARTY_LICENSES. [iggarish]
- Create CHANGELOG.md. [iggarish]
- Create PULL_REQUEST_TEMPLATE.md. [iggarish]
- Added initial content in README file. [ilesh Garish]
- Added initial content in README file. [ilesh Garish]
- Initial code. [ilesh garish]
- Initial commit. [Amazon GitHub Automation]


