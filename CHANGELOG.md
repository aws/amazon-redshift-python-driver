Changelog
=========

v2.1.9 (2025-10-14)
-------------------
- Removed unsupported client/stdin COPY protocol implementation that was no longer maintained or supported
- Added LRU (Least Recently Used) cache for prepared statements to improve memory management


v2.1.8 (2025-07-01)
-------------------
- Added support for TCP keepalive properties tcp_keepalive_idle, tcp_keepalive_interval, and tcp_keepalive_count. This allows users to configure TCP keepalive settings, helping to maintain and verify the integrity of long-running database connections
- Added version constraint for lxml dependency to maintain compatibility and prevent breaking changes introduced in lxml 6.0.0


v2.1.7 (2025-05-27)
-------------------
- Modified connection parameter ssl_insecure to be False by default.
- Addressed security issue as detailed in CVE-2025-5279


v2.1.6 (2025-05-06)
-------------------
- Directly imports a package as a fallback mechanism for retrieving a package’s version number during identity provider authentication.
- Fixed stale prepared statement caching issue by clearing the cache upon execution of DROP or ROLLBACK commands.


v2.1.5 (2024-12-23)
-------------------
- Updated the logic for retrieving database metadata through the
  get_catalogs, get_schemas, get_tables, and get_columns API methods.
- Added developer dependency on Selenium. 
- Addressed security issues as detailed in CVE-2024-12745.


v2.1.4 (2024-11-21)
-------------------
- This driver version has been recalled. Python Driver v2.1.3 is recommended for use instead. 


v2.1.3 (2024-07-31)
-------------------
- Added support for a new browser authentication plugin called
  BrowserIdcAuthPlugin to facilitate single-sign-on integration with AWS
  IAM Identity Center. [Brooke White]
- Chore: publish inline type annotations (#224) [James Dow, James Dow]

  Allow inline type hints to be packaged and distributed
  following PEP561 specification
  https://peps.python.org/pep-0561/#specification


v2.1.2 (2024-06-19)
-------------------
- Temporarily reverted the following commit which caused connection
  failure for some Docker containers when SSL was enabled:
  “refactor(Connection): explicitly use TLS protocol for underlying
  connection socket. previously TLS protocol was used by default.
  resolves deprecation warnings in Python 3.11" [Brooke White]


v2.1.1 (2024-04-10)
-------------------
- Fix(auth, AdfsCredentialsProvider): Fixes a login issue that did not
  allow the loginToRp connection parameter in the
  AdfsCredentialsProvider to be set. [Brooke White]


v2.1.0 (2024-02-20)
-------------------
- Fix(execute): fixed a bug which resulted in Pandas Timestamp datatype
  to be sent to Redshift as the DATE instead of TIMESTAMP/TIMESTAMPTZ
  when statements are executed using bind parameters. issue #206.
  [Brooke White]
- Test(datatype): Enable intervaly2m, intervald2s integration tests.
  Correct numeric integration tests. [Brooke White]
- Feat(auth, AdfsCredentialsProvider): Add support for connection
  parameter loginToRp in AdfsCredentialsProvider. This parameter
  specifies a custom relying party trust to use for ADFS authentication.
  Default behavior remains unchanged and backwards compatibility with
  previous versions is maintained The loginToRp parameter can be used to
  define a custom relying party trust used for authenticating with ADFS.
  [Brooke White]
- Doc(README): adjusts connection parameter table formatting. [Brooke
  White]
- Fix(datatype): Fix data ingestion to properly handle NUMERIC data
  types when using the binary transfer protocol to prevent incorrect
  results. issue #207. [Brooke White]
- Fix(cursor, execute): log underlying connection state using class
  variables when exception is raised. resolves db-api extension warnings
  #204. [Brooke White]
- Test: re-enable datatype integration tests. [Brooke White]
- Test: disable or adjust flaky integration tests. [Brooke White]


v2.0.918 (2023-12-14)
---------------------
- Refactor(Connection): explicitly use TLS client protocol for
  underlying connection socket. previously TLS protocol was used by
  default. resolves deprecation warnings in Python 3.11. [Brooke White]
- Fix(Cursor, write_dataframe): Convert pandas dataframe holding bind
  parameters to Python list before query execution. Ensures Python
  datatypes are sent to Redshift server rather than NumPy datatypes.
  [Brooke White]
- Test(Cursor, insert_data_bulk): table_name with and without schema.
  [Brooke White]
- Fix(Cursor, __has_valid_columns): replace table_schema by schema_name.
  resolves unknown column error seen in insert_data_bulk. [Brooke White]
- Fix(Cursor, __is_valid_table): replace table_schema by schema_name
  (#199) [Maximiliano Urrutia]

  SVV_ALL_TABLES does not have table_schema column


v2.0.917 (2023-11-20)
---------------------
- Chore: lint codebase. [Brooke White]
- Feat: Adds ability to connect to datashare databases for clusters and
  serverless workgroups running the PREVIEW_2023 track. [Brooke White]
- Chore: Removed BrowserIdcAuthPlugin. [Brooke White]


v2.0.916 (2023-11-13)
---------------------
- Feat(auth): Added support for Custom Cluster Names (CNAME) for Amazon
  Redshift Serverless. [Brooke White]
- Feat(datatype): Added support for IntervalY2M and IntervalD2S. [Brooke
  White]
- Docs(paramstyle): expanded descriptions for setting paramstyle on
  module and cursor level. added more examples of how to set paramstyle.
  [Brooke White]
- Feat(auth, iam): allowed use of getClusterCredentials V2 API when IdP
  Plugin authentication is used and group_federation=True. Previously an
  unsupported error was thrown. [Brooke White]


v2.0.915 (2023-10-16)
---------------------
- Fix(connection, logging): cast redshift_types as a str to resolve
  exceptions seen when using redshift-connector with logbook. [Brooke
  White]
- Chore: lint codebase. [Brooke White]


v2.0.914 (2023-09-11)
---------------------
- Feat: Identity Center authentication support with new plugins. [Brooke
  White]
- Feat: logging improvements. [Brooke White]
- Fix(auth, cname): log at debug level when
  describe_custom_domain_associations fails. [Brooke White]
- Add parallelization to pytest using pytest-xdist. [Brooke White]
- Feat(auth): automatically determine region. [Brooke White]
- Chore: bump dev pytest version. [Brooke White]
- Chore: lint codebase. [Brooke White]
- Chore: update pre-commit-config. [Brooke White]
- Test(iam, adfs): temporary disable integration tests. [Brooke White]
- Fix(connection): unpack_from error caused by network issues (#185)
  [Soksamnang Lim, soksamnanglim]

  * fix(connection): Raise InterfaceError for network issue related to closed server side socket (#164)

  * add unit test and integration test
  * rectify unit test
  * rectify integration test
  * raise InterfaceError with Broken Pipe for timeout and blocking modes

  * fix(connection): add empty buffer check upon connect and unit tests

  * fix(connection): unpack_from caused by network issues
  * rectify unit tests when network disabled

  ---------
- Fix(connection, cursor): raise DataError when bind parameter limit is
  exceeded (#165) (#187) [Soksamnang Lim, soksamnanglim]

  * raise DataError in Connection execute
  * rethrow error in Cursor insert_data_bulk
  * add integration and unit test for cursor
  * rectify integration test
- Fix(connection): Raise OperationalError for socket timeouts (#179)
  [Jessie Chen]

  * Raise OperationalError for socket timeouts

  * add unit test and integrationt test

  * rectify unit test

  * rectify integration test for socket_timeout

  * remove empty config.ini
- Fix typo for package name in missing module error. (#177) [Kyle
  Demeule]


v2.0.913 (2023-07-12)
---------------------
- Fix(auth, iam): log if boto3 version insufficient for cname. [Brooke
  White]


v2.0.912 (2023-07-05)
---------------------
- Test(cursor): correct test_insert_data_invalid_column_raises. [Brooke
  White]
- Feat(auth): Support Redshift custom domain name. [Brooke White]
- Fix(cursor, __has_valid_columns): raise exception if column validity
  check returns nothing. [Brooke White]
- Doc: python 3.11 support. [Brooke White]
- Refactor: logging improvements. [Brooke White]


v2.0.911 (2023-06-05)
---------------------
- Docs(tutorials, 001): Add example for Azure Native IDP AD. [Brooke
  White]
- Fix(cursor, write_dataframe): respect paramstyle. [Brooke White]
- Fix(cursor, execute): bind param parsing for multiline comments, colon
  character issue. [Brooke White]
- Fix(cursor, execute): remove unnecessary bind param parsing. [Brooke
  White]
- Docs(tutorial): clarify connection methods for IdP and Auth_profile
  (#144) [jiezhen-chen]

  * clarify connection methods for IdP and Auth_profile

  * improve tutorial


v2.0.910 (2023-01-25)
---------------------
- Fix(metadata): views when cast null. [Brooke White]
- Fix(cursor): Always return `pandas.DataFrame` (#141) [Brooke White,
  Fred]

  * Always return `pandas.DataFrame`

  * test(cursor, test_fetch_dataframe_no_results): always return df
- Fix(docs): change variable names from con to conn (#139) [Daiki
  Katsuragawa]


v2.0.909 (2022-09-22)
---------------------
- Bump version to 2.0.909. [Brooke White]
- Refactor: use importlib instead of pkg_resources. [Brooke White]
- Refactor(Connection, handle_PARAMETER_STATUS): comply with PEP 632.
  [Brooke White]
- Chore: add dependency on setuptools. [Brooke White]
- Chore: release constraints on max version for pytz and requests (#119)
  [Pierre Souchay, Pierre Souchay]

  Fix https://github.com/aws/amazon-redshift-python-driver/issues/118
- Docs(tutorials): fix serverless iam example. [Brooke White]


v2.0.908 (2022-06-30)
---------------------
- Docs(connection, serverless): add new connection params. [Brooke
  White]
- Fix(auth, redshift_auth_profile): read auth profile before validating
  conn params. [Brooke White]
- Refactor(type_utils): pg_types -> redshift_types. [Brooke White]
- Refactor(test, test_oids): type checking for py36. [Brooke White]
- Feat(get_datatype_name): support getting datatype name from oid.
  [Brooke White]
- Refactor(auth, iam): support serverless get_workgroup. [Brooke White]
- Refactor(auth, iam): support group federation. [Brooke White]
- Feat(auth, iam): support group federation. [Brooke White]
- Refactor(cursor, description): return column label as str. [Brooke
  White]
- Feat(serverless): support nlb connection. [Brooke White]
- Fix(cursor, __build_local_schema_columns_query): fix numeric scale
  issue with Numeric data type of an external table. [Brooke White]
- Chore: rework tests. [Brooke White]
- Refactor(idp): define IPlugin ABC, remove duplicated code. [Brooke
  White]
- Feat(iam, serverless): support workgroup. [Brooke White]
- Refactor: update plugin process. [Brooke White]
- Chore(deps): update requests requirement (#108) [dependabot[bot]]
- Docs(readme): remove json from data type table (#102) [Noah Holm]
- Docs(readme): document connect timeout parameter (#101) [H​eikki
  H​okkanen]
- Feat(idp, open_browser): Updated SAML Plugin browser launch process.
  [Brooke White]


v2.0.907 (2022-05-05)
---------------------
- Feat(idp, open_browser): validate login URL for SAML plugin. [Brooke
  White]
- Docs: db_groups parameter takes a list, not str (#99) [H​eikki
  H​okkanen]


v2.0.906 (2022-04-15)
---------------------
- Feat(connection, application_name): set to calling module if
  unspecified. [Brooke White]
- Docs(Connection, numeric_to_float): add disclaimer for precision
  tradeoffs. [Brooke White]
- Feat(Connection, numeric_to_float): add connection option to convert
  numeric datatype to Python float. [Brooke White]
- Chore(deps): update pytz requirement (#94) [dependabot[bot]]

  Updates the requirements on [pytz](https://github.com/stub42/pytz) to permit the latest version.
  - [Release notes](https://github.com/stub42/pytz/releases)
  - [Commits](https://github.com/stub42/pytz/compare/release_2020.1...release_2022.1)

  ---
  updated-dependencies:
  - dependency-name: pytz
    dependency-type: direct:production
  ...
- Fix(idp, BrowserAzureCredentialsProvider): remove client_secret
  parameter. [Brooke White]
- Test(auth): sha256 password. [Brooke White]
- Feat(auth): support sha256 password. [Brooke White]
- Docs(readme): add native auth connection params. [Brooke White]
- Test(native-auth): manual, unit tests and fixture. [Brooke White]
- Feat: support Redshift native authentication, Add Azure Oauth2 IdP.
  [Brooke White]


v2.0.904 (2022-02-07)
---------------------
- Docs(readme): clarify pandas df insert. [Brooke White]
- Docs(tutorials): add redshift datatype examples. [Brooke White]
- Refactor(cursor, insert_data_bulk): add batch_size parameter. [Brooke
  White]
- Test(cursor, test_insert_data_column_stmt): Adjust for py36. [Brooke
  White]
- Chore(cursor): lint. [Brooke White]
- Docs(connection): redshift wire methods. [Brooke White]
- Feat(cursor): Add redshift_rowcount for SELECT rowcount support.
  [Brooke White]
- Feat(datatype): support geography. [Brooke White]
- Docs(changelog): add generation instructions. [Brooke White]
- Chore(workflow): set 15 min timeout. [Brooke White]
- Chore: init GitHub actions. [Brooke White]
- Feat(cursor): Add method insert_data_bulk (#81) [Yash Goel]


v2.0.903 (2022-01-10)
---------------------
- Feat(datatype, abstime): support abstime. [Brooke White]
- Chore(tests): enable system table query tests in CI. [Brooke White]
- Docs: add integrations, exception handling, resources. [Brooke White]
- Chore: disable manual serverless tests. [Brooke White]
- Feat(datatype, interval): support interval, timedelta. [Brooke White]
- Feat(connection): support redshift serverless. [Brooke White]
- Chore(deps): update requests requirement (#83) [dependabot[bot]]
- Fix(connection): load system certificates (#76) [Brooke White]

  fix(connection): load system certificates
- Load system certificates in addition to redshift's cert. [evgenyx00]


v2.0.902 (2021-12-14)
---------------------
- Docs(paramstyle, named): fix example. [Brooke White]
- Chore: bump lxml >=4.6.5. [Brooke White]


v2.0.901 (2021-12-06)
---------------------
- Refactor(test, paramstyle): adjust to order result set. [Brooke White]
- Test(iam, aws_credentials): allow aws creds from env var. [Brooke
  White]
- Fix(iam, aws_credentials): allow aws credentials from env var. [Brooke
  White]
- Test(paramstyle): add integration tests, rework. [Brooke White]
- Fix(paramstyle): resolve issue with pyformat. [Brooke White]


v2.0.900 (2021-11-19)
---------------------
- Chore: update package classifiers. [Brooke White]
- Chore: support boto3>=1.9.201,<2.0.0, botocore>=1.12.201,<2.0.0.
  [Brooke White]
- Refactor(test): code clean up. [Brooke White]
- Tests(plugin, adfs): run AdfsCredentialsProvider integration tests.
  [Brooke White]
- Fix(docs): correct minimum supported Python version (#66) [Ramiro
  Morales, Ramiro Morales]


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


