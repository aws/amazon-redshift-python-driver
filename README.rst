=======================================================
redshift_connector
=======================================================

|Python Version| |PyPi|

.. |PyPi| image:: https://img.shields.io/pypi/v/redshift_connector.svg?maxAge=432000&style=flat-square
   :target: https://pypi.org/project/redshift_connector/

.. |Python Version| image:: https://img.shields.io/badge/python->=3.6-brightgreen.svg
   :target: https://pypi.org/project/redshift_connector/

``redshift_connector`` is the Amazon Redshift connector for
Python. Easy integration with `pandas <https://github.com/pandas-dev/pandas>`_ and `numpy <https://github.com/numpy/numpy>`_, as well as support for numerous Amazon Redshift specific features help you get the most out of your data

Supported Amazon Redshift features include:

- IAM authentication
- Identity provider (IdP) authentication
- Redshift specific data types


This pure Python connector implements `Python Database API Specification 2.0 <https://www.python.org/dev/peps/pep-0249/>`_.


Getting Started
---------------

Install from Binary
~~~~~~~~~~~~~~~~~~~

+----------------------------------------------------------------+--------------------+-----------------------------------------------------+
| Package Manager                                                | Downloads          | Installation Command                                |
+================================================================+====================+=====================================================+
| `PyPi <https://pypi.org/project/redshift-connector/>`_         |  |PyPi Downloads|  | ``pip install redshift_connector``                  |
+----------------------------------------------------------------+--------------------+-----------------------------------------------------+
| `Conda <https://anaconda.org/conda-forge/redshift_connector>`_ |  |Conda Downloads| | ``conda install -c conda-forge redshift_connector`` |
+----------------------------------------------------------------+--------------------+-----------------------------------------------------+

.. |PyPi Downloads| image:: https://pepy.tech/badge/redshift_connector
.. |Conda Downloads| image:: https://img.shields.io/conda/dn/conda-forge/redshift_connector.svg


Install from Source
~~~~~~~~~~~~~~~~~~~
You may install from source by cloning this repository.

.. code-block:: sh

    $ git clone https://github.com/aws/amazon-redshift-python-driver.git
    $ cd redshift_connector
    $ pip install .

Tutorials
~~~~~~~~~
- `001 - Connecting to Amazon Redshift <https://github.com/aws/amazon-redshift-python-driver/blob/master/tutorials/001%20-%20Connecting%20to%20Amazon%20Redshift.ipynb>`_
- `002 - Data Science Library Integrations <https://github.com/aws/amazon-redshift-python-driver/blob/master/tutorials/002%20-%20Data%20Science%20Library%20Integrations.ipynb>`_
- `003 - Amazon Redshift Feature Support <https://github.com/aws/amazon-redshift-python-driver/blob/master/tutorials/003%20-%20Amazon%20Redshift%20Feature%20Support.ipynb>`_
- `004 - Amazon Redshift Datatypes <https://github.com/aws/amazon-redshift-python-driver/blob/master/tutorials/004%20-%20Amazon%20Redshift%20Datatypes.ipynb>`_

We are working to add more documentation and would love your feedback. Please reach out to the team by `opening an issue <https://github.com/aws/amazon-redshift-python-driver/issues/new/choose>`__ or `starting a discussion <https://github.com/aws/amazon-redshift-python-driver/discussions/new>`_ to help us fill in the gaps in our documentation.

Integrations
~~~~~~~~~~~~
``redshift_connector`` integrates with various open source projects to provide an interface to Amazon Redshift. Please `open an issue <https://github.com/aws/amazon-redshift-python-driver/issues/new/choose>`__ with our project to request new integrations or get support for a ``redshift_connector`` issue seen in an existing integration.

- `apache-airflow <https://github.com/apache/airflow>`_
- `querybook <https://github.com/pinterest/querybook>`_
- `sqlalchemy-redshift <https://github.com/sqlalchemy-redshift/sqlalchemy-redshift>`_

Basic Example
~~~~~~~~~~~~~
.. code-block:: python

    import redshift_connector

    # Connects to Redshift cluster using AWS credentials
    conn = redshift_connector.connect(
        host='examplecluster.abc123xyz789.us-west-1.redshift.amazonaws.com',
        database='dev',
        user='awsuser',
        password='my_password'
     )

    cursor: redshift_connector.Cursor = conn.cursor()
    cursor.execute("create Temp table book(bookname varchar,author varchar)")
    cursor.executemany("insert into book (bookname, author) values (%s, %s)",
                        [
                            ('One Hundred Years of Solitude', 'Gabriel García Márquez'),
                            ('A Brief History of Time', 'Stephen Hawking')
                        ]
                      )
    cursor.execute("select * from book")

    result: tuple = cursor.fetchall()
    print(result)
    >> (['One Hundred Years of Solitude', 'Gabriel García Márquez'], ['A Brief History of Time', 'Stephen Hawking'])

Enabling autocommit
~~~~~~~~~~~~~~~~~~~
**Following the DB-API specification, autocommit is off by default**. It can be turned on by using the autocommit property of the connection.

.. code-block:: py3

    # Make sure we're not in a transaction
    con.rollback()
    con.autocommit = True
    con.run("VACUUM")
    con.autocommit = False


Configuring cursor paramstyle
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The paramstyle for a cursor can be modified via ``cursor.paramstyle``. The default paramstyle used is ``format``. Valid values for ``paramstyle`` include ``qmark, numeric, named, format, pyformat``.

.. code-block:: python

    # qmark
    redshift_connector.paramstyle = 'qmark'
    sql = 'insert into foo(bar, jar) VALUES(?, ?)'
    cursor.execute(sql, (1, "hello world"))

    # numeric
    redshift_connector.paramstyle = 'numeric'
    sql = 'insert into foo(bar, jar) VALUES(:1, :2)'
    cursor.execute(sql, (1, "hello world"))

    # named
    redshift_connector.paramstyle = 'named'
    sql = 'insert into foo(bar, jar) VALUES(:p1, :p2)'
    cursor.execute(sql, {"p1":1, "p2":"hello world"})

    # format
    redshift_connector.paramstyle = 'format'
    sql = 'insert into foo(bar, jar) VALUES(%s, %s)'
    cursor.execute(sql, (1, "hello world"))

    # pyformat
    redshift_connector.paramstyle = 'pyformat'
    sql = 'insert into foo(bar, jar) VALUES(%(bar)s, %(jar)s)'
    cursor.execute(sql, {"bar": 1, "jar": "hello world"})


Exception Handling
~~~~~~~~~~~~~~~~~~~
``redshift_connector`` uses the guideline for exception handling specified in the `Python DB-API <https://www.python.org/dev/peps/pep-0249/#exceptions>`_. For exception definitions, please see `redshift_connector/error.py <https://github.com/aws/amazon-redshift-python-driver/blob/master/redshift_connector/error.py>`_

Example using IAM Credentials
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
IAM Credentials can be supplied directly to ``connect(...)`` using an AWS profile as shown below:

.. code-block:: python

    import redshift_connector

    # Connects to Redshift cluster using IAM credentials from default profile defined in ~/.aws/credentials
    conn = redshift_connector.connect(
        iam=True,
        database='dev',
        db_user='awsuser',
        password='',
        user='',
        cluster_identifier='examplecluster',
        profile='default'
     )

.. code-block:: bash

    # ~/.aws/credentials
    [default]
    aws_access_key_id="my_aws_access_key_id"
    aws_secret_access_key="my_aws_secret_access_key"
    aws_session_token="my_aws_session_token"

    # ~/.aws/config
    [default]
    region=us-west-2

If a region is not provided in `~/.aws/config` or you would like to override its value, `region` may be passed to ``connect(...)``.

Alternatively, IAM credentials can be supplied directly to ``connect(...)`` using AWS credentials as shown below:

.. code-block:: python

    import redshift_connector

    # Connects to Redshift cluster using IAM credentials from default profile defined in ~/.aws/credentials
    conn = redshift_connector.connect(
        iam=True,
        database='dev',
        db_user='awsuser',
        password='',
        user='',
        cluster_identifier='examplecluster',
        access_key_id="my_aws_access_key_id",
        secret_access_key="my_aws_secret_access_key",
        session_token="my_aws_session_token",
        region="us-east-2"
     )

Integration with pandas
~~~~~~~~~~~~~~~~~~~~~~~

Retrieving query results as a ``pandas.DataFrame``

.. code-block:: python

    import pandas
    cursor.execute("create Temp table book(bookname varchar,author varchar)")
    cursor.executemany("insert into book (bookname, author) values (%s, %s)",
                       [
                           ('One Hundred Years of Solitude', 'Gabriel García Márquez'),
                           ('A Brief History of Time', 'Stephen Hawking')

                       ])
    cursor.execute("select * from book")
    result: pandas.DataFrame = cursor.fetch_dataframe()
    print(result)
    >>                         bookname                 author
    >> 0  One Hundred Years of Solitude  Gabriel García Márquez
    >> 1        A Brief History of Time         Stephen Hawking


Insert data stored in a ``pandas.DataFrame`` into an Amazon Redshift table

.. code-block:: python

    import numpy as np
    import pandas as pd

    df = pd.DataFrame(
        np.array(
            [
                ["One Hundred Years of Solitude", "Gabriel García Márquez"],
                ["A Brief History of Time", "Stephen Hawking"],
            ]
        ),
        columns=["bookname", "author‎"],
    )
    with con.cursor() as cursor:
        cursor.write_dataframe(df, "book")
        cursor.execute("select * from book; ")
        result = cursor.fetchall()


Integration with numpy
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    import numpy
    cursor.execute("select * from book")

    result: numpy.ndarray = cursor.fetch_numpy_array()
    print(result)
    >> [['One Hundred Years of Solitude' 'Gabriel García Márquez']
    >>  ['A Brief History of Time' 'Stephen Hawking']]

Query using functions
~~~~~~~~~~~~~~~~~~~~~
.. code-block:: python

    cursor.execute("SELECT CURRENT_TIMESTAMP")
    print(cursor.fetchone())
    >> [datetime.datetime(2020, 10, 26, 23, 3, 54, 756497, tzinfo=datetime.timezone.utc)]


Connection Parameters
~~~~~~~~~~~~~~~~~~~~~
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| Name                              | Type | Description                                                                                                                                                                                                                                                                                                                                                                                               | Default Value        | Required |
+===================================+======+===========================================================================================================================================================================================================================================================================================================================================================================================================+======================+==========+
| access_key_id                     | str  | The access key for the IAM role or IAM user configured for IAM database authentication                                                                                                                                                                                                                                                                                                                    | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| allow_db_user_override            | bool | True specifies the driver uses the DbUser value from the SAML assertion while False indicates the value in the DbUser connection parameter is used                                                                                                                                                                                                                                                        | FALSE                | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| app_name                          | str  | The name of the IdP application used for authentication                                                                                                                                                                                                                                                                                                                                                   | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| auth_profile                      | str  | The name of an Amazon Redshift Authentication profile having connection properties as JSON. See the RedshiftProperty class to learn how connection parameters should be named.                                                                                                                                                                                                                            | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| auto_create                       | bool | Indicates whether the user should be created if they do not exist                                                                                                                                                                                                                                                                                                                                         | FALSE                | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| client_id                         | str  | The client id from Azure IdP                                                                                                                                                                                                                                                                                                                                                                              | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| client_secret                     | str  | The client secret from Azure IdP                                                                                                                                                                                                                                                                                                                                                                          | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| cluster_identifier                | str  | The cluster identifier of the Amazon Redshift Cluster                                                                                                                                                                                                                                                                                                                                                     | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| credentials_provider              | str  | The IdP that will be used for authenticating with Amazon Redshift.                                                                                                                                                                                                                                                                                                                                        | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| database                          | str  | The name of the database to connect to                                                                                                                                                                                                                                                                                                                                                                    | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| database_metadata_current_db_only | bool | Indicates if application supports multi-database datashare catalogs. Default value of  True indicates application does not support multi-database datashare catalogs for backwards compatibility                                                                                                                                                                                                          | TRUE                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| db_groups                         | list | A list of existing database group names that the DbUser joins for the current session                                                                                                                                                                                                                                                                                                                     | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| db_user                           | str  | The user ID to use with Amazon Redshift                                                                                                                                                                                                                                                                                                                                                                   | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| endpoint_url                      | str  | The Amazon Redshift endpoint url. This option is only used by AWS internal teams.                                                                                                                                                                                                                                                                                                                         | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| host                              | str  | The hostname of Amazon Redshift cluster                                                                                                                                                                                                                                                                                                                                                                   | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| iam                               | bool | If IAM Authentication is enabled                                                                                                                                                                                                                                                                                                                                                                          | FALSE                | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| iam_disable_cache                 | bool | This option specifies whether the IAM credentials are cached. By default the IAM credentials are cached. This improves performance when requests to the API gateway are throttled.                                                                                                                                                                                                                        | FALSE                | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| idp_response_timeout              | int  | The timeout for retrieving SAML assertion from IdP                                                                                                                                                                                                                                                                                                                                                        | 120                  | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| idp_tenant                        | str  | The IdP tenant                                                                                                                                                                                                                                                                                                                                                                                            | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| listen_port                       | int  | The listen port IdP will send the SAML assertion to                                                                                                                                                                                                                                                                                                                                                       | 7890                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| login_url                         | str  | The SSO Url for the IdP                                                                                                                                                                                                                                                                                                                                                                                   | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| max_prepared_statements           | int  | The maximum number of prepared statements that can be open at once                                                                                                                                                                                                                                                                                                                                        | 1000                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| numeric_to_float                  | bool | Specifies if NUMERIC datatype values will be converted from decimal.Decimal to float. By default NUMERIC values are received as decimal.Decimal. Enabling this option is not recommended for use cases which prefer the most precision as results may be rounded. Please reference the Python docs on decimal.Decimal to see the tradeoffs between decimal.Decimal and float before enabling this option. | False                | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| partner_sp_id                     | str  | The Partner SP Id used for authentication with Ping                                                                                                                                                                                                                                                                                                                                                       | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| password                          | str  | The password to use for authentication                                                                                                                                                                                                                                                                                                                                                                    | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| port                              | Int  | The port number of the Amazon Redshift cluster                                                                                                                                                                                                                                                                                                                                                            | 5439                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| preferred_role                    | str  | The IAM role preferred for the current connection                                                                                                                                                                                                                                                                                                                                                         | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| principal_arn                     | str  | The ARN of the IAM entity (user or role) for which you are generating a policy                                                                                                                                                                                                                                                                                                                            | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| profile                           | str  | The name of a profile in a AWS credentials file that contains AWS credentials.                                                                                                                                                                                                                                                                                                                            | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| provider_name                     | str  | The name of the Redshift Native Auth Provider.                                                                                                                                                                                                                                                                                                                                                            | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| region                            | str  | The AWS region where the cluster is located                                                                                                                                                                                                                                                                                                                                                               | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| role_arn                          | str  | The Amazon Resource Name (ARN) of the role that the caller is assuming. This parameter is used by JwtCredentialsProvider. For this provider, this is a mandatory parameter.                                                                                                                                                                                                                               | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| role_session_name                 | str  | An identifier for the assumed role session. Typically, you pass the name or identifier that is associated with the user who is using your application. That way, the temporary security credentials that your application will use are associated with that user. This parameter is used by JwtCredentialsProvider. For this provider, this is an optional parameter.                                     | jwt_redshift_session | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| scope                             | str  | Scope for BrowserAzureOauth2CredentialsProvider authentication.                                                                                                                                                                                                                                                                                                                                           | ""                   | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| secret_access_key_id              | str  | The secret access key for the IAM role or IAM user configured for IAM database authentication                                                                                                                                                                                                                                                                                                             | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| session_token                     | str  | The access key for the IAM role or IAM user configured for IAM database authentication. Not required unless temporary AWS credentials are being used.                                                                                                                                                                                                                                                     | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| ssl                               | bool | If SSL is enabled                                                                                                                                                                                                                                                                                                                                                                                         | TRUE                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| ssl_insecure                      | bool | Specifies if IDP hosts server certificate will be verified                                                                                                                                                                                                                                                                                                                                                | TRUE                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| sslmode                           | str  | The security of the connection to Amazon Redshift. verify-ca and verify-full are supported.                                                                                                                                                                                                                                                                                                               | verify_ca            | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| user                              | str  | The username to use for authentication                                                                                                                                                                                                                                                                                                                                                                    | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| web_identity_token                | str  | The OAuth 2.0 access token or OpenID Connect ID token that is provided by the identity provider. Your application must get this token by authenticating the user who is using your application with a web identity provider. This parameter is used by JwtCredentialsProvider. For this provider, this is a mandatory parameter.                                                                          | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+

Supported Datatypes
~~~~~~~~~~~~~~~~~~~
``redshift_connector`` supports the following Amazon Redshift datatypes. ``redshift_connector`` will attempt to treat unsupported datatypes as strings.
Incoming data from Amazon Redshift is treated as follows:

+--------------------------+-------------------+
| Amazon Redshift Datatype | Python Datatype   |
+==========================+===================+
| ACLITEM                  | str               |
+--------------------------+-------------------+
| BOOLEAN                  | bool              |
+--------------------------+-------------------+
| INT8                     | int               |
+--------------------------+-------------------+
| INT4                     | int               |
+--------------------------+-------------------+
| INT2                     | int               |
+--------------------------+-------------------+
| VARCHAR                  | str               |
+--------------------------+-------------------+
| OID                      | int               |
+--------------------------+-------------------+
| REGPROC                  | int               |
+--------------------------+-------------------+
| XID                      | int               |
+--------------------------+-------------------+
| JSON                     | dict              |
+--------------------------+-------------------+
| FLOAT4                   | float             |
+--------------------------+-------------------+
| FLOAT8                   | float             |
+--------------------------+-------------------+
| TEXT                     | str               |
+--------------------------+-------------------+
| CHAR                     | str               |
+--------------------------+-------------------+
| DATE                     | datetime.date     |
+--------------------------+-------------------+
| TIME                     | datetime.time     |
+--------------------------+-------------------+
| TIMETZ                   | datetime.time     |
+--------------------------+-------------------+
| TIMESTAMP                | datetime.datetime |
+--------------------------+-------------------+
| TIMESTAMPTZ              | datetime.datetime |
+--------------------------+-------------------+
| NUMERIC                  | decimal.Decimal   |
+--------------------------+-------------------+
| GEOMETRY                 | str               |
+--------------------------+-------------------+
| SUPER                    | str               |
+--------------------------+-------------------+
| VARBYTE                  | bytes             |
+--------------------------+-------------------+
| GEOGRAPHY                | str               |
+--------------------------+-------------------+

Logging
~~~~~~~~~~~~
``redshift_connector`` uses logging for providing detailed error messages regarding IdP authentication. A do-nothing handler is enabled by default as to prevent logs from being output to ``sys.stderr``.

Enable logging in your application to view logs output by ``redshift_connector`` as described in
the `documentation for Python logging module <https://docs.python.org/3/library/logging.html#/>`_.

Client Transfer Protocol
~~~~~~~~~~~~~~~~~~~~~~~~

``redshift_connector`` requests the Amazon Redshift server use the  highest transfer protocol version supported. As of v2.0.879 binary transfer protocol is requested by default. If necessary, the requested transfer protocol can be modified via the ``client_protocol_version`` parameter of ``redshift_connector.connect(...)``. Please see the Connection Parameters table for more details.


Getting Help
~~~~~~~~~~~~
- Ask a question on `Stack Overflow <https://stackoverflow.com/>`_ and tag it with redshift_connector
- Open a support ticket with `AWS Support <https://console.aws.amazon.com/support/home#/>`_
- If you may have found a bug, please `open an issue <https://github.com/aws/amazon-redshift-python-driver/issues/new>`_

Contributing
~~~~~~~~~~~~
We look forward to collaborating with you! Please read through  `CONTRIBUTING <https://github.com/aws/amazon-redshift-python-driver/blob/master/CONTRIBUTING.md#Reporting-Bugs/Feature-Requests>`_ before submitting any issues or pull requests.

Changelog Generation
~~~~~~~~~~~~~~~~~~~~
An entry in the changelog is generated upon release using `gitchangelog <https://github.com/vaab/gitchangelog>`_. Please use the configuration file, ``.gitchangelog.rc`` when generating the changelog.

Running Tests
-------------
You can run tests by using ``pytest test/unit``. This will run all unit tests. Integration tests require providing credentials for an Amazon Redshift cluster as well as IdP attributes in ``test/config.ini``.

Additional Resources
~~~~~~~~~~~~~~~~~~~~
- `LICENSE <https://github.com/aws/amazon-redshift-python-driver/blob/master/LICENSE>`_
- `Python Database API Specification v2.0 (PEP 249) <https://www.python.org/dev/peps/pep-0249/>`_
- `PostgreSQL Frontend/Backend Protocol <https://www.postgresql.org/docs/9.3/protocol.html>`_
