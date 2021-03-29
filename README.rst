=======================================================
redshift_connector
=======================================================

|Python Version| |PyPi|

.. |PyPi| image:: https://img.shields.io/pypi/v/redshift_connector.svg?maxAge=432000&style=flat-square
   :target: https://pypi.org/project/redshift_connector/

.. |Python Version| image:: https://img.shields.io/badge/python->=3.5-brightgreen.svg
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

+----------------------------------------------------------------+--------------------+-----------------------------------------------------+
| Source                                                         | Downloads          | Installation Command                                |
+================================================================+====================+=====================================================+
| `PyPi <https://pypi.org/project/redshift-connector/>`_         |  |PyPi Downloads|  | ``pip install redshift_connector``                  |
+----------------------------------------------------------------+--------------------+-----------------------------------------------------+
| `Conda <https://anaconda.org/conda-forge/redshift_connector>`_ |  |Conda Downloads| | ``conda install -c conda-forge redshift_connector`` |
+----------------------------------------------------------------+--------------------+-----------------------------------------------------+

.. |PyPi Downloads| image:: https://pepy.tech/badge/redshift_connector
.. |Conda Downloads| image:: https://img.shields.io/conda/dn/conda-forge/redshift_connector.svg


Additionally, you may install from source by cloning this repository.

.. code-block:: sh

    $ git clone https://github.com/aws/amazon-redshift-python-driver.git
    $ cd redshift_connector
    $ pip install .

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
    cursor.execute("create Temp table book(bookname varchar,author‎ varchar)")
    cursor.executemany("insert into book (bookname, author‎) values (%s, %s)",
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
    redshift_connector.paramstyle = 'numeric'
    sql = 'insert into foo(bar, jar) VALUES(:p1, :p2)'
    cursor.execute(sql, p1=1, p2="hello world")

    # format
    redshift_connector.paramstyle = 'format'
    sql = 'insert into foo(bar, jar) VALUES(%s, %s)'
    cursor.execute(sql, (1, "hello world"))

    # pyformat
    redshift_connector.paramstyle = 'pyformat'
    sql = 'insert into foo(bar, jar) VALUES(%(bar)s, %(jar)s)'
    cursor.execute(sql, {"bar": 1, "jar": "hello world"})



Example using IAM Credentials
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
.. code-block:: python

    import pandas
    cursor.execute("create Temp table book(bookname varchar,author‎ varchar)")
    cursor.executemany("insert into book (bookname, author‎) values (%s, %s)",
                       [
                           ('One Hundred Years of Solitude', 'Gabriel García Márquez'),
                           ('A Brief History of Time', 'Stephen Hawking')

                       ])
    cursor.execute("select * from book")
    result: pandas.DataFrame = cursor.fetch_dataframe()
    print(result)
    >>                         bookname                 author‎
    >> 0  One Hundred Years of Solitude  Gabriel García Márquez
    >> 1        A Brief History of Time         Stephen Hawking


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
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| Name                              | Type | Description                                                                                                                                                                                                                                                                                                                                                           | Default Value        | Required |
+===================================+======+=======================================================================================================================================================================================================================================================================================================================================================================+======================+==========+
| database                          | str  | The name of the database to connect to                                                                                                                                                                                                                                                                                                                                |                      | Yes      |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| host                              | str  | The hostname of Amazon Redshift cluster                                                                                                                                                                                                                                                                                                                               |                      | Yes      |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| password                          | str  | The password to use for authentication                                                                                                                                                                                                                                                                                                                                |                      | Yes      |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| user                              | str  | The username to use for authentication                                                                                                                                                                                                                                                                                                                                |                      | Yes      |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| access_key_id                     | str  | The access key for the IAM role or IAM user configured for IAM database authentication                                                                                                                                                                                                                                                                                | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| allow_db_user_override            | bool | True specifies the driver uses the DbUser value from the SAML assertion while False indicates the value in the DbUser connection parameter is used                                                                                                                                                                                                                    | FALSE                | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| app_name                          | str  | The name of the IdP application used for authentication                                                                                                                                                                                                                                                                                                               | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| auto_create                       | bool | Indicates whether the user should be created if they do not exist                                                                                                                                                                                                                                                                                                     | FALSE                | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| client_id                         | str  | The client id from Azure IdP                                                                                                                                                                                                                                                                                                                                          | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| client_secret                     | str  | The client secret from Azure IdP                                                                                                                                                                                                                                                                                                                                      | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| cluster_identifier                | str  | The cluster identifier of the Amazon Redshift Cluster                                                                                                                                                                                                                                                                                                                 | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| credentials_provider              | str  | The IdP that will be used for authenticating with Amazon Redshift.                                                                                                                                                                                                                                                                                                    | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| db_groups                         | str  | A comma-separated list of existing database group names that the DbUser joins for the current session                                                                                                                                                                                                                                                                 | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| db_user                           | str  | The user ID to use with Amazon Redshift                                                                                                                                                                                                                                                                                                                               | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| iam                               | bool | If IAM Authentication is enabled                                                                                                                                                                                                                                                                                                                                      | FALSE                | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| idp_response_timeout              | int  | The timeout for retrieving SAML assertion from IdP                                                                                                                                                                                                                                                                                                                    | 120                  | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| idp_tenant                        | str  | The IdP tenant                                                                                                                                                                                                                                                                                                                                                        | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| listen_port                       | int  | The listen port IdP will send the SAML assertion to                                                                                                                                                                                                                                                                                                                   | 7890                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| login_url                         | str  | The SSO Url for the IdP                                                                                                                                                                                                                                                                                                                                               | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| max_prepared_statements           | int  | The maximum number of prepared statements that can be open at once                                                                                                                                                                                                                                                                                                    | 1000                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| port                              | Int  | The port number of the Amazon Redshift cluster                                                                                                                                                                                                                                                                                                                        | 5439                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| preferred_role                    | str  | The IAM role preferred for the current connection                                                                                                                                                                                                                                                                                                                     | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| profile                           | str  | The name of a profile in a AWS credentials file that contains AWS credentials.                                                                                                                                                                                                                                                                                        | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| region                            | str  | The AWS region where the cluster is located                                                                                                                                                                                                                                                                                                                           | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| secret_access_key_id              | str  | The secret access key for the IAM role or IAM user configured for IAM database authentication                                                                                                                                                                                                                                                                         | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| session_token                     | str  | The access key for the IAM role or IAM user configured for IAM database authentication. Not required unless temporary AWS credentials are being used.                                                                                                                                                                                                                 | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| ssl                               | bool | If SSL is enabled                                                                                                                                                                                                                                                                                                                                                     | TRUE                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| ssl_insecure                      | bool | Specifies if IDP hosts server certificate will be verified                                                                                                                                                                                                                                                                                                            | TRUE                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| sslmode                           | str  | The security of the connection to Amazon Redshift. verify-ca and verify-full are supported.                                                                                                                                                                                                                                                                           | verify_ca            | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| database_metadata_current_db_only | bool | Indicates if application supports multi-database datashare catalogs. Default value of  True indicates application does not support multi-database datashare catalogs for backwards compatibility                                                                                                                                                                      | TRUE                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| web_identity_token                | str  | The OAuth 2.0 access token or OpenID Connect ID token that is provided by the identity provider. Your application must get this token by authenticating the user who is using your application with a web identity provider. This parameter is used by JwtCredentialsProvider. For this provider, this is a mandatory parameter.                                      | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| role_session_name                 | str  | An identifier for the assumed role session. Typically, you pass the name or identifier that is associated with the user who is using your application. That way, the temporary security credentials that your application will use are associated with that user. This parameter is used by JwtCredentialsProvider. For this provider, this is an optional parameter. | jwt_redshift_session | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+
| role_arn                          | str  | The Amazon Resource Name (ARN) of the role that the caller is assuming. This parameter is used by JwtCredentialsProvider. For this provider, this is a mandatory parameter.                                                                                                                                                                                           | None                 | No       |
+-----------------------------------+------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------+

Logging
~~~~~~~~~~~~
``redshift_connector`` uses logging for providing detailed error messages regarding IdP authentication. A do-nothing handler is enabled by default as to prevent logs from being output to ``sys.stderr``.

Enable logging in your application to view logs output by ``redshift_connector`` as described in
the `documentation for Python logging module <https://docs.python.org/3/library/logging.html#/>`_.

Getting Help
~~~~~~~~~~~~
- Ask a question on `Stack Overflow <https://stackoverflow.com/>`_ and tag it with redshift_connector
- Open a support ticket with `AWS Support <https://console.aws.amazon.com/support/home#/>`_
- If you may have found a bug, please `open an issue <https://github.com/aws/amazon-redshift-python-driver/issues/new>`_

Contributing
~~~~~~~~~~~~
We look forward to collaborating with you! Please read through  `CONTRIBUTING <https://github.com/aws/amazon-redshift-python-driver/blob/master/CONTRIBUTING.md#Reporting-Bugs/Feature-Requests>`_ before submitting any issues or pull requests.

Running Tests
-------------
You can run tests by using ``pytest test/unit``. This will run all unit tests. Integration tests require providing credentials for an Amazon Redshift cluster as well as IdP attributes in ``test/config.ini``.

Additional Resources
~~~~~~~~~~~~~~~~~~~~
- `LICENSE <https://github.com/aws/amazon-redshift-python-driver/blob/master/LICENSE>`_
