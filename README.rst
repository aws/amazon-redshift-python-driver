=======================================================
redshift_connector
=======================================================

redshift_connector is the Amazon Redshift connector for
Python. Easy integration with `pandas <https://github.com/pandas-dev/pandas>`_ and `numpy <https://github.com/numpy/numpy>`_, as well as support for numerous Amazon Redshift specific features help you get the most out of your data

Supported Amazon Redshift features include:

- IAM authentication
- Identity provider (IdP) authentication
- Redshift specific data types


This pure Python connector implements `Python Database API Specification 2.0 <https://www.python.org/dev/peps/pep-0249/>`_.


Getting Started
---------------
The easiest way to get started with redshift_connector is via `pip <https://pip.pypa.io/en/stable/>`_

``pip install redshift_connector``

Note: redshift_connector requires Python >= 3.5


You can install from source by cloning this repository. Assuming that you have Python and ``virtualenv`` installed, set up your environment and install the required dependencies like this:

.. code-block:: sh

    $ git clone https://github.com/aws/amazon-redshift-python-driver.git
    $ cd redshift_connector
    $ virtualenv venv
    $ . venv/bin/activate
    $ python -m pip install -r requirements.txt
    $ python -m pip install -e .
    $ python -m pip install redshift_connector

Basic Example
~~~~~~~~~~~~~
.. code-block:: python

    import redshift_connector

    # Connects to Redshift cluster using AWS credentials
    conn = redshift_connector.connect(
        host='examplecluster.abc123xyz789.us-west-1.redshift.amazonaws.com',
        port=5439,
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
+-------------------------+--------------------------------------------------------------------------------------------+---------------+----------+
| Name                    | Description                                                                                | Default Value | Required |
+=========================+============================================================================================+===============+==========+
| database                | String. The name of the database to connect to                                             |               | Yes      |
+-------------------------+--------------------------------------------------------------------------------------------+---------------+----------+
| user                    | String. The username to use for authentication                                             |               | Yes      |
+-------------------------+--------------------------------------------------------------------------------------------+---------------+----------+
| password                | String. The password to use for authentication                                             |               | Yes      |
+-------------------------+--------------------------------------------------------------------------------------------+---------------+----------+
| host                    | String. The hostname of Amazon Redshift cluster                                            |               | Yes      |
+-------------------------+--------------------------------------------------------------------------------------------+---------------+----------+
| port                    | Int. The port number of the Amazon Redshift cluster                                        | 5439          | No       |
+-------------------------+--------------------------------------------------------------------------------------------+---------------+----------+
| ssl                     | Bool. If SSL is enabled                                                                    | True          | No       |
+-------------------------+--------------------------------------------------------------------------------------------+---------------+----------+
| iam                     | Bool. If IAM Authentication is enabled                                                     | False         | No       |
+-------------------------+--------------------------------------------------------------------------------------------+---------------+----------+
| sslmode                 | String. The security of the connection to Amazon Redshift.                                 | 'verify-ca'   | No       |
|                         | 'verify-ca' and 'verify-full' are supported.                                               |               |          |
+-------------------------+--------------------------------------------------------------------------------------------+---------------+----------+
| idp_response_timeout    | Int. The timeout for retrieving SAML assertion from IdP                                    | 120           | No       |
+-------------------------+--------------------------------------------------------------------------------------------+---------------+----------+
| idp_port                | Int. The listen port IdP will send the SAML assertion to                                   | 7890          | No       |
+-------------------------+--------------------------------------------------------------------------------------------+---------------+----------+
| log_level               | Int. The level of logging enabled, increasing in granularity (values [0,4] are valid)      | 0             | No       |
+-------------------------+--------------------------------------------------------------------------------------------+---------------+----------+
| log_path                | String. The file path to the log file                                                      | 'driver.log'  | No       |
+-------------------------+--------------------------------------------------------------------------------------------+---------------+----------+
| max_prepared_statements | Int. The maximum number of prepared statements that can be open at once                    | 1000          | No       |
+-------------------------+--------------------------------------------------------------------------------------------+---------------+----------+
| idp_tenant              | String. The IdP tenant                                                                     | None          | No       |
+-------------------------+--------------------------------------------------------------------------------------------+---------------+----------+
| credential_provider     | String. The IdP that will be used for authenticating with Amazon Redshift.                 | None          | No       |
|                         | 'OktaCredentialsProvider', 'AzureCredentialsProvider', 'BrowserAzureCredentialsProvider',  |               |          |
|                         | 'PingCredentialsProvider', 'BrowserSamlCredentialsProvider', and 'AdfsCredentialsProvider' |               |          |
|                         | are supported.                                                                             |               |          |
+-------------------------+--------------------------------------------------------------------------------------------+---------------+----------+
| cluster_identifier      | String. The cluster identifier of the Amazon Redshift Cluster                              | None          | No       |
+-------------------------+--------------------------------------------------------------------------------------------+---------------+----------+
| db_user                 | String. The user ID to use with Amazon Redshift                                            | None          | No       |
+-------------------------+--------------------------------------------------------------------------------------------+---------------+----------+
| login_url               | String. The SSO Url for the IdP                                                            | None          | No       |
+-------------------------+--------------------------------------------------------------------------------------------+---------------+----------+
| preferred_role          | String. The IAM role preferred for the current connection                                  | None          | No       |
+-------------------------+--------------------------------------------------------------------------------------------+---------------+----------+
| client_secret           | String. The client secret from Azure IdP                                                   | None          | No       |
+-------------------------+--------------------------------------------------------------------------------------------+---------------+----------+
| client_id               | String. The client id from Azure IdP                                                       | None          | No       |
+-------------------------+--------------------------------------------------------------------------------------------+---------------+----------+
| region                  | String. The AWS region where the cluster is located                                        | None          | No       |
+-------------------------+--------------------------------------------------------------------------------------------+---------------+----------+
| app_name                | String. The name of the IdP application used for authentication.                           | None          | No       |
+-------------------------+--------------------------------------------------------------------------------------------+---------------+----------+


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
