## Redshift Python Connector

Redshift Python Connector requires Python3 and pip3. This connector has many Redshift specific features such as,

  * IAM authentication
  * IDP authentication
  * Redshift specific datatypes support
  * API to explore database objects such as get_tables(), get_columns() etc.
  * External schema support as part of get_tables() and get_columns() API
  * Easy to integrate with Pandas and NumPy
  * Pure Python Driver

  This connector supports Python Database API Specification v2.0.

  ## Build Driver
  On Unix system run:
  ```
  build.sh
  ```
  It builds **redshift_connector-{version}-py3-none-any.whl** and **redshift_connector-{version}.tar.gz** files under **dist** directory.
  The Wheel file is the Redshift Python Connector built distribution.
  The zip file is the source archive of the Redshift Python Connector.

  ## Report Bugs

  See [CONTRIBUTING](CONTRIBUTING.md#Reporting-Bugs/Feature-Requests) for more information.

  ## Contributing Code Development

  See [CONTRIBUTING](CONTRIBUTING.md#Contributing-via-Pull-Requests) for more information.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

