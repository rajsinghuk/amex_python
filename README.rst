Amex_Python
---------

Simple python project for downloading recent transactions data from AmericanExpress.com and loading it into MySQL.


Prerequisites
============

Install the Python requirements with pip

.. code:: bash

    pip install -r requirements.txt

American Express card required, and account on Americanexpress.com.  At this stage, the Account Token must be manually identified.  The Account Token is a unique identifier for the particular credit or charge card in question.  This can be easily located by logging into the customer website and searching an inspector for "account_token".

MySQL installation required.  Information needed is server host, user name, password and database name.

All items above are configured in amex.ini.  Sample configuration file has been included.  An alternative configuration filename can be selected using

.. code:: bash

    python amex.py ---config-file=amex2.ini
