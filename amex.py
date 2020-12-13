#!/usr/bin/env python

import click
import requests
import json
import mysql.connector

from configparser import ConfigParser

def store_json(mydb, jsonfile):

    dbcur = mydb.cursor()
    dbcur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = 'datalake_json'
        """)
    if dbcur.fetchone()[0] == 0:
        dbcur.execute("""
        CREATE TABLE datalake_json(
            Id INT NOT NULL AUTO_INCREMENT,
            PRIMARY KEY(Id),
            JsonDump TEXT(65535) NOT NULL,
            DateCreated DATETIME NOT NULL
        )
        """)

        print ('Table datalake_json created')
    else:
        print ('Table datalake_json already exists')

    data = json.dumps(jsonfile)
 
    dbcur.execute("INSERT INTO datalake_json (JsonDump, DateCreated) VALUES (%s, NOW())",(data,))
    mydb.commit()
    dbcur.close()

    
def store_transactions(mydb, jsonfile):

    dbcur = mydb.cursor()
    dbcur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = 'datalake_transactions'
        """)

    if dbcur.fetchone()[0] == 0:
        dbcur.execute("""
        CREATE TABLE datalake_transactions(
            Id INT NOT NULL AUTO_INCREMENT,
            PRIMARY KEY(Id),
            DateCreated DATETIME NOT NULL,
            identifier NVARCHAR(255) NOT NULL,
            description NVARCHAR(255) NOT NULL,
            statement_end_date DATE NOT NULL,
            charge_date DATE NOT NULL,
            supplementary_index NVARCHAR(50) NULL,
            amount DECIMAL(8,2) NULL,
            type NVARCHAR(50) NULL,
            reference_id NVARCHAR(255) NOT NULL UNIQUE,
            post_date DATE NOT NULL,
            first_name NVARCHAR(100) NULL,
            last_name NVARCHAR(100) NULL,
            embossed_name NVARCHAR(100) NULL,
            account_token NVARCHAR(100) NULL
        )
        """)

        print ('Table datalake_transactions created')
    else:
        print ('Table datalake_transactions already exists')

    for ord in jsonfile["transactions"]:
        print("description:", ord["description"])
        dbcur.execute("""
        INSERT IGNORE INTO datalake_transactions (
            DateCreated, identifier, description, statement_end_date, charge_date, supplementary_index, amount, type, reference_id, post_date, first_name, last_name, embossed_name, account_token
        ) VALUES (NOW(), %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (ord["identifier"],ord["description"],ord["statement_end_date"],ord["charge_date"],ord["supplementary_index"],ord["amount"],ord["type"],ord["reference_id"],ord["post_date"],ord["first_name"],ord["last_name"],ord["embossed_name"],ord["account_token"]))
        mydb.commit()


@click.command()

@click.option(
    '--config-file',
    default="amex.ini",
    type=click.Path(exists=True, dir_okay=True, readable=True),
)


def cmd(config_file):

    ## read configuration ini
    config = ConfigParser()
    config.read(config_file)
    account_token = config.get('account', 'account_token')

    ## initialise db connection
    mydb = mysql.connector.connect(
    host=config.get('mysql', 'host', fallback="localhost"),
    user=config.get('mysql', 'user', fallback=""),
    password=config.get('mysql', 'password', fallback=""),
    database=config.get('mysql', 'database', fallback="amex")
    )

    ## fetch login cookies
    login_headers = {'content-type' : 'application/x-www-form-urlencoded; charset=utf-8'}
    login_payload = 'request_type=login&Logon=Logon&version=4&UserID=' + config.get('account', 'username') + '&Password=' + config.get('account', 'password')
    login_response = requests.post(config.get('amex', 'login_url'), data = login_payload, headers = login_headers)

    loggedin_headers = {'content-type' : 'application/json', 'account_tokens' : account_token }

    """ Initial test to grab balance data
    balances_response = requests.get(config.get('amex', 'balances_url'), headers = loggedin_headers, cookies = login_response.cookies)
    balance = balances_response.json()[0]
    print(balance['statement_balance_amount'])
    """

    trans_response = requests.get(config.get('amex', 'trans_url'), headers = loggedin_headers, cookies = login_response.cookies)
    transactions = trans_response.json()
 
    store_json(mydb, transactions)
    store_transactions(mydb, transactions)

if __name__ == '__main__':
    cmd()


    
