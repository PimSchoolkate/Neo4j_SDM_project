import os
import pandas as pd
import random
import math
import argparse
from neo4j import GraphDatabase


def get_parser():
    p = argparse.ArgumentParser()
    p.add_argument("-i", "--input",
                   help="input file, should be authors.csv as provided in the data/evolving, if no alteration in "
                        "the document structure has been made this should automatically find the correct file",
                   default="/data/evolving/authors.csv",
                   type=str)
    p.add_argument("-d", "--driver_url",
                   help="URL of the neo4j database",
                   type=str, default="neo4j://localhost:7687", )
    p.add_argument("-u", "--username",
                   help="Authentication username of the Neo4j database",
                   type=str, default="neo4j")
    p.add_argument("-p", "--password",
                   help="Authentication password of the Neo4j database",
                   type=str, default="louis")
    return p

def get_file_url(url):
    if url == "/data/evolving/authors.csv":
        return "'file:///{}'".format(os.path.abspath(os.path.join(os.getcwd(), os.pardir)) +
                                     "\\data\\evolving\\authors.csv")
    else:
        return "'file:///{}'".format(url)


def get_query_header(url):
    return "LOAD CSV WITH HEADERS FROM {} AS line FIELDTERMINATOR ';' WITH line ".format(url)


def get_driver(d_url, username, password):
    return GraphDatabase.driver(d_url, auth=(username, password))


def get_update_affiliations():
    with open('cypher_queries/update_affiliation.txt', 'r') as file:
        update_affiliations =  file.read()
    return update_affiliations


def get_update_decision():
    with open('cypher_queries/update_decision.txt', 'r') as file:
        update_decision=  file.read()
    return update_decision


def get_update_review():
    with open('cypher_queries/update_review.txt', 'r') as file:
        update_review =  file.read()
    return update_review


def compose_query(header, query):
    return header + query


def transaction_function(tx, query):
    tx.run(query)


if __name__ == "__main__":
    print("_"*100)
    print("")
    parser = get_parser()
    args = parser.parse_args()
    url = get_file_url(args.input)

    header = get_query_header(url)
    aff = compose_query(header, get_update_affiliations())
    rev = compose_query(header, get_update_review())
    dec = get_update_decision()


    driver = get_driver(args.driver_url, args.username, args.password)
    with driver.session() as session:
        session.write_transaction(transaction_function, aff)
        print('successfully updated all affiliations')
        session.write_transaction(transaction_function, rev)
        print('successfully updated all reviews')
        session.write_transaction(transaction_function, dec)
        print('successfully set all decisions based on the reviews for each paper with reviews')

    print("_"*100)
