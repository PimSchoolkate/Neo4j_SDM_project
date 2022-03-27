import argparse
from neo4j import GraphDatabase


def get_parser():
    p = argparse.ArgumentParser()
    p.add_argument("-q", "--query",
                   help="define which query has to be executed. By default B1",
                   choices=['B1', "B2", "B3", "B4"],
                   default="B1",
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


def get_driver(d_url, username, password):
    return GraphDatabase.driver(d_url, auth=(username, password))


def get_B(b):
    with open('cypher_queries/{}.txt'.format(b)) as file:
        query = file.read()
    return query

def transaction_B1(tx, query):
    for record in tx.run(query):
        print("")
        print("")
        print("Conference: \t" + record['conference'])
        print("Date: \t \t" + record['date'])
        print("First paper: \t" + record['first_paper'])
        print("Times cited: \t" + str(record['times_cited_1']))
        print("Second paper: \t" + record['second_paper'])
        print("Times cited: \t" + str(record['times_cited_2']))
        print("Third paper: \t" + record['third_paper'])
        print("Times cited: \t" + str(record['times_cited_3']))


def transaction_B2(tx, query):
    for record in tx.run(query):
        print(record)


def transaction_B3(tx, query):
    for record in tx.run(query):
        print("")
        print("Name of Journal: \t" + record["name"])
        print("Year of the Journal: \t" + str(record['year']))
        print("Impact factor: \t \t" + str(record['impact_factor']))


def transaction_B4(tx, query):
    for record in tx.run(query):
        print("h-index: " + str(record['h_index']) + "\t Author: " + record["author"])


def run_query(query, driver, function):
    with driver.session() as session:
        session.write_transaction(function, query)


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    if args.query == "B1":
        stat = "B1: Find the top 3 most cited papers of each conference:"
        query = get_B(args.query)
        function = transaction_B1
    elif args.query == "B2":
        stat = "B2. For each conference and its community: i.e., those authors that have " \
               "published papers on that conference in, at least, 4 different editions:"
        query = get_B(args.query)
        function = transaction_B2
    elif args.query == "B3":
        stat = "B3: Find the impact factors of the journals in your graph:"
        query = get_B(args.query)
        function = transaction_B3
    elif args.query == "B4":
        stat = "B4: Find the h-indexes of the authors in your graph:"
        query = get_B(args.query)
        function = transaction_B4
    else:
        raise ValueError("Could not find the proper query")

    print("_" * len(stat))
    print(stat)

    print("")
    print("______")
    print("Query:")
    print("")
    print(query)

    print("")
    print("_______")
    print("Result:")

    driver = get_driver(args.driver_url, args.username, args.password)

    run_query(query, driver, function)

    print("_"*100)