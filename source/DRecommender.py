import argparse
from neo4j import GraphDatabase


def get_parser():
    p = argparse.ArgumentParser()
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


def get_D():
    with open('cypher_queries/DRecommender.txt') as file:
        queries = file.read()
    return queries


def split_queries(queries):
    return queries.split("||")


def transaction_function(tx,query):
    tx.run(query)


def transaction_function_last(tx,query):
    for record in tx.run(query):
        print("Name: " + record["author"] + " \t Amount of top papers written: " +
              str(record['amount_of_top_papers_written']))


def transaction_function_first(tx,query):
    print("Proportions of conferences")
    for record in tx.run(query):
        print("Proportion in the community: " + str(record['prop_in_community']) +
              "Conference: " + record["journal"])
    print("")


def transaction_function_middle(tx,query):
    print("Proportions of Journals")
    for record in tx.run(query):
        print("Proportion in the community: " + str(record['prop_in_community']) +
              "Journal: " + record["conference"])
    print("")


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    queries = get_D()
    queries = split_queries(queries)

    driver = get_driver(args.driver_url, args.username, args.password)

    print("_"*100)

    with driver.session() as session:
        for i in range(0, len(queries)):
            if i == 2:
                session.write_transaction(transaction_function_first, queries[i])
            elif i == 3:
                session.write_transaction(transaction_function_middle, queries[i])
            elif i == 6:
                session.write_transaction(transaction_function_last, queries[i])
            else:
                session.write_transaction(transaction_function, queries[i])




