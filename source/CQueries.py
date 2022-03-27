import argparse
from neo4j import GraphDatabase


def get_parser():
    p = argparse.ArgumentParser()
    p.add_argument("-a", "--algorithm",
                   help="define which algorithm has to be executed. By default node similarity",
                   choices=['node similarity', 'louvain'],
                   default="node similarity",
                   type=str)
    p.add_argument("-head", "--head",
                   help="amount of output lines for the algorithms",
                   default=10,
                   type=int)
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


def get_C(c):
    with open('cypher_queries/{}.txt'.format(c)) as file:
        query = file.read()
    return query


def transaction_function(tx,query):
    tx.run(query)


def transaction_C1(tx,query, head):
    j = 0
    for record in tx.run(query):
        if j < head:
            print("Similarity: " + str(record['similarity']) + "\t Conference 1: " + record['Conference1'] + "\t Conference 2: " +
                  record['Conference2'])
        else:
            break


def transaction_C2(tx,query, head):
    j = 0
    for record in tx.run(query):
        if j < head:
            print("Community id: " + str(record['communityId']) + "\t Name: " + record['name'])
            j = j + 1
        else:
            break


def transaction_C22(tx,query):
    print("")
    for record in tx.run(query):
        print("Total communities in this graph: " + str(record['communityCount']))


def split_queries(queries):
    return queries.split("||")


def run_query(query, driver, function):
    with driver.session() as session:
        session.write_transaction(function, query)


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    driver = get_driver(args.driver_url, args.username, args.password)

    if args.algorithm == 'node similarity':
        print("_" * 100)
        print("Applying the Similarity algorithm to the conferences")
        print("")
        queries = split_queries(get_C("C1"))
        transaction = transaction_C1
        with driver.session() as session:
            for i in range(0, len(queries)):
                if i == 2:
                    session.write_transaction(transaction_C1, queries[i], args.head)
                else:
                    session.write_transaction(transaction_function, queries[i])
        print("_" * 100)

    elif args.algorithm == "louvain":
        print("_"*100)
        print("Applying the Louvain algorithm to the authors to find communities")
        print("")
        queries = split_queries(get_C("C2"))
        transaction = transaction_C2
        with driver.session() as session:
            for i in range(0, len(queries)):
                if i == 2:
                    session.write_transaction(transaction_C2, queries[i], args.head)
                elif i == 3:
                    session.write_transaction(transaction_C22, queries[i])
                else:
                    session.write_transaction(transaction_function, queries[i])
        print("_" * 100)

    else:
        raise ValueError("Something went wrong, please make sure you read the instruction carefully")
