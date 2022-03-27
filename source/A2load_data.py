import argparse
import os
from neo4j import GraphDatabase

def get_parser():
    p = argparse.ArgumentParser()
    p.add_argument("-i", "--input",
                   help="input file, should be authors.csv as provided in the data/evolving, if no alteration in "
                        "the document structure has been made this should automatically find the correct file",
                   choices=["article.csv", "proceeding_papers.csv"],
                   type=str)
    p.add_argument("-ia", "--input_alternative",
                   help="Location of the input csv file if this location has been changed.",
                   type=str)
    p.add_argument("-l", "--limit",
                   help="Amount of lines read of the csv",
                   type=int, default=100)
    p.add_argument("-c", "--constraints",
                   help="Determine if constraints should be added, this only needs to be done once, default 'n'",
                   type=str, choices=['y', 'n'], default="n")
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


def get_file_url(file):
    return "'file:///{}'".format(os.path.abspath(os.path.join(os.getcwd(), os.pardir)) +
                                     "\\data\\processed\\{}".format(file))


def create_file_url(url):
    return "'file:///{}'".format(url)


def get_query_header(url, limit):
    return "LOAD CSV WITH HEADERS FROM {} AS line FIELDTERMINATOR ';' WITH line LIMIT {} ".format(url, limit)


def get_driver(d_url, username, password):
    return GraphDatabase.driver(d_url, auth=(username, password))


def detect_file(url):
    if '/' in url:
        file = url.split('/')[-1]
    else:
        file = url.split("\\")[-1]

    if file == 'article.csv':
        return 'article'
    elif file == 'proceeding_papers.csv':
        return 'proceedings'
    else:
        raise ValueError("The file you uploaded is not supported. This program only handles specifically prepared csv files named: 'article.csv', or 'proceedings_papers.csv'")


def article_query():
    with open('cypher_queries/article.txt', 'r') as file:
        article = file.read()
    return article


def proceedings_query():
    with open('cypher_queries/proceedings.txt', 'r') as file:
        proceedings = file.read()
    return proceedings


def get_constraints():
    with open('cypher_queries/constraints.txt', 'r') as file:
        constraints = file.read()
    return constraints


def transaction_function(tx, query):
    tx.run(query)


def compose_query(header, query):
    return header + query


def run_query(file_url, file_name, d_url, username, password, limit):

    driver = get_driver(d_url, username, password)
    header = get_query_header(file_url, limit)

    # load in the constraints if needed
    if args.constraints == 'y':
        c_query = get_constraints()
        queries = c_query.split('\n')
        with driver.session() as session:
            for q in queries:
                session.write_transaction(transaction_function, q)
        print("Constraints loaded correctly")

    if file_name == 'article.csv':
        query = article_query()
    elif file_name == 'proceeding_papers.csv':
        query = proceedings_query()
    else:
        raise Exception('It should not be possible to see this exception :D')

    query = compose_query(header, query)

    with driver.session() as session:
        session.write_transaction(transaction_function, query)
    driver.close()

    print('{} loaded correctly in the graph'.format(file_name))
    print("_"*110)


if __name__ == "__main__":
    print('_'*110)
    print("Please make sure that within the settings of the database "
          "'dbms.directories.import=import', is commented out")
    print('')
    parser = get_parser()
    args = parser.parse_args()

    if args.input_alternative:
        file_name = detect_file(args.input_alternative)
        file_url = create_file_url(args.input_alternative)
    else:
        file_url = get_file_url(args.input)

    run_query(file_url, args.input, args.driver_url, args.username, args.password, args.limit)
