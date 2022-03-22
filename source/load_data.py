import argparse
from neo4j import GraphDatabase

def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input",
                        help="Location of the input csv file",
                        type=str)
    parser.add_argument("-l", "--limit",
                        help="Amount of lines read of the csv",
                        type=int, default=-1)
    #parser.add_argument("-b", "--batches",
    #                    help="If data needs to be loaded in batches, and if so how many batches per iteration",
    #                    type=int, default=-1)
    parser.add_argument("-d", "--driver_url",
                        help="URL of the neo4j database",
                        type=str, default="neo4j://localhost:7687",)
    parser.add_argument("-u", "--username",
                        help="Authentication username of the Neo4j database",
                        type=str, default="neo4j")
    parser.add_argument("-p", "--password",
                        help="Authentication password of the Neo4j database",
                        type=str, default="louis")
    return parser


def get_file_url(url):
    return "'file:///{}'".format(url)


def get_query_header(url, limit):
    return "LOAD CSV WITH HEADERS FROM {} AS line FIELDTERMINATOR ';' WITH line LIMIT {}".format(url, limit)


def get_driver(d_url, username, password):
    return GraphDatabase.driver(d_url, auth=(username, password))


def detect_file(url):
    file = url.split('/')[-1]
    if file == 'article.csv':
        return 'article'
    elif file == 'proceeding_papers.csv':
        return 'proceedings'
    else:
        raise ValueError("The file you uploaded is not supported. This program only handles specifically prepared csv files named: 'article.csv', or 'proceedings_papers.csv'")


def article_query():
    with open('article.txt', 'r') as file:
        article = file.read()
    return article


def proceedings_query():
    with open('proceedings.txt', 'r') as file:
        proceedings = file.read()
    return proceedings


def transaction_function(tx, query):
    tx.run(query)


def compose_query(header, query):
    return header + query


def run_query(url, d_url, username, password, limit):
    file_name = detect_file(url)
    file_url = get_file_url(url)
    driver = get_driver(d_url, username, password)
    header = get_query_header(file_url, limit)
    if file_name == 'article':
        query = article_query()
    elif file_name == 'proceedings':
        query = proceedings_query()
    else:
        raise Exception('It should not be possible to see this exception :D')

    query = compose_query(header, query)

    with driver.session() as session:
        session.write_transaction(transaction_function, query)


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    run_query(args.input, args.driver_url, args.username, args.password, args.limit)
