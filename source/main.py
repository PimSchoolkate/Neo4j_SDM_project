from neo4j import GraphDatabase
from nodebuild_utils import *

driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "louis"))

def add_friend(tx, name, friend_name):
    tx.run("MERGE (a:Person {name: $name}) "
           "MERGE (a)-[:KNOWS]->(friend:Person {name: $friend_name})",
           name=name, friend_name=friend_name)

def print_friends(tx, name):
    for record in tx.run("MATCH (a:Person)-[:KNOWS]->(friend) WHERE a.name = $name "
                         "RETURN friend.name ORDER BY friend.name", name=name):
        print(record["friend.name"])

with driver.session() as session:
    session.write_transaction(add_friend, "Arthur", "Guinevere")
    session.write_transaction(add_friend, "Arthur", "Lancelot")
    session.write_transaction(add_friend, "Arthur", "Merlin")
    session.read_transaction(print_friends, "Arthur")

driver.close()


test_labels = ("name", "age", "school")
test_label_values = ("Pim", 24, "UPC")
test_label_values2 = ("Louis", 21, "UPC")

print(label_constructor(test_labels, test_label_values))

test_node_1 = node_constructor("type", test_labels, test_label_values)
test_node_2 = node_constructor("type", test_labels, test_label_values2)

edge = edge_constructor("friend_of", labels=tuple(("quality", "extra")), label_values=tuple(("good", "spicy")))

print(test_node_1.replace("__name__", "a"))
driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "louis"))
with driver.session() as session:
    #session.write_transaction(build_node, "type", test_labels, test_label_values)
    #session.write_transaction(build_node, "type", test_labels, test_label_values2)
    session.write_transaction(build_relation, test_node_1, test_node_2, edge)
