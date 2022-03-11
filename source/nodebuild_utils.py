from neo4j import GraphDatabase

def label_constructor(labels, label_values):
    assert len(labels) == len(label_values)
    assert type(labels) == tuple
    assert type(label_values) == tuple
    label = "{"

    for i in range(0, len(labels)):
        ## add more functionality for different data types
        if type(label_values[i]) == int or type(label_values[i]) == float:
            value = str(label_values[i])
        else:
            value = "'" + str(label_values[i]) + "'"
        if i == len(labels) - 1:
            label = label + str(labels[i]) + ":" + value
        else:
            label = label + str(labels[i]) + ":" + value + ', '

    label = label + "}"
    return label


def build_node(tx, node_type, labels=(), label_values=()):
    node = node_constructor(node_type, labels, label_values)
    node = node.replace("__name__", 'a')
    tx.run("CREATE {}".format(node))


def node_constructor(node_type, labels=(), label_values=()):
    label = label_constructor(labels, label_values)
    return "(__name__:{} {})".format(node_type, label)


def edge_constructor(edge_type, labels=(), label_values=()):
    label = label_constructor(labels, label_values)
    return "[__name__:{} {}]".format(edge_type, label)


def build_relation(tx, n1, n2, e):
    node1 = n1.replace("__name__", "a")
    node2 = n2.replace("__name__", "b")
    edge = e.replace("__name__", "r")
    tx.run("MATCH {}, {}"
           "MERGE (a) - {} -> (b)".format(node1, node2, edge))


def build_bi_relation(tx, n1, n2, e1, e2):
    build_relation(tx, n1, n2, e1)
    build_relation(tx, n2, n1, e2)


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
