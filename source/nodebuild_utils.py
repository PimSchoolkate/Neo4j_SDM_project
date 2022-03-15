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


def node_constructor(node_type, labels=(), label_values=()):
    label = label_constructor(labels, label_values)
    return "(__name__:{} {})".format(node_type, label)


def edge_constructor(edge_type, labels=(), label_values=()):
    label = label_constructor(labels, label_values)
    return "[__name__:{} {}]".format(edge_type, label)


def build_node(tx, node):
    node = node.replace("__name__", 'a')
    tx.run("CREATE {}".format(node))


def build_relation(tx, n1, n2, e):
    node1 = n1.replace("__name__", "a")
    node2 = n2.replace("__name__", "b")
    edge = e.replace("__name__", "r")
    tx.run("MATCH {}, {}"
           "MERGE (a) - {} -> (b)".format(node1, node2, edge))


def build_bi_relation(tx, n1, n2, e1, e2):
    build_relation(tx, n1, n2, e1)
    build_relation(tx, n2, n1, e2)
