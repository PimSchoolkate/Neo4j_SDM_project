def property_constructor(property_keys, property_values):
    """ Builds a property string for Neo4j queries in the form {}"""

    assert len(property_keys) == len(property_values)
    assert type(property_keys) == tuple
    assert type(property_values) == tuple

    property = "{"

    for i in range(0, len(property_keys)):
        ## add more functionality for different data types
        if type(property_values[i]) == int or type(property_values[i]) == float:
            value = str(property_values[i])
        else:
            value = "'" + str(property_values[i]) + "'"
        if i == len(property_keys) - 1:
            property = property + str(property_keys[i]) + ":" + value
        else:
            property = property + str(property_keys[i]) + ":" + value + ', '

    property = property + "}"
    return property


def label_constructor(labels):
    assert type(labels) == tuple
    assert len(labels) > 0
    label = ""
    for l in labels:
        label = label + ":" + str(l)
    return label


def node_constructor(node_labels=(), property_keys=(), property_values=()):
    if len(property_keys) > 0:
        property = property_constructor(property_keys, property_values)
    else:
        property = ""

    if node_labels == ():
        return "(__name__ {})".format(property)
    else:
        labels = label_constructor(node_labels)
        return "(__name__:{} {})".format(labels, property)


def type_constructor(edge_type):
    assert type(edge_type) == str
    assert ":" not in edge_type
    return ":" + edge_type


def edge_constructor(edge_type=(), property_keys=(), property_values=()):
    if len(edge_type) == 1:
        type = type_constructor(edge_type)
    else:
        raise AssertionError("Only one edge type can be specified")

    if len(property_keys) > 0:
        property = property_constructor(property_keys, property_values)
    else:
        property = ""
    return "[__name__{} {}]".format(type, property)


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


def return_build_function():
    return build_node