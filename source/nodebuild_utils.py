import math

def property_constructor(property_keys, property_values):
    """ Builds a property string for Neo4j queries in the form {}"""

    assert len(property_keys) == len(property_values)
    assert type(property_keys) == tuple
    assert type(property_values) == tuple

    property = "{"

    for i in range(0, len(property_keys)):
        ## add more functionality for different data types
        if type(property_values[i]) == int:
            value = str(property_values[i])
        elif type(property_values[i]) == float:
            if is_nan(property_values[i]):
                value = "'" + str(property_values[i]) + "'"
            else:
                value = str(property_values[i])
        elif type(property_values[i]) == str:
            value = "'" + str(property_values[i]) + "'"
        else:
            value = "'" + str(property_values[i]) + "'"
        if i == len(property_keys) - 1:
            property = property + str(property_keys[i]) + ":" + value
        else:
            property = property + str(property_keys[i]) + ":" + value + ', '

    property = property + "}"
    return property

def is_nan(value):
    return math.isnan(float(value))

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
        return "(__name__{} {})".format(labels, property)


def type_constructor(edge_type):
    assert type(edge_type) == str
    assert ":" not in edge_type
    return ":" + edge_type


def edge_constructor(edge_type = (), property_keys=(), property_values=()):
    if len(edge_type) == 1:
        type = type_constructor(edge_type[0])
    else:
        raise AssertionError("Only one edge type can be specified")

    if len(property_keys) > 0:
        property = property_constructor(property_keys, property_values)
    else:
        property = ""
    return "[__name__{} {}]".format(type, property)


def node_query(node):
    return "MERGE {}".format(node)


def build_node(tx, node):
    node = node.replace("__name__", 'a')
    tx.run("MERGE {}".format(node))


def edge_query(n1, n2, e):
    node1 = n1.replace("__name__", "a")
    node2 = n2.replace("__name__", "b")
    edge = e.replace("__name__", "")
    return "MATCH {}, {} MERGE (a) - {} -> (b)".format(node1, node2, edge)


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


def execute(tx, query):
    tx.run(query)


def seperate_nodes(node_properties):
    new_nodes = [node_properties]
    for i in range(0, len(node_properties[0])):
        if type(node_properties[1][i]) == str:
            if "|" in node_properties[1][i]:
                values = node_properties[1][i].split("|")
                new_nodes = []
                for v in values:
                    new_nodes.append([node_properties[0],
                                      node_properties[1][0:i] + [v] +
                                      node_properties[1][i + 1:len(node_properties[1])]])
                break

    return new_nodes


def create_neo4j_queries(edges, edge_properties, node_properties):
    node_strings = {}
    edge_queries = []
    node_queries = []
    for label in node_properties.keys():
        nodes = seperate_nodes(node_properties[label])
        node_strings[label] = []
        for keys, values in nodes:
            node_string = node_constructor(node_labels=(label,),
                                            property_keys=tuple(keys),
                                            property_values=tuple(values))
            node_strings[label].append(node_string)
            node_queries.append(node_query(node_string))

    for edge in edges:
        node1, node2, type = edge.split("_")
        if len(edge_properties) == 0:
            edge_string = edge_constructor(edge_type=(type,))
        else:
            edge_string = edge_constructor(edge_type=(type,),
                                           property_keys=tuple(edge_properties[edge][0]),
                                           property_values=tuple(edge_properties[edge][1]))
        for n1 in node_strings[node1]:
            for n2 in node_strings[node2]:
                edge_queries.append(edge_query(n1, n2, edge_string))

    return node_queries, edge_queries


def merge_queries(queries):
    query = ""
    for q in queries:
        if "__name__" in q:
            q = q.replace("__name__", "")
        query = query + q + " "
    return query


def old_property_constructor(property_keys, property_values):
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


