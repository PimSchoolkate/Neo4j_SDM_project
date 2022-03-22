from nodebuild_utils import *
#import neo4j
import pandas as pd
from neo4j import GraphDatabase

class GraphPattern():
    def __init__(self):
        self.nodes = {}
        self.edges = {}
        self.data_graph_mapping = {}
        self.graph_data_mapping = {}
        self.initialized = False
        self.loaded_nodes = {}

    class Node():
        def __init__(self, name, prop_name):
            self.name = name
            self.adjacent_nodes = {}
            self.properties = [prop_name]
            self.labels = [name]
            self.initialized = False

        def add_properties(self, properties: list):
            self.properties = self.properties + properties

        # def add_properties(self, properties: list, properties_ref: list):
        #     assert len(properties) == len(properties_ref)
        #     for property, ref in zip(properties, properties_ref):
        #         self.properties[property] = ref

        def add_labels(self, labels: list):
            self.labels = self.labels + labels

        def add_edge(self, an, edge):
            if an in self.adjacent_nodes.keys():
                self.adjacent_nodes[an].append(edge)
            else:
                self.adjacent_nodes[an] = [edge]

        def print_node(self):
            print('_'*40)
            print('Node: \t\t\t' + self.name)
            print('Initialized: \t\t' + str(self.initialized))
            print('')
            print('Node Label(s): ')
            for label in self.labels:
                print('  - ' + label)
            print('')
            print('Node Properties:')
            for property in self.properties:
                print('  - ' + property)
            print('')
            print('Adjacent Nodes and Edges')
            for node in self.adjacent_nodes.keys():
                print('Node: ' + node + "\t\t Edge: " + self.adjacent_nodes[node])

        def initialize(self):
            self.labels = tuple(self.labels)
            self.properties = tuple(self.properties)
            self.node_constructor = node_constructor
            self.initialized = True

        def node_string(self, properties: tuple, values: tuple) -> str:
            if self.initialized == False:
                raise ValueError("Node has not been initialized")
            else:
                return self.node_constructor(self.labels, properties, values)

    class Edge():
        def __init__(self, sn_name: str, en_name: str, type="blank"):
            self.sn = sn_name
            self.en = en_name
            self.name = sn_name + "_" + en_name + "_" + type
            self.properties = []
            self.type = type
            self.initialized = False

        def add_properties(self, properties: list):
            self.properties = self.properties + properties

        def update_type(self, type: str):
            self.type = type

        def print_edge(self):
            print('_'*40)
            print('Edge: \t\t\t' + self.name)
            print('Edge type: \t\t' + self.type)
            print('Starting node: \t\t' + self.sn)
            print('End node: \t\t' + self.en)
            print('Initialized: \t\t' + str(self.initialized))
            print('')
            print('Edge Properties:')
            for property in self.properties:
                print('  - ' + property)

        def initialize(self):
            self.properties = tuple(self.properties)
            self.type = tuple(self.type)
            self.edge_constructor = edge_constructor
            self.initialized = True

        def edge_string(self, properties: tuple, values: tuple) -> str:
            if self.initialized == False:
                raise ValueError("Node has not been initialized")
            else:
                return self.edge_constructor(self.type, properties, values)

    def add_node(self, node_name, ref, prop_name):
        self.nodes[node_name] = self.Node(node_name, prop_name)
        self.data_graph_mapping[ref] = {'node property': (node_name, prop_name)}
        self.graph_data_mapping[prop_name] = {'node property': (node_name, ref)}

    def add_node_labels(self, node_name, labels):
        #assert len(labels) == len(label_refs)
        self.nodes[node_name].add_labels(labels)
        # for ref, label in label_refs, labels:
        #     self.data_graph_mapping[ref] = {'node label': (node_name, label)}
        #     self.graph_data_mapping[label] = {'node label': (node_name, ref)}

    def add_node_properties(self, node_name, properties, properties_ref):
        assert len(properties) == len(properties_ref)
        self.nodes[node_name].add_properties(properties)
        for ref, property in zip(properties_ref, properties):
            self.data_graph_mapping[ref] = {'node property': (node_name, property)}
            self.graph_data_mapping[property] = {"node property": (node_name, ref)}

    def add_edge(self, sn_name, en_name, ref="", type=""):
        ## Check if the nodes are in the graph and otherwise, return an error
        assert sn_name in self.nodes.keys()
        assert en_name in self.nodes.keys()

        self.edges[sn_name + "_" + en_name + "_" + type] = self.Edge(sn_name, en_name, type)
        self.data_graph_mapping[ref] = {'edge': sn_name + "_" + en_name + "_" + type}
        self.graph_data_mapping[sn_name + "_" + en_name + "_" + type] = {'edge': ref}

    def add_edge_type(self, edge_name, type, type_ref):
        self.edges[edge_name].update_type(type)
        self.data_graph_mapping[type_ref] = {'edge type': (edge_name, type)}
        self.graph_data_mapping[type] = {'edge type': (edge_name, type_ref)}

    def add_edge_properties(self, edge_name, properties, properties_ref):
        assert len(properties) == len(properties_ref)
        self.nodes[edge_name].add_properties(properties)
        for ref, property in zip(properties_ref, properties):
            self.data_graph_mapping[ref] = {'edge property': (edge_name, property)}
            self.graph_data_mapping[property] = {'edge property': (edge_name, ref)}

    # def update_ref(self, name, ref):
    #     if name in self.edges:
    #         self.edges[name].update_ref(ref)
    #         self.data_graph_mapping[ref] = {'edge': name}
    #         self.graph_data_mapping[name] = {'edge': ref}
    #     elif name in self.nodes:
    #         self.nodes[name].update_ref(ref)
    #         self.data_graph_mapping[ref] = {'node': name}
    #         self.graph_data_mapping[name] = {'node': ref}
    #     else:
    #         raise NameError("Node or Edge does not exist")

    def initialize_graph(self):
        for node in self.nodes.keys():
            self.nodes[node].initialize()

        for edge in self.edges.keys():
            self.edges[edge].initialize()

        self.initialized = True

    def find_nodes_and_edges_in_pandas_row(self, row):
        references = list(row.index)
        nodes = []
        node_properties = {}
        edge_properties = {}
        for ref in references:
            if ref in self.data_graph_mapping.keys():
                ele = self.data_graph_mapping[ref]

                if list(ele.keys())[0] == 'node property':
                    if ele['node property'][0] in node_properties.keys():
                        node_properties[ele['node property'][0]][0].append(ele['node property'][1])
                        node_properties[ele['node property'][0]][1].append(row[ref])
                    else:
                        node_properties[ele['node property'][0]] = [[ele['node property'][1]],[row[ref]]]
                        nodes.append(ele['node property'][0])

                elif list(ele.keys())[0] == 'edge property':
                     if ele['edge property'][0] in edge_properties.keys():
                         edge_properties[ele['edge property'][0]].append(ele['node property'][1])
                     else:
                         edge_properties[ele['edge property'][0]] = [ele['node property'][1]]

        edges = []

        if len(nodes) > 1:
            for i in range(0, len(nodes)):
                for j in range(0, len(nodes)):
                    if j == i:
                        pass
                    else:
                        pot_node = nodes[i] + "_" + nodes[j]
                        for l in self.edges:
                            if pot_node in l:
                                edges.append(l)
        return edges, nodes, edge_properties, node_properties

    # def seperate_nodes(self, node_properties):
    #     new_nodes = [node_properties]
    #     for i in range(0, len(node_properties[0])):
    #         if type(node_properties[1][i]) == str:
    #             if "|" in node_properties[1][i]:
    #                 values = node_properties[1][i].split("|")
    #                 new_nodes = []
    #                 for v in values:
    #                     new_nodes.append([node_properties[0],
    #                                       node_properties[1][0:i] + [v] +
    #                                       node_properties[1][i + 1:len(node_properties[1])]])
    #                 break
    #
    #     return new_nodes


    def create_neo4j_queries(self, edges, edge_properties, node_properties):
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

    def connect_neo4j(self, url, auth):
        self.driver = GraphDatabase.driver(url, auth=auth)

    def execute_queries(self, node_queries, edge_queries):
        self.execute_query(merge_queries(node_queries))
        #self.execute_query(self.merge_queries(edge_queries))
        for eq in edge_queries:
            self.execute_query(eq)

    def execute_query(self, query):
        with self.driver.session() as session:
            session.write_transaction(execute, query)

    def merge_queries(self, queries):
        query = ""
        for q in queries:
            if "__name__" in q:
                q = q.replace("__name__", "")
            query = query + q + " "
        return query

    def row_to_neo4j(self, row):
        e, n, ep, np = self.find_nodes_and_edges_in_pandas_row(row)
        nq, eq = create_neo4j_queries(e, ep, np)
        self.execute_queries(nq, eq)