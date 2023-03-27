"""
Some description
"""
import pandas as pd
from rdflib import Graph, Namespace

class PizzaKG(object):

    def __init__(self, file) -> None:
        super().__init__()
        # Setup input file as data
        self.file = file
        self.data = pd.read_csv(file, delimiter=',', quotechar='"', escapechar='\\')

        # Initialise the graph
        self.graph = Graph()

        # Setup customised name space
        self.cw_str = 'http://www.semanticweb.org/city/in3067-inm713/2023/restaurants#'
        self.cw = Namespace(self.cw_str)

        # Prefixes
        self.graph.bind('cw', self.cw)
        self.graph.bind('dc', 'http://purl.org/dc/elements/1.1/')
        self.graph.bind('owl', 'http://www.w3.org/2002/07/owl#')
        self.graph.bind('rdf', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#')
        self.graph.bind('rdfs', 'http://www.w3.org/2000/01/rdf-schema#')
        self.graph.bind('skos', 'http://www.w3.org/2004/02/skos/core#')
        self.graph.bind('xml', 'http://www.w3.org/XML/1998/namespace')
        self.graph.bind('xsd', 'http://www.w3.org/2001/XMLSchema#')

        # Initialise lookup service
        # TODO


    def csv_rdf_conversion(self):
        # TODO
        return 0
