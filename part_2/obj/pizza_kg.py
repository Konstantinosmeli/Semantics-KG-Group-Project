"""
Some description
"""
import pandas as pd
from rdflib import Graph, Namespace

import lookup

# Default prefixes represent data from public knowledge graphs
DEFAULT_PREFIXES = [
    ("dc", "http://purl.org/dc/elements/1.1/"),
    ("owl", "http://www.w3.org/2002/07/owl#"),
    ("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
    ("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
    ("skos", "http://www.w3.org/2004/02/skos/core#"),
    ("xml", "http://www.w3.org/XML/1998/namespace"),
    ("xsd", "http://www.w3.org/2001/XMLSchema#"),
]


class PizzaKG(object):

    # Setting of knowledge graphs object
    file_path: str
    name_space_str: str
    prefix: str
    string_uri_dict: dict

    def __init__(
        self,
        _file_path,
        _name_space_str,
        _name_space_prefix,
        _prefixes=DEFAULT_PREFIXES,
    ) -> None:

        super().__init__()

        # Setup input file as data
        self.file_path = _file_path
        self.data = pd.read_csv(
            self.file_path, delimiter=",", quotechar='"', escapechar="\\"
        )

        # Initialise the graph
        self.graph = Graph()

        # Setup customised name space
        self.name_space_str = _name_space_str
        self.name_space = Namespace(self.name_space_str)
        self.graph.bind(_name_space_prefix, _name_space_str)

        # Binding the prefixes
        self.bind_prefixes(_prefixes)

        # Initialise lookup service
        self.dbpedia = lookup.DBpediaLookup()
        self.wikidata = lookup.WikidataAPI()
        self.google_kg = lookup.GoogleKGLookup()

    def bind_prefixes(self, prefixes):
        for prefix in prefixes:
            self.graph.bind(prefix[0], prefix[1])