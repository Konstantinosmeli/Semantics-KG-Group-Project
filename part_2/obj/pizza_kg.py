"""
Some description
"""
import pandas as pd
from rdflib import Graph, Namespace

import obj.lookup as lookup

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

        # Preprocess all columns
        for column in self.data.columns:
            if self.data[column].dtype not in ["int", "float64"]:
                self.column_preprocessing(column)

    def bind_prefixes(self, prefixes):
        for prefix in prefixes:
            self.graph.bind(prefix[0], prefix[1])

    def column_preprocessing(self, column: str):
        # Convert all data (including missing to string)
        self.data[column] = self.data[column].astype(str)

        # We check all character in the column
        chars = list(set(self.data[column].sum()))

        # List all character is not alphanumeric and and white space
        non_alphanumeric_chars = [
            e for e in chars if (not e.isalnum()) & (e not in [" ", "'"])
        ]

        # Dictionary to replace meaningful non-alphanumeric characters
        meaningful_non_alphanumeric = {
            "@": "at",
            "&": "and",
            "+": "with"
        }

        # Remove meaningful non-alphanumeric characters from the list
        # Since we will not remove them, but replace them
        non_alphanumeric_chars = [
            e for e in non_alphanumeric_chars if e not in meaningful_non_alphanumeric.keys()
        ]

        # Replace all meaningful non-alphanumeric characters
        self.data.replace({column: meaningful_non_alphanumeric}, inplace=True)

        # Remove all non-meaningful non-alphanumeric characters
        for e in non_alphanumeric_chars:
            self.data[column] = self.data[column].str.replace(e, " ", regex=False)

        # Remove consecutive white-trailing
        self.data[column] = self.data[column].str.replace(r" +", " ", regex=False)
