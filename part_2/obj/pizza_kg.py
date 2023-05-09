"""
Some description
"""
import re

import pandas as pd
import numpy as np
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
                self.str_column_preprocessing(column)
            self.numeric_column_preprocessing(column)

        # Preprocess pizza name
        self.data["menu item"] = self.data["menu item"].apply(
            lambda row: self.menu_name_preprocessing(row)
        )

    def bind_prefixes(self, prefixes):
        """
        Since we will link the data with public KG, we will need to define
        and bind the prefixes for them
        :param prefixes:
        :return: None
        """
        for prefix in prefixes:
            self.graph.bind(prefix[0], prefix[1])


    def str_column_preprocessing(self, column: str):
        """
        Only do preprocessing with string column, not numeric
        We will remove special character that might broke the seach
        However, there are some character that contain meaning, we will not
        remove them, instead, we will replace
        :param column:
        :return: None
        """

        # Fill all missing values with space
        self.data[column] = self.data[column].fillna(" ")

        # Convert all data (including missing to string)
        self.data[column] = self.data[column].astype(str)

        # We check all character in the column
        chars = list(set(self.data[column].sum()))

        # List all character is not alphanumeric and and white space
        non_alphanumeric_chars = [
            e for e in chars if (not e.isalnum()) & (e not in [" ", "'", ","])
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

        # Remove all non-meaningful non-alphanumeric char        # Fill all missing values
        #         self.data[column] = self.data[column].fillna("hahaha")acters
        for e in non_alphanumeric_chars:
            self.data[column] = self.data[column].str.replace(e, " ", regex=False)

        # Remove consecutive white-trailing
        self.data[column] = self.data[column].str.replace(r" +", " ", regex=False)


    def numeric_column_preprocessing(self, column: str):
        """
        For numeric column, we will replace with numpy.nan
        :param column:
        :return:
        """
        # Fill all missing values of numeric columns
        self.data[column] = self.data[column].fillna(np.nan)

    def menu_name_preprocessing(self, item_name: str):
        """
        We do notice that there are some pizza name that in this format:
        "Pizza, Margherita", we will try to find and match them using
        regular expression and then change them to format "margherita pizza"
        :param item_name:
        :return: processed item name
        """

        item_name = item_name.lower()

        # Menu item pattern, for example "Pizza, Margherita"
        pattern = re.compile(r"^pizza\s?,\s?[a-z\s]+.$")

        # Match result
        matched = re.search(pattern, item_name)

        # Check if match exist and then replace
        if matched:
            return re.sub(r'(\w+), (\w+)', r'\2 \1', item_name)
        return item_name