"""
Some description
"""
import re

import pandas as pd
import numpy as np
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, XSD

import obj.lookup as lookup
from obj.isub import isub

# Default prefixes represent data from public knowledge graphs
DEFAULT_PREFIXES = [
    ("dc", "http://purl.org/dc/elements/1.1/"),
    ("owl", "http://www.w3.org/2002/07/owl#"),
    ("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
    ("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
    ("skos", "http://www.w3.org/2004/02/skos/core#"),
    ("xml", "http://www.w3.org/XML/1998/namespace"),
    ("xsd", "http://www.w3.org/2001/XMLSchema#"),
    ("dbr", "http://dbpedia.org/resource/")
]


class PizzaKG(object):
    # Setting of knowledge graphs object
    file_path: str
    name_space_str: str
    prefix: str
    entity_uri_dict: dict = {}
    enable_external_uri: bool = True
    external_uri_score_threshold = 0.4

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

        columns = ['name','address','city','country','postcode','state','categories','menu item','item value','currency','item description']

        #Check valid dataset
        valid = self.check_dataset_cols(columns)

        if valid:
            # Preprocess all columns
            for column in self.data.columns:
                if self.data[column].dtype not in ["int", "float64"]:
                    self.str_column_preprocessing(column)
                self.numeric_column_preprocessing(column)

            # Preprocess pizza name
            self.data["menu item"] = self.data["menu item"].apply(
                lambda row: self.menu_name_preprocessing(row)
            )


    def check_dataset_cols(self,columns):
        """
        Before doing any pre-prcessing the dataset needs to be validated to
        avoid unnescesary work, by checking that all columns that are needed
        to continue are identified
        """
        if set([columns]).issubset(self.data.columns):
            print('Dataset contains correct columns')
            return True
        else:
            print('Columns expected are missing')
            return False


    ######### MAIN TASK: CSV TO RDF CONVERSION #########
    def convert_csv_to_rdf(self, _enable_external_uri: bool = enable_external_uri):
        """
        Convert data from dataframe to rdf
        :param _enable_external_uri:
        :return:
        """
        # Country
        self.data["country"].apply(
            lambda x: self.generate_type_triple(
                entity=x,
                class_type=self.name_space.Country,
                _enable_external_uri=_enable_external_uri,
                _category_filter="http://dbpedia.org/resource/Category:Lists_of_countries",
            )
        )

        self.data.apply(
            lambda row: self.generate_literal_triple(
                entity=row["country"],
                predicate=self.name_space.name,
                literal=row["country"],
                datatype=XSD.string
            ),
            axis=1
        )

        # Currency



    ######### PREPROCESSINGS #########
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
        meaningful_non_alphanumeric = {"@": "at", "&": "and", "+": "with"}

        # Remove meaningful non-alphanumeric characters from the list
        # Since we will not remove them, but replace them
        non_alphanumeric_chars = [
            e
            for e in non_alphanumeric_chars
            if e not in meaningful_non_alphanumeric.keys()
        ]

        # Replace all meaningful non-alphanumeric characters
        self.data.replace({column: meaningful_non_alphanumeric}, inplace=True)

        # Remove all non-meaningful non-alphanumeric char
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
            return re.sub(r"(\w+), (\w+)", r"\2 \1", item_name)
        return item_name

    def process_entity_lexical(self, _entity: str):
        """
        Remove characters that could break URI
        :param _entity:
        :return:
        """
        return _entity.replace(" ", "_").replace("(", "").replace(")", "")

    ######### ENTITY GENERATION #########
    def generate_uri(
        self,
        entity: str,
        _enable_external_uri: bool = enable_external_uri,
        _category_filter: str = "",
    ):
        """
        We will generate URI for the entity, note that there are some logic:
        - We can choose if we need to use external KG
        - We can also choose the threshold for lexical similiarity
            For example: if the lexical similarity is too low, we would rather create entity URI
            in our default namespace
        :param entity:
        :param _enable_external_uri:
        :param _category_filter:
        :return:
        """
        uri = self.name_space_str + self.process_entity_lexical(_entity=entity)

        if _enable_external_uri:
            _uri, _score = self.generate_external_uri(
                _query=entity,
                _category_filter=_category_filter,
            )
            if (_uri != "") & (_score > self.external_uri_score_threshold):
                uri = _uri

        self.entity_uri_dict[entity.lower()] = uri

        return uri

    def generate_external_uri(
        self, _query: str, _category_filter: str = "", _limit: int = 5
    ):
        """
        Use pre-written lookup code to look for the enitity on services.
        Currently, we are implementing DBpedia and Wikidata.
        We will expect to extend the search to Google KG in the future
        since entity URI from Google KG is different from other services
        :param _query:
        :param _category_filter:
        :param _limit:
        :return: uri:
        """

        # Query all services and return
        dbpedia_result = self.dbpedia.getKGEntities(
            query=_query, limit=_limit, category_filter=_category_filter
        )
        wikidata_result = self.wikidata.getKGEntities(query=_query, limit=_limit)

        # Mute wikidata if we specify search category inside DBpedia
        if re.search(r"dbpedia\.org", _category_filter):
            wikidata_result = []

        # Concentrate the result and then return
        entities = [
            *dbpedia_result,
            *wikidata_result,
        ]

        # Parameters for comparation
        score = -1
        uri = ""

        # Iterate returned array and check for the most correct result
        if entities:
            for entity in entities:
                _score = isub(_query, entity.label)
                if _score > score:
                    uri = entity.ident
                    score = _score

        return uri, score

    ######### TRIPLES GENERATIONS #########
    def generate_type_triple(
        self,
        entity: str,
        class_type: str,
        _enable_external_uri: bool = enable_external_uri,
        _category_filter: str = "",
    ):
        """
        Generate type triple: Example: ns:London rdf:type ns:City
        :param entity:
        :param class_type:
        :param _enable_external_uri:
        :param _category_filter:
        :return:
        """
        # Check blank or empty
        if self.is_missing(entity):
            return

        # Check if item exist in dictionary so that we don't have to call API again
        if entity.lower() in self.entity_uri_dict:
            uri = self.entity_uri_dict[entity.lower()]
        else:
            uri = self.generate_uri(
                entity=entity,
                _enable_external_uri=_enable_external_uri,
                _category_filter=_category_filter,
            )

        # Add type triple
        self.graph.add((URIRef(uri), RDF.type, class_type))

    def generate_literal_triple(
        self, entity: str, predicate: str, literal: str, datatype: str
    ):
        """
        Generate literal triple: Example: ns:London ns:name "London"^^xsd:string
        :param entity:
        :param predicate:
        :param literal:
        :param datatype:
        :return:
        """
        # If the literal is blank or empty, we will pass it
        if self.is_missing(literal):
            pass

        # Get the URI from dictionary
        uri = self.entity_uri_dict[entity.lower()]

        # Get the literal
        _literal = Literal(literal, datatype=datatype)

        # Add literal to graph
        self.graph.add((URIRef(uri), predicate, _literal))

    ######### SAVE GRAPH #########
    def save_graph(self, output_file: str, _format: str = "ttl"):
        """
        Just a function to save graph as file
        :param output_file:
        :param _format:
        :return:
        """
        self.graph.serialize(destination=output_file, format=_format)

    ######### VALIDATIONS #########
    def is_missing(self, value: str):
        """
        Check if a value is empty, blank or missing
        :param value:
        :return:
        """
        return (value != value) or (value is None) or (value == "") or (value == np.nan)
