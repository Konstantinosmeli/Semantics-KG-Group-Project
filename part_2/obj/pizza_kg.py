"""
Some description
"""
import re
import time

import pandas as pd
import numpy as np
from rdflib import Graph, Namespace, URIRef, Literal, RDFS
from rdflib.namespace import RDF, XSD
from rdflib.util import guess_format
import owlrl

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
    ("dbr", "http://dbpedia.org/resource/"),
    ("dbo", "http://dbpedia.org/ontology/"),
    ("wd", "http://www.wikidata.org/entity/"),
    ("wdt", "http://www.wikidata.org/prop/direct/"),
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
            self.file_path,
            delimiter=",",
            quotechar='"',
            escapechar="\\",
            dtype={"postcode": str},
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

    ######### MAIN TASK: CSV TO RDF CONVERSION #########
    def convert_csv_to_rdf(self, _enable_external_uri: bool = enable_external_uri):
        """
        Convert data from dataframe to rdf
        :param _enable_external_uri:
        :return:
        """

        start_time = time.time()
        print("######### STARTING CONVERSION #########")

        # Country
        self.generate_subclass_triple(parent="Location", child="Country")
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
                datatype=XSD.string,
            ),
            axis=1,
        )

        # State
        self.generate_subclass_triple(parent="Location", child="State")
        self.data["state"].apply(
            lambda x: self.generate_type_triple(
                entity=x,
                class_type=self.name_space.State,
                _category_filter="http://dbpedia.org/resource/Category:States_of_the_United_States",
                _external_uri_score_threshold=0.8,
            )
        )
        self.data.apply(
            lambda row: self.generate_literal_triple(
                entity=row["state"],
                predicate=self.name_space.name,
                literal=row["state"],
                datatype=XSD.string,
            ),
            axis=1,
        )
        self.data.apply(
            lambda row: self.generate_object_triple(
                subject=row["state"],
                predicates=[
                    self.name_space.isStateOf,
                    self.name_space.locatedIn,
                    self.name_space.locatedInCountry,
                ],
                object=row["country"],
            ),
            axis=1,
        )

        # City
        self.generate_subclass_triple(parent="Location", child="City")
        self.data["city"].apply(
            lambda x: self.generate_type_triple(
                entity=x,
                class_type=self.name_space.City,
                _category_filter="http://dbpedia.org/resource/Category:Cities_in_the_United_States",
                _external_uri_score_threshold=0.7,
            )
        )
        self.data.apply(
            lambda row: self.generate_literal_triple(
                entity=row["city"],
                predicate=self.name_space.name,
                literal=row["city"],
                datatype=XSD.string,
            ),
            axis=1,
        )
        self.data.apply(
            lambda row: self.generate_object_triple(
                subject=row["city"],
                predicates=[self.name_space.locatedInState, self.name_space.locatedIn],
                object=row["state"],
            ),
            axis=1,
        )
        self.data.apply(
            lambda row: self.generate_object_triple(
                subject=row["city"],
                predicates=[self.name_space.locatedCountry, self.name_space.locatedIn],
                object=row["country"],
            ),
            axis=1,
        )

        # Address
        # We will have to concat a few field to make address identifier
        self.data["address_id"] = self.data[["address", "city", "state"]].apply(
            lambda row: "_".join(row.astype(str)).replace(" ", "_"), axis=1
        )
        self.generate_subclass_triple(parent="Location", child="Address")
        self.data["address_id"].apply(
            lambda x: self.generate_type_triple(
                entity=x, class_type=self.name_space.Address, _enable_external_uri=False
            )
        )
        self.data.apply(
            lambda row: self.generate_literal_triple(
                entity=row["address_id"],
                predicate=self.name_space.firstLineAddress,
                literal=row["address"],
                datatype=XSD.string,
            ),
            axis=1,
        )
        self.data.apply(
            lambda row: self.generate_literal_triple(
                entity=row["address_id"],
                predicate=self.name_space.postCode,
                literal=row["postcode"],
                datatype=XSD.string,
            ),
            axis=1,
        )
        self.data.apply(
            lambda row: self.generate_object_triple(
                subject=row["address_id"],
                predicates=[self.name_space.locatedCity, self.name_space.locatedIn],
                object=row["city"],
            ),
            axis=1,
        )

        # Restaurant
        self.data["restaurant_id"] = self.data[["name", "address_id"]].apply(
            lambda row: "_".join(row.astype(str)).replace(" ", "_"), axis=1
        )
        self.generate_subclass_triple(parent="Location", child="Restaurant")
        self.data["categories"].apply(
            lambda row: [
                self.generate_subclass_triple(
                    child=x.replace(" ", ""),
                    parent="Restaurant",
                    _enable_external_uri=False,
                )
                for x in row.split(",")
                if x != "Restaurant"
            ]
        )
        self.data.apply(
            lambda row: [
                self.generate_type_triple(
                    entity=row["restaurant_id"],
                    class_type=self.name_space[x.replace(" ", "")],
                    _enable_external_uri=False,
                )
                for x in row["categories"].split(",")
            ],
            axis=1,
        )
        self.data.apply(
            lambda row: self.generate_object_triple(
                subject=row["restaurant_id"],
                predicates=[self.name_space.locatedAddress, self.name_space.locatedIn],
                object=row["address_id"],
            ),
            axis=1,
        )
        self.data.apply(
            lambda row: self.generate_literal_triple(
                entity=row["restaurant_id"],
                predicate=self.name_space.name,
                literal=row["name"],
                datatype=XSD.string,
            ),
            axis=1,
        )

        # Currency
        self.data["currency"].apply(
            lambda x: self.generate_type_triple(
                entity=x,
                class_type=self.name_space.Currency,
                _external_uri_score_threshold=0.8,
            )
        )
        self.data.apply(
            lambda row: self.generate_literal_triple(
                entity=row["currency"],
                predicate=self.name_space.name,
                literal=row["currency"],
                datatype=XSD.string,
            ),
            axis=1,
        )

        # Item value
        self.data["item_value_id"] = (
            self.data["item value"].astype(str) + self.data["currency"]
        )
        self.data["item_value_id"].apply(
            lambda x: self.generate_type_triple(
                entity=x,
                class_type=self.name_space.ItemValue,
                _enable_external_uri=False,
            )
        )
        self.data.apply(
            lambda row: self.generate_object_triple(
                subject=row["item_value_id"],
                predicates=[self.name_space.amountCurrency],
                object=row["currency"],
            ),
            axis=1,
        )
        self.data.apply(
            lambda row: self.generate_literal_triple(
                entity=row["item_value_id"],
                predicate=self.name_space.amount,
                literal=row["item value"],
                datatype=XSD.double,
            ),
            axis=1,
        )

        print(
            "######### CONVERSION FINISHED IN: {} SECONDS #########".format(
                time.time() - start_time
            )
        )

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
            e for e in chars if (not e.isalnum()) & (e not in [" ", "'", ",", "-"])
        ]

        # Dictionary to replace meaningful non-alphanumeric characters
        meaningful_non_alphanumeric = {
            "@": "at",
            "&": "and",
            "+": "with",
            "-": "_",
            "'": "_",
        }

        # Remove meaningful non-alphanumeric characters from the list
        # Since we will not remove them, but replace them
        non_alphanumeric_chars = [
            e
            for e in non_alphanumeric_chars
            if e not in meaningful_non_alphanumeric.keys()
        ]

        # Replace all meaningful non-alphanumeric characters
        for x, y in meaningful_non_alphanumeric.items():
            self.data[column] = self.data[column].str.replace(x, y, regex=False)

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
        _external_uri_score_threshold: float = external_uri_score_threshold,
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
            if (_uri != "") & (_score > _external_uri_score_threshold):
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
        # wikidata_result = self.wikidata.getKGEntities(query=_query, limit=_limit)

        # Mute wikidata if we specify search category inside DBpedia
        if re.search(r"dbpedia\.org", _category_filter):
            wikidata_result = []

        # Concentrate the result and then return
        entities = [
            *dbpedia_result,
            #        *wikidata_result,
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
        _external_uri_score_threshold: float = external_uri_score_threshold,
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
                _external_uri_score_threshold=_external_uri_score_threshold,
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
            return

        # Get the URI from dictionary
        uri = self.entity_uri_dict[entity.lower()]

        # Get the literal
        _literal = Literal(literal, datatype=datatype)

        # Add literal to graph
        self.graph.add((URIRef(uri), predicate, _literal))

    def generate_object_triple(self, subject: str, predicates: [str], object: str):
        """
        Generate literal triple: Example: ns:USD ns:isCurrencyOf ns:US
        :param subject:
        :param predicate:
        :param object:
        :return:
        """
        # If the literal is blank or empty, we will pass it
        if self.is_missing(subject) or self.is_missing(object):
            return

        # Get the URI from dictionary
        subject_uri = self.entity_uri_dict[subject.lower()]
        object_uri = self.entity_uri_dict[object.lower()]

        for predicate in predicates:
            self.graph.add((URIRef(subject_uri), predicate, URIRef(object_uri)))

    def generate_subclass_triple(
        self,
        parent: str,
        child: str,
        _enable_external_uri: bool = False,
        _external_uri_score_threshold: float = external_uri_score_threshold,
        _parent_category_filter: str = "",
        _child_category_filter: str = "",
    ):
        """
        Generate literal triple: Example: ns:AmericanRestaurant rdfs:isSubClassOf ns:Restaurant
        :param parent:
        :param child:
        :return:
        """
        if self.is_missing(parent) or self.is_missing(child):
            return

            # Check if item exist in dictionary so that we don't have to call API again
        if parent.lower() in self.entity_uri_dict:
            parent_uri = self.entity_uri_dict[parent.lower()]
        else:
            parent_uri = self.generate_uri(
                entity=parent,
                _enable_external_uri=_enable_external_uri,
                _category_filter=_parent_category_filter,
                _external_uri_score_threshold=_external_uri_score_threshold,
            )

        if child.lower() in self.entity_uri_dict:
            child_uri = self.entity_uri_dict[child.lower()]
        else:
            child_uri = self.generate_uri(
                entity=child,
                _enable_external_uri=_enable_external_uri,
                _category_filter=_child_category_filter,
                _external_uri_score_threshold=_external_uri_score_threshold,
            )

        self.graph.add((URIRef(child_uri), RDFS.subClassOf, URIRef(parent_uri)))

    ######### REASONING #########
    def perform_reasoning(self, ontology: str):
        # Load the ontology file
        self.graph.load(ontology, format=guess_format(ontology))

        # print("Triples including ontology: '" + str(len(self.graph) + "'."))

        owlrl.DeductiveClosure(
            owlrl.OWLRL.OWLRL_Semantics,
            axiomatic_triples=False,
            datatype_axioms=False,
        ).expand(self.graph)

        print("Triples after OWL 2 RL reasoning: '" + str(len(self.graph)) + "'.")

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
        return (
            (value != value)
            or (value is None)
            or (value == "")
            or (value == " ")
            or (value == np.nan)
        )
