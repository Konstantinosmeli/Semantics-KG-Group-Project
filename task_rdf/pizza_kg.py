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

import task_rdf.lookup as lookup
from task_rdf.isub import isub

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
    namespace_str: str
    prefix: str
    entity_uri_dict: dict = {}
    enable_external_uri: bool = True
    external_uri_score_threshold = 0.4
    noises = ["and", "/", "&", "."]
    category_noise = ["Restaurant", "restaurant"]
    meaningful_noise = {"and": ",", "or": ","}

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
        self.namespace_str = _name_space_str
        self.namespace = Namespace(self.namespace_str)
        self.graph.bind(_name_space_prefix, _name_space_str)

        # Binding the prefixes
        self.bind_prefixes(_prefixes)

        # Initialise lookup service
        self.dbpedia = lookup.DBpediaLookup()
        self.wikidata = lookup.WikidataAPI()
        self.google_kg = lookup.GoogleKGLookup()

        # Preprocessing menu item
        self.data["menu item"] = self.data["menu item"].apply(
            lambda e: self.menu_name_preprocessing(e)
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
        self.graph.add(
            (self.namespace.Country, RDFS.subClassOf, self.namespace.Location)
        )
        self.data["country"].apply(
            lambda x: self.generate_type_triple(
                entity=x,
                class_type=self.namespace.Country,
                _enable_external_uri=_enable_external_uri,
                _category_filter="http://dbpedia.org/resource/Category:Lists_of_countries",
            )
        )
        self.data.apply(
            lambda row: self.generate_literal_triple(
                entity=row["country"],
                predicate=self.namespace.name,
                literal=row["country"],
                datatype=XSD.string,
            ),
            axis=1,
        )

        # State
        self.graph.add((self.namespace.State, RDFS.subClassOf, self.namespace.Location))
        self.data["state"].apply(
            lambda x: self.generate_type_triple(
                entity=x,
                class_type=self.namespace.State,
                _category_filter="http://dbpedia.org/resource/Category:States_of_the_United_States",
                _external_uri_score_threshold=0.8,
            )
        )
        self.data.apply(
            lambda row: self.generate_literal_triple(
                entity=row["state"],
                predicate=self.namespace.name,
                literal=row["state"],
                datatype=XSD.string,
            ),
            axis=1,
        )
        self.data.apply(
            lambda row: self.generate_object_triple(
                subject=row["state"],
                predicates=[
                    self.namespace.isStateOf,
                    self.namespace.locatedIn,
                    self.namespace.locatedInCountry,
                ],
                object=row["country"],
            ),
            axis=1,
        )

        # City
        self.graph.add((self.namespace.City, RDFS.subClassOf, self.namespace.Location))
        self.data["city"].apply(
            lambda x: self.generate_type_triple(
                entity=x,
                class_type=self.namespace.City,
                _category_filter="http://dbpedia.org/resource/Category:Cities_in_the_United_States",
                _external_uri_score_threshold=0.8,
            )
        )
        self.data.apply(
            lambda row: self.generate_literal_triple(
                entity=row["city"],
                predicate=self.namespace.name,
                literal=row["city"],
                datatype=XSD.string,
            ),
            axis=1,
        )
        self.data.apply(
            lambda row: self.generate_object_triple(
                subject=row["city"],
                predicates=[self.namespace.locatedInState, self.namespace.locatedIn],
                object=row["state"],
            ),
            axis=1,
        )
        self.data.apply(
            lambda row: self.generate_object_triple(
                subject=row["city"],
                predicates=[self.namespace.locatedCountry, self.namespace.locatedIn],
                object=row["country"],
            ),
            axis=1,
        )

        # Address
        # We will have to concat a few field to make address identifier
        self.graph.add(
            (self.namespace.Address, RDFS.subClassOf, self.namespace.Location)
        )
        self.data["address_id"] = self.data[["address", "city", "state"]].apply(
            lambda row: " ".join(row.astype(str)), axis=1
        )
        self.data["address_id"].apply(
            lambda x: self.generate_type_triple(
                entity=x, class_type=self.namespace.Address, _enable_external_uri=False
            )
        )
        self.data.apply(
            lambda row: self.generate_literal_triple(
                entity=row["address_id"],
                predicate=self.namespace.firstLineAddress,
                literal=row["address"],
                datatype=XSD.string,
            ),
            axis=1,
        )
        self.data.apply(
            lambda row: self.generate_literal_triple(
                entity=row["address_id"],
                predicate=self.namespace.postCode,
                literal=row["postcode"],
                datatype=XSD.string,
            ),
            axis=1,
        )
        self.data.apply(
            lambda row: self.generate_object_triple(
                subject=row["address_id"],
                predicates=[self.namespace.locatedCity, self.namespace.locatedIn],
                object=row["city"],
            ),
            axis=1,
        )
        self.data.apply(
            lambda row: self.generate_object_triple(
                subject=row["address_id"],
                predicates=[self.namespace.locatedState, self.namespace.locatedIn],
                object=row["state"],
            ),
            axis=1,
        )
        self.data.apply(
            lambda row: self.generate_object_triple(
                subject=row["address_id"],
                predicates=[self.namespace.locatedCountry, self.namespace.locatedIn],
                object=row["country"],
            ),
            axis=1,
        )

        # Restaurant
        self.graph.add(
            (self.namespace.Restaurant, RDFS.subClassOf, self.namespace.Location)
        )
        self.data["restaurant_id"] = self.data[["name", "address_id"]].apply(
            lambda row: " ".join(row.astype(str)), axis=1
        )
        self.data["restaurant_id"].apply(
            lambda x: self.generate_type_triple(
                entity=x,
                class_type=self.namespace.Restaurant,
                _enable_external_uri=False,
            )
        )
        self.data.apply(
            lambda row: self.generate_literal_triple(
                entity=row["restaurant_id"],
                predicate=self.namespace.name,
                literal=row["name"],
                datatype=XSD.string,
            ),
            axis=1,
        )
        self.data.apply(
            lambda row: self.generate_object_triple(
                subject=row["restaurant_id"],
                predicates=[self.namespace.locatedAddress, self.namespace.locatedIn],
                object=row["address_id"],
            ),
            axis=1,
        )
        self.data["categories"].apply(
            lambda e: [
                self.graph.add(
                    (
                        URIRef(self.generate_internal_class_name(_e)),
                        RDFS.subClassOf,
                        self.namespace.Restaurant,
                    )
                )
                for _e in self.preprocessing_array_string(
                    e, self.noises, self.category_noise
                )
            ]
        )
        self.data.apply(
            lambda row: [
                self.generate_type_triple(
                    entity=row["restaurant_id"],
                    class_type=(URIRef(self.generate_internal_class_name(_e))),
                    _enable_external_uri=False,
                )
                for _e in self.preprocessing_array_string(
                    _input=row["categories"],
                    _noises=self.noises,
                    _element_noise=self.category_noise,
                )
            ],
            axis=1,
        )

        # Currency
        self.data["currency"].apply(
            lambda x: self.generate_type_triple(
                entity=x,
                class_type=self.namespace.Currency,
                _external_uri_score_threshold=0.8,
            )
        )
        self.data.apply(
            lambda row: self.generate_object_triple(
                subject=row["currency"],
                predicates=[self.namespace.currencyOfCountry],
                object=row["country"],
            ),
            axis=1,
        )
        self.data.apply(
            lambda row: self.generate_literal_triple(
                entity=row["currency"],
                predicate=self.namespace.name,
                literal=row["currency"],
                datatype=XSD.string,
            ),
            axis=1,
        )

        # Item value
        # We will need to preprocess item value
        self.data["item_value_id"] = self.data[["item value", "currency"]].apply(
            lambda row: "".join(row.astype(str))
            if (
                self.is_missing(row["item value"]) == False
                and self.is_missing(row["currency"]) == False
            )
            else "",
            axis=1,
        )
        self.data["item_value_id"].apply(
            lambda x: self.generate_type_triple(
                entity=x,
                class_type=self.namespace.ItemValue,
                _enable_external_uri=False,
            )
        )
        self.data.apply(
            lambda row: self.generate_object_triple(
                subject=row["item_value_id"],
                predicates=[self.namespace.amountCurrency],
                object=row["currency"],
            ),
            axis=1,
        )
        self.data.apply(
            lambda row: self.generate_literal_triple(
                entity=row["item_value_id"],
                predicate=self.namespace.amount,
                literal=row["item value"],
                datatype=XSD.double,
            ),
            axis=1,
        )

        # Pizza & Ingredient
        self.graph.add((self.namespace.MenuItem, RDFS.subClassOf, self.namespace.Food))
        self.graph.add((self.namespace.Ingredient, RDFS.subClassOf, self.namespace.Food))
        self.data["item_id"] = self.data[["menu item", "restaurant_id"]].apply(
            lambda row: " ".join(row.astype(str)), axis=1
        )
        self.data["item_id"].apply(
            lambda x: self.generate_type_triple(
                entity=x,
                class_type=self.namespace.MenuItem,
                _external_uri_score_threshold=0.7,
            )
        )
        self.data.apply(
            lambda row: self.generate_literal_triple(
                entity=row["item_id"],
                predicate=self.namespace.name,
                literal=row["menu item"],
                datatype=XSD.string,
            ),
            axis=1,
        )
        self.data.apply(
            lambda row: self.generate_object_triple(
                subject=row["item_id"],
                predicates=[self.namespace.servedInRestaurant],
                object=row["restaurant_id"],
            ),
            axis=1,
        )
        self.data.apply(
            lambda row: self.generate_object_triple(
                subject=row["item_id"],
                predicates=[self.namespace.hasValue],
                object=row["item_value_id"],
            ),
            axis=1,
        )

        # Item description
        self.data.apply(
            lambda row: self.generate_description(
                row, _enable_external_uri=self.enable_external_uri
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

    def menu_name_preprocessing(self, item_name: str):
        """
        We do notice that there are some pizza name that in this format:
        "Pizza, Margherita", we will try to find and match them using
        regular expression and then change them to format "margherita pizza"
        :param item_name:
        :return: processed item name
        """

        # Menu item pattern, for example "Pizza, Margherita"
        pattern = re.compile(r"^pizza\s?,\s?[a-z\s]+.$")

        # Match result
        matched = re.search(pattern, item_name.lower())

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
        pattern = re.compile("[\W_]+")
        return pattern.sub("_", _entity)

    def preprocessing_array_string(
        self,
        _input: str,
        _noises: [str],
        _element_noise: [str] = [],
        _meaningful_noise={},
    ):
        for noise, replacement in _meaningful_noise.items():
            _input = _input.replace(noise, replacement)
        for noise in _noises:
            _input = _input.replace(noise, "")
        _input = re.sub(" +", " ", _input).strip()
        _input_arr = _input.split(",")
        _input_arr = [e.rstrip("s").strip() for e in _input_arr]
        _input_arr = [e for e in _input_arr if e not in _element_noise]
        return _input_arr

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
        uri = self.namespace_str + self.process_entity_lexical(_entity=entity)

        if _enable_external_uri:
            _uri, _score = self.generate_external_uri(
                _query=entity,
                _category_filter=_category_filter,
            )
            if (_uri != "") & (_score >= _external_uri_score_threshold):
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

    def generate_internal_class_name(self, _name: str):
        pattern = re.compile("[\W_]+")
        return self.namespace_str + pattern.sub("", _name).capitalize()

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

    def generate_description(
        self, row, _enable_external_uri: bool = enable_external_uri
    ):
        if not _enable_external_uri:
            self.generate_literal_triple(
                entity=row["item_id"],
                predicate=self.namespace.description,
                literal=row["item description"],
                datatype=XSD.string,
            )
        else:
            if self.is_missing(row["item description"]):
                pass
            else:
                desc_array = self.preprocessing_array_string(
                    _input=row["item description"],
                    _noises=self.noises,
                    _meaningful_noise=self.meaningful_noise,
                )
                for desc in desc_array:
                    if self.is_missing(desc):
                        pass
                    else:
                        uri = self.generate_uri(
                            entity=desc,
                            _enable_external_uri=_enable_external_uri,
                            _category_filter="https://dbpedia.org/page/Category:Food_ingredients",
                            _external_uri_score_threshold=0.65,
                        )
                        if uri.startswith(self.namespace):
                            self.generate_literal_triple(
                                entity=row["item_id"],
                                predicate=self.namespace.description,
                                literal=desc,
                                datatype=XSD.string,
                            )
                        else:
                            self.graph.add((URIRef(uri), RDFS.subClassOf, self.namespace.Ingredient))
                            ingredient_id = desc + "_" + row["item_id"]
                            self.generate_type_triple(
                                entity=ingredient_id,
                                class_type=URIRef(uri),
                                _enable_external_uri=False
                            )
                            self.generate_object_triple(
                                subject=ingredient_id,
                                predicates=[self.namespace.isIngredientOf],
                                object=row["item_id"],
                            )

    ######### REASONING #########
    def perform_reasoning(self, ontology: str):
        """
        Perform reasoning with existing ontology
        :param ontology:
        :return:
        """
        # Load the ontology file
        self.graph.load(ontology, format=guess_format(ontology))

        # Load and expand reasoner
        owlrl.DeductiveClosure(
            owlrl.OWLRL.OWLRL_Semantics,
            axiomatic_triples=False,
            datatype_axioms=False,
        ).expand(self.graph)

        print("Done reasoning, triples count: '" + str(len(self.graph)) + "'.")

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
            or (value == "_")
            or (value == "nan")
        )
