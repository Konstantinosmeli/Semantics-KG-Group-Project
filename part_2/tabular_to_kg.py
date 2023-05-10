from obj.pizza_kg import PizzaKG
import numpy as np

# Constrains and settings
FILE_PATH = "./cw_data/IN3067-INM713_coursework_data_pizza_500.csv"
NAMESPACE_STR = "http://www.semanticweb.org/city/in3067-inm713/2023/restaurants#"
NAMESPACE_PREFIX = "cw"


if __name__ == '__main__':
    pizza_kg = PizzaKG(_file_path=FILE_PATH,
                       _name_space_str=NAMESPACE_STR,
                       _name_space_prefix=NAMESPACE_PREFIX)
    # print(pizza_kg.generate_external_uri("country"))
    # print(pizza_kg.entity_uri_dict)

    pizza_kg.convert_csv_to_rdf()
    pizza_kg.perform_reasoning("./pizza-restaurants-model-ontology/pizza-restaurants-ontology.ttl")
    # print(pizza_kg.entity_uri_dict)
    pizza_kg.save_graph("test1.ttl")

    # print(pizza_kg.generate_uri("USD", _external_uri_score_threshold=0.8))