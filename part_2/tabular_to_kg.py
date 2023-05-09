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

    pizza_kg.generate_uri(entity="London")
    print(pizza_kg.entity_uri_dict)