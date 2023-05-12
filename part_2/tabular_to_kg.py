from obj.pizza_kg import PizzaKG


# Constrains and settings
FILE_PATH = "./cw_data/IN3067-INM713_coursework_data_pizza_500.csv"
NAMESPACE_STR = "http://www.semanticweb.org/city/in3067-inm713/2023/restaurants#"
NAMESPACE_PREFIX = "cw"
ONTOLOGY = "./pizza-restaurants-model-ontology/pizza-restaurants-ontology.ttl"


if __name__ == '__main__':
    pizza_kg = PizzaKG(_file_path=FILE_PATH,
                       _name_space_str=NAMESPACE_STR,
                       _name_space_prefix=NAMESPACE_PREFIX)

    pizza_kg.convert_csv_to_rdf()
    pizza_kg.save_graph("test1.ttl")
    # pizza_kg.perform_reasoning(ontology=ONTOLOGY)
    # pizza_kg.save_graph("pizza_restaurant_reasoned.ttl")
    # array = "Prosciutto and roasted red peppers topped with aged white cheddar, mozzarella, parmesan and arugula, drizzled with balsamic glaze"
    # noises = ["and", "/", "&"]
    # element_noise = ["Restaurant", "restaurant"]
    # meaningful_noise = {
    #     "and": ",",
    #     "or": ",",
    #     "with": ","
    # }
    #
    # print(pizza_kg.preprocessing_array_string(array, noises, element_noise, meaningful_noise))
