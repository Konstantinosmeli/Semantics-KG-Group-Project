from task_rdf.pizza_kg import PizzaKG


# Constrains and settings
FILE_PATH = "../cw_data/IN3067-INM713_coursework_data_pizza_500.csv"
NAMESPACE_STR = "http://www.semanticweb.org/city/in3067-inm713/2023/restaurants#"
NAMESPACE_PREFIX = "cw"
ONTOLOGY = "../cw_onto/pizza-restaurants-ontology.ttl"


if __name__ == "__main__":
    pizza_kg = PizzaKG(
        _file_path=FILE_PATH,
        _name_space_str=NAMESPACE_STR,
        _name_space_prefix=NAMESPACE_PREFIX,
    )

    """
    TASK RDF.2: RDF generation
    In this task, we will perform the creating RDFs from CSV file without external public KG vocabulary usage.
    In order to do this, please check `pizza_kg.py` and change line:
    `enable_external_uri: bool = False`
    """
    pizza_kg.convert_csv_to_rdf()
    pizza_kg.save_graph("pizza_restaurant_offline.ttl")

    """
    TASK RDF.3: Perform reasoning with `cw_onto`
    We will run the reasoning with offline file
    """
    pizza_kg.perform_reasoning(ontology=ONTOLOGY)
    pizza_kg.save_graph("pizza_restaurant_offline_reasoned.ttl")

    """
    TASK RDF.4: Reuse URIs from state-of-the art knowledge graphs 
    To run this task, please put TASK RDF.2 & TASK RDF.3 in comment.
    And check `pizza_kg.py` and change line:
    `enable_external_uri: bool = False`
    """
    pizza_kg.convert_csv_to_rdf()
    pizza_kg.save_graph("pizza_restaurant.ttl")
    pizza_kg.perform_reasoning(ontology=ONTOLOGY)
    pizza_kg.save_graph("pizza_restaurant_reasoned.ttl")

    """
    TASK RDF.5: Exploit an external Knowledge Graph to perform disambiguation
    We already exploit external KGs to perform logical taskes, however, we didn't choose
    to correct location name.
    As the `description` column of this data is mixed between normal pizza description
    and some ingredient, we use external KG to identify what is the description and
    what is the ingredient. With external KG disabled, ingredient data in `description`
    columns won't be classified.
    We will discuss this further in report.
    """
