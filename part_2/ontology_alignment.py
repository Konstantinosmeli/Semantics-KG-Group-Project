from rdflib import Graph, Namespace, URIRef, OWL, RDFS
from rdflib.plugins.sparql import prepareQuery
import Levenshtein as lev

# Task OA1
def find_equivalences(onto1, onto2):
    # Defining the owl namespace to be added to the turtle file
    owl = Namespace("http://www.w3.org/2002/07/owl#")
    owl_equivalentClass = owl.equivalentClass
    
    # Iteration of the entities in the cw_onto file
    for entity1 in onto1.subjects():
        name1 = onto1.value(subject=entity1, predicate=URIRef("http://www.w3.org/2000/01/rdf-schema#label"))

        # Iteration of the entities in the pizza.owl file
        for entity2 in onto2.subjects():
            name2 = onto2.value(subject=entity2, predicate=URIRef("http://www.w3.org/2000/01/rdf-schema#label"))

            #Computation of jaro-winkler similarity to compare both inputed strings
            similarity = lev.jaro_winkler(name1, name2)

            #check if similarity is high enough to be considered equivalent
            if similarity > 0.9:
                #add triple to the alignment_graph
                equivalence_triple = (entity1, owl_equivalentClass, entity2)
                alignment_graph.add(equivalence_triple)

#TASK OA2
#The function combines all sources in a single graph 
def create_graph_with_reasoning():
    single_combined_graph = Graph()
    #TASK OA2.1
    single_combined_graph += cw_onto_graph
    #TASK OA2.2
    single_combined_graph += pizza_graph
    #TASK OA2.3
    single_combined_graph += alignment_graph
    #TASK OA2.4
    single_combined_graph += cw_data_graph
    #Reasoning being applied
    single_combined_graph.bind("owl", OWL)
    single_combined_graph.bind("rdfs", RDFS)
    single_combined_graph.bind("cw", "http://www.semanticweb.org/city/in3067-inm713/2023/restaurants#")

    #Turtle file is created of the combined graph
    single_combined_graph.serialize("combined_task2.ttl", format="turtle")

def query_pizza(onto):
    pizza = Namespace("http://www.co-ode.org/ontologies/pizza/pizza.owl#")
    
    onto.bind("pizza", pizza)

    query = '''
        SELECT ?restaurant ?pizza
        WHERE {
            ?restaurant a pizza:Restaurant ;
                        pizza:servesPizza ?pizza .
        }
    '''
    results = onto.query(query)

    for row in results:
        restaurant = row['restaurant']
        pizza = row['pizza']
        print(f'Restaurant: {restaurant}, Pizza: {pizza}')

pizza_loc = 'part_2/pizza-ontology/pizza.owl'
cw_onto_loc = 'part_2/pizza-restaurants-model-ontology/pizza-restaurants-ontology.ttl'
cw_data_loc = 'part_2/pizza-ontology/test.ttl'

pizza_graph = Graph()
pizza_graph.parse(pizza_loc,format="xml")


cw_onto_graph = Graph()
cw_onto_graph.parse(cw_onto_loc,format="ttl")

cw_data_graph = Graph()
cw_data_graph.parse(cw_data_loc,format="ttl")

query_pizza(pizza_graph)

alignment_graph = Graph()

find_equivalences(cw_onto_graph, pizza_graph)

alignment_graph.serialize("equivalences.ttl", format="turtle")

create_graph_with_reasoning()

