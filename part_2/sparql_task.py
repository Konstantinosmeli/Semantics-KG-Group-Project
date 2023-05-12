from rdflib import Graph, Namespace
from rdflib.plugins.sparql import prepareQuery


class SparqlQuery:
    def __init__(self,filename):
        ttl_file = '.ttl'
        rdf_file = '.rdf'
        self.graph = Graph()
        if filename.find(ttl_file):
            self.graph.parse(filename,format='turtle')
        elif filename.find(rdf_file):
            self.graph.parse(filename,format='xml')
        else:
            print('File type given was incorrect. Needs to be in .ttl or .rdf format')

    def make_query_to_csv(self,query,output_file):
        result = self.graph.query(query)
        print(f'Result: {result} Length: {len(result)}')
        for row in result:
            print(row)
        result.serialize(destination=f'{output_file}_results.csv',format='csv')
    


if __name__ == "__main__":
    filename = 'part_2/pizza-ontology/pizza_restaurant_reasoned.ttl'

    sparql_query = SparqlQuery(filename)
            
    def task1():
        query = '''
        SELECT ?name ?address ?city ?state ?postcode ?country
        WHERE {
            ?value cw:locatedCity ?city .
        }
        '''
        sparql_query.make_query_to_csv(query,'SPARQL1.1_subtask')

    def task2():
        query = '''
        SELECT ?city (AVG(?menu_item_price) AS ?avg_value)
        WHERE {
            ?restaurant fp:name ?city .
            ?restaurant fp:menu_item ?menu_item .
            ?menu_item fp:menu_item_price ?menu_item_price .
            FILTER (?menu_item_price > 10)
        }
        GROUP BY ?city
        '''
        sparql_query.make_query_to_csv(query,'SPARQL2_subtask')

    def print_predicates():
        predicates = set(sparql_query.graph.predicates())

        prefixes = sparql_query.graph.namespace_manager.namespaces()

        # Print the predicates
        # for predicate in predicates:
        #     print(predicate)

        # Print the prefixes
        for prefix, namespace in prefixes:
            print(f"Prefix: {prefix}, Namespace: {namespace}")

    #print_predicates()
    task1()