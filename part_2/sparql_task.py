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
    filename = 'part_2/pizza-ontology/test.ttl'

    sparql_query = SparqlQuery(filename)
            
    def task1():
        query = ''' 
        SELECT ?name ?firstLineAddress ?cityName  ?stateName
        WHERE {
            ?restaurant cw:name ?name .
            ?restaurant a cw:Restaurant .
            ?restaurant cw:locatedAddress ?address .
            ?address cw:firstLineAddress ?firstLineAddress .
            ?address cw:locatedCity ?city .
            ?city cw:name ?cityName .
            ?address cw:locatedState ?state .
            ?state cw:name ?stateName .
            FILTER (?stateName = "TX")
        }
        '''
        sparql_query.make_query_to_csv(query,'SPARQL1.1_subtask')

    def task2():
        query = '''
        SELECT ?restaurantName (AVG(?menu_item_price) AS ?avg_value)
        WHERE {
            ?menuItem cw:servedInRestaurant ?restaurant .
            ?menuItem cw:hasValue ?value .
            ?value cw:amount ?menu_item_price .
            ?restaurant a cw:Restaurant .
            ?restaurant cw:name ?restaurantName .
            FILTER (?restaurantName = "Burgers & Cupcakes")
        }
        '''
        sparql_query.make_query_to_csv(query,'SPARQL2_subtask')

    def task3():
        query = '''
        SELECT ?cityName (COUNT(?restaurant) AS ?num_restaurants)
        WHERE {
                ?restaurant cw:locatedAddress ?address .
                ?address cw:firstLineAddress ?firstLineAddress .
                ?address cw:locatedCity ?city .
                ?address cw:locatedState ?state .
                ?state cw:name ?stateName .
                ?city cw:name ?cityName .
                FILTER (?stateName != "WA")
        }
        GROUP BY ?cityName
        HAVING (COUNT(?restaurant))
        '''
        sparql_query.make_query_to_csv(query,'SPARQL3_subtask')

    def task4():
        query = '''
        SELECT ?cityName (COUNT(?restaurant) AS ?num_restaurants) (AVG(?menu_item_price) AS ?avg_value)
        WHERE {
            ?restaurant cw:locatedAddress ?address .
            ?address cw:firstLineAddress ?firstLineAddress .
            ?address cw:locatedCity ?city .
            ?city cw:name ?cityName .
            ?menuItem cw:servedInRestaurant ?restaurant .
            ?menuItem cw:hasValue ?value .
            ?value cw:amount ?menu_item_price .
        }
        GROUP BY ?cityName
        HAVING (COUNT(?restaurant) > 7 && AVG(?menu_item_price) > 10)
        ORDER BY DESC(?num_restaurants) ?avg_value
        '''
        sparql_query.make_query_to_csv(query,'SPARQL4_subtask')

    def task5():
        query = '''
        SELECT ?name ?cityName ?menu_item_price
        WHERE {
            {
                ?restaurant cw:name ?name .
                ?restaurant a cw:Restaurant .
                ?restaurant cw:locatedAddress ?address .
                ?address cw:firstLineAddress ?firstLineAddress .
                ?address cw:locatedCity ?city .
                ?city cw:name "New York".
            } UNION {
                ?restaurant cw:name ?name .
                ?restaurant a cw:Restaurant .
                ?restaurant cw:locatedAddress ?address .
                ?address cw:firstLineAddress ?firstLineAddress .
                ?address cw:locatedCity ?city .
                ?menuItem cw:servedInRestaurant ?restaurant .
                ?menuItem cw:hasValue ?value .
                ?value cw:amount ?menu_item_price .
                ?city cw:name ?cityName .
                FILTER NOT EXISTS {
                    ?menuItem cw:servedInRestaurant ?restaurant .
                    ?menuItem cw:hasValue ?value .
                    ?value cw:amount ?menu_item_price .
                    FILTER (?menu_item_price > 5)
                }
            }
        }
        '''
        sparql_query.make_query_to_csv(query,'SPARQL5_subtask')

    def print_predicates():
        predicates = set(sparql_query.graph.predicates())

        prefixes = sparql_query.graph.namespace_manager.namespaces()

        # Print the predicates
        for predicate in predicates:
            print(predicate)

        # Print the prefixes
        # for prefix, namespace in prefixes:
        #     print(f"Prefix: {prefix}, Namespace: {namespace}")

    #print_predicates()
    task5()