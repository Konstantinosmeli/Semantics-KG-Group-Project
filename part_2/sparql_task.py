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
    filename = 'part_2/pizza-ontology/pizza_restaurants_with_reasoning_sparql1.ttl'

    sparql_query = SparqlQuery(filename)

    def task1():
        query = '''
        SELECT ?name ?address ?city ?state ?postcode ?country
        WHERE {
            ?restaurant fp:name ?name .
            ?restaurant fp:address ?address .
            ?restaurant fp:city ?city .
            ?restaurant fp:state ?state .
            ?restaurant fp:postcode ?postcode .
            ?restaurant fp:country ?country
            FILTER regex(?state, "TX", "i")
        }
        '''
        sparql_query.make_query_to_csv(query,'SPARQL1_subtask')

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

    task2()