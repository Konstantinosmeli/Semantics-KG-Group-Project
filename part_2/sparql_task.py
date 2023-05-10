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
        # result.serialize(destination=f'{output_file}_results.csv',format='csv')

if __name__ == "__main__":
    filename = 'part_2/pizza-restaurants-model-ontology/pizza-restaurants-ontology_with_data.ttl'

    sparql_query = SparqlQuery(filename)

    def task1():
        query = '''
        SELECT DISTINCT ?state
        WHERE {
            ?Restaurant cw:locatedInState 'TX' .
        }
        '''
        sparql_query.make_query_to_csv(query,'SPARQL1_subtask')

    task1()