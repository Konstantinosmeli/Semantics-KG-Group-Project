from rdflib import Graph, Namespace
from rdflib.plugins.sparql import prepareQuery


class SparqlQuery:
    def __init__(self, filename):
        ttl_file = ".ttl"
        rdf_file = ".rdf"
        self.graph = Graph()
        if filename.find(ttl_file):
            self.graph.parse(filename, format="turtle")
        elif filename.find(rdf_file):
            self.graph.parse(filename, format="xml")
        else:
            print("File type given was incorrect. Needs to be in .ttl or .rdf format")

    # Creates the query and saves the result into a csv file.
    def make_query_to_csv(self, query, output_file):
        result = self.graph.query(query)
        print(f"Result: {result} Length: {len(result)}")
        for row in result:
            print(row)
        result.serialize(destination=f"{output_file}_results.csv", format="csv")


if __name__ == "__main__":
    filename = "../task_rdf/pizza_restaurant.ttl"

    sparql_query = SparqlQuery(filename)

    # This query retrieves all the restaurants in the state of Texas
    def task1():
        query = """ 
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
        """
        sparql_query.make_query_to_csv(query, "./sparql_result/SPARQL1_subtask")

    # This query returns the average price of all items on the Burgers & Cupcakes menu
    def task2():
        query = """
        SELECT ?restaurantName (AVG(?menu_item_price) AS ?avg_value)
        WHERE {
            ?menuItem cw:servedInRestaurant ?restaurant .
            ?menuItem cw:hasValue ?value .
            ?value cw:amount ?menu_item_price .
            ?restaurant a cw:Restaurant .
            ?restaurant cw:name ?restaurantName .
            FILTER (?restaurantName = "Burgers & Cupcakes")
        }
        """
        sparql_query.make_query_to_csv(query, "./sparql_result/SPARQL2_subtask")

    # This query returns the number of restaurants in all the cities except the ones in the
    # state of Washington
    def task3():
        query = """
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
        """
        sparql_query.make_query_to_csv(query, "./sparql_result/SPARQL3_subtask")

    # This query returns the cities with a number of restaurants higher than 7 AND
    # an average item price of more than 10
    def task4():
        query = """
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
        """
        sparql_query.make_query_to_csv(query, "./sparql_result/SPARQL4_subtask")

    # This query returns the names of the restaurants that are either in New York City
    # or don't have any items on the menu worth higher than 5 USD
    def task5():
        query = """
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
        """
        sparql_query.make_query_to_csv(query, "./sparql_result/SPARQL5_subtask")

    task5()
