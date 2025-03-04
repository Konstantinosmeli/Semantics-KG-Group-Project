"""
Created on 19 Mar 2019

@author: ejimenez-ruiz
"""
import json
import time
from urllib import parse, request

from task_rdf.entity import KGEntity


class Lookup(object):
    def __init__(self, lookup_url):
        self.service_url = lookup_url

    def getJSONRequest(self, params, attempts=3):
        try:
            # urllib has been split up in Python 3.
            # The urllib.urlencode() function is now urllib.parse.urlencode(),
            # and the urllib.urlopen() function is now urllib.request.urlopen().
            # url = service_url + '?' + urllib.urlencode(params)
            url = self.service_url + "?" + parse.urlencode(params)
            # print(url)
            # response = json.loads(urllib.urlopen(url).read())

            req = request.Request(url)
            # Customize headers. For example dbpedia lookup returns xml by default
            req.add_header("Accept", "application/json")

            # print(request.urlopen(req).read())
            response = json.loads(request.urlopen(req).read())

            return response

        except Exception:
            print("Lookup '%s' failed. Attempts: %s" % (url, str(attempts)))
            time.sleep(60)  # to avoid limit of calls, sleep 60s
            attempts -= 1
            if attempts > 0:
                return self.getJSONRequest(params, attempts)
            else:
                return None


class DBpediaLookup(Lookup):
    """
    DBpedia lookup access
    """

    def __init__(self):
        """
        Constructor
        """
        super().__init__(self.getURL())

    def getURL(self):
        # OLD lookup: https://github.com/dbpedia/lookup
        # return "http://lookup.dbpedia.org/api/search/KeywordSearch"

        # NEW lookup: https://github.com/dbpedia/lookup-application
        # return "http://akswnc7.informatik.uni-leipzig.de/lookup/api/search"
        return "http://lookup.dbpedia.org/api/search"

        # TODO: prefix search allows for partial searches
        # return "http://lookup.dbpedia.org/api/search/PrefixSearch"

    def __createParams(self, query, limit, query_cls=""):
        if query_cls == "":
            params = {
                "query": query,
                "maxResults": limit,
                "format": "json",
            }

        else:
            params = {
                "typeName": query_cls,
                "query": query,
                "maxResults": limit,
                "format": "json",
            }
            # 'QueryClass' : query_cls,
            # 'QueryString': query,
            # 'MaxHits': limit,

        return params

    def getKGName(self):
        return "DBpedia"

    def __extractKGEntities(self, json, category_filter, filter=""):
        """
        Returns list of ordered entities according to relevance: dbpedia
        """
        entities = list()

        for element in json["docs"]:
            types = set()

            if "type" in element:
                for _t in element["type"]:
                    if _t != "http://www.w3.org/2002/07/owl#Thing":
                        if (
                            _t.startswith("http://dbpedia.org/ontology/")
                            or _t.startswith("http://www.wikidata.org/entity/")
                            or _t.startswith("http://schema.org/")
                        ):
                            types.add(_t)

            description = ""
            if "comment" in element:
                description = element["comment"]

            # Expected only one
            uri = ""
            if "resource" in element:
                for _u in element["resource"]:
                    uri = _u

            # Expected only one
            label = ""
            if "label" in element:
                for _l in element["label"]:
                    label = _l

            kg_entity = KGEntity(uri, label, description, types, self.getKGName())

            if "category" in element:
                # Prioritise good matches to category first - prepend
                if category_filter in element["category"]:
                    entities.insert(0, kg_entity)
                # Else we filter according to given URI (not used)
                elif filter == "" or uri == filter:
                    # print(kg_entity)
                    entities.append(kg_entity)

        # if len(entities) > 0:
        #     for entity in entities:
        #         print("entities", entity)
        return entities

    def getKGEntities(self, query, limit, category_filter=""):
        json = self.getJSONRequest(self.__createParams(query, limit), 3)

        if json is None:
            print("None results for", query)
            return list()

        return self.__extractKGEntities(
            json, category_filter
        )  # Optionally filter by URI


class WikidataAPI(Lookup):
    """
    Wikidata web search API
    """

    def __init__(self):
        """
        Constructor
        """
        super().__init__(self.getURL())

    def getURL(self):
        return "https://www.wikidata.org/w/api.php"

    def __createParams(self, query, limit, type="item"):
        params = {
            "action": "wbsearchentities",
            "format": "json",
            "search": query,
            "type": type,
            "limit": limit,
            "language": "en",
        }

        return params

    def getKGName(self):
        return "Wikidata"

    def __extractKGEntities(self, json, filter=""):
        """
        Returns list of ordered entities according to relevance: wikidata
        """
        entities = list()

        for element in json["search"]:
            # empty list of type from wikidata lookup
            types = set()

            description = ""
            if "description" in element:
                description = element["description"]

            kg_entity = KGEntity(
                element["concepturi"],
                element["label"],
                description,
                types,
                self.getKGName(),
            )

            # We filter according to givem URI
            if filter == "" or element["concepturi"] == filter:
                entities.append(kg_entity)

        # for entity in entities:
        #    print(entity)
        return entities

    def getKGEntities(self, query, limit, type="item", filter=""):
        json = self.getJSONRequest(self.__createParams(query, limit, type), 3)

        if json is None:
            print("None results for", query)
            return list()

        return self.__extractKGEntities(json, filter)  # Optionally filter by URI


class GoogleKGLookup(Lookup):
    """
    Entity search for Google KG
    """

    def __init__(self):
        """
        Constructor
        """
        super().__init__(self.getURL())

        self.api_key = "AIzaSyA6Bf9yuMCCPh7vpElzrfBvE2ENCVWr-84"
        # open('.api_key').read()

    # def getAPIKey(self):
    #    return self.api_key

    def getURL(self):
        return "https://kgsearch.googleapis.com/v1/entities:search"

    def __createParams(self, query, limit):
        params = {
            "query": query,
            "limit": limit,
            "indent": True,
            "key": self.api_key,
        }

        return params

    def getKGName(self):
        return "GoogleKG"

    """
    Returns list of ordered entities according to relevance: google
    """

    def __extractKGEntities(self, json, filter=""):
        entities = list()

        for element in json["itemListElement"]:
            types = set()

            for t in element["result"]["@type"]:
                if t != "Thing":
                    types.add("http://schema.org/" + t)

            description = ""
            if "description" in element["result"]:
                description = element["result"]["description"]

            kg_entity = KGEntity(
                element["result"]["@id"],
                element["result"]["name"],
                description,
                types,
                self.getKGName(),
            )

            # We filter according to givem URI
            if filter == "" or element["result"]["@id"] == filter:
                entities.append(kg_entity)
            # print(kg_entity)

        # for entity in entities:
        #    print(entity)
        return entities

    def getKGEntities(self, query, limit, filter=""):
        json = self.getJSONRequest(self.__createParams(query, limit), 3)

        if json is None:
            print("None results for", query)
            return list()

        return self.__extractKGEntities(json, filter)  # Optionally filter by URI


if __name__ == "__main__":
    # query="Chicago Bulls"
    query = "Congo"

    # Max entities to be returned
    limit = 10

    print("Entities from Google KG:")
    kg = GoogleKGLookup()
    entities = kg.getKGEntities(query, limit)
    for ent in entities:
        print(ent)

    print("\n")

    dbpedia = DBpediaLookup()
    entities = dbpedia.getKGEntities(query, limit)
    print("Entities from DBPedia:")
    for ent in entities:
        print(ent)

    print("\n")

    type = "item"
    # type="property"
    wikidata = WikidataAPI()
    entities = wikidata.getKGEntities(query, limit, "item")
    print("Entities from Wikidata:")
    for ent in entities:
        print(ent)
