from rdflib import Graph
from rdflib.util import guess_format


class GraphCombination(object):
    graph_paths: [str]
    output_path: str
    graph: Graph

    def __init__(self, _graph_paths: [str], _output_path: str) -> None:
        super().__init__()
        self.graph_paths = _graph_paths
        self.output_path = _output_path
        self.graph = Graph()

    def combination_and_save(self, _output_format: str = "ttl"):
        for _e in self.graph_paths:
            self.graph.load(_e, format=guess_format(_e))

        self.graph.serialize(destination=self.output_path, format=_output_format)
        return self.output_path
