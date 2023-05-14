# IN3067/INM713 Semantic Web Technologies and Knowledge Graphs

Initial requirement:
```python
# Make sure that you are running this project on Python 3.8 or older
# Since OWL2Vec* requires scikit-learn~=0.24 which can't be installed on
# Python 3.9 or later
python –version
# Sould returns: python 3.8
```

## TASK RDF

### TASK RDF.2: RDF generation
In this task, we will perform the creating RDFs from CSV file without external public KG vocabulary usage.
In order to do this, please check `pizza_kg.py` and change line:

```
enable_external_uri: bool = False
```

### TASK RDF.3: Perform reasoning with `cw_onto`
We will run the reasoning with offline file:
```python
pizza_kg.perform_reasoning(ontology=ONTOLOGY)
pizza_kg.save_graph("pizza_restaurant_offline_reasoned.ttl")
```

### TASK RDF.4: Reuse URIs from state-of-the art knowledge graphs 
To run this task, please put `TASK RDF.2` & `TASK RDF.3` in comment.And check `pizza_kg.py` and change line:
```
enable_external_uri: bool = False
```

### TASK RDF.5: Exploit an external Knowledge Graph to perform disambiguation
We already implemented this while doing `TASK RDF.4`, we will discuss more about this in the report.


## TASK SPARQL

To run this task, you have to go to the [task_sparql](https://github.com/Konstantinosmeli/Semantics-KG-Group-Project/tree/final_branch/task_sparql) folder and then open up the [sparql_task.py](https://github.com/Konstantinosmeli/Semantics-KG-Group-Project/blob/final_branch/task_sparql/sparql_task.py) file.

In said file in order to tryout different subtasks you need to change the function being called at the end of the file with each number corresponding to each subtask (eg task1() is subtask SPARQL.1 and task2() is subtask SPARQL.2 etc.).

Running the python file for each subtask will produce the appropriately named .csv file (SPARQL1_subtask_results for task1()) in the
[sparql_result](https://github.com/Konstantinosmeli/Semantics-KG-Group-Project/tree/final_branch/task_sparql/sparql_result) folder.

## TASK OA

For this task, you need to go to the [task_alignment](https://github.com/Konstantinosmeli/Semantics-KG-Group-Project/tree/final_branch/task_alignment) folder and run the [task_alignment.py](https://github.com/Konstantinosmeli/Semantics-KG-Group-Project/blob/final_branch/task_alignment/task_alignment.py) to run all subtasks with in TASK OA.

## TASK VECTOR
In order to run this, please install required library and put install `OWL2Vec*` using `setuptools` or `python install`.
After that, in terminal, we point to correct folder and run the configurations:
```python
cd OWL2Vec-Star-master/
owl2vec_star standalone --config_file ../owl2vec_config/config_1.cfg
owl2vec_star standalone --config_file ../owl2vec_config/config_2.cfg
owl2vec_star standalone --config_file ../owl2vec_config/config_3.cfg
```
Then, simply open `./task_vector/task_vector.ipynb` and run the notebook from beginning.
