Arga Théo  
Cadorel Jules  
Fisher Daniel  

# PageRank2024 - Large Scale Data Management
Ce projet compare les performances de l'algorithme PageRank en utilisant deux implémentations PySpark :
une basée sur des DataFrames et l'autre sur des RDDs. Nous avons testé les deux implémentations sur différents clusters (1, 2 et 4 nœuds), avec et sans URL partitioning.

## Résultats :
### Résultat avec https://raw.githubusercontent.com/momo54/large_scale_data_management/main/small_page_links.nt
|          Number of Nodes :          |  1 |  2 |  4 |
|:-----------------------------------:|:--:|:--:|:--:|
| DataFrames with URL partitioning    | 22 | 35 | 37 |
| DataFrames without URL partitioning | 56 | 36 | 34 |
| RDD with URL partitioning           | 18 |    |    |
| RDD without URL partitioning        |    |    |    |

\[Image graphe\]



## Entitées avec les plus gros page rank :
### Avec https://raw.githubusercontent.com/momo54/large_scale_data_management/main/small_page_links.nt  
L'entité http://dbpedia.org/resource/Anatolia possède le meilleur pagerank avec 0.293785843268494.  
Le jeu de données est petit et ne comprend que des entités commençant par "A", ce qui donne un avantage à l'Anatolie dans cette sélection limitée. De plus, étant une région grande et historiquement importante, elle a probablement de nombreux liens sortants vers d'autres sujets, ce qui augmente son pagerank.  

L'entité http://dbpedia.org/resource/Acronym_and_initialism possède le deuxième meilleur pagerank avec 0.29080001821188584.
L'entité http://dbpedia.org/resource/Ada_Lovelace possède le troisième meilleur pagerank avec 0.2872584197502371.
