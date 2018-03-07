# Scala pour Spark

## 1. Chargement Spark dans Ambari

```scala
sudo su spark
export SPARK_MAJOR_VERSION=2   #préciser version 2
cd /usr/hdp/current/spark2-client/
hdfs dfs -copyFromLocal /etc/hadoop/conf/log4j.properties /tmp/data.txt
./bin/spark-shell
```

###      CountWords

```scala
val data = spark.read.textFile("/tmp/data.txt").as[String]
val words = data.flatMap(value => value.split("\\s+"))
val groupedWords = words.groupByKey(_.toLowerCase)
val counts = groupedWords.count()
counts.show()
```

## 2. Dataset Wildlife

Chargement du dataset depuis le serveur:
```scala
sudo su spark
export SPARK_MAJOR_VERSION=2
cd /usr/hdp/current/spark2-client/
hdfs dfs -copyFromLocal /etc/hadoop/conf/log4j.properties /user/hdfs/wildlife.csv
./bin/spark-shell
```

Mettre correctement le header et le bon type de format de données par colonne lors du chargement du fichier:

```scala
val data = spark.read.option("header", "true").option("inferSchema", "true").csv("/user/hdfs/wildlife.csv")
```
Pour les noms de colonnes contenant des . ou des chiffres, mettre des backticks (accent grave) ou changer le nom de la colonne:

```scala
var df = data.withColumnRenamed("App.","App")
```
Fonction Filter: On ne garde que les données datant d'après 2016.

On réapplique un filtre, pour ne garder que les mammifères.

```scala
val year = data.filter(data("Year") > 2016)
year.show(5)

+----+----+------------------+--------+-------------+------------+---------+--------+--------+------+--------------------------+--------------------------+---------+----+-------+------+
|Year|App.|             Taxon|   Class|        Order|      Family|    Genus|Importer|Exporter|Origin|Importer reported quantity|Exporter reported quantity|     Term|Unit|Purpose|Source|
+----+----+------------------+--------+-------------+------------+---------+--------+--------+------+--------------------------+--------------------------+---------+----+-------+------+
|2017|   I|Loxodonta africana|Mammalia|  Proboscidea|Elephantidae|Loxodonta|      ZA|      MW|  null|                      null|                      34.8|specimens|   l|      S|     W|
|2017|   I|Loxodonta africana|Mammalia|  Proboscidea|Elephantidae|Loxodonta|      ZA|      MW|  null|                      null|                      20.0|specimens|  ml|      S|     W|
|2017|   I|Loxodonta africana|Mammalia|  Proboscidea|Elephantidae|Loxodonta|      ZA|      MW|  null|                      null|                     120.0|specimens|null|      S|     W|
|2017|   I|  Falco rusticolus|    Aves|Falconiformes|  Falconidae|    Falco|      SI|      AE|    ES|                       1.0|                      null|     live|null|      T|     C|
|2017|   I|  Falco rusticolus|    Aves|Falconiformes|  Falconidae|    Falco|      SI|      AE|    GB|                       2.0|                      null|     live|null|      T|     C|
+----+----+------------------+--------+-------------+------------+---------+--------+--------+------+--------------------------+--------------------------+---------+----+-------+------+

val mamm2017 = year.filter(year("Class") === "Mammalia")
mamm2017.show(5)
+----+----+------------------+--------+-----------+------------+---------+--------+--------+------+--------------------------+--------------------------+---------+----+-------+------+
|Year|App.|             Taxon|   Class|      Order|      Family|    Genus|Importer|Exporter|Origin|Importer reported quantity|Exporter reported quantity|     Term|Unit|Purpose|Source|
+----+----+------------------+--------+-----------+------------+---------+--------+--------+------+--------------------------+--------------------------+---------+----+-------+------+
|2017|   I|Loxodonta africana|Mammalia|Proboscidea|Elephantidae|Loxodonta|      ZA|      MW|  null|                      null|                      34.8|specimens|   l|      S|     W|
|2017|   I|Loxodonta africana|Mammalia|Proboscidea|Elephantidae|Loxodonta|      ZA|      MW|  null|                      null|                      20.0|specimens|  ml|      S|     W|
|2017|   I|Loxodonta africana|Mammalia|Proboscidea|Elephantidae|Loxodonta|      ZA|      MW|  null|                      null|                     120.0|specimens|null|      S|     W|
|2017|   I|  Acinonyx jubatus|Mammalia|  Carnivora|     Felidae| Acinonyx|      BG|      NA|  null|                       1.0|                      null| trophies|null|      H|     W|
|2017|   I|  Acinonyx jubatus|Mammalia|  Carnivora|     Felidae| Acinonyx|      MW|      ZA|  null|                      14.0|                      null|     live|null|      N|     W|
+----+----+------------------+--------+-----------+------------+---------+--------+--------+------+--------------------------+--------------------------+---------+----+-------+------+
``` 
Fonction: Count. 56 mammifères ont été échangés après les années 2016.

```scala
mamm2017.count()
res10: Long = 56
```
Sauvegarde

```scala
mamm2017.rdd.repartition(1).saveAsTextFile("/tmp/wildlife")
```
Fonction: printSchema

```scala
data.printSchema()
root
 |-- Year: integer (nullable = true)
 |-- App.: string (nullable = true)
 |-- Taxon: string (nullable = true)
 |-- Class: string (nullable = true)
 |-- Order: string (nullable = true)
 |-- Family: string (nullable = true)
 |-- Genus: string (nullable = true)
 |-- Importer: string (nullable = true)
 |-- Exporter: string (nullable = true)
 |-- Origin: string (nullable = true)
 |-- Importer reported quantity: double (nullable = true)
 |-- Exporter reported quantity: double (nullable = true)
 |-- Term: string (nullable = true)
 |-- Unit: string (nullable = true)
 |-- Purpose: string (nullable = true)
 |-- Source: string (nullable = true)
```

Clause: groupBy à coupler avec une fonction d'aggrégation (min, max, avg, etc.).

On somme pour chaque classe le nombre d'individus comptabilisés dans le dataset.

```scala
data.groupBy("Class").count().show()
+--------------+-----+
|         Class|count|
+--------------+-----+
|          Aves| 6861|
|      Bivalvia|  269|
|      Amphibia|  420|
|       Insecta|  310|
|      Mammalia| 8505|
|Elasmobranchii|  113|
|     Arachnida|   67|
|          null|20224|
|    Gastropoda|  191|
|      Reptilia|18430|
|   Actinopteri| 2759|
|      Anthozoa| 8781|
|     Dipneusti|    4|
| Holothuroidea|   10|
|  Hirudinoidea|   34|
|   Coelacanthi|    2|
|      Hydrozoa|  181|
+-----
```

Fonction select.

On choisit 2 colonnes du dataset d'origine, class et taxon.

```scala
scala> data.select("Class", "Taxon").show(5)
+-----+--------------------+
|Class|               Taxon|
+-----+--------------------+
| Aves|      Aquila heliaca|
| Aves|      Aquila heliaca|
| Aves|Haliaeetus albicilla|
| Aves|Haliaeetus albicilla|
| Aves|Haliaeetus albicilla|
+-----+--------------------+
```

Fonction: sort.

On range par ordre alphabétique les sources de prélèvements.

```scala
data.select("Order", "Taxon", "Source").sort("Taxon").show(5)
+--------+--------------------+------+
|   Order|               Taxon|Source|
+--------+--------------------+------+
|Liliales|"Galanthus nivali.."|     A|
|Liliales|"Galanthus nivali.."|     A|
|Liliales|"Galanthus nivali.."|     A|
|Liliales|"Galanthus nivali.."|     A|
|Liliales|"Galanthus nivali.."|     A|
+--------+--------------------+------+
```
Fonction agg: aggregate à coupler avec une méthode d'aggrégation.

Calcule la somme, le minimum, le maximum et la moyenne des quantités importés.

```scala
data.agg(sum("Importer reported quantity"),min("Importer reported quantity"),max("Importer reported quantity"), avg("Importer reported quantity")).show()
+-------------------------------+-------------------------------+-------------------------------+-------------------------------+
|sum(Importer reported quantity)|min(Importer reported quantity)|max(Importer reported quantity)|avg(Importer reported quantity)|
+-------------------------------+-------------------------------+-------------------------------+-------------------------------+
|           1.3965036677264196E8|                            0.0|                    1.9524978E7|              4382.425367873029|
+-------------------------------+-------------------------------+-------------------------------+-------------------------------+
```

Fonction join: pour joindre 2 dataframes ("data" et "db2").

Option Seq: précise les colonnes identiques, pour éviter les duplicats (colonnes Year et Source):

```scala
db2.show(5)
+----+-----+----+------+
|Year|  irq| erq|Source|
+----+-----+----+------+
|2016| null| 1.0|     C|
|2016| null| 1.0|     O|
|2016| null|43.0|     W|
|2016| null|43.0|     W|
|2016|700.0|null|     W|
+----+-----+----+------+

val newdb = data.join(db2, Seq("Year", "Source"))
newdb: org.apache.spark.sql.DataFrame = [Year: int, Source: string ... 16 more fields]

newdb.printSchema()
root
 |-- Year: integer (nullable = true)
 |-- Source: string (nullable = true)
 |-- App.: string (nullable = true)
 |-- Taxon: string (nullable = true)
 |-- Class: string (nullable = true)
 |-- Order: string (nullable = true)
 |-- Family: string (nullable = true)
 |-- Genus: string (nullable = true)
 |-- Importer: string (nullable = true)
 |-- Exporter: string (nullable = true)
 |-- Origin: string (nullable = true)
 |-- Importer reported quantity: double (nullable = true)
 |-- Exporter reported quantity: double (nullable = true)
 |-- Term: string (nullable = true)
 |-- Unit: string (nullable = true)
 |-- Purpose: string (nullable = true)
 |-- irq: double (nullable = true)
 |-- erq: double (nullable = true)
```

### Requêtes SQL:

Besoin de créer un dataset "lisible" en SQL (TempView)
```scala
newdb.createOrReplaceTempView("wildlife")

spark.sql("SELECT Taxon, irq FROM wildlife WHERE irq = (SELECT max(irq) FROM wildlife)").show()
+--------------------+-----------+
|               Taxon|        irq|
+--------------------+-----------+
|  Araucaria araucana|1.9524978E7|
|  Araucaria araucana|1.9524978E7|
|  Araucaria araucana|1.9524978E7|
|  Araucaria araucana|1.9524978E7|
|Discocactus hepta...|1.9524978E7|
| Discocactus horstii|1.9524978E7|
|Discocactus place...|1.9524978E7|
|Discocactus zehnt...|1.9524978E7|
|    Saussurea costus|1.9524978E7|
|    Saussurea costus|1.9524978E7|
|    Saussurea costus|1.9524978E7|
|    Saussurea costus|1.9524978E7|
|Euphorbia ambovom...|1.9524978E7|
|Euphorbia ambovom...|1.9524978E7|
|Euphorbia cylindr...|1.9524978E7|
|Euphorbia cylindr...|1.9524978E7|
|Euphorbia cylindr...|1.9524978E7|
|   Euphorbia decaryi|1.9524978E7|
|   Euphorbia moratii|1.9524978E7|
|      Aloe bellatula|1.9524978E7|
+--------------------+-----------+
only showing top 20 rows
```

## 3. JAR

*la suite dans le prochain épisode...*
