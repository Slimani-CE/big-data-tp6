# Analyse des Incidents Hospitaliers avec Spark Structured Streaming ✨

Ce dépôt contient une application Java utilisant Spark Structured Streaming pour analyser en continu les incidents reçus par l'hôpital.

## Introduction

Cette application utilise Spark pour lire les incidents en streaming depuis des fichiers CSV, puis effectue des analyses pour répondre à deux questions spécifiques.

## Connexion à Spark
```java
SparkSession spark = SparkSession.builder()
                .appName("HospitalIncidentsStreamingFromNetcat")
                .getOrCreate();

Dataset<Row> streamingDF = spark.readStream()
        .format("socket")
        .option("host", "localhost")
        .option("port", "8090")
        .load();
```

## Question 1 : Nombre d'incidents par service

```java
// Task 1: Continuously show the number of incidents per service and write to a named in-memory table
streamingDF.selectExpr("split(value, ',') as values")
        .selectExpr("values[3] as service")
        .groupBy("service")
        .count()
        .writeStream()
        .outputMode("complete")
        .format("memory")
        .queryName("incidentsByServiceTable")
        .start();
```

## Question 2 : Nombre d'incidents par service et par type

```java
// Task 2: Continuously identify the two years with the most incidents and write to a named in-memory table
streamingDF.selectExpr("split(value, ',') as values")
        .selectExpr("substring(values[4], 1, 4) as year")
        .groupBy("year")
        .count()
        .orderBy(col("count").desc())
        .limit(2)
        .writeStream()
        .outputMode("complete")
        .format("memory")
        .queryName("incidentsByYearTable")
        .start()
        .awaitTermination();
```

## Lancement de l'application
![img](assets/img.png)