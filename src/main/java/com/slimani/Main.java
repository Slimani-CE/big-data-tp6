package com.slimani;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;

public class Main {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                .appName("HospitalIncidentsStreamingFromNetcat")
                .getOrCreate();

        // Read data from a Netcat server
        Dataset<Row> streamingDF = spark.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", "8090")
                .load();

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

        spark.stop();
    }
}