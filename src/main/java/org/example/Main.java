package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String csv[]) {
        SparkSession sparkSession= SparkSession.builder().master("local").appName("Read_CSV").getOrCreate();

        Dataset<Row> dataset = sparkSession.sqlContext().read().format("com.databricks.spark.csv")
                .load("D:\\Practica\\Proiect_practica\\src\\main\\resources\\erasmus.csv");

        dataset.show();
    }
}