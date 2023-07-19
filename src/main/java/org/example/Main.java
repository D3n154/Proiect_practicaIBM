package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Main {
    public static void main(String csv[]) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("Read_CSV")
                .getOrCreate();

        Dataset<Row> dataset = sparkSession.sqlContext()
                .read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .load("D:\\Practica\\Proiect_practica\\src\\main\\resources\\erasmus.csv");

        // Filtrăm doar datele relevante pentru țările LV, MK și MT
        Dataset<Row> filteredData = dataset.filter(dataset.col("Receiving Country Code").isin("LV", "MK", "MT"));

        // Numărăm participanții pentru fiecare țară destinatară și ordonăm descrescător după numărul de participanți
        Dataset<Row> groupedAndOrderedData = filteredData.groupBy("Receiving Country Code", "Sending Country Code")
                .count()
                .orderBy("Receiving Country Code");

        // Afișăm rezultatele
        groupedAndOrderedData.show();

        // Salvăm datele în baza de date MySQL
        String url = "jdbc:mysql://localhost:3306/erasmus_db";
        String user = "root";
        String password = "Noiembrie0611";

        try (Connection connection = DriverManager.getConnection(url, user, password)) {
            String insertQuery = "INSERT INTO participanti (Receiving_Country_Code, Sending_Country_Code, count) VALUES (?, ?, ?)";

            try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
                for (Row row : groupedAndOrderedData.collectAsList()) {
                    String receivingCountryCode = row.getString(0);
                    String sendingCountryCode = row.getString(1);
                    Long count = row.getLong(2);

                    preparedStatement.setString(1, receivingCountryCode);
                    preparedStatement.setString(2, sendingCountryCode);
                    preparedStatement.setLong(3, count);

                    preparedStatement.executeUpdate();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}