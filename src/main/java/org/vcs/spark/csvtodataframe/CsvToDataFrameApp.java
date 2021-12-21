package org.vcs.spark.csvtodataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

/**
 * CSV ingestion in a dataframe.
 *
 * @author jgp
 */
class CsvToDataframeApp {

    /**
     * main() is your entry point to the application.
     *
     * @param args
     */
    public static void main(String[] args) {
        CsvToDataframeApp app = new CsvToDataframeApp();
        app.start();
    }

    /**
     * The processing code.
     */
    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("CSV to Dataset")
                .master("local")
                .getOrCreate();

        // Reads a CSV file with header, called books.csv, stores it in a
        // dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .load("data/books.csv")
                        .filter("id<2");

        System.out.println("latest data\n");
        //df.sort(df.col("authorId").desc()).show();
        // Shows at most 5 rows from the dataframe
       df.show(5,70);
       // df.printSchema();
        //df.select(col("id"), col("releaseDate")).show();
        //df.filter(col("id").gt(18)).show();
        df.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");

        //sqlDF.show();
        Dataset<Row> rowDataset = df.withColumn("movie details",concat(df.col("title"), lit("            "),df.col("releaseDate")));

        //rowDataset.show();
    }


}