package org.vcs.spark.sqltodataframe;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * MySQL injection to Spark, using the Sakila sample database.
 *
 * @author jgp
 */
public class MySQLToDatasetApp {

    /**
     * main() is your entry point to the application.
     *
     * @param args
     */
    public static void main(String[] args) {
        MySQLToDatasetApp app = new MySQLToDatasetApp();
        app.start();
    }

    /**
     * The processing code.
     */
    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName(
                        "MySQL to Dataframe using a JDBC Connection")
                .master("local")
                .getOrCreate();

        // Using properties
        Properties props = new Properties();
        byte[] password=System.getenv("DB_PASSWORD").getBytes(StandardCharsets.UTF_8);
        props.put("user", "root");
        props.put("password",new String(password));

        props.put("partitionColumn", "city");
        props.put("lowerBound", "1");
        props.put("upperBound", "1000");
        props.put("numPartitions", "3");

        password=null;
        props.put("useSSL", "false");
        String sqlQuery="select from emp where sal>7000 ";

        Dataset<Row> df = spark.read().jdbc(
                "jdbc:mysql://localhost:3306/emp",
                "empdetails",
                props);

        Row sal = df.orderBy(df.col("sal").desc()).first();
        System.out.println(sal);
        // Displays the dataframe and some of its metadata
        df.show(5);
        df.printSchema();
        System.out.println("The dataframe contains " + df
                .count() + " record(s).");

        System.out.println("The dataframe is split over " + df.rdd()
                .getPartitions().length + " partition(s).");

    }
}