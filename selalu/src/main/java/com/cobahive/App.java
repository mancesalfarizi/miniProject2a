package com.cobahive;

import java.io.File;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;



public class App 
{
    public static void main( String[] args )
    {
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark =SparkSession
            .builder()
            .appName("Java Spark Hive Example")
            .config("spark.sql.warehouse.dir", warehouseLocation)
            .enableHiveSupport()
            .getOrCreate();
    
        spark.sql("CREATE TABLE datacovid2020 (iso_code string, continent string, location string, "
        + "date date, total_cases int, new_cases int, total_deaths int, new_deaths int, "
        + "total_cases_per_million int, new_cases_per_million int, total_deaths_per_million int,"
        + "new_deaths_per_million int, new_tests int, total_tests int, total_tests_per_thousand int,"
        + "new_tests_per_thousand int, new_tests_smoothed int, new_tests_smoothed_per_thousand int," 
        + "tests_per_case int, positive_rate int, tests_units int, stringency_index int, population int," 
        + "population_density int, median_age int, aged_65_older int, aged_70_older int, gdp_per_capita int, "
        + "extreme_poverty int, cardiovasc_death_rate int, diabetes_prevalence int, female_smokers int, "
        + "male_smokers int, handwashing_facilities int, hospital_beds_per_thousand int, life_expectancy int)"
        + "row format delimited fields terminated by ',' "
        + "tblproperties(skip.header.line.count = 1)");
    
        spark.sql("LOAD DATA INPATH '/user/mancesalfarizi/owid-covid-data.csv' OVERWRITE INTO TABLE datacovid2020");
        Dataset<Row> allCovid = spark.sql("SELECT * FROM ddatacovidefault.2019");
        App hive = new App();
        hive.hasilRataRataPenambahanKasus(allCovid);
        hive.penambahanKasusTiapNegara(allCovid);
        spark.sql("select * from userdb.rerata_penambahan_kasus limit 20").show();
        spark.sql("select * from userdb.penambahan_kasus_tiap_negara limit 20").show();
    }
    
    private void hasilRataRataPenambahanKasus(Dataset<Row> data) {
        Dataset<Row> res = data.select("location", "new_cases").groupBy("location").avg("new_cases")
                .withColumnRenamed("avg(new_cases)", "rerata");

        res.write().mode("overwrite").format("orc").saveAsTable("userdb.rerata_penambahan_kasus");
    }
    private void penambahanKasusTiapNegara(Dataset<Row> data) {
        Dataset<Row> res = data.select("location", "date", "new_cases", "total_deaths", "new_deaths")
                .orderBy(desc("new_cases"), asc("date"))
                .orderBy(asc("location"))
                .where("new_cases is not null")
                .dropDuplicates("location");                
        res.write().mode("overwrite").format("orc").saveAsTable("userdb.penambahan_kasus_tiap_negara");        
    }

}
