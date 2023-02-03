package centres;/*import centres.functions.CentreStatFunction;
import centres.reader.CsvReader;
import centres.writer.CsvWriter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.* ;

import static org.apache.spark.sql.functions.count;

@Slf4j
public class centres.SparkApp {

    public static void main(String[] args) {

        Config config = ConfigFactory.load("application.conf");
        String inputPathStr = config.getString("3il.path.input");
        String outputPathStr = config.getString("3il.path.output");
        String masterUrl = config.getString("3il.spark.master");

        SparkConf sparkConf = new SparkConf().setMaster(masterUrl).setAppName("centres.SparkApp");

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();



        CentreStatFunction centreStatFunction = new CentreStatFunction();
        CsvReader csvReader = new CsvReader(sparkSession , inputPathStr) ;
        CsvWriter csvWriter = new CsvWriter(outputPathStr) ;
        csvWriter.accept(centreStatFunction.apply(csvReader.get()));




    }
}*/




import centres.beans.CentreVaccination;
import centres.reader.CentreVaccinationReader;
import centres.writer.CsvWriter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;


@Slf4j
public class SparkApp {

    public static void main(String[] args) throws InterruptedException, IOException {
        log.info("Hello world!");


        Config config = ConfigFactory.load("application.conf");
        String masterUrl = config.getString("app.master");
        String appName = config.getString("app.name");

        String inputPathStr = config.getString("app.path.input");
        String outputPathStr = config.getString("app.path.output");

        SparkSession sparkSession = SparkSession.builder()
                .master(masterUrl)
                .appName(appName)
                .getOrCreate();

        FileSystem hdfs = FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());
        log.info("fileSystem got from sparkSession in the main : hdfs.getScheme = {}", hdfs.getScheme());


        CentreVaccinationReader reader = CentreVaccinationReader.builder()
                .sparkSession(sparkSession)
                .hdfs(hdfs)
                .inputPathStr(inputPathStr)
                .build();

        CsvWriter<CentreVaccination> writer = new CsvWriter<>(outputPathStr);


        Dataset<CentreVaccination> centrevaccinationDataset = reader.get().cache();
        centrevaccinationDataset.printSchema();
        centrevaccinationDataset.show(5, false);

        log.info("nb centre vaccination = {}", centrevaccinationDataset.count());

        writer.accept(centrevaccinationDataset);

        log.info("done");
        //Thread.sleep(1000 * 60 * 10);

    }
}