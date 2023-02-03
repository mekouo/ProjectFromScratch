package centres;

import centres.beans.CentreVaccination;
import centres.reader.CentreVaccinationReader;
import centres.writer.CsvWriter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import java.io.IOException;

@Slf4j
public class HadoopMain {
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

        CsvWriter writer = new CsvWriter(outputPathStr);


        Dataset<CentreVaccination> prixDataset = reader.get().cache();
        prixDataset.printSchema();
        prixDataset.show(5, false);

        log.info("prix = {}", prixDataset.count());

        writer.accept(prixDataset);

        log.info("done");

    }
}
