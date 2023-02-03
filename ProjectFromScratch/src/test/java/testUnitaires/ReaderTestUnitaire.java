package testUnitaires;

import centres.reader.CentreVaccinationReader;
import centres.reader.CsvReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


public class ReaderTestUnitaire {
    @Test
    public void readerTest() throws IOException {
        SparkSession spark = SparkSession.builder()
                .appName("fromScratch")
                .master("local[2]")
                .getOrCreate();

        FileSystem hdfs = FileSystem.get(spark.sparkContext().hadoopConfiguration()) ;

        String inpath = "src/test/resources/data/input/centres-vaccination.csv";

        CsvReader reader = new CsvReader(spark,inpath,hdfs);

        Dataset<Row> dataset = reader.get();
        assertNotNull(dataset);
        assertEquals(9763, dataset.count());


    }
}
