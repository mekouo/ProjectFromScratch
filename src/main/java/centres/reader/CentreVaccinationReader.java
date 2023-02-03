package centres.reader;

import centres.functions.parser.CentreMapper;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import centres.beans.CentreVaccination;
import java.io.IOException;
import java.util.function.Supplier;

@Builder
@Slf4j
public class CentreVaccinationReader implements Supplier<Dataset<CentreVaccination>> {

    private SparkSession sparkSession;
    private FileSystem hdfs;
    private String inputPathStr;
    private final CentreMapper centreMapper = new CentreMapper();


    @Override
    public Dataset<CentreVaccination> get() {
        try {
            if (hdfs.exists(new Path(inputPathStr))) {
                Dataset<String> rowDataset = sparkSession.read().textFile(inputPathStr);
                rowDataset.printSchema();
                rowDataset.show(5, false);


                return centreMapper.apply(rowDataset);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return sparkSession.emptyDataset(Encoders.bean(CentreVaccination.class));
    }
}

