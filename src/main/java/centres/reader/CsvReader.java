package centres.reader;

import lombok.RequiredArgsConstructor;
import org.apache.hadoop.fs.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Supplier;

@RequiredArgsConstructor
public class CsvReader implements Supplier<Dataset<Row>>{


        private final SparkSession sparkSession;
        private final String inputPath;
        private final FileSystem hdfs;


        @Override
        public Dataset<Row> get() {
            Dataset<Row> ds = null;
            try {
                Path pathinput = new Path(inputPath);
                if (hdfs.exists(pathinput)) {
                    FileStatus[] listfiles = hdfs.listStatus(pathinput);

                    String[] inputPaths = Arrays.stream(listfiles).filter(t -> !t.isDirectory())
                            .map(f -> f.getPath().toString())
                            .toArray(String[]::new);


                    ds = sparkSession.read()
                            .option("delimiter", ";")
                            .option("header", "true").csv(inputPaths);
                    ds.show(5, false);
                    return ds;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return  sparkSession.emptyDataFrame();
        }

    }

