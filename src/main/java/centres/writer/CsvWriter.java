package centres.writer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import java.util.function.Consumer;

@Slf4j
@RequiredArgsConstructor
public class CsvWriter <T> implements Consumer<Dataset<T>>{

        private final String outputPathStr;

        @Override
        public void accept(Dataset<T> tDataset) {

            log.info("writing data into {} ...", outputPathStr);
            tDataset
                    //.repartition(2)
                    .write()
                    .mode("overwrite")
                    .json(outputPathStr);
        }
    }