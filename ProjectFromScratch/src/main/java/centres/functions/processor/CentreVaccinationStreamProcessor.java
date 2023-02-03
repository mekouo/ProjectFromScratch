package centres.functions.processor;

import centres.writer.CsvWriter;
import centres.beans.CentreVaccination;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;


    @Slf4j
    @RequiredArgsConstructor
    public class CentreVaccinationStreamProcessor implements VoidFunction<JavaRDD<CentreVaccination>> {
        private final String outputPathStr;


        @Override
        public void call(JavaRDD<CentreVaccination> centreVaccinationJavaRDD) throws Exception {
            long ts = System.currentTimeMillis();
            log.info("micro-batch stored in folder={}", ts);

            if (centreVaccinationJavaRDD.isEmpty()) {
                log.info("no data found!");
                return;
            }

            log.info("data under processing...");
            final SparkSession sparkSession = SparkSession.active();


            Dataset<CentreVaccination> centreVaccinationDataset = sparkSession.createDataset(
                   centreVaccinationJavaRDD.rdd(),
                    Encoders.bean(CentreVaccination.class)
            );


           centreVaccinationDataset.printSchema();
            centreVaccinationDataset.show(5, false);

            log.info("nb centrevaccination = {}", centreVaccinationDataset.count());


            CsvWriter<CentreVaccination> writer = new CsvWriter<>(outputPathStr + "/time=" + ts);
            writer.accept(centreVaccinationDataset);

            centreVaccinationDataset.unpersist();
            log.info("done");
        }
    }


