package centres;

import centres.beans.CentreVaccination;
import centres.receiver.KafkaReceiver;
import centres.writer.CsvWriter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.io.IOException;
import java.util.HashMap;import java.util.List;
import java.util.Map;import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public class KafkaMain {

    public static void main(String[] args) throws IOException, InterruptedException {

        log.info("Using kafka........");

        Config config = ConfigFactory.load("application.conf");
        String masterUrl = config.getString("app.master");
        String appName = config.getString("app.name");

        String inputPathStr = config.getString("app.path.input");
        String outPathStr = config.getString("app.path.output");

        String checkpointStr = config.getString("app.path.checkpoint");

        List<String> topics = null;

        SparkSession sparkSession = SparkSession
                .builder()
                .master(masterUrl)
                .appName(appName)
                .getOrCreate();

        FileSystem hdfs = FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());
        log.info("fileSystem got from sparkSession in the main : hdfs.getScheme = {}", hdfs.getScheme());

        final Function<String, CentreVaccination> mapper = null;
//

        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(
                checkpointStr,
                () -> {
                    JavaStreamingContext javaStreamingContext = new JavaStreamingContext(
                            JavaSparkContext.fromSparkContext(sparkSession.sparkContext()),
                            new Duration(1000 * 10)
                    );
                    javaStreamingContext.checkpoint(checkpointStr);
//
                    KafkaReceiver kafkaReceiver = new KafkaReceiver(topics,javaStreamingContext);

                    JavaDStream<CentreVaccination> prixJavaDStream = kafkaReceiver.get();

                    prixJavaDStream.foreachRDD(
                            prixJavaRDD -> {
                                log.info("batch at {}", System.currentTimeMillis());
                                Dataset<CentreVaccination> centreVaccinationDataset = SparkSession.active().createDataset(
                                        prixJavaRDD.rdd(),
                                        Encoders.bean(CentreVaccination.class)
                                ).cache();

                                centreVaccinationDataset.printSchema();
                                centreVaccinationDataset.show(10, false);
                                log.info("le nbre est {}", centreVaccinationDataset.count());

                                CsvWriter writer = new CsvWriter(outPathStr);
                                writer.accept(centreVaccinationDataset);
                                centreVaccinationDataset.unpersist();
                                log.info("done..........");

//

                            }
                    );


                    return javaStreamingContext;
                },
                sparkSession.sparkContext().hadoopConfiguration()

        );
//
        jsc.start();
        jsc.awaitTermination();

    }


}
