/*import centres.functions.CentreStatFunction;
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
public class SparkApp {

    public static void main(String[] args) {

        Config config = ConfigFactory.load("application.conf");
        String inputPathStr = config.getString("3il.path.input");
        String outputPathStr = config.getString("3il.path.output");
        String masterUrl = config.getString("3il.spark.master");

        SparkConf sparkConf = new SparkConf().setMaster(masterUrl).setAppName("SparkApp");

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();



        CentreStatFunction centreStatFunction = new CentreStatFunction();
        CsvReader csvReader = new CsvReader(sparkSession , inputPathStr) ;
        CsvWriter csvWriter = new CsvWriter(outputPathStr) ;
        csvWriter.accept(centreStatFunction.apply(csvReader.get()));




    }
}*/




import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
//import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import scala.Tuple2;

import java.io.IOException;
import java.io.StringWriter;
//import java.nio.file.FileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.commons.io.IOUtils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@Slf4j
public class SparkApp {

    //private static Logger logger = LoggerFactory.getLogger(JavaMain.class);
    private static DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    public static void main(String[] args) throws IOException {
        Config config = ConfigFactory.load("app.properties");
        String appName = config.getString("app.name");
        String appID = formatter.format(LocalDateTime.now()) + "_" + new Random().nextLong();
        FileSystem hdfs = FileSystem.get(new Configuration());

        /**DECLATION DES VARIABLES**/
        Path inputPath = new Path(config.getString("app.data.input"));
        Path outputPath = new Path(config.getString("app.data.output"));


        /**DATA PROCESSING**/
        log.info("Listing files from inputPath={} ...", inputPath);
        RemoteIterator<LocatedFileStatus> fileIterator = hdfs.listFiles(inputPath, true);

        while (fileIterator.hasNext()) {
            LocatedFileStatus locatedFileStatus = fileIterator.next();
            log.info("fileInfo=<getName={}, getLen={}, getBlockSize={}, getPath={}>",
                    locatedFileStatus.getPath().getName(),
                    locatedFileStatus.getLen(),
                    locatedFileStatus.getBlockSize(),
                    locatedFileStatus.getPath().toString());


        }

        /**DATA PROCESSING**/
        log.info("Listing files from inputPath={} ...", inputPath);
        while (fileIterator.hasNext()) {
            LocatedFileStatus locatedFileStatus = fileIterator.next();
            FSDataInputStream instream = hdfs.open(locatedFileStatus.getPath());
            StringWriter writer = new StringWriter();
            IOUtils.copy(instream, writer, "UTF-8");
            Stream<String> lines = Arrays.stream(writer.toString().split("\n")).filter(l -> !l.startsWith("\"")); // remove header
            Map<String, Integer> wordCount = lines.flatMap(l -> Arrays.stream(l.split(";"))).filter(x -> !NumberUtils.isNumber(x)).map(x -> new Tuple2<>(x, 1)).collect(Collectors.toMap(Tuple2::_1, Tuple2::_2, Integer::sum));
            log.info("wordCount");
            wordCount.forEach((k, v) -> log.info("({} -> {})", k, v));
            String outlines = wordCount.entrySet().stream().map(e -> String.format("%s,%d", e.getKey(), e.getValue())).collect(Collectors.joining("\n"));
            FSDataOutputStream outstream = hdfs.create(outputPath.suffix(String.format("/wordcount/%s", locatedFileStatus.getPath().getName())));
            IOUtils.write(outlines,
                    outstream,
                    "UTF-8");
            outstream.close();
            instream.close();

        }


        /**DATA SAVING**/
        log.info("Copying files from inputPath={} to outputPath={}...", inputPath, outputPath);
        FileUtil.copy(
                FileSystem.get(inputPath.toUri(), hdfs.getConf()),
                inputPath,
                FileSystem.get(outputPath.toUri(), hdfs.getConf()),
                outputPath,
                false,
                hdfs.getConf()
        );



    }
}

