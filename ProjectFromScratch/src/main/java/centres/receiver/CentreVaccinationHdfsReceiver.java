package centres.receiver;

import centres.beans.CentreVaccination;
import centres.functions.parser.TextToCentreVaccinationFunc;
import centres.types.CentreVaccinationFileInputFormat;
import centres.types.CentreVaccinationLongWritable;
import centres.types.CentreVaccinationText;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public class CentreVaccinationHdfsReceiver implements Supplier<JavaDStream<CentreVaccination>>  {

        private final JavaStreamingContext javaStreamingContext;
        private final String inputPathStr;

        private final TextToCentreVaccinationFunc textToActeDecesFunc = new TextToCentreVaccinationFunc();
        private final Function<String, CentreVaccination> mapper = textToActeDecesFunc::apply;
        private final Function<Path, Boolean> filter = p -> p.getName().endsWith(".txt");

        @Override
        public JavaDStream<CentreVaccination> get() {
            JavaPairInputDStream<CentreVaccinationLongWritable, CentreVaccinationText> inputDStream = javaStreamingContext
                    .fileStream(
                            inputPathStr,
                            CentreVaccinationLongWritable.class,
                            CentreVaccinationText.class,
                            CentreVaccinationFileInputFormat.class,
                            filter,
                            true
                    );
            return inputDStream.map(t -> t._2().toString()).map(mapper);
        }
    }



