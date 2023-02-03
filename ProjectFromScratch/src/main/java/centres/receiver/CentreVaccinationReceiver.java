package centres.receiver;

import centres.beans.CentreVaccination;
import centres.functions.parser.TextToCentreVaccinationFunc;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.spark.api.java.function.Function;
import java.util.function.Supplier;


@AllArgsConstructor
@Builder
@Slf4j
@RequiredArgsConstructor
public class CentreVaccinationReceiver implements Supplier<JavaDStream<CentreVaccination>> {


    private final String hdfsInputPathStr;
    private final JavaStreamingContext jsc;

    private final TextToCentreVaccinationFunc textToActeDecesFunc = new TextToCentreVaccinationFunc();
    private final Function<String, CentreVaccination> mapper = textToActeDecesFunc::apply;
    private final Function<Path, Boolean> filter = p -> p.getName().endsWith(".csv");


    @Override
    public JavaDStream<CentreVaccination> get() {
        JavaPairInputDStream<LongWritable, Text> inputDStream = jsc
                .fileStream(
                        hdfsInputPathStr,
                        LongWritable.class,
                        Text.class,
                        TextInputFormat.class,
                        filter,
                        true
                );
        return inputDStream.map(t -> t._2().toString()).map(mapper);
    }

    }

