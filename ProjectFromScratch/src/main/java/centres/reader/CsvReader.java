package centres.reader;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import centres.beans.CentreVaccination;

import java.util.function.Supplier;

@RequiredArgsConstructor

public class CsvReader  implements Supplier<Dataset<Row>> {

    private  final SparkSession sparkSession ;
    private  final String inputPathStr ;


    @Override
    public Dataset<Row> get() {


        /*Dataset<Row> ds = sparkSession.read().option("delimiter" , ";")*/
        Dataset<Row> inputDS = sparkSession.read().option("delimiter" , ";")
                .option("header" , "true")
                .csv(inputPathStr);

        inputDS.show(5, false);
        return inputDS;


    }
}
