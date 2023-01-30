package centres.functions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.function.Function;

import static org.apache.spark.sql.functions.count;

/*public class CentreStatFuncGroup  implements Function<Dataset<Row>, Dataset<Row>> {
    @Override
    public Dataset<Row> apply(Dataset<Row> rowDataset) {
        Object inputPathStr;
        Dataset<Row> inputDS = sparkSession.read().option("delimiter" , ";")
                .option("header" , "true")
                .csv(inputPathStr);
        Dataset<Row> statds = ds.groupBy("com_cp").agg(count("nom").as("Lolo"));
        statds.show();
        statds.printSchema();
        statds.show(5, false);
        return statds;
    }
    }*/