package centres.functions;

import centres.beans.CentreVaccination;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.util.function.Function;

public class CentreMapperGroup implements Function<Dataset<Row>, Dataset<CentreVaccination>> {
    private final RowToCentreGroup  mp = new RowToCentreGroup();
    private final MapFunction<Row, CentreVaccination> task = mp::apply;
    //private final MapFunction<Row, Prix> task = new RowToPrix();

    @Override
    public Dataset<CentreVaccination> apply(Dataset<Row> inputDS) {
        return inputDS.map(task , Encoders.bean(CentreVaccination.class)) ;
    }
}
