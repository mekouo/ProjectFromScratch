package centres.functions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import centres.beans.CentreVaccination;
import centres.functions.RowToCentre;


import java.util.function.Function;


public class CentreMapper implements Function<Dataset<Row>, Dataset<CentreVaccination>> {
    private final RowToCentre  parser = new RowToCentre();
    private final MapFunction<Row, CentreVaccination> task = parser::apply;
    //private final MapFunction<Row, Prix> task = new RowToPrix();

    @Override
    public Dataset<CentreVaccination> apply(Dataset<Row> inputDS) {
        return inputDS.map(task , Encoders.bean(CentreVaccination.class)) ;
    }
}


