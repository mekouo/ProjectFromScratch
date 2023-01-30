package centres.functions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import centres.beans.CentreVaccination;

import java.util.function.Function;

public class CentreStatFunction  implements Function<Dataset<Row>, Dataset<CentreVaccination> >{
    @Override
    public Dataset<CentreVaccination> apply(Dataset<Row> rowDataset) {
        Dataset<CentreVaccination> cleanDs = new CentreMapper().apply(rowDataset) ;
        cleanDs.printSchema();
        cleanDs.show(5, false);
        return cleanDs;
    }
}
