package centres.functions;

import centres.beans.CentreVaccination;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.function.Function;

public class RowToCentreGroup implements Function<Row, CentreVaccination>, Serializable {

    @Override
    public CentreVaccination apply(Row r) {

        String COM_CP= r.getAs("COM_CP");
        String NOM = r.getAs("NOM");



    return CentreVaccination.builder()
            .nom(NOM)
            .com_cp(COM_CP)
            .build();
    }
}
