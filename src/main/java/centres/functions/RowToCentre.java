package centres.functions;

import centres.beans.CentreVaccination;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.function.Function;

public class RowToCentre implements Function<Row, CentreVaccination>, Serializable {
    @Override
    public CentreVaccination apply(Row row) {

         String GID = row.getAs("GID");
         String NOM = row.getAs("NOM");
         String ADR_VOIE = row.getAs("ADR_VOIE");
         String COM_CP= row.getAs("COM_CP");




        return CentreVaccination.builder()
                .gid(GID)
                .nom(NOM)
                .adr_voie(ADR_VOIE)
                .com_cp(COM_CP)
                .build();

    }


}
