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
        String ARRETE_PREFECTURE_NUMERO = row.getAs("ARRETE_PREFECTURE_NUMERO");
        String XY_PRECIS = row.getAs("XY_PRECIS");
        String ID_ADR = row.getAs("ID_ADR");
         String ADR_NUM = row.getAs("ADR_NUM");
         String ADR_VOIE = row.getAs("ADR_VOIE");
         String COM_CP= row.getAs("COM_CP");
        String COM_INSEE= row.getAs("COM_INSEE");
        String COM_NOM= row.getAs("COM_NOM");
         String LAT_COOR1 = row.getAs("LAT_COOR1");



        return CentreVaccination.builder()
                .gid(GID)
                .nom(NOM)
                .arrete_pref_numero(ARRETE_PREFECTURE_NUMERO)
                .xy_precis(XY_PRECIS)
                .id_adr(ID_ADR)
                .adr_num(ADR_NUM)
                .adr_voie(ADR_VOIE)
                .com_cp(COM_CP)
                .com_insee(COM_INSEE)
                .com_nom(COM_NOM)
                .lat_coor1(LAT_COOR1)
                .build();

    }


}
