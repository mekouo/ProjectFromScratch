package test.unitaires;


import org.apache.spark.sql.Row;

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import org.junit.Test;
import centres.beans.CentreVaccination;
import centres.functions.RowToCentre;

import static org.assertj.core.api.Assertions.assertThat;

public class RowToCentreTestUnitaire {

   @Test
    public void testRowToCentreFunc() {

        RowToCentre f = new RowToCentre();
        StructType sh = new StructType(
                new StructField[]{
                        new StructField(
                                "GID",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()

                        ),

                        new StructField(
                                "NOM",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()

                        ),

                        new StructField(
                                "ARRETE_PREFECTURE_NUMERO",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()
                                ),


                        new StructField(
                                "XY_PRECIS",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()

                        ),

                        new StructField(
                                "ID_ADR",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()

                        ),

                        new StructField(
                                "ADR_NUM",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()

                        ),

                        new StructField(
                                "ADR_VOIE",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()

                        ),

                        new StructField(
                                "COM_CP",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()

                        ),

                        new StructField(
                                "COM_INSEE",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()

                        ),

                        new StructField(
                                "COM_NOM",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()

                        ),

                        new StructField(
                                "LAT_COOR1",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()

                        ),



                }
        );

        String values[] = {"2727", "Centre de vaccination - Parc d'activité Saint-Ouen-l’Aumône", " ","95572_0225_00019","19","Avenue de l’Eguillette","95310","95572","Saint-Ouen-l'Aumône","49.0532"} ;
        Row row = new GenericRowWithSchema(values , sh);
        CentreVaccination expected = CentreVaccination.builder()
                .gid("2727")
                .nom("Centre de vaccination - Parc d'activité Saint-Ouen-l’Aumône")
                .arrete_pref_numero(" ")
                .xy_precis("1")
                .id_adr("95572_0225_00019")
                .adr_num("19")
                .adr_voie("Avenue de l’Eguillette")
                .com_cp("95310")
                .com_insee("95572")
                .com_nom("Saint-Ouen-l'Aumône")
                .lat_coor1("49.0532")
                .build();

        CentreVaccination actual = f.apply(row);


        assertThat(actual).isEqualTo(expected);

    }
}



