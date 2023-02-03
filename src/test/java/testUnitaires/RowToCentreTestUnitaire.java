package testUnitaires;

import centres.beans.CentreVaccination;
import centres.functions.parser.RowToCentreVaccinationSparkFunc;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RowToCentreTestUnitaire {
    @Test
    public void testCall() {
        RowToCentreVaccinationSparkFunc f = new RowToCentreVaccinationSparkFunc();
        String line = "2727*Centre de vaccination - Parc d'activité Saint-Ouen-l’Aumône/                                                                   Avenue de l’Eguillette                                                                95310";

        CentreVaccination expected = CentreVaccination.builder()
                .gid("2727").nom("Centre de vaccination - Parc d'activité Saint-Ouen-l’Aumône").adr_voie("venue de l’Eguillette").com_cp("95310")
                .build();

        CentreVaccination actual = f.call(line);

        assertThat(actual).isEqualTo(expected);
    }
}
