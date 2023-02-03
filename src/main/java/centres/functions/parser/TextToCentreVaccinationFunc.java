package centres.functions.parser;

import centres.beans.CentreVaccination;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.function.Function;

public class TextToCentreVaccinationFunc implements Function<String, CentreVaccination>, Serializable {
    @Override
    public CentreVaccination apply(String line) {
        String[] nomGid = StringUtils.splitByWholeSeparatorPreserveAllTokens(line.substring(0, 80), "*", 2);

        String gid = nomGid[0].trim();
        String nom = nomGid[1].trim().replace("/", "");

        return CentreVaccination.builder()
                .gid(gid)
                .nom(nom)
                .adr_voie((line.substring(80, 81)))
                .com_cp((line.substring(80, 81)))
                .build();

    }
}
