package centres.functions.parser;

import centres.beans.CentreVaccination;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.MapFunction;

@Slf4j
public class RowToCentreVaccinationSparkFunc implements MapFunction<String, CentreVaccination> {


        @Override
        public CentreVaccination call(String line) {
            log.info("current line = {}", line);

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

