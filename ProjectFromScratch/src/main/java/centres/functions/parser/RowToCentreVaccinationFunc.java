package centres.functions.parser;

import centres.beans.CentreVaccination;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Row;
import java.io.Serializable;
import java.util.function.Function;

public class RowToCentreVaccinationFunc implements Function<Row, CentreVaccination>, Serializable {


        private final TextToCentreVaccinationFunc textToActeDecesFunc = new TextToCentreVaccinationFunc();
        @Override
        public CentreVaccination apply(Row row) {
            String line = row.getAs("value") ;
            return textToActeDecesFunc.apply(line);
        }
    }

