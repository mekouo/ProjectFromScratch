package centres.writer;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import centres.beans.CentreVaccination;

import java.util.function.Consumer;

@RequiredArgsConstructor
public class CsvWriter implements Consumer<Dataset<CentreVaccination>> {

    private  final String outputPath ;

    @Override
    public void accept(Dataset<CentreVaccination> centreVaccinationDataset) {
        centreVaccinationDataset.write().csv(outputPath);
    }
}
