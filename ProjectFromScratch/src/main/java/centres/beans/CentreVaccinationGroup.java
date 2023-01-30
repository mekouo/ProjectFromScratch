package centres.beans;


import lombok.*;

@AllArgsConstructor
@RequiredArgsConstructor
@Getter
@Builder
@Data

public class CentreVaccinationGroup {

    private String nom;
    private String com_cp;
}
