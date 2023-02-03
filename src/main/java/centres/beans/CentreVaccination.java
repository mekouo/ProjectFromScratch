package centres.beans;

import java.io.Serializable;

import lombok.*;

@AllArgsConstructor
@RequiredArgsConstructor
@Getter
@Builder
@Data

public class CentreVaccination implements Serializable {

    private String gid;
    private String nom;
    private String adr_voie;
    private String com_cp;



}