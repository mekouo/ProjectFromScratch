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
    private String arrete_pref_numero;
    private String xy_precis;
    private String id_adr;
    private String adr_num;
    private String adr_voie;
    private String com_cp;
    private String com_insee;
    private String com_nom;
    private String lat_coor1;


}