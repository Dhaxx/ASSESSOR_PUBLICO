package utils

import (
	"ASSESSOR_PUBLICO/CONEXAO"
	"database/sql"
	"fmt"
	"strconv"
)

func EstourouSubgr(codigo int, subgrupo string, grupo string, con *sql.DB) []string {
	var subgrupoNovo string
	var codigoStr string

	if codigo < 1000 {
		codigoStr = zfill(strconv.Itoa(codigo),3)
		return []string{subgrupo, codigoStr}		
	} else if codigo < 10000 {
		codigoStr = strconv.Itoa(codigo)
		subgrupoNovo = codigoStr[:1]
		subgrupoNovo = `9`+subgrupoNovo+string(subgrupo[2])
		codigoStr = codigoStr[1:]
	} else if codigo >= 10000 {
		codigoStr = strconv.Itoa(codigo)
		subgrupoNovo = codigoStr[:2]
		subgrupoNovo = subgrupoNovo+string(subgrupo[2])
		codigoStr = codigoStr[2:]
	}

	tx, err := con.Begin()
	if err != nil {
		_ = err.Error()
	}

	_, err = tx.Exec(`INSERT INTO CADSUBGR (GRUPO, SUBGRUPO, NOME, OCULTAR) select grupo, ?, nome, 'N' from cadsubgr where grupo = ? and subgrupo = ?`, subgrupoNovo, grupo, subgrupo)
	if err != nil {
		_ = err.Error()
	}

	// Comita a transação
	if err := tx.Commit(); err != nil {
		panic("Falha ao comitar transação em EstourouSubgr: " + err.Error())
	}

	return []string{subgrupoNovo, codigoStr}
}

func zfill(s string, length int) string {
    return fmt.Sprintf("%0*s", length, s)
}

func GetEmpresa() int {
	cnx_aux, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnx_aux.Close()

	var empresa int

	err = cnx_aux.QueryRow(`select empresa from cadcli`).Scan(&empresa)
	if err != nil {
		panic("Falha ao executar select: " + err.Error())
	}

	return empresa
}

func CriaFornConversao() {
	cnx_aux, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnx_aux.Close()

	_, err = cnx_aux.Exec(`insert into desfor (codif, nome) select max(codif)+1, 'CONVERSÃO' from DESFOR`)
	if err != nil {
		panic("Falha ao executar insert: " + err.Error())
	}
}

func Contains(slice []int, value int) bool {
    for _, v := range slice {
        if v == value {
            return true
        }
    }
    return false
}

func AtualizaCadpat() {
	cnx_aux, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}

	_, err = cnx_aux.Exec(`EXECUTE BLOCK AS
		DECLARE VARIABLE codigo_pat_mov integer;
		DECLARE VARIABLE codigo_cpl_ant_mov integer;
		DECLARE VARIABLE unid_ant integer;
		DECLARE VARIABLE subunid_ant integer;
		DECLARE VARIABLE codigo_set_mov integer;
		BEGIN
			FOR
				SELECT
					codigo_pat_mov,
					codigo_cpl_ant_mov,
					UNID_ANT,
					SUBUNID_ANT
				FROM
					(
					SELECT
						codigo_pat_mov,
						codigo_cpl_ant_mov,
						UNID_ANT,
						SUBUNID_ANT,
						ROW_NUMBER() OVER (PARTITION BY codigo_pat_mov
					ORDER BY
						codigo_mov) AS rn
					FROM
						pt_movbem
					WHERE
						tipo_mov IN ('T', 'P')
				) AS Movimentacoes
				INTO :codigo_pat_mov, :codigo_cpl_ant_mov, :unid_ant, :subunid_ant
			DO
				BEGIN
					SELECT COALESCE(codigo_set,0) FROM pt_cadpats WHERE codigo_des_set = :unid_ant AND subunid_ant = :subunid_ant INTO :codigo_set_mov;
				
					UPDATE pt_movbem SET CODIGO_SET_MOV = :CODIGO_SET_MOV, CODIGO_CPL_MOV = :codigo_cpl_ant_mov WHERE CODIGO_PAT_MOV = :codigo_pat_mov AND TIPO_MOV = 'A';
				END;
		END`)
	if err != nil {
		panic("Falha ao executar update: " + err.Error())
	}

	_, err = cnx_aux.Exec(`MERGE into pt_cadpat destino USING (SELECT codigo_cpl_mov, codigo_set_mov, codigo_pat_mov FROM PT_MOVBEM WHERE tipo_mov = 'A') origem 
		ON (destino.codigo_pat = origem.codigo_pat_mov)
		WHEN MATCHED THEN 
			UPDATE SET destino.codigo_set_pat = origem.codigo_set_mov, 
			destino.codigo_cpl_pat = origem.codigo_cpl_mov `)
	if err != nil {
		panic("Falha ao executar merge: " + err.Error())
	}

	_, err = cnx_aux.Exec(`MERGE INTO PT_CADPAT d USING (SELECT codigo_pat_mov, data_mov, codigo_bai_mov FROM PT_MOVBEM WHERE tipo_mov = 'B') o
		ON (d.codigo_pat = o.codigo_pat_mov)
		WHEN MATCHED THEN 
			UPDATE SET d.dtpag_pat = o.data_mov, d.codigo_bai_pat = o.codigo_bai_mov`)
	if err != nil {
		panic("Falha ao executar merge: " + err.Error())
	}

	_, err = cnx_aux.Exec(`MERGE INTO PT_CADPAT d USING (SELECT codigo_pat_mov, data_mov, valor_mov FROM PT_MOVBEM WHERE tipo_mov = 'R' and depreciacao_mov = 'N') o
		ON (d.codigo_pat = o.codigo_pat_mov)
		WHEN MATCHED THEN 
			UPDATE SET d.dtlan_pat = o.data_mov--, d.valatu_pat = o.valor_mov`)
	if err != nil {
		panic("Falha ao executar merge: " + err.Error())
	}

	_, err = cnx_aux.Exec(`MERGE INTO PT_CADPAT d USING (SELECT codigo_pat_mov, sum(valor_mov) valor_mov FROM PT_MOVBEM GROUP BY 1) o
		ON (d.codigo_pat = o.codigo_pat_mov)
		WHEN MATCHED THEN 
			UPDATE SET d.valatu_pat = o.valor_mov`)
	if err != nil {
		panic("Falha ao executar merge: " + err.Error())
	}
	cnx_aux.Close()
}