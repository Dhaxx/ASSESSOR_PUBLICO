package compras

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