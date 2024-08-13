package compras

import (
	"ASSESSOR_PUBLICO/conexao"
	"strconv"
	"fmt"
)

func EstourouSubgr(codigo int, subgrupo string, grupo string) []string {
	// Cria Conexão com os bancos
	cnx_aux, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnx_aux.Close()

	var subgrupoNovo string
	var codigoStr string

	if codigo > 999 && codigo < 10000 {
		codigoStr = strconv.Itoa(codigo)
		subgrupoNovo = codigoStr[:1]
		subgrupoNovo = `0`+subgrupoNovo+string(subgrupo[2])
		codigoStr = codigoStr[1:]
	} else if codigo > 9999 {
		codigoStr = strconv.Itoa(codigo)
		subgrupoNovo = codigoStr[:2]
		subgrupoNovo = subgrupoNovo+string(subgrupo[2])
		codigoStr = codigoStr[2:]
	} else {
		codigoStr = zfill(strconv.Itoa(codigo),3)
		return []string{subgrupo, codigoStr}		
	}

	_, err = cnx_aux.Exec(`INSERT INTO CADSUBGR (GRUPO, SUBGRUPO, NOME, OCULTAR) select grupo, ?, nome, 'N' from cadsubgr where grupo = ? and subgrupo = ?`, subgrupoNovo, grupo, subgrupo)
	if err != nil {
		panic("Falha ao inserir dados: " + err.Error())
	}
	cnx_aux.Close()

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