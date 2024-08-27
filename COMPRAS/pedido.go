package compras

import (
	"ASSESSOR_PUBLICO/CONEXAO"
	"fmt"
	"time"

	"github.com/gobuffalo/nulls"
)

func Cadped() {
	start := time.Now()

	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		fmt.Println(err)
	}
	defer cnx_fdb.Close()

	cnx_pg, err := conexao.ConexaoOrigem()
	if err != nil {
		fmt.Println(err)
	}
	defer cnx_pg.Close()

	// Limpando
	cnx_fdb.Exec("DELETE from ICADPED")
	cnx_fdb.Exec("DELETE from CADPED")

	// Query 
	rows, err := cnx_pg.Query(`select
			to_char(autforid,
			'fm00000/')|| autforano%2000 numped,
			to_char(autfornumero,
			'fm00000') num,
			autforano,
			cast(autfordataemissao as varchar) data,
			autforfornecedorid codif,
			'N' entrou,
			autforid id_cadped,
			autforugid empresa,
			autforforprocessoid numlic,
			'AF - '||to_char(autfornumero,
			'fm00000/')|| autforano%2000,
			to_char(autfornumero,
			'fm00000/')|| autforano%2000 numpedant
		from
			autorizacaofornecimento a
		where
			autforugid = $1`, GetEmpresa())
	if err != nil {
		panic("Erro ao consultar dados: " + err.Error())
	}

	// Prepara Insert
	insert, err := cnx_fdb.Prepare(`insert into cadped (numped, num, ano, datped, codif, entrou, id_cadped, empresa, numlic, obs, numpedant) values (?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		panic("Erro ao inserir dados: " + err.Error())
	}

	for rows.Next() {
		var numped, num, ano, datped, entrou, numpedant, obs nulls.String
		var codif, id_cadped, empresa, numlic nulls.Int
		err = rows.Scan(&numped, &num, &ano, &datped, &codif, &entrou, &id_cadped, &empresa, &numlic, &obs, &numpedant)
		if err != nil {
			panic("Erro ao ler dados: " + err.Error())
		}

		_, err = insert.Exec(numped, num, ano, datped, codif, entrou, id_cadped, empresa, numlic, obs, numpedant)
		if err != nil {
			panic("Erro ao inserir dados: " + err.Error())
		}
	}
	fmt.Println("Cadped - Finalizado em: ", time.Since(start))
}