package compras

import (
	conexao "ASSESSOR_PUBLICO/CONEXAO"
	"fmt"

	"github.com/gobuffalo/nulls"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

func Cadped(p *mpb.Progress) {
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
			'fm00000/')|| autforano%2000 obs,
			to_char(autfornumero,
			'fm00000/')|| autforano%2000 numpedant,
			min(codccusto) codccusto
		from
			autorizacaofornecimento a
		join icadorc b on a.autforforprocessoid = b.pedidocompraforprocessoid 
		where
			autforugid = $1
		group by autforid, autforano, autfornumero, autfordataemissao, autforfornecedorid, entrou, autforugid, autforforprocessoid`, GetEmpresa())
	if err != nil {
		panic("Erro ao consultar dados: " + err.Error())
	}

	var count int
	err = cnx_pg.QueryRow(`select count(*) from () as rn`).Scan(&count)
	if err != nil {
		panic(`Erro ao contar registros` + err.Error())
	}
	bar15 := p.AddBar(int64(count),
		mpb.PrependDecorators(
			decor.Name("Cadped - "),
			decor.CountersNoUnit("%d/%d", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(
			decor.Percentage(decor.WCSyncSpace),
		),
	)

	// Prepara Insert
	insert, err := cnx_fdb.Prepare(`insert into cadped (numped, num, ano, datped, codif, entrou, id_cadped, empresa, numlic, obs, numpedant, codccusto) values (?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		panic("Erro ao inserir dados: " + err.Error())
	}

	for rows.Next() {
		var numped, num, ano, datped, entrou, numpedant, obs nulls.String
		var codif, id_cadped, empresa, numlic, codccusto nulls.Int
		err = rows.Scan(&numped, &num, &ano, &datped, &codif, &entrou, &id_cadped, &empresa, &numlic, &obs, &numpedant, &codccusto)
		if err != nil {
			panic("Erro ao ler dados: " + err.Error())
		}

		_, err = insert.Exec(numped, num, ano, datped, codif, entrou, id_cadped, empresa, numlic, obs, numpedant, codccusto)
		if err != nil {
			panic("Erro ao inserir dados: " + err.Error())
		}
		bar15.Increment()
	}
}

func Icadped(p *mpb.Progress) {
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

	// Query
	rows, err := cnx_pg.Query(`select
			to_char(a.autforid,
			'fm00000/')|| autforano%2000 numped,
			autforitemid,
			c.itemcompramaterialid,
			a.autforitemqtdeaut,
			a.autforitemvalorunitario,
			a.autforitemqtdeaut * a.autforitemvalorunitario total,
			a.autforid,
			min(d.codccusto) codccusto
		from
			autorizacaofornecimentoitem a
		join autorizacaofornecimento b on
			a.autforid = b.autforid
		join itemcompra c on
			a.autforitemcompraid = c.itemcompraid
		join icadorc d on d.pedidocompraforprocessoid = autforforprocessoid and d.codreduz = c.itemcompramaterialid
		--where a.autforid in (1457, 11864, 12251, 12823, 14903, 18231, 18762, 18999, 19000, 19615, 20127)
		group by a.autforid, b.autforano, autforitemid, c.itemcompramaterialid, a.autforitemqtdetotal, a.autforitemvalorunitario, a.autforid`)
	if err != nil {
		panic("Erro ao consultar dados: " + err.Error())
	}

	var count int
	err = cnx_pg.QueryRow(`select count(*) from () as rn`).Scan(&count)
	if err != nil {
		panic(`Erro ao contar registros` + err.Error())
	}
	bar16 := p.AddBar(int64(count),
		mpb.PrependDecorators(
			decor.Name("Icadped - "),
			decor.CountersNoUnit("%d/%d", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(
			decor.Percentage(decor.WCSyncSpace),
		),
	)

	// Consulta Auxiliar
	cadpros := make(map[int]string)
	aux1, err := cnx_fdb.Query(`select cadpro, codreduz from cadest`)
	if err != nil {
		panic("Erro ao consultar cadpro" + err.Error())
	}
	for aux1.Next() {
		var cadpro string
		var codreduz int
		err = aux1.Scan(&cadpro, &codreduz)
		if err != nil {
			panic("Erro ao scannear cadpro" + err.Error())
		}
		cadpros[codreduz] = cadpro
	}

	// Prepara Insert
	insert, err := cnx_fdb.Prepare(`insert into icadped (numped, item, cadpro, qtd, prcunt, prctot, codccusto, id_cadped) values (?,?,?,?,?,?,?,?)`)
	if err != nil {
		panic("Erro ao inserir dados: " + err.Error())
	}

	for rows.Next() {
		var numped nulls.String
		var item, codccusto, id_cadped, codreduz nulls.Int
		var qtd, prcunt, prctot nulls.Float64
		var cadpro string
		err = rows.Scan(&numped, &item, &codreduz, &qtd, &prcunt, &prctot, &id_cadped, &codccusto)
		if err != nil {
			panic("Erro ao ler dados: " + err.Error())
		}

		cadpro = cadpros[codreduz.Int]

		_, err = insert.Exec(numped, item, cadpro, qtd, prcunt, prctot, codccusto, id_cadped)
		if err != nil {
			panic("Erro ao inserir dados: " + err.Error())
		}
		bar16.Increment()
	}
}
