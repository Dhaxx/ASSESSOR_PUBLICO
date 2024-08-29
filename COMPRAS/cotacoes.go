package compras

import (
	conexao "ASSESSOR_PUBLICO/CONEXAO"

	"github.com/gobuffalo/nulls"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

func Cadorc(p *mpb.Progress) {
	// Cria Conexão com os bancos
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnx_fdb.Close()

	cnx_pg, err := conexao.ConexaoOrigem()
	if err != nil {
		panic("Falha ao conectar com o banco de origem: " + err.Error())
	}
	defer cnx_pg.Close()

	// Cria Campo
	cnx_fdb.Exec("alter table cadorc add numorc_ant varchar(10)")
	cnx_fdb.Exec("alter table cadorc add flg_cotacao varchar(1)")
	cnx_fdb.Exec("alter table cadorc add id_ant integer")

	// Limpa tabelas
	cnx_fdb.Exec("delete from icadorc")
	cnx_fdb.Exec("delete from cadorc")

	// Prepara o insert
	insert, err := cnx_fdb.Prepare(`insert
										into
										cadorc (id_cadorc,
										num,
										ano,
										numorc,    
										dtorc,
										descr,  
										prioridade,
										obs,
										status,
										liberado,
										codccusto,
										liberado_tela,
										empresa,
										solicitante,
										numorc_ant,
										flg_cotacao,
										id_ant,
										numlic) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		panic("Falha ao preparar insert: " + err.Error())
	}

	rows, err := cnx_pg.Query(`
		--Solicitações
		select
			a.pedidocompraid,
			'9'||to_char(row_number() over (partition by pedidocompraano order by pedidocompradata),'fm0000') num,
			pedidocompraano ano,
			'9'||to_char(row_number() over (partition by pedidocompraano order by pedidocompradata),'fm0000')||'/'||pedidocompraano % 2000 numorc,
			cast(pedidocompradata as varchar) data,
			substring(pedidocomprajustificativa, 1, 1024) descr,
			'NORMAL' prioridade,
			'Nº Solicitação: '||to_char(pedidocomprapedido, 'fm00000') || '/' || pedidocompraano % 2000 || coalesce(' - ' || pedidocompraobservacao,'') obs,
			case when pedidocompraforprocessoid is not null then 'EC' when pedidocomprasituacao = 2 then 'AB' when pedidocomprasituacao = 3 then 'AP' else 'CA' end status,
			case when a.pedidocompraforprocessoid is not null then 'S' else 'N' end liberado,
			coalesce(a.pedidocompraunidorcid,0) codccusto,
			'L' liberado_tela,
			c.pessoanome,
			to_char(pedidocomprapedido, 'fm00000') || '/' || pedidocompraano % 2000 numorc_ant,
			'N' flg_cotacao,
			a.pedidocompraid id_ant,
			pedidocompraforprocessoid numlic
		from
			pedidocompra a
		left join cotacaoprecos b on
			a.pedidocompracotacaoid = b.cotacaoprecosid and a.pedidocompracotacaoversao = b.cotacaoprecosversao 
		left join pessoa c on
			a.pedidocomprasolicitanteid = c.pessoaid
		where a.pedidocompraugid = $1 and b.cotacaoprecosnumero is null
		union all
		--Cotações
		select
			pedidocompraid,
			to_char(cotacaoprecosnumero,'fm00000') num,
			cotacaoprecosano ano,
			to_char(cotacaoprecosnumero, 'fm00000')||'/'||cotacaoprecosano % 2000 numorc,
			cast(pedidocompradata as varchar) data,
			substring(cotacaoprecosdescricao, 1, 1024) descr,
			'NORMAL' prioridade,
			'Nº Solicitação: '||to_char(pedidocomprapedido, 'fm00000') || '/' || pedidocompraano % 2000 || coalesce(' - ' || pedidocompraobservacao,'') obs,
			case when cotacaoprecossituacao = 1 then 'CO' when pedidocomprasituacao = 2 then 'EC' else 'CA' end status,
			case when a.pedidocompraforprocessoid is not null then 'S' else 'N' end liberado,
			coalesce(a.pedidocompraunidorcid,0) codccusto,
			'L' liberado_tela,
			c.pessoanome,
			to_char(pedidocomprapedido, 'fm00000') || '/' || pedidocompraano % 2000 numorc_ant,
			'S',
			cotacaoprecosid,
			null --pedidocompraforprocessoid numlic
		from
			pedidocompra a
		join cotacaoprecos b on
			a.pedidocompracotacaoid = b.cotacaoprecosid and a.pedidocompracotacaoversao = b.cotacaoprecosversao 
		left join pessoa c on
			a.pedidocomprasolicitanteid = c.pessoaid
		where a.pedidocompraugid = $2
		order by data desc`, GetEmpresa(), GetEmpresa())
	if err != nil {
		panic("Falha ao buscar pedidos de compra: " + err.Error())
	}

	// Conta Registros
	var count int
	err = cnx_pg.QueryRow(`select count(*) from (--Solicitações
		select
			a.pedidocompraid,
			'9'||to_char(row_number() over (partition by pedidocompraano order by pedidocompradata),'fm0000') num,
			pedidocompraano ano,
			'9'||to_char(row_number() over (partition by pedidocompraano order by pedidocompradata),'fm0000')||'/'||pedidocompraano % 2000 numorc,
			cast(pedidocompradata as varchar) data,
			substring(pedidocomprajustificativa, 1, 1024) descr,
			'NORMAL' prioridade,
			'Nº Solicitação: '||to_char(pedidocomprapedido, 'fm00000') || '/' || pedidocompraano % 2000 || coalesce(' - ' || pedidocompraobservacao,'') obs,
			case when pedidocompraforprocessoid is not null then 'EC' when pedidocomprasituacao = 2 then 'AB' when pedidocomprasituacao = 3 then 'AP' else 'CA' end status,
			case when a.pedidocompraforprocessoid is not null then 'S' else 'N' end liberado,
			coalesce(a.pedidocompraunidorcid,0) codccusto,
			'L' liberado_tela,
			c.pessoanome,
			to_char(pedidocomprapedido, 'fm00000') || '/' || pedidocompraano % 2000 numorc_ant,
			'N' flg_cotacao,
			a.pedidocompraid id_ant,
			pedidocompraforprocessoid numlic
		from
			pedidocompra a
		left join cotacaoprecos b on
			a.pedidocompracotacaoid = b.cotacaoprecosid and a.pedidocompracotacaoversao = b.cotacaoprecosversao 
		left join pessoa c on
			a.pedidocomprasolicitanteid = c.pessoaid
		where a.pedidocompraugid = $1 and b.cotacaoprecosnumero is null
		union all
		--Cotações
		select
			pedidocompraid,
			to_char(cotacaoprecosnumero,'fm00000') num,
			cotacaoprecosano ano,
			to_char(cotacaoprecosnumero, 'fm00000')||'/'||cotacaoprecosano % 2000 numorc,
			cast(pedidocompradata as varchar) data,
			substring(cotacaoprecosdescricao, 1, 1024) descr,
			'NORMAL' prioridade,
			'Nº Solicitação: '||to_char(pedidocomprapedido, 'fm00000') || '/' || pedidocompraano % 2000 || coalesce(' - ' || pedidocompraobservacao,'') obs,
			case when cotacaoprecossituacao = 1 then 'CO' when pedidocomprasituacao = 2 then 'EC' else 'CA' end status,
			case when a.pedidocompraforprocessoid is not null then 'S' else 'N' end liberado,
			coalesce(a.pedidocompraunidorcid,0) codccusto,
			'L' liberado_tela,
			c.pessoanome,
			to_char(pedidocomprapedido, 'fm00000') || '/' || pedidocompraano % 2000 numorc_ant,
			'S',
			cotacaoprecosid,
			null --pedidocompraforprocessoid numlic
		from
			pedidocompra a
		join cotacaoprecos b on
			a.pedidocompracotacaoid = b.cotacaoprecosid and a.pedidocompracotacaoversao = b.cotacaoprecosversao 
		left join pessoa c on
			a.pedidocomprasolicitanteid = c.pessoaid
		where a.pedidocompraugid = $2
		order by data desc) as rn`, GetEmpresa(), GetEmpresa()).Scan(&count)
	bar6 := p.AddBar(int64(count),
		mpb.PrependDecorators(
			decor.Name("CADORC: "),
			decor.Percentage(),
		),
	)

	var id_cadorc, codccusto, id_ant, numlic nulls.Int
	var num, ano, numorc, dtorc, descr, prioridade, obs, status, liberado, liberado_tela, solicitante, numorc_ant, flg_cotacao nulls.String
	empresa := nulls.NewInt(GetEmpresa())

	for rows.Next() {
		err = rows.Scan(&id_cadorc, &num, &ano, &numorc, &dtorc, &descr, &prioridade, &obs, &status, &liberado, &codccusto, &liberado_tela, &solicitante, &numorc_ant, &flg_cotacao, &id_ant, &numlic)
		if err != nil {
			panic("Falha ao ler pedidos de compra: " + err.Error())
		}

		_, err = insert.Exec(id_cadorc, num, ano, numorc, dtorc, descr, prioridade, obs, status, liberado, codccusto, liberado_tela, empresa, solicitante, numorc_ant, flg_cotacao, id_ant, numlic)
		if err != nil {
			// panic("Falha ao Inserir Registro na Cadorc: " + err.Error())
			continue
		}
		bar6.Increment()
	}
}

func Icadorc(p *mpb.Progress) {
	// Cria Conexão com os bancos
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnx_fdb.Close()

	cnx_pg, err := conexao.ConexaoOrigem()
	if err != nil {
		panic("Falha ao conectar com o banco de origem: " + err.Error())
	}
	defer cnx_pg.Close()

	// Limpa tabelas
	cnx_fdb.Exec("delete from icadorc")

	// Criar Campo
	cnx_fdb.Exec("alter table icadorc add item_ant integer")

	// Prepara o insert
	insert, err := cnx_fdb.Prepare(`insert into icadorc (numorc, item, cadpro, qtd, valor, itemorc, codccusto, itemorc_ag, id_cadorc) values (?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		panic("Falha ao preparar insert: " + err.Error())
	}

	// Consulta Auxiliar
	var cadpro, codreduz string
	aux1, err := cnx_fdb.Query("select cadpro, codreduz from cadest")
	if err != nil {
		panic("Falha ao buscar cadpro: " + err.Error())
	}

	cadpros := make(map[string]string)
	for aux1.Next() {
		err = aux1.Scan(&cadpro, &codreduz)
		if err != nil {
			panic("Falha ao ler cadpro: " + err.Error())
		}
		cadpros[codreduz] = cadpro
	}

	// Preparar Insert
	var numorc, flg_cotacao, id_cadorc string
	var id_ant int
	var qtd, valor nulls.Float64
	var item, codccusto nulls.Int

	rows, err := cnx_pg.Query(`select distinct 
									--loteordem,
									coalesce(d.estimativaitemid,b.itemcompraordem) item,
									b.itemcompramaterialid AS codreduz,
									SUM(b.itemcompraquantidade) AS total_quantidade, -- Soma as quantidades
									0 AS valor,
									coalesce(a.pedidocompraunidorcid,0) codccusto,
									CASE 
										WHEN pedidocompracotacaoid IS NULL THEN 'N' 
										ELSE 'S' 
									END AS flg_cotacao,
									COALESCE(a.pedidocompracotacaoid, a.pedidocompraid) AS id_ant
								FROM
									pedidocompra a
								JOIN itemcompra b ON
									a.pedidocompraid = b.itemcomprapedidoid
									AND a.pedidocompraversao = b.itemcompraversao
								LEFT JOIN estimativa c ON c.estimativacotacaoid = a.pedidocompracotacaoid
									AND c.estimativacotacaoversao = a.pedidocompracotacaoversao 
								LEFT JOIN estimativaitem d ON d.estimativaid = c.estimativaid and d.estimativaitemmaterialid = b.itemcompramaterialid
								LEFT JOIN lote e ON e.loteid = d.estimativaitemloteid
									AND e.loteversao = d.estimativaitemloteversao 
								LEFT JOIN cotacaoprecos f ON f.cotacaoprecosid = a.pedidocompracotacaoid
									AND f.cotacaoprecosversao = a.pedidocompracotacaoversao 
								WHERE
									a.pedidocompraugid = $1 
									AND itemcompraorigem = 1 
									and itemcompramaterialid is not null
									--AND COALESCE(a.pedidocompracotacaoid, a.pedidocompraid) = 2
								GROUP by
									loteordem,
									itemcompraordem,
									estimativaitemid,
									itemcompramaterialid,
									coalesce(a.pedidocompraunidorcid,0),
									CASE 
										WHEN pedidocompracotacaoid IS NULL THEN 'N' 
										ELSE 'S' 
									END,
									COALESCE(a.pedidocompracotacaoid, a.pedidocompraid),
									pedidocompracotacaoid
								order by id_ant, coalesce(d.estimativaitemid,b.itemcompraordem)`,GetEmpresa())
	if err != nil {
		panic("Falha ao buscar itens de pedido de compra: " + err.Error())
	}

	// Conta Registros
	var count int
	cnx_pg.QueryRow(`select count(*) from (select distinct 
									--loteordem,
									coalesce(d.estimativaitemid,b.itemcompraordem) item,
									b.itemcompramaterialid AS codreduz,
									SUM(b.itemcompraquantidade) AS total_quantidade, -- Soma as quantidades
									0 AS valor,
									coalesce(a.pedidocompraunidorcid,0) codccusto,
									CASE 
										WHEN pedidocompracotacaoid IS NULL THEN 'N' 
										ELSE 'S' 
									END AS flg_cotacao,
									COALESCE(a.pedidocompracotacaoid, a.pedidocompraid) AS id_ant
								FROM
									pedidocompra a
								JOIN itemcompra b ON
									a.pedidocompraid = b.itemcomprapedidoid
									AND a.pedidocompraversao = b.itemcompraversao
								LEFT JOIN estimativa c ON c.estimativacotacaoid = a.pedidocompracotacaoid
									AND c.estimativacotacaoversao = a.pedidocompracotacaoversao 
								LEFT JOIN estimativaitem d ON d.estimativaid = c.estimativaid and d.estimativaitemmaterialid = b.itemcompramaterialid
								LEFT JOIN lote e ON e.loteid = d.estimativaitemloteid
									AND e.loteversao = d.estimativaitemloteversao 
								LEFT JOIN cotacaoprecos f ON f.cotacaoprecosid = a.pedidocompracotacaoid
									AND f.cotacaoprecosversao = a.pedidocompracotacaoversao 
								WHERE
									a.pedidocompraugid = $1
									AND itemcompraorigem = 1 
									and itemcompramaterialid is not null
									--AND COALESCE(a.pedidocompracotacaoid, a.pedidocompraid) = 2
								GROUP by
									loteordem,
									itemcompraordem,
									estimativaitemid,
									itemcompramaterialid,
									coalesce(a.pedidocompraunidorcid,0),
									CASE 
										WHEN pedidocompracotacaoid IS NULL THEN 'N' 
										ELSE 'S' 
									END,
									COALESCE(a.pedidocompracotacaoid, a.pedidocompraid),
									pedidocompracotacaoid
								order by id_ant, coalesce(d.estimativaitemid,b.itemcompraordem)) as rn`, GetEmpresa()).Scan(&count)

	// Consulta Auxiliar
	aux2, err := cnx_fdb.Query("select numorc, id_cadorc, flg_cotacao, id_ant from cadorc")
	if err != nil {
		panic("Falha ao buscar numorc: " + err.Error())
	}

	numorcs := make(map[string]map[int][]string)
	for aux2.Next() {
		err = aux2.Scan(&numorc, &id_cadorc, &flg_cotacao, &id_ant)
		if err != nil {
			panic("Falha ao ler numorc: " + err.Error())
		}

		if _, exists := numorcs[flg_cotacao]; !exists {
			numorcs[flg_cotacao] = make(map[int][]string)
		}
		numorcs[flg_cotacao][id_ant] = []string{numorc, id_cadorc}
	}

	bar7 := p.AddBar(int64(count),
		mpb.PrependDecorators(
			decor.Name("ICADORC: "),
			decor.Percentage(),
		),
	)
	for rows.Next() {
		err = rows.Scan(&item, &codreduz, &qtd, &valor, &codccusto, &flg_cotacao, &id_ant)
		if err != nil {
			panic("Falha ao ler itens de pedido de compra: " + err.Error())
		}

		cadpro = cadpros[codreduz]
		numorc = numorcs[flg_cotacao][id_ant][0]
		id_cadorc = numorcs[flg_cotacao][id_ant][1]

		_, err = insert.Exec(numorc, item, cadpro, qtd, valor, item, codccusto, item, id_cadorc)
		if err != nil {
			continue
		}
		bar7.Increment()
	}
}

func Fcadorc(p *mpb.Progress) {
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnx_fdb.Close()

	cnx_pg, err := conexao.ConexaoOrigem()
	if err != nil {
		panic("Falha ao conectar com o banco de origem: " + err.Error())
	}

	// Prepara o insert
	insert, err := cnx_fdb.Prepare(`insert into fcadorc(numorc,codif, nome, valorc, id_cadorc) values (?,?,?,?,?)`)
	if err != nil {
		panic("Falha ao preparar insert: " + err.Error())
	}

	rows, err := cnx_pg.Query(`SELECT
									numorc,
									pessoaid codif,
									substring(pessoanome,1,70) nome,
									SUM(valorctot) AS valorc
								FROM
									(
									SELECT
										d.pedidocompracotacaoid,
										b.pessoaid,
										b.pessoanome,
										SUM(COALESCE(c.itemcompraquantidade, 0)) AS qtd,
										a.itemcompracotacaovalorunitario,
										SUM(ROUND(COALESCE(a.itemcompracotacaovalorunitario, 0) * COALESCE(c.itemcompraquantidade, 0))) AS valorctot,
										c.itemcompraordem,
										TO_CHAR(e.cotacaoprecosnumero, 'fm00000') || '/' || e.cotacaoprecosano % 2000 AS numorc,
										NULL AS classe,
										NULL AS ganhou,
										a.itemcompracotacaovencedora,
										a.itemcompracotacaoempatada
									FROM
										itemcompracotacao a
									JOIN
										pessoa b ON a.itemcompracotacaofornecedorid = b.pessoaid
									JOIN
										itemcompra c ON c.itemcompraid = a.itemcompraid AND a.itemcompraversao = c.itemcompraversao
									JOIN
										pedidocompra d ON d.pedidocompraid = c.itemcomprapedidoid AND d.pedidocompraversao = c.itemcomprapedidoversao
									JOIN
										cotacaoprecos e ON e.cotacaoprecosid = d.pedidocompracotacaoid AND e.cotacaoprecosversao = d.pedidocompracotacaoversao
									WHERE
										d.pedidocompraugid = $1
									GROUP BY
										d.pedidocompracotacaoid,
										b.pessoaid,
										b.pessoanome,
										a.itemcompracotacaovalorunitario,
										c.itemcompraordem,
										e.cotacaoprecosnumero,
										e.cotacaoprecosano,
										a.itemcompracotacaovencedora,
										a.itemcompracotacaoempatada
									ORDER BY
										numorc,
										c.itemcompraordem,
										a.itemcompracotacaovencedora
									) AS rn
								GROUP BY
									numorc,
									pessoaid,
									pessoanome;`, GetEmpresa())
	if err != nil {
		panic("Falha ao buscar fornecedores: " + err.Error())
	}

	// Conta Registros
	var count int
	err = cnx_pg.QueryRow(`select count(*) from (SELECT
									numorc,
									pessoaid codif,
									substring(pessoanome,1,70) nome,
									SUM(valorctot) AS valorc
								FROM
									(
									SELECT
										d.pedidocompracotacaoid,
										b.pessoaid,
										b.pessoanome,
										SUM(COALESCE(c.itemcompraquantidade, 0)) AS qtd,
										a.itemcompracotacaovalorunitario,
										SUM(ROUND(COALESCE(a.itemcompracotacaovalorunitario, 0) * COALESCE(c.itemcompraquantidade, 0))) AS valorctot,
										c.itemcompraordem,
										TO_CHAR(e.cotacaoprecosnumero, 'fm00000') || '/' || e.cotacaoprecosano % 2000 AS numorc,
										NULL AS classe,
										NULL AS ganhou,
										a.itemcompracotacaovencedora,
										a.itemcompracotacaoempatada
									FROM
										itemcompracotacao a
									JOIN
										pessoa b ON a.itemcompracotacaofornecedorid = b.pessoaid
									JOIN
										itemcompra c ON c.itemcompraid = a.itemcompraid AND a.itemcompraversao = c.itemcompraversao
									JOIN
										pedidocompra d ON d.pedidocompraid = c.itemcomprapedidoid AND d.pedidocompraversao = c.itemcomprapedidoversao
									JOIN
										cotacaoprecos e ON e.cotacaoprecosid = d.pedidocompracotacaoid AND e.cotacaoprecosversao = d.pedidocompracotacaoversao
									WHERE
										d.pedidocompraugid = $1
									GROUP BY
										d.pedidocompracotacaoid,
										b.pessoaid,
										b.pessoanome,
										a.itemcompracotacaovalorunitario,
										c.itemcompraordem,
										e.cotacaoprecosnumero,
										e.cotacaoprecosano,
										a.itemcompracotacaovencedora,
										a.itemcompracotacaoempatada
									ORDER BY
										numorc,
										c.itemcompraordem,
										a.itemcompracotacaovencedora
									) AS rn
								GROUP BY
									numorc,
									pessoaid,
									pessoanome) as rn`, GetEmpresa()).Scan(&count)
	if err != nil {
		panic("Falha ao contar registros: " + err.Error())
	}
	bar8 := p.AddBar(int64(count),
		mpb.PrependDecorators(
			decor.Name("FCADORC: "),
			decor.Percentage(),
		),
	)

	// Limpa tabelas
	cnx_fdb.Exec("delete from fcadorc")

	aux1, err := cnx_fdb.Query("select numorc, id_cadorc from cadorc")
	if err != nil {
		panic("Falha ao buscar numorc: " + err.Error())
	}
	idsCadorc := make(map[string]int)

	for aux1.Next() {
		var numorc string
		var id_cadorc int
		err = aux1.Scan(&numorc, &id_cadorc)
		if err != nil {
			panic("Falha ao ler numorc: " + err.Error())
		}
		idsCadorc[numorc] = id_cadorc
	}

	var numorc, nome nulls.String
	var codif, id_cadorc int
	var valorc float64
	for rows.Next() {
		err = rows.Scan(&numorc, &codif, &nome, &valorc)
		if err != nil {
			panic("Falha ao ler fornecedores: " + err.Error())
		}

		id_cadorc = idsCadorc[numorc.String]

		_, err = insert.Exec(numorc, codif, nome, valorc, id_cadorc)
		if err != nil {
			panic("Falha ao inserir fornecedores: " + err.Error())
		}
		bar8.Increment()
	}
}

func Vcadorc(p *mpb.Progress) {
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnx_fdb.Close()

	cnx_pg, err := conexao.ConexaoOrigem()
	if err != nil {
		panic("Falha ao conectar com o banco de origem: " + err.Error())
	}

	// Limpa tabelas
	cnx_fdb.Exec("delete from vcadorc")

	// Cria Campo
	cnx_fdb.Exec("alter table vcadorc add vencedor_ant varchar(1)")

	tx, err := cnx_fdb.Begin()
	if err != nil {
		panic("Falha ao iniciar transação: " + err.Error())
	}

	// Prepara o insert
	insert, err := tx.Prepare(`insert into vcadorc(numorc, item, codif, vlruni, vlrtot, id_cadorc, vencedor_ant) values (?,?,?,?,?,?,?)`)
	if err != nil {
		panic("Falha ao preparar insert: " + err.Error())
	}

	rows, err := cnx_pg.Query(`SELECT
									numorc,
									item,
									pessoaid AS codif, 
									coalesce(itemcompracotacaovalorunitario,0) AS vlruni,
									valorctot AS vlrtot,
									itemcompracotacaovencedora
								FROM (
									SELECT
										d.pedidocompracotacaoid,
										b.pessoaid,
										b.pessoanome,
										SUM(COALESCE(c.itemcompraquantidade, 0)) AS qtd,
										a.itemcompracotacaovalorunitario,
										SUM(ROUND(COALESCE(a.itemcompracotacaovalorunitario, 0) * COALESCE(c.itemcompraquantidade, 0))) AS valorctot,
										coalesce(g.estimativaitemid,c.itemcompraordem) item,
										TO_CHAR(e.cotacaoprecosnumero, 'fm00000') || '/' || e.cotacaoprecosano % 2000 AS numorc,
										NULL AS classe,
										NULL AS ganhou,
										a.itemcompracotacaovencedora,
										a.itemcompracotacaoempatada
									FROM
										itemcompracotacao a
									JOIN 
										pessoa b ON a.itemcompracotacaofornecedorid = b.pessoaid
									JOIN 
										itemcompra c ON c.itemcompraid = a.itemcompraid
											AND a.itemcompraversao = c.itemcompraversao
									JOIN 
										pedidocompra d ON d.pedidocompraid = c.itemcomprapedidoid
											AND d.pedidocompraversao = c.itemcomprapedidoversao
									JOIN 
										cotacaoprecos e ON e.cotacaoprecosid = d.pedidocompracotacaoid
											AND e.cotacaoprecosversao = d.pedidocompracotacaoversao
									left join estimativa f on f.estimativacotacaoid = e.cotacaoprecosid and f.estimativacotacaoversao = e.cotacaoprecosversao
									left join estimativaitem g ON f.estimativaid = g.estimativaid and g.estimativaitemmaterialid = c.itemcompramaterialid
									WHERE
										d.pedidocompraugid = $1
									GROUP BY
										d.pedidocompracotacaoid,
										b.pessoaid,
										b.pessoanome,
										a.itemcompracotacaovalorunitario,
										e.cotacaoprecosnumero,
										e.cotacaoprecosano,
										a.itemcompracotacaovencedora,
										a.itemcompracotacaoempatada,
										estimativaitemid,
										c.itemcompraordem
									ORDER BY
										numorc,
										item,
										a.itemcompracotacaovencedora
								) AS rn;`, GetEmpresa())
	if err != nil {
		panic("Falha ao buscar fornecedores: " + err.Error())
	}

	// Consulta Auxiliar
	var count int
	err = cnx_pg.QueryRow(`select count(*) from (SELECT
									numorc,
									item,
									pessoaid AS codif, 
									coalesce(itemcompracotacaovalorunitario,0) AS vlruni,
									valorctot AS vlrtot,
									itemcompracotacaovencedora
								FROM (
									SELECT
										d.pedidocompracotacaoid,
										b.pessoaid,
										b.pessoanome,
										SUM(COALESCE(c.itemcompraquantidade, 0)) AS qtd,
										a.itemcompracotacaovalorunitario,
										SUM(ROUND(COALESCE(a.itemcompracotacaovalorunitario, 0) * COALESCE(c.itemcompraquantidade, 0))) AS valorctot,
										coalesce(g.estimativaitemid,c.itemcompraordem) item,
										TO_CHAR(e.cotacaoprecosnumero, 'fm00000') || '/' || e.cotacaoprecosano % 2000 AS numorc,
										NULL AS classe,
										NULL AS ganhou,
										a.itemcompracotacaovencedora,
										a.itemcompracotacaoempatada
									FROM
										itemcompracotacao a
									JOIN 
										pessoa b ON a.itemcompracotacaofornecedorid = b.pessoaid
									JOIN 
										itemcompra c ON c.itemcompraid = a.itemcompraid
											AND a.itemcompraversao = c.itemcompraversao
									JOIN 
										pedidocompra d ON d.pedidocompraid = c.itemcomprapedidoid
											AND d.pedidocompraversao = c.itemcomprapedidoversao
									JOIN 
										cotacaoprecos e ON e.cotacaoprecosid = d.pedidocompracotacaoid
											AND e.cotacaoprecosversao = d.pedidocompracotacaoversao
									left join estimativa f on f.estimativacotacaoid = e.cotacaoprecosid and f.estimativacotacaoversao = e.cotacaoprecosversao
									left join estimativaitem g ON f.estimativaid = g.estimativaid and g.estimativaitemmaterialid = c.itemcompramaterialid
									WHERE
										d.pedidocompraugid = $1
									GROUP BY
										d.pedidocompracotacaoid,
										b.pessoaid,
										b.pessoanome,
										a.itemcompracotacaovalorunitario,
										e.cotacaoprecosnumero,
										e.cotacaoprecosano,
										a.itemcompracotacaovencedora,
										a.itemcompracotacaoempatada,
										estimativaitemid,
										c.itemcompraordem
									ORDER BY
										numorc,
										item,
										a.itemcompracotacaovencedora) as q) as rn`, GetEmpresa()).Scan(&count)
	if err != nil {
		panic("Falha ao contar registros: " + err.Error())
	}
	bar9 := p.AddBar(int64(count),
		mpb.PrependDecorators(
			decor.Name("VCADORC: "),
			decor.Percentage(),
		),
	)

	aux1, err := cnx_fdb.Query("select numorc, id_cadorc from cadorc")
	if err != nil {
		panic("Falha ao buscar numorc: " + err.Error())
	}
	idsCadorc := make(map[string]int)

	for aux1.Next() {
		var numorc string
		var id_cadorc int
		err = aux1.Scan(&numorc, &id_cadorc)
		if err != nil {
			panic("Falha ao ler numorc: " + err.Error())
		}
		idsCadorc[numorc] = id_cadorc
	}

	var numorc, vencedor_ant string
	var codif, id_cadorc, item int
	var vlruni, vlrtot float64

	for rows.Next() {
		err = rows.Scan(&numorc, &item, &codif, &vlruni, &vlrtot, &vencedor_ant)
		if err != nil {
			panic("Falha ao ler fornecedores: " + err.Error())
		}

		id_cadorc = idsCadorc[numorc]

		_, err = insert.Exec(numorc, item, codif, vlruni, vlrtot, id_cadorc, vencedor_ant)
		if err != nil {
			panic("Falha ao inserir fornecedores: " + err.Error())
		}
		bar9.Increment()
	}
	err = tx.Commit()
	if err != nil {
		panic("Falha ao commitar transação: " + err.Error())
	}

	cnx_fdb.Exec(`EXECUTE BLOCK AS
					DECLARE VARIABLE ID_CADORC INTEGER;
					declare variable ITEM INTEGER;
					declare variable CODIF INTEGER;
					DECLARE VARIABLE VLRUNI DOUBLE PRECISION;
					BEGIN
						FOR 
							SELECT id_cadorc, item, codif, vlruni FROM vcadorc WHERE vencedor_ant = 'S' INTO :ID_CADORC, :ITEM, :CODIF, :VLRUNI
						DO
						BEGIN
							UPDATE vcadorc SET ganhou = :codif, vlrganhou = :vlruni WHERE id_cadorc = :id_cadorc AND item = :item;
						END
					END`)
}