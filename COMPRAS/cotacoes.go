package compras

import (
	conexao "ASSESSOR_PUBLICO/CONEXAO"
	"fmt"
	"time"

	"github.com/gobuffalo/nulls"
)

func Cadorc() {
	start := time.Now()
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
										id_ant) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
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
			case when pedidocomprasituacao = 2 then 'AB' when pedidocomprasituacao = 3 then 'AP' else 'CA' end status,
			'S' liberado,
			coalesce(a.pedidocompraunidorcid,0) codccusto,
			'L' liberado_tela,
			c.pessoanome,
			to_char(pedidocomprapedido, 'fm00000') || '/' || pedidocompraano % 2000 numorc_ant,
			'N' flg_cotacao,
			a.pedidocompraid id_ant
		from
			pedidocompra a
		left join cotacaoprecos b on
			a.pedidocompracotacaoid = b.cotacaoprecosid and a.pedidocompracotacaoversao = b.cotacaoprecosversao 
		left join pessoa c on
			a.pedidocomprasolicitanteid = c.pessoaid
		where a.pedidocompraugid = 2 and b.cotacaoprecosnumero is null
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
			'S' liberado,
			coalesce(a.pedidocompraunidorcid,0) codccusto,
			'L' liberado_tela,
			c.pessoanome,
			to_char(pedidocomprapedido, 'fm00000') || '/' || pedidocompraano % 2000 numorc_ant,
			'S',
			cotacaoprecosid
		from
			pedidocompra a
		join cotacaoprecos b on
			a.pedidocompracotacaoid = b.cotacaoprecosid and a.pedidocompracotacaoversao = b.cotacaoprecosversao 
		left join pessoa c on
			a.pedidocomprasolicitanteid = c.pessoaid
		where a.pedidocompraugid = 2
		order by data desc
	`) // GetEmpresa()
	if err != nil {
		panic("Falha ao buscar pedidos de compra: " + err.Error())
	}

	var id_cadorc, codccusto, id_ant nulls.Int
	var num, ano, numorc, dtorc, descr, prioridade, obs, status, liberado, liberado_tela, solicitante, numorc_ant, flg_cotacao nulls.String
	empresa := nulls.NewInt(GetEmpresa())

	for rows.Next() {
		err = rows.Scan(&id_cadorc, &num, &ano, &numorc, &dtorc, &descr, &prioridade, &obs, &status, &liberado, &codccusto, &liberado_tela, &solicitante, &numorc_ant, &flg_cotacao, &id_ant)
		if err != nil {
			panic("Falha ao ler pedidos de compra: " + err.Error())
		}

		_, err = insert.Exec(id_cadorc, num, ano, numorc, dtorc, descr, prioridade, obs, status, liberado, codccusto, liberado_tela, empresa, solicitante, numorc_ant, flg_cotacao, id_ant)
		if err != nil {
			fmt.Println("Falha ao Inserir Registro na Cadorc: ", err)
			continue
		}
	}

	fmt.Println("solicitacoes - Tempo de execução: ", time.Since(start))
}

func Icadorc() {
	start := time.Now()
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
	var item, itemorc, codccusto nulls.Int

	rows, err := cnx_pg.Query(`select distinct 
									--loteordem,
									b.itemcompraordem,
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
								LEFT JOIN estimativaitem d ON d.estimativaid = c.estimativaid
								LEFT JOIN lote e ON e.loteid = d.estimativaitemloteid
									AND e.loteversao = d.estimativaitemloteversao 
								LEFT JOIN cotacaoprecos f ON f.cotacaoprecosid = a.pedidocompracotacaoid
									AND f.cotacaoprecosversao = a.pedidocompracotacaoversao 
								WHERE
									a.pedidocompraugid = 2 
									AND itemcompraorigem = 1 
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
								order by id_ant, itemcompraordem`)
	if err != nil {
		panic("Falha ao buscar itens de pedido de compra: " + err.Error())
	}

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

	for rows.Next() {
		err = rows.Scan(&item, &codreduz, &qtd, &valor, &codccusto, &flg_cotacao, &id_ant)
		if err != nil {
			panic("Falha ao ler itens de pedido de compra: " + err.Error())
		}

		cadpro = cadpros[codreduz]
		numorc = numorcs[flg_cotacao][id_ant][0]
		id_cadorc = numorcs[flg_cotacao][id_ant][1]

		_, err = insert.Exec(numorc, item, cadpro, qtd, valor, itemorc, codccusto, item, id_cadorc)
		if err != nil {
			fmt.Println("Falha ao inserir itens de pedido de compra: ", err)
			continue
		}
	}
	cnx_fdb.Exec("UPDATE ICADORC SET ITEMORC = ITEM")
	fmt.Println("itens - Tempo de execução: ", time.Since(start))
}

func Fcadorc() {
	start := time.Now()
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
	cnx_fdb.Exec("delete from fcadorc")

	// Prepara o insert
	insert, err := cnx_fdb.Prepare(`insert into fcadorc(numorc,codif, nome, valorc, id_cadorc) values (?,?,?,?,?)`)
	if err != nil {
		panic("Falha ao preparar insert: " + err.Error())
	}

	rows, err := cnx_pg.Query(`select
								d.pedidocompracotacaoid,
								b.pessoaid,
								sum(coalesce(c.itemcompraquantidade, 0)) qtd,
								a.itemcompracotacaovalorunitario,
								sum(round((coalesce(a.itemcompracotacaovalorunitario, 0) * coalesce(c.itemcompraquantidade, 0)))) valorctot,
								c.itemcompraordem,
								to_char(e.cotacaoprecosnumero,
								'fm00000')|| '/' || e.cotacaoprecosano%2000 numorc,
								null classe,
								null ganhou,
								a.itemcompracotacaovencedora,
								a.itemcompracotacaoempatada
							from
								itemcompracotacao a
							join pessoa b on
								a.itemcompracotacaofornecedorid = b.pessoaid
							join itemcompra c on
								c.itemcompraid = a.itemcompraid
								and a.itemcompraversao = c.itemcompraversao
							join pedidocompra d on
								d.pedidocompraid = c.itemcomprapedidoid
								and d.pedidocompraversao = c.itemcomprapedidoversao
							join cotacaoprecos e on
								e.cotacaoprecosid = d.pedidocompracotacaoid
								and e.cotacaoprecosversao = d.pedidocompracotacaoversao
							where
								d.pedidocompraugid = 2
							group by
								1,
								2,
								itemcompracotacaovalorunitario,
								itemcompraordem,
								cotacaoprecosnumero,
								cotacaoprecosano,
								a.itemcompracotacaovencedora,
								a.itemcompracotacaoempatada
							order by
								numorc,
								itemcompraordem,
								itemcompracotacaovencedora`)
	if err != nil {
		panic("Falha ao buscar fornecedores: " + err.Error())
	}

	var numorc, nome nulls.String
	var codif, id_cadorc int
	var valorc float64

	for rows.Next() {
		err = rows.Scan(&id_cadorc, &codif, &nome, &valorc, &numorc)
		if err != nil {
			panic("Falha ao ler fornecedores: " + err.Error())
		}

		_, err = insert.Exec(numorc, codif, nome, valorc, id_cadorc)
		if err != nil {
			panic("Falha ao inserir fornecedores: " + err.Error())
		}
	}
	fmt.Println("fornecedores - Tempo de execução: ", time.Since(start))
}
