package compras

import (
	"fmt"
	"time"
	"ASSESSOR_PUBLICO/CONEXAO"
	"github.com/gobuffalo/nulls"
)

func Solicitacao() {
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
										numorc_ant) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		panic("Falha ao preparar insert: " + err.Error())
	}

	rows, err := cnx_pg.Query(`
		--Solicitações
		select
			pedidocompraid,
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
			to_char(pedidocomprapedido, 'fm00000') || '/' || pedidocompraano % 2000 numorc_ant
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
			to_char(pedidocomprapedido, 'fm00000') || '/' || pedidocompraano % 2000 numorc_ant
		from
			pedidocompra a
		join cotacaoprecos b on
			a.pedidocompracotacaoid = b.cotacaoprecosid and a.pedidocompracotacaoversao = b.cotacaoprecosversao 
		left join pessoa c on
			a.pedidocomprasolicitanteid = c.pessoaid
		where a.pedidocompraugid = 2
		order by data desc
	`)  // GetEmpresa()
	if err != nil {
		panic("Falha ao buscar pedidos de compra: " + err.Error())
	}

	var id_cadorc, codccusto nulls.Int 
	var num, ano, numorc, dtorc, descr, prioridade, obs, status, liberado, liberado_tela, solicitante, numorc_ant nulls.String

	for rows.Next() {
		err = rows.Scan(&id_cadorc, &num, &ano, &numorc, &dtorc, &descr, &prioridade, &obs, &status, &liberado, &codccusto, &liberado_tela, &solicitante, &numorc_ant)
		if err != nil {
			panic("Falha ao ler pedidos de compra: " + err.Error())
		}
		empresa := nulls.NewInt(GetEmpresa())

		_, err = insert.Exec(id_cadorc, num, ano, numorc, dtorc, descr, prioridade, obs, status, liberado, codccusto, liberado_tela, empresa, solicitante, numorc_ant)
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
	cnx_fdb.Exec("alter table icadorc add num_lote integer")
	cnx_fdb.Exec("alter table icadorc add item_ant integer")
	cnx_fdb.Exec("alter table icadorc add item_por_lote integer")

	// Prepara o insert
	insert, err := cnx_fdb.Prepare(`insert into icadorc (numorc, item, cadpro, qtd, valor, itemorc, codccusto, itemorc_ag, id_cadorc, num_lote, item_ant, item_por_lote) values (?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		panic("Falha ao preparar insert: " + err.Error())
	}

	rows, err := cnx_pg.Query(`select
									a.cotacaoprecosid,
									to_char(a.cotacaoprecosnumero,'fm00000')||'/'||cotacaoprecosano%2000 numorc,
									e.loteordem lote,
									row_number() over (partition by cotacaoprecosnumero, cotacaoprecosano order by estimativaitemid, cotacaoprecosnumero, cotacaoprecosano) item,
									row_number() over (partition by loteordem, cotacaoprecosnumero, cotacaoprecosano order by loteordem, estimativaitemid) item_por_lote,
									c.estimativaitemid,
									c.estimativaitemmaterialid codreduz,
									c.estimativaitemqtde,
									c.estimativaitemmenorvalor,
									d.pedidocompraunidorcid
								from cotacaoprecos a
								join estimativa b on a.cotacaoprecosid = b.estimativacotacaoid and a.cotacaoprecosversao = b.estimativacotacaoversao 
								join estimativaitem c on b.estimativaid = c.estimativaid 
								join pedidocompra d on a.cotacaoprecosid = d.pedidocompracotacaoid and a.cotacaoprecosversao = b.estimativacotacaoversao 
								left join lote e on e.loteid = c.estimativaitemloteid AND e.loteversao = c.estimativaitemloteversao
								where a.cotacaoprecosugid = 2 and estimativaitemmaterialid is not null --and a.cotacaoprecosnumero = 373 and cotacaoprecosano = 2022`)
	if err != nil {
		panic("Falha ao buscar itens de cotação: " + err.Error())
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
	var numorc string
	var id_cadorc, item_por_lote int
	var qtd, valor nulls.Float64
	var item, itemorc, lote, item_ant, codccusto nulls.Int

	for rows.Next() {
		err = rows.Scan(&id_cadorc, &numorc, &lote, &item, &item_por_lote, &item_ant, &codreduz, &qtd, &valor, &codccusto)
		if err != nil {
			panic("Falha ao ler itens de cotação: " + err.Error())
		}

		cadpro = cadpros[codreduz]
		
		_, err = insert.Exec(numorc, item, cadpro, qtd, valor, itemorc, codccusto, item, id_cadorc, lote, item_ant, item_por_lote)
		if err != nil {
			panic("Falha ao inserir itens de cotação: " + err.Error())
		}
	}

	rows, err = cnx_pg.Query(`select
									a.pedidocompraid,
									0 lote,
									b.itemcompraordem item,
									null item_ant,
									b.itemcompramaterialid codreduz,
									b.itemcompraquantidade,
									0 valor,
									a.pedidocompraunidorcid
								from
									pedidocompra a
								join itemcompra b on
									a.pedidocompraid = b.itemcomprapedidoid
									and a.pedidocompraversao = b.itemcompraversao
								where
									a.pedidocompracotacaoid is null and a.pedidocompraugid = 2`)
	if err != nil {
		panic("Falha ao buscar itens de pedido de compra: " + err.Error())
	}

	// Consulta Auxiliar
	aux2, err := cnx_fdb.Query("select numorc, id_cadorc from cadorc")
	if err != nil {
		panic("Falha ao buscar numorc: " + err.Error())
	}

	numorcs := make(map[int]string)
	for aux2.Next() {
		err = aux2.Scan(&numorc, &id_cadorc)
		if err != nil {
			panic("Falha ao ler numorc: " + err.Error())
		}
		numorcs[id_cadorc] = numorc
	}

	for rows.Next() {
		err = rows.Scan(&id_cadorc, &lote, &item, &item_ant, &codreduz, &qtd, &valor, &codccusto)
		if err != nil {
			panic("Falha ao ler itens de pedido de compra: " + err.Error())
		}

		cadpro = cadpros[codreduz]
		numorc = numorcs[id_cadorc]
		
		_, err = insert.Exec(numorc, item, cadpro, qtd, valor, itemorc, codccusto, item, id_cadorc, lote, item_ant, nil)
		if err != nil {
			fmt.Println("Falha ao inserir itens de pedido de compra: ", err)
			continue
		}
	}
	fmt.Println("itens - Tempo de execução: ", time.Since(start))
}