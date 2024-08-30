package compras

import (
	conexao "ASSESSOR_PUBLICO/CONEXAO"
	utils "ASSESSOR_PUBLICO/MODULOS/utils"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

func Cadunimedida(p *mpb.Progress) {
	// start := time.Now()
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

	cnx_fdb.Exec("DELETE FROM CADEST")                          // Limpa tabela
	cnx_fdb.Exec("DELETE FROM CADUNIMEDIDA")                    // Limpa tabela
	cnx_fdb.Exec("ALTER TABLE CADUNIMEDIDA ADD ID_ANT INTEGER") // Cria Campo de identificação

	// Prepara Insert
	insert, err := cnx_fdb.Prepare(`INSERT INTO CADUNIMEDIDA(sigla, descricao, id_ant) VALUES(?,?,?)`)
	if err != nil {
		panic("Falha ao preparar insert: " + err.Error())
	}

	// Executa Select
	rows, err := cnx_pg.Query(`select
									trim(substring(unidademedidadescricao,1,30)) descricao,
									trim(substring(unidademedidasigla,1,5)) sigla,
									unidademedidaid
								from
									unidademedida u`)
	if err != nil {
		panic("Falha ao executar select: " + err.Error())
	}

	// Conta registros
	var count int
	err = cnx_pg.QueryRow(`SELECT COUNT(*) FROM (select
									trim(substring(unidademedidadescricao,1,30)) descricao,
									trim(substring(unidademedidasigla,1,5)) sigla,
									unidademedidaid
								from
									unidademedida) as rn`).Scan(&count)
	if err != nil {
		panic("Falha ao contar registros: " + err.Error())
	}

	// Itera sobre o resultado
	var sigla, descricao string
	var id_ant int
	bar1 := p.AddBar(int64(count),
		mpb.PrependDecorators(
			decor.Name("CADUNIMEDIDA: "),
			decor.Percentage(),
		),
	)

	for rows.Next() {
		err = rows.Scan(&descricao, &sigla, &id_ant)
		if err != nil {
			panic("Falha ao ler resultado: " + err.Error())
		}

		_, err = insert.Exec(sigla, descricao, id_ant)
		if err != nil {
			panic("Falha ao inserir dados: " + err.Error())
		}
		bar1.Increment()
	}
	// fmt.Println("Cadunimedida - Tempo de execução: ", time.Since(start))
}

func GrupoSubgrupo(p *mpb.Progress) {
	// start := time.Now()
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

	// Limpa tabela
	cnx_fdb.Exec("DELETE FROM CADEST")
	cnx_fdb.Exec("DELETE FROM CADSUBGR")
	cnx_fdb.Exec("DELETE FROM CADGRUPO")

	// Cria Campo de identificação
	cnx_fdb.Exec("ALTER TABLE CADGRUPO ADD GRUPO_ANT INTEGER")
	cnx_fdb.Exec("ALTER TABLE CADSUBGR ADD GRUPO_ANT INTEGER")
	cnx_fdb.Exec("ALTER TABLE CADSUBGR ADD SUBGRUPO_ANT INTEGER")

	// Prepara Insert
	insertGrupo, err := cnx_fdb.Prepare(`INSERT INTO CADGRUPO(grupo, nome, ocultar, grupo_ant) VALUES(?,?,?,?)`)
	if err != nil {
		panic("Falha ao preparar insert: " + err.Error())
	}
	insertSubgrupo, err := cnx_fdb.Prepare(`INSERT INTO CADSUBGR(grupo, subgrupo, nome, ocultar, grupo_ant, subgrupo_ant) VALUES(?,?,?,?,?,?)`)
	if err != nil {
		panic("Falha ao preparar insert: " + err.Error())
	}

	// Executa Select
	rows, err := cnx_pg.Query(`select
									'0'||substring(hierarquiaconcatniveis, 1, 2) grupo,
									'0'||substring(hierarquiaconcatniveis, 4, 2) subgrupo,
									hierarquianivel,
									substring(hierarquiadesc, 1, 45) nome,
									case when hierarquiasituacao = 'A' then 'N' else 'S' end ocultar,
									hierarquiagrupoid,
									coalesce(hierarquiasubgrupoid,0) subgrupoid
								from
									hierarquia h 
								order by hierarquianivel`)
	if err != nil {
		panic("Falha ao executar select: " + err.Error())
	}

	// Conta registros
	var count int
	err = cnx_pg.QueryRow(`SELECT COUNT(*) FROM (select
									'0'||substring(hierarquiaconcatniveis, 1, 2) grupo,
									'0'||substring(hierarquiaconcatniveis, 4, 2) subgrupo,
									hierarquianivel,
									substring(hierarquiadesc, 1, 45) nome,
									case when hierarquiasituacao = 'A' then 'N' else 'S' end ocultar,
									hierarquiagrupoid,
									coalesce(hierarquiasubgrupoid,0) subgrupoid
								from
									hierarquia h 
								order by hierarquianivel) as rn`).Scan(&count)
	if err != nil {
		panic("Falha ao contar registros: " + err.Error())
	}
	bar2 := p.AddBar(int64(count),
		mpb.PrependDecorators(
			decor.Name("GRUPO/SUBGRUPO: "),
			decor.Percentage(),
		),
	)

	// Itera sobre o resultado
	var grupo, subgrupo, nome, ocultar string
	var nivel, grupo_ant, subgrupo_ant int
	for rows.Next() {
		err = rows.Scan(&grupo, &subgrupo, &nivel, &nome, &ocultar, &grupo_ant, &subgrupo_ant)
		if err != nil {
			panic("Falha ao ler resultado: " + err.Error())
		}

		if nivel == 1 {
			_, err = insertGrupo.Exec(grupo, nome, ocultar, grupo_ant)
			if err != nil {
				panic("Falha ao inserir dados: " + err.Error())
			}
		} else {
			_, err = insertSubgrupo.Exec(grupo, subgrupo, nome, ocultar, grupo_ant, subgrupo_ant)
			if err != nil {
				panic("Falha ao inserir dados: " + err.Error())
			}
		}
		bar2.Increment()
	}
	// fmt.Println("GrupoSubgrupo - Tempo de execução: ", time.Since(start))
}

func Cadest(p *mpb.Progress) {
	// start := time.Now()
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

	// Limpa tabela
	cnx_fdb.Exec("DELETE FROM CADEST")

	tx, err := cnx_fdb.Begin()
	if err != nil {
		panic("Falha ao iniciar transação: " + err.Error())
	}

	// Prepara Insert
	insert, err := tx.Prepare(`INSERT
								INTO
								Cadest(cadpro,
								grupo,
								subgrupo,
								codigo,
								disc1,
								tipopro,
								unid1,
								discr1,
								codreduz,
								ocultar)
							VALUES(?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		panic("Falha ao preparar insert: " + err.Error())
	}

	// Executa Select
	rows, err := cnx_pg.Query(`select
									'0'||substring(hierarquiaconcatniveis, 1, 2) grupo,
									'0'||substring(hierarquiaconcatniveis, 4, 2) subgrupo,
									row_number() over (partition by hierarquiaconcatniveis order by hierarquiaconcatniveis) codigo,
									a.materialdescricao disc1,
									case when materialtipo = 2 then 'S' when materialtipo = 1 then 'C' else 'P' end tipopro,
									substring(c.unidademedidasigla,1,5) unid1,
									case when a.materialcaract is null or a.materialcaract = '' then a.materialconciddesc else materialcaract end as descr1,
									a.materialid codreduz,
									case when materialsituacao = 'A' then 'N' else 'S' end as ocultar
								from
									material a
								join hierarquia b on
									a.materialhierarquiaid = b.hierarquiaid
								join unidademedida c on
									a.materialundmedidaid = c.unidademedidaid`)
	if err != nil {
		panic("Falha ao executar select: " + err.Error())
	}

	// Conta registros
	var count int
	err = cnx_pg.QueryRow(`SELECT COUNT(*) FROM (select
									'0'||substring(hierarquiaconcatniveis, 1, 2) grupo,
									'0'||substring(hierarquiaconcatniveis, 4, 2) subgrupo,
									row_number() over (partition by hierarquiaconcatniveis order by hierarquiaconcatniveis) codigo,
									a.materialdescricao disc1,
									case when materialtipo = 2 then 'S' when materialtipo = 1 then 'C' else 'P' end tipopro,
									substring(c.unidademedidasigla,1,5) unid1,
									case when a.materialcaract is null or a.materialcaract = '' then a.materialconciddesc else materialcaract end as descr1,
									a.materialid codreduz,
									case when materialsituacao = 'A' then 'N' else 'S' end as ocultar
								from
									material a
								join hierarquia b on
									a.materialhierarquiaid = b.hierarquiaid
								join unidademedida c on
									a.materialundmedidaid = c.unidademedidaid) as rn`).Scan(&count)
	if err != nil {
		panic("Falha ao contar registros: " + err.Error())
	}
	bar5 := p.AddBar(int64(count),
		mpb.PrependDecorators(
			decor.Name("CADEST: "),
			decor.Percentage(),
		),
	)

	// Itera sobre o resultado
	var grupo, subgrupo, disc1, tipopro, unid1, discr1, codreduz, ocultar string
	var intCodigo int

	for rows.Next() {
		err = rows.Scan(&grupo, &subgrupo, &intCodigo, &disc1, &tipopro, &unid1, &discr1, &codreduz, &ocultar)
		if err != nil {
			panic("Falha ao ler resultado: " + err.Error())
		}

		subgrupoCodigo := utils.EstourouSubgr(intCodigo, subgrupo, grupo, cnx_fdb)

		cadpro := grupo + "." + subgrupoCodigo[0] + "." + subgrupoCodigo[1]
		subgrupo = subgrupoCodigo[0]
		codigo := subgrupoCodigo[1]

		_, err = insert.Exec(cadpro, grupo, subgrupo, codigo, disc1, tipopro, unid1, discr1, codreduz, ocultar)
		if err != nil {
			panic("Falha ao inserir dados: " + err.Error())
		}
		bar5.Increment()
	}
	// Comita a transação em Cadest
	if err := tx.Commit(); err != nil {
		panic("Falha ao commitar transação: " + err.Error())
	}
	// fmt.Println("Cadest - Tempo de execução: ", time.Since(start))
}

func Destino(p *mpb.Progress) {
	// start := time.Now()
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

	// Limpa tabela
	cnx_fdb.Exec("DELETE FROM DESTINO")

	// Prepara Insert
	insert, err := cnx_fdb.Prepare(`INSERT INTO DESTINO(COD, DESTI, EMPRESA) VALUES(?,?,?)`)
	if err != nil {
		panic("Falha ao preparar insert: " + err.Error())
	}

	// Executa Select
	rows, err := cnx_pg.Query(`select
									to_char(almoxarifadoid, 'fm000000000') cod,
									almoxarifadodescricao
								from
									almoxarifado a`)
	if err != nil {
		panic("Falha ao executar select: " + err.Error())
	}

	// Conta registros
	var count int
	err = cnx_pg.QueryRow(`SELECT COUNT(*) FROM (select
									to_char(almoxarifadoid, 'fm000000000') cod,
									almoxarifadodescricao
								from
									almoxarifado a) as rn`).Scan(&count)
	if err != nil {
		panic("Falha ao contar registros: " + err.Error())
	}
	bar3 := p.AddBar(int64(count),
		mpb.PrependDecorators(
			decor.Name("DESTINO: "),
			decor.Percentage(),
		),
	)

	// Itera sobre o resultado
	var cod, desti string
	var empresa int
	for rows.Next() {
		err = rows.Scan(&cod, &desti)
		if err != nil {
			panic("Falha ao ler resultado: " + err.Error())
		}

		empresa = utils.GetEmpresa()

		_, err = insert.Exec(cod, desti, empresa)
		if err != nil {
			panic("Falha ao inserir dados: " + err.Error())
		}
		bar3.Increment()
	}
	// fmt.Println("Destino - Tempo de execução: ", time.Since(start))
}

func CentroCusto(p *mpb.Progress) {
	// start := time.Now()
	// Cria Conexão com os bancos
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Failed to count rows: " + err.Error())
	}
	defer cnx_fdb.Close()

	cnx_pg, err := conexao.ConexaoOrigem()
	if err != nil {
		panic("Falha ao conectar com o banco de origem: " + err.Error())
	}
	defer cnx_pg.Close()

	// Cria Campo de identificação
	cnx_fdb.Exec("ALTER TABLE CENTROCUSTO ADD ID_ANT INTEGER")      // Cria Campo de identificação
	cnx_fdb.Exec("ALTER TABLE CENTROCUSTO ADD COD_ANT VARCHAR(30)") // Cria Campo de identificação

	// Limpa tabela
	cnx_fdb.Exec("DELETE FROM CENTROCUSTO")

	// Prepara Insert
	insert, err := cnx_fdb.Prepare(`insert
                                into
                                centrocusto (poder,
                                orgao,
                                destino,
                                ccusto,
                                descr,
                                obs,
                                placa,
                                codccusto,
                                empresa,
                                unidade,
                                ocultar,
								id_ant,
								cod_ant)
                            values (?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		panic("Falha ao preparar insert: " + err.Error())
	}

	// Executa Select
	rows, err := cnx_pg.Query(`select
									substring(undorccodigo, 1, 2) poder,
									substring(undorccodigo, 4, 2) orgao,
									0 destino,
									'001' ccusto,
									undorcdescricao descr,
									undorccodigodesc obs,
									0 placa,
									undorcid codccusto,
									substring(undorccodigo, 7, 2) unidade,
									case when undorcsituacao = 'A' then 'N' else 'S' end as ocultar,
									undorcid id_ant,
									undorccodigo cod_ant
								from
									unidadeorcamentaria u
								join orgao o on u.undorcorgaoid = o.orgaoid 
								where cast(substring(o.orgaocodigodesc, 2, 1) as integer) = $1`, utils.GetEmpresa())
	if err != nil {
		panic("Falha ao executar select: " + err.Error())
	}

	// Conta registros
	var count int
	err = cnx_pg.QueryRow(`select count(*) from (select
									substring(undorccodigo, 1, 2) poder,
									substring(undorccodigo, 4, 2) orgao,
									0 destino,
									'001' ccusto,
									undorcdescricao descr,
									undorccodigodesc obs,
									0 placa,
									undorcid codccusto,
									substring(undorccodigo, 7, 2) unidade,
									case when undorcsituacao = 'A' then 'N' else 'S' end as ocultar,
									undorcid id_ant,
									undorccodigo cod_ant
								from
									unidadeorcamentaria u
								join orgao o on u.undorcorgaoid = o.orgaoid 
								where cast(substring(o.orgaocodigodesc, 2, 1) as integer) = $1)`, utils.GetEmpresa()).Scan(&count)
	if err != nil {
		panic("Falha ao contar registros: " + err.Error())
	}

	bar4 := p.AddBar(int64(count),
		mpb.PrependDecorators(
			decor.Name("CENTRO DE CUSTO: "),
			decor.Percentage(),
		),
	)

	// Itera sobre o resultado
	var poder, orgao, destino, ccusto, descr, obs, placa, unidade, ocultar, cod_ant string
	var codccusto, id_ant int
	for rows.Next() {
		err = rows.Scan(&poder, &orgao, &destino, &ccusto, &descr, &obs, &placa, &codccusto, &unidade, &ocultar, &id_ant, &cod_ant)
		if err != nil {
			panic("Falha ao ler resultado: " + err.Error())
		}

		_, err = insert.Exec(poder, orgao, destino, ccusto, descr, obs, placa, codccusto, utils.GetEmpresa(), unidade, ocultar, id_ant, cod_ant)
		if err != nil {
			panic("Falha ao inserir dados: " + err.Error())
		}
		bar4.Increment()
	}
	cnx_fdb.Exec(`INSERT
						INTO
						centrocusto (poder,
						orgao,
						destino,
						ccusto,
						descr,
						obs,
						placa,
						codccusto,
						empresa,
						unidade,
						ocultar,
						id_ant,
						cod_ant)
					SELECT FIRST 1
						poder,
						orgao,
						DESTINO,
						CCUSTO,
						'CONVERSAO',
						NULL,
						NULL,
						0,
						empresa,
						NULL,
						'N',
						NULL,
						NULL
					FROM CENTROCUSTO `)
	// fmt.Println("CentroCusto - Tempo de execução: ", time.Since(start))
}
