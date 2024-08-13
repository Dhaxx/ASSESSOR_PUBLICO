package compras

import (
	"ASSESSOR_PUBLICO/conexao"
	"time"
	"fmt"
)

func Cadunimedida() {
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

	cnx_fdb.Exec("DELETE FROM CADEST")  // Limpa tabela
    cnx_fdb.Exec("DELETE FROM CADUNIMEDIDA")  // Limpa tabela
	cnx_fdb.Exec("ALTER TABLE CADUNIMEDIDA ADD ID_ANT INTEGER")  // Cria Campo de identificação

	// Prepara Insert
	insert, err := cnx_fdb.Prepare(`INSERT INTO CADUNIMEDIDA(sigla, descricao, id_ant) VALUES(?,?,?)`)
	if err != nil {
		panic("Falha ao preparar insert: " + err.Error())
	}

	// Executa Select
	rows, err := cnx_pg.Query(`select
									substring(unidademedidadescricao,1,30) descricao,
									unidademedidasigla,
									unidademedidaid
								from
									unidademedida u`)
	if err != nil {
		panic("Falha ao executar select: " + err.Error())
	}

	// Itera sobre o resultado
	var sigla, descricao string
	var id_ant int

	for rows.Next() {
		err = rows.Scan(&descricao, &sigla, &id_ant)
		if err != nil {
			panic("Falha ao ler resultado: " + err.Error())
		}

		_, err = insert.Exec(sigla, descricao, id_ant)
		if err != nil {
			panic("Falha ao inserir dados: " + err.Error())
		}
	}
	fmt.Println("Cadunimedida - Tempo de execução: ", time.Since(start))
}

func GrupoSubgrupo() {
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

	// Limpa tabela
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
									hierarquiadesc,
									case when hierarquiasituacao = 'A' then 'N' else 'S' end ocultar,
									hierarquiagrupoid,
									hierarquiasubgrupoid
								from
									hierarquia h 
								order by hierarquianivel`)
	if err != nil {
		panic("Falha ao executar select: " + err.Error())
	}

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
	}
	fmt.Println("GrupoSubgrupo - Tempo de execução: ", time.Since(start))
}

func Cadest() {
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

	// Limpa tabela
	cnx_fdb.Exec("DELETE FROM CADEST")

	// Prepara Insert
	insert, err := cnx_fdb.Prepare(`INSERT
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
									c.unidademedidasigla unid1,
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

	// Itera sobre o resultado
	var grupo, subgrupo, disc1, tipopro, unid1, discr1, codreduz, ocultar string
	var intCodigo int
	for rows.Next() {
		err = rows.Scan(&grupo, &subgrupo, &intCodigo, &disc1, &tipopro, &unid1, &discr1, &codreduz, &ocultar)
		if err != nil {
			panic("Falha ao ler resultado: " + err.Error())
		}

		subgrupoCodigo := EstourouSubgr(intCodigo, subgrupo, grupo)

		cadpro := grupo +"."+subgrupoCodigo[0]+"."+ subgrupoCodigo[1]
		subgrupo = subgrupoCodigo[0]
		codigo := subgrupoCodigo[1]
		
		_, err = insert.Exec(cadpro, grupo, subgrupo, codigo, disc1, tipopro, unid1, discr1, codreduz, ocultar)
		if err != nil {
			panic("Falha ao inserir dados: " + err.Error())
		}
	}
	fmt.Println("Cadest - Tempo de execução: ", time.Since(start))
}

func Destino() {
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

	// Itera sobre o resultado
	var cod, desti string
	var empresa int
	for rows.Next() {
		err = rows.Scan(&cod, &desti)
		if err != nil {
			panic("Falha ao ler resultado: " + err.Error())
		}

		empresa = GetEmpresa()

		_, err = insert.Exec(cod, desti, empresa)
		if err != nil {
			panic("Falha ao inserir dados: " + err.Error())
		}
	}
	fmt.Println("Destino - Tempo de execução: ", time.Since(start))
}

func CentroCusto() {
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
                                ocultar)
                            values (?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		panic("Falha ao preparar insert: " + err.Error())
	}

	// Executa Select
	rows, err := cnx_pg.Query(`select
									substring(undorccodigo, 1, 2) poder,
									substring(undorccodigo, 4, 2) orgao,
									null destino,
									'001' ccusto,
									undorcdescricao descr,
									undorccodigodesc obs,
									null placa,
									row_number() over (partition by substring(undorccodigo, 1, 2) order by substring(undorccodigo, 1, 8)) codccusto,
									substring(undorccodigo, 7, 2) unidade,
									case when undorcsituacao = 'A' then 'N' else 'S' end as ocultar
								from
									unidadeorcamentaria u
								where undorcorgaoid = ?`, GetEmpresa())
	if err != nil {
		panic("Falha ao executar select: " + err.Error())
	}

	// Itera sobre o resultado
	var poder, orgao, destino, ccusto, descr, obs, placa, unidade, ocultar string
	var codccusto int
	for rows.Next() {
		err = rows.Scan(&poder, &orgao, &destino, &ccusto, &descr, &obs, &placa, &codccusto, &unidade, &ocultar)
		if err != nil {
			panic("Falha ao ler resultado: " + err.Error())
		}

		_, err = insert.Exec(poder, orgao, destino, ccusto, descr, obs, placa, codccusto, GetEmpresa(), unidade, ocultar)
		if err != nil {
			panic("Falha ao inserir dados: " + err.Error())
		}
	}
	fmt.Println("CentroCusto - Tempo de execução: ", time.Since(start))
}