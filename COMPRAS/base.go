package compras

import (
	"ASSESSOR_PUBLICO/conexao"
)

func Cadunimedida() {
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
}

func GrupoSubgrupo() {
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
}