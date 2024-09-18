package patrimonio

import (
	conexao "ASSESSOR_PUBLICO/CONEXAO"
	utils "ASSESSOR_PUBLICO/MODULOS/utils"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

func TipoMov(p *mpb.Progress) {
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic(err)
	}
	defer cnx_fdb.Close()

	// Limpa Tabela
	cnx_fdb.Exec("DELETE FROM PT_TIPOMOV")

	valores := make(map[string]string)

	valores["A"] = "AQUISIÇÃO"
	valores["B"] = "BAIXA"
	valores["T"] = "TRANSFERÊNCIA"
	valores["R"] = "PR. CONTÁBIL"
	valores["P"] = "TRANS. PLANO"

	bar19 := p.AddBar(1,
		mpb.PrependDecorators(
			decor.Name("PT_TIPOMOV: "),
		),
		mpb.AppendDecorators(
			decor.Percentage(),
		),
	)

	for sigla, valor := range valores {
		_, err := cnx_fdb.Exec("INSERT INTO PT_TIPOMOV (codigo_tmv, descricao_tmv) VALUES (?, ?)", sigla, valor)
		if err != nil {
			panic(err)
		}
	}
	bar19.Increment()
}

func TiposAjuste(p *mpb.Progress) {
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic(err)
	}
	defer cnx_fdb.Close()

	// Limpa Tabela
	cnx_fdb.Exec("DELETE FROM PT_CADAJUSTE")

	bar20 := p.AddBar(1, mpb.PrependDecorators(
		decor.Name("PT_CADAJUSTE: "),
		), mpb.AppendDecorators(
		decor.Percentage(),
	))
	cnx_fdb.Exec("INSERT INTO PT_CADAJUSTE (CODIGO_AJU, EMPRESA_AJU, DESCRICAO_AJU) VALUES (1, ?, 'REAVALIAÇÃO (ANTES DO CORTE)')", utils.GetEmpresa())
	bar20.Increment()
}

func TiposBaixa(p *mpb.Progress) {
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic(err)
	}
	defer cnx_fdb.Close()


	cnx_psq, err := conexao.ConexaoOrigem()
	if err != nil {
		panic(err)
	}
	defer cnx_psq.Close()

	// Limpa Tabela
	cnx_fdb.Exec("DELETE FROM PT_CADBAI")

	// Prepara insert

	// Query
	rows, err := cnx_psq.Query(`select
						distinct(baixaoperacao) codigo_bai,
						case when baixaoperacao = 1 then 'ALIENAÇÃO'
							when baixaoperacao = 2 then 'DOAÇÃO'	
							when baixaoperacao = 3 then 'PERMUTA'	
							when baixaoperacao = 4 then 'COMODATO'	
							when baixaoperacao = 5 then 'FURTO/ROUBO/PERDA'	
							when baixaoperacao = 6 then 'INSERVÍVEL/OBSOLETO'	
							when baixaoperacao = 7 then 'OUTRAS BAIXAS'	
						end descricao_bai
					from
						baixa b`)
	if err != nil {
		panic(err)
	}

	bar21 := p.AddBar(1, mpb.PrependDecorators(
		decor.Name("PT_CADBAI: "),
		), mpb.AppendDecorators(
		decor.Percentage(),
	))

	for rows.Next() {
		var codigo_bai int
		var descricao_bai string

		err = rows.Scan(&codigo_bai, &descricao_bai)
		if err != nil {
			panic(err)
		}

		_, err = cnx_fdb.Exec("INSERT INTO PT_CADBAI (CODIGO_BAI, EMPRESA_BAI, DESCRICAO_BAI) VALUES (?, ?, ?)", codigo_bai, utils.GetEmpresa(), descricao_bai)
		if err != nil {
			panic(err)
		}
	}
	bar21.Increment()
}

func TiposSituacao(p *mpb.Progress) {
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic(err)
	}
	defer cnx_fdb.Close()

	cnx_psq, err := conexao.ConexaoOrigem()
	if err != nil {
		panic(err)
	}
	defer cnx_psq.Close()

	// Limpa Tabela
	cnx_fdb.Exec("DELETE FROM PT_CADSIT")

	// Prepara insert
	cnx_fdb.Exec("INSERT INTO PT_CADSIT (CODIGO_SIT, EMPRESA_SIT, DESCRICAO_SIT) VALUES (?, ?, ?)", utils.GetEmpresa())

	// Query
	rows, err := cnx_psq.Query(`select
		distinct(incorporacaosituacao) codigo_sit, 
		case
		when incorporacaosituacao = 1 then 'Novo'
		when incorporacaosituacao = 2 then 'Bom'
		when incorporacaosituacao = 3 then 'Ruim'
		when incorporacaosituacao = 4 then 'Péssimo' end descricao_sit
		from
			incorporacao i`)
	if err != nil {
		panic(err)
	}

	bar22 := p.AddBar(1, mpb.PrependDecorators(
		decor.Name("PT_CADSIT: "),
		), mpb.AppendDecorators(
		decor.Percentage(),
	))

	for rows.Next() {
		var codigo_sit int
		var descricao_sit string

		err = rows.Scan(&codigo_sit, &descricao_sit)
		if err != nil {
			panic(err)
		}

		_, err = cnx_fdb.Exec("INSERT INTO PT_CADSIT (CODIGO_SIT, EMPRESA_SIT, DESCRICAO_SIT) VALUES (?, ?, ?)", codigo_sit, utils.GetEmpresa(), descricao_sit)
		if err != nil {
			panic(err)
		}
	}
	bar22.Increment()
}

func TiposBens(p *mpb.Progress) {
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic(err)
	}
	defer cnx_fdb.Close()

	cnx_psq, err := conexao.ConexaoOrigem()
	if err != nil {
		panic(err)
	}
	defer cnx_psq.Close()

	// Limpa Tabela
	cnx_fdb.Exec("DELETE FROM PT_CADTIP")

	// Cria Coluna
	cnx_fdb.Exec("ALTER TABLE PT_CADTIP ADD cod_ant integer")

	// Prepara insert
	insert, err := cnx_fdb.Prepare("INSERT INTO PT_CADTIP (CODIGO_TIP, EMPRESA_TIP, DESCRICAO_TIP, CODIGO_TCE_TIP, OCULTAR_TIP) VALUES (?, ?, ?, ?, 'N')")
	if err != nil {
		panic("Erro ao Prepara Insert: "+err.Error())
	}

	// Query
	rows, err := cnx_psq.Query(`SELECT
			contacontabilid,
			LEFT(descricao, 60) AS descricao,
			codigo_tce
		FROM
			(
				SELECT DISTINCT
					b.contacontabilid,
					REPLACE(b.contacontabildescricao, '(P)', '') AS descricao,
					CAST(REPLACE(b.contacontabilcodigoniveltce, '.', '') AS INTEGER) AS codigo_tce
				FROM
					incorporacao a
				JOIN
					contacontabil b ON a.incorpcontacontabilid = b.contacontabilid
			) AS rn;`)
	if err != nil {
		panic(err)
	}

	bar23 := p.AddBar(1, mpb.PrependDecorators(
		decor.Name("PT_CADTIP: "),
		), mpb.AppendDecorators(
		decor.Percentage(),
	))
	
	for rows.Next() {
		var descricao string
		var codigo, codigo_tce int
		err = rows.Scan(&codigo, &descricao, &codigo_tce)
		if err != nil {
			panic(err)
		}

		_, err = insert.Exec(codigo, utils.GetEmpresa(), descricao, codigo_tce)
		if err != nil {
			panic(err)
		}
	}
	bar23.Increment()
}

func Unidades(p *mpb.Progress) {
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic(err)
	}
	defer cnx_fdb.Close()

	cnx_psq, err := conexao.ConexaoOrigem()
	if err != nil {
		panic(err)
	}
	defer cnx_psq.Close()

	// Limpa Tabela
	cnx_fdb.Exec("DELETE FROM PT_CADPATS")
	cnx_fdb.Exec("DELETE FROM PT_CADPATD")

	// Query
	rows, err := cnx_psq.Query(`SELECT undorcid, cast(substring(undorccodigo, 1, 2) as integer) empresa, undorcdescricao, case when undorcsituacao = 'A' then 'N' else 'S' end ocultar FROM public.unidadeorcamentaria x 
								where cast(substring(undorccodigo, 1, 2) as integer) = $1`,  utils.GetEmpresa())
	if err != nil {
		panic(err)
	}

	// Prepara insert
	insert, err := cnx_fdb.Prepare("INSERT INTO PT_CADPATD (codigo_des, empresa_des, nauni_des, ocultar_des) VALUES (?, ?, ?, ?)")
	if err != nil {
		panic(err)
	}

	var count int
	err = cnx_psq.QueryRow("SELECT COUNT(*) FROM public.unidadeorcamentaria x where cast(substring(undorccodigo, 1, 2) as integer) = $1", utils.GetEmpresa()).Scan(&count)
	if err != nil {
		panic(err)
	}

	bar24 := p.AddBar(int64(count), mpb.PrependDecorators(
		decor.Name("PT_CADPATD: "),
	), mpb.AppendDecorators(
		decor.Percentage(),
	))

	for rows.Next() {
		var codigo, empresa, descricao, ocultar string
		err = rows.Scan(&codigo, &empresa, &descricao, &ocultar)
		if err != nil {
			panic(err)
		}

		_, err = insert.Exec(codigo, empresa, descricao, ocultar)
		if err != nil {
			panic(err)
		}
		bar24.Increment()
	}
}

func Subunidade(p*mpb.Progress) {
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic(err)
	}
	defer cnx_fdb.Close()

	cnx_psq, err := conexao.ConexaoOrigem()
	if err != nil {
		panic(err)
	}
	defer cnx_psq.Close()

	// Limpa Tabela
	tx, err := cnx_fdb.Begin()
	if err != nil {
		panic(err)
	}
	tx.Exec("DELETE FROM PT_CADPATS")
	tx.Commit()

	// Cria Campos 
	cnx_fdb.Exec("ALTER TABLE PT_CADPATS ADD subunid_ant integer")

	// Query
	rows, err := cnx_psq.Query(`WITH UniqueRecords AS (
			SELECT
				DISTINCT
				b.undorcid,
				b.undorcdescricao,
				a.incorporacaodestinoid,
				c.destinodescricao,
				CASE
					WHEN c.destinosituacao = 'A' THEN 'N'
					ELSE 'S'
				END AS ocultar
			FROM
				incorporacao a
			JOIN unidadeorcamentaria b ON
				a.incorpundorcid = b.undorcid
			JOIN destino c ON
				c.destinoid = a.incorporacaodestinoid
			JOIN orgao d ON
				d.orgaoid = b.undorcorgaoid
			WHERE
				CAST(d.orgaocodigo AS integer) = $1
		)
		SELECT
			undorcid,
			undorcdescricao,
			ROW_NUMBER() OVER (ORDER BY undorcid, incorporacaodestinoid, destinodescricao) AS "codigo_set",
			incorporacaodestinoid,
			destinodescricao,
			ocultar
		FROM
			UniqueRecords;`, utils.GetEmpresa())
	if err != nil {
		panic(err)
	}

	// Prepara insert
	insertSub, err := cnx_fdb.Prepare("insert into pt_cadpats (codigo_set, empresa_set, codigo_des_set, noset_set, ocultar_set, subunid_ant) values (?,?,?,?,?,?)")
	if err != nil {
		panic(err)
	}

	bar29 := p.AddBar(1, mpb.PrependDecorators(
		decor.Name("PT_CADPATS: "),
		), mpb.AppendDecorators(
		decor.Percentage(),
	))

	for rows.Next() {
		var codigo_des, codigo_set, subunid_ant int
		var nauni_des, noset_set, ocultar_des string
		err = rows.Scan(&codigo_des, &nauni_des, &codigo_set, &subunid_ant, &noset_set, &ocultar_des)
		if err != nil {
			panic(err)
		}

		_, err = insertSub.Exec(codigo_set, utils.GetEmpresa(), codigo_des, noset_set, ocultar_des, subunid_ant)
		if err != nil {
			println(err.Error())
		}
	}
	bar29.Increment()
}	

func Grupos(p *mpb.Progress) {
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic(err)
	}
	defer cnx_fdb.Close()

	cnx_psq, err := conexao.ConexaoOrigem()
	if err != nil {
		panic(err)
	}
	defer cnx_psq.Close()

	// Limpa Tabela
	cnx_fdb.Exec("DELETE FROM PT_CADPATG")

	// Query 
	rows, err := cnx_psq.Query(`select distinct 
		coalesce(incorporacaonatureza,0),
		case
			when incorporacaonatureza = 1 then 'Móveis'
			when incorporacaonatureza = 2 then 'Imóveis'
			when incorporacaonatureza = 3 then 'Intangível'
			else 'Geral'
		end nogru_gru
		from
			incorporacao a`)
	if err != nil {
		panic(err)
	}

	// Prepara insert
	insert, err := cnx_fdb.Prepare("INSERT INTO PT_CADPATG (CODIGO_GRU, EMPRESA_GRU, NOGRU_GRU, ocultar_gru) VALUES (?, ?, ?, ?)")
	if err != nil {
		panic(err)
	}

	bar25 := p.AddBar(1, mpb.PrependDecorators(
		decor.Name("PT_CADPATG: "),
		), mpb.AppendDecorators(
		decor.Percentage(),
	))

	for rows.Next() {
		var codigo, descricao string
		err = rows.Scan(&codigo, &descricao)
		if err != nil {
			panic(err)
		}

		_, err = insert.Exec(codigo, utils.GetEmpresa(), descricao, `N`)
		if err != nil {
			panic(err)
		}
	}
	bar25.Increment()
}