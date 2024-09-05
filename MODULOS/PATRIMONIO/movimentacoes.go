package patrimonio

import (
	conexao "ASSESSOR_PUBLICO/CONEXAO"
	utils "ASSESSOR_PUBLICO/MODULOS/utils"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"github.com/gobuffalo/nulls"
)

func Aquisicoes(p *mpb.Progress) {
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic(err)
	}
	defer cnx_fdb.Close()

	bar27 := p.AddBar(1, mpb.PrependDecorators(
		decor.Name("PT_MOVBEM: "),
		), mpb.AppendDecorators(
		decor.Percentage(),
		),
	)

	cnx_fdb.Exec("alter SEQUENCE gen_pt_movbem_id RESTART WITH (select max(codigo_mov) from pt_movbem);")
	_, err = cnx_fdb.Exec(`EXECUTE BLOCK AS
						BEGIN
							DELETE FROM PT_MOVBEM WHERE TIPO_MOV = 'A';
							INSERT
								INTO
								pt_movbem (empresa_mov,
								codigo_mov,
								codigo_pat_mov,
								data_mov,
								tipo_mov,
								codigo_cpl_mov,
								codigo_set_mov,
								valor_mov,
								documento_mov,
								historico_mov,
								HASH_SINC)
							SELECT
								EMPRESA_PAT,
								gen_id(gen_pt_movbem_id,1) as seq,
								CODIGO_PAT,
								DTLAN_PAT,
								'A' tipo_mov,
								CODIGO_CPL_PAT,
								CODIGO_SET_PAT,
								VALAQU_PAT,
								NOTA_PAT,
								'AQUISIÇÃO' HISTORICO_MOV,
								CODIGO_PAT 
							FROM PT_CADPAT a 
							WHERE  NOT EXISTS (SELECT 1 FROM pt_movbem b WHERE a.CODIGO_PAT = b.codigo_pat_mov AND b.tipo_mov = 'A');
							
							UPDATE PT_MOVBEM SET HASH_SINC = HASH_SINC*1000;
							UPDATE PT_MOVBEM SET HASH_SINC = CODIGO_MOV;
						END`)
	if err != nil {
		panic(err)
	}
	bar27.Increment()
}

func Transferencias(p *mpb.Progress) {
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

	cnx_fdb.Exec("alter table pt_movbem add unid_ant integer")
	cnx_fdb.Exec("alter table pt_movbem add subunid_ant integer")

	// Limpa Tabela
	cnx_fdb.Exec("DELETE FROM PT_MOVBEM WHERE TIPO_MOV IN ('T','P');")

	// Query
	rows, err := cnx_psq.Query(`select
			c.transfbemitemincorporacaoid codigo_mov,
			'TRANSFERÊNCIA DE BENS: '||transferenciabemcodigo||' - '|| transferenciabemdata historico_mov,
			c.transferbemitemanterundorcid unidade_ant,
			c.transferbemitemanterdestinoid subunid_ant,
			transferenciabemunidorcid unidade_dest,
			transferenciabemdestinoid subunid_dest,
			replace(b.contacontabilcodigoniveltce,'.','') codigo_cpl_mov,
			cast(transferenciabemdata as varchar) data_mov,
			transferenciabemgestoraid empresa,
			case when transferenciabemunidorcid is null and transferenciabemdestinoid is null then 'P' else 'T' end tipo_mov,
			replace(b2.contacontabilcodigoniveltce,'.','') cpl_mov_ant
		from
			transferenciabem a
		join contacontabil b on a.transferenciabemctactbid = b.contacontabilid 
		join transferenciabemitem c on c.transferenciabemid = a.transferenciabemid 
		join contacontabil b2 on b2.contacontabilid = c.transferbemitemantercontacontabilid
		where
			transferenciabemgestoraid = $1
		order by a.transferenciabemid`, utils.GetEmpresa())
	if err != nil {
		panic(err)
	}

	var count int
	err = cnx_psq.QueryRow(`SELECT COUNT(*) FROM (select
			c.transfbemitemincorporacaoid codigo_mov,
			'TRANSFERÊNCIA DE BENS: '||transferenciabemcodigo||' - '|| transferenciabemdata historico_mov,
			c.transferbemitemanterundorcid unidade_ant,
			c.transferbemitemanterdestinoid subunid_ant,
			transferenciabemunidorcid unidade_dest,
			transferenciabemdestinoid subunid_dest,
			replace(b.contacontabilcodigoniveltce,'.','') codigo_cpl_mov,
			cast(transferenciabemdata as varchar) data_mov,
			transferenciabemgestoraid empresa,
			case when transferenciabemunidorcid is null and transferenciabemdestinoid is null then 'P' else 'T' end tipo_mov,
			replace(b2.contacontabilcodigoniveltce,'.','') cpl_mov_ant
		from
			transferenciabem a
		join contacontabil b on a.transferenciabemctactbid = b.contacontabilid 
		join transferenciabemitem c on c.transferenciabemid = a.transferenciabemid 
		join contacontabil b2 on b2.contacontabilid = c.transferbemitemantercontacontabilid
		where
			transferenciabemgestoraid = $1
		order by a.transferenciabemid) as rn`, utils.GetEmpresa()).Scan(&count)
	if err != nil {
		panic(err)
	}
	bar28 := p.AddBar(int64(count), mpb.PrependDecorators(
		decor.Name("PT_MOVBEM: "),
		), mpb.AppendDecorators(
		decor.Percentage(),
		),
	)

	// Consulta auxiliar
	setores := make(map[Key]int)
	aux, err := cnx_fdb.Query(`select codigo_set, codigo_des_set, subunid_ant from pt_cadpats`)
	if err != nil {
		panic(err)
	}
	for aux.Next() {
		var codigo_set, codigo_des_set, subunid_ant int
		err = aux.Scan(&codigo_set, &codigo_des_set, &subunid_ant)
		if err != nil {
			panic(err)
		}
		setores[Key{codigo_des_set, subunid_ant}] = codigo_set
	}

	var codigo_mov int
	cnx_fdb.QueryRow("select max(codigo_mov) from pt_movbem").Scan(&codigo_mov)

	tx, err := cnx_fdb.Begin()
	if err != nil {
		panic(err)
	}
	// Prepara Insert
	insert, err := tx.Prepare(`insert into pt_movbem (codigo_mov, empresa_mov, codigo_pat_mov, data_mov, tipo_mov, codigo_cpl_mov, codigo_set_mov, historico_mov, hash_sinc, unid_ant, subunid_ant, codigo_cpl_ant_mov) values (?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		panic(err)
	}

	for rows.Next() {
		codigo_mov++
		var codigo_pat_mov, unidade, subunid_ant, cpl_mov, empresa, unidade_ant, subunid_dest, cpl_mov_ant nulls.Int
		var historico_mov, data_mov, tipo_mov nulls.String
		err = rows.Scan(&codigo_pat_mov, &historico_mov, &unidade_ant, &subunid_ant, &unidade, &subunid_dest, &cpl_mov, &data_mov, &empresa, &tipo_mov, &cpl_mov_ant)
		if err != nil {
			panic(err)
		}

		codigo_set_mov := setores[Key{unidade.Int, subunid_dest.Int}]
		_, err = insert.Exec(codigo_mov, empresa, codigo_pat_mov, data_mov, tipo_mov, cpl_mov, codigo_set_mov, historico_mov, codigo_mov, unidade_ant, subunid_ant, cpl_mov_ant)
		if err != nil {
			panic(err)
		}
		bar28.Increment()
	}
	tx.Commit()
}

func Baixas(p *mpb.Progress) {
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic(err)
	}

	cnx_psq, err := conexao.ConexaoOrigem()
	if err != nil {
		panic(err)
	}

	// Limpa Tabela
	cnx_fdb.Exec("DELETE FROM PT_MOVBEM WHERE TIPO_MOV = 'B';")

	// Query
	rows, err := cnx_psq.Query(`select
			baixagestoraid,
			b.baixaincorporacaoid,
			cast(a.baixadata as varchar) baixadata,
			baixaoperacao codigo_bai_mov,
			'B' tipo_mov,
			-b.baixaincorporacaovalor,
			baixahistorico
		from
			baixa a
		join baixaincorporacao b on
			a.baixaid = b.baixaid
		where
			baixagestoraid = $1`, utils.GetEmpresa())
	if err != nil {
		panic(err)
	}

	var count int
	err = cnx_psq.QueryRow(`SELECT COUNT(*) FROM (select
			baixagestoraid,
			b.baixaincorporacaoid,
			cast(a.baixadata as varchar) baixadata,
			baixaoperacao codigo_bai_mov,
			'B' tipo_mov,
			b.baixaincorporacaovalor,
			baixahistorico
		from
			baixa a
		join baixaincorporacao b on
			a.baixaid = b.baixaid
		where
			baixagestoraid = $1) as rn`, utils.GetEmpresa()).Scan(&count)
	if err != nil {
		panic(err)
	}

	bar30 := p.AddBar(int64(count), mpb.PrependDecorators(
		decor.Name("PT_MOVBEM: "),
		), mpb.AppendDecorators(
		decor.Percentage(),
		),
	)

	var codigo_mov int
	cnx_fdb.QueryRow("select max(codigo_mov) from pt_movbem").Scan(&codigo_mov)

	tx, err := cnx_fdb.Begin()
	if err != nil {
		panic(err)
	}
	// Prepara Insert
	insert, err := tx.Prepare(`insert into pt_movbem (empresa_mov, codigo_mov, codigo_pat_mov, data_mov, tipo_mov, valor_mov, historico_mov, codigo_bai_mov, hash_sinc) values (?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		panic(err)
	}

	for rows.Next() {
		codigo_mov++
		var empresa, codigo_pat_mov, codigo_bai_mov nulls.Int
		var data_mov, tipo_mov, historico_mov nulls.String
		var valor_mov nulls.Float64
		err = rows.Scan(&empresa, &codigo_pat_mov, &data_mov, &codigo_bai_mov, &tipo_mov, &valor_mov, &historico_mov)
		if err != nil {
			panic(err)
		}

		_, err = insert.Exec(utils.GetEmpresa(), codigo_mov, codigo_pat_mov, data_mov, tipo_mov, valor_mov, historico_mov, codigo_bai_mov, codigo_mov)
		if err != nil {
			panic(err)
		}
		bar30.Increment()
	}
	tx.Commit()

	cnx_fdb.Exec(``)
}

func Reavaliacao(p *mpb.Progress) {
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic(err)
	}

	cnx_psq, err := conexao.ConexaoOrigem()
	if err != nil {
		panic(err)
	}

	// Limpa Tabela
	cnx_fdb.Exec("DELETE FROM PT_MOVBEM WHERE TIPO_MOV = 'R' and depreciacao_mov = 'N';")

	// Insert
	insert, err := cnx_fdb.Prepare(`insert into pt_movbem (empresa_mov, codigo_mov, codigo_pat_mov, data_mov, tipo_mov, depreciacao_mov, historico_mov, valor_mov, hash_sinc, codigo_cpl_mov) values (?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		panic(err)
	}

	// Consulta
	rows, err := cnx_psq.Query(`select
			incorpgestoraid,
			a.incorporacaoid,
			'R' tipo_mov,
			'N' depreciacao_mov,
			'REAVALIAÇÃO' as historico_mov,
			cast(a.incorporacaohistoricodata as varchar) data_mov,
			cast(replace(replace(incorporacaohistoricode,'.',''),',','.') as float)-cast(replace(replace(incorporacaohistoricode,'.',''),',','.') as float) valor_mov
		from
			incorporacaohistorico a
		join incorporacao b on
			a.incorporacaoid = b.incorporacaoid
		where
			incorporacaohistoricoacao = 10
			and incorpgestoraid = $1`, utils.GetEmpresa())
	if err != nil {
		panic(err)
	}

	var count int
	err = cnx_psq.QueryRow(`SELECT COUNT(*) FROM (select
			incorpgestoraid,
			a.incorporacaoid,
			'R' tipo_mov,
			'N' depreciacao_mov,
			'REAVALIAÇÃO' as historico_mov,
			cast(a.incorporacaohistoricodata as varchar) data_mov,
			cast(replace(replace(incorporacaohistoricode,'.',''),',','.') as float)-cast(replace(replace(incorporacaohistoricode,'.',''),',','.') as float) valor_mov
		from
			incorporacaohistorico a
		join incorporacao b on
			a.incorporacaoid = b.incorporacaoid
		where
			incorporacaohistoricoacao = 10
			and incorpgestoraid = $1) as rn`, utils.GetEmpresa()).Scan(&count)
	if err != nil {
		panic(err)
	}

	bar31 := p.AddBar(int64(count), mpb.PrependDecorators(
		decor.Name("PT_MOVBEM: "),
		), mpb.AppendDecorators(
		decor.Percentage(),
		),
	)

	var codigo_mov int
	cnx_fdb.QueryRow("select max(codigo_mov) from pt_movbem").Scan(&codigo_mov)

	for rows.Next() {
		codigo_mov++
		var empresa, codigo_pat_mov, valor_mov nulls.Float64
		var data_mov, tipo_mov, depreciacao_mov, historico_mov nulls.String

		err = rows.Scan(&empresa, &codigo_pat_mov, &tipo_mov, &depreciacao_mov, &historico_mov, &data_mov, &valor_mov)
		if err != nil {
			panic(err)
		}

		_, err = insert.Exec(empresa, codigo_mov, codigo_pat_mov, data_mov, tipo_mov, depreciacao_mov, historico_mov, valor_mov, codigo_mov, "237110301")
		if err != nil {
			panic(err)
		}
		bar31.Increment()
	}	
}

func Depreciacao(p *mpb.Progress) {
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic(err)
	}

	cnx_psq, err := conexao.ConexaoOrigem()
	if err != nil {
		panic(err)
	}

	// Cria Campos
	cnx_fdb.Exec("ALTER TABLE PT_MOVBEM ADD valor_taxa double precision;")
	cnx_fdb.Exec("ALTER TABLE PT_MOVBEM ADD qtd_meses integer;")

	// Limpa Tabela
	cnx_fdb.Exec("DELETE FROM PT_MOVBEM WHERE TIPO_MOV = 'R' and depreciacao_mov = 'S';")

	// Insert
	insert, err := cnx_fdb.Prepare(`insert into pt_movbem (
			empresa_mov,
			codigo_mov,
			codigo_pat_mov,
			data_mov,
			tipo_mov,
			depreciacao_mov,
			codigo_cpl_mov,
			valor_mov,
			historico_mov,
			hash_sinc,
			valor_taxa,
			qtd_meses) values (?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		panic(err)
	}		

	// Consulta
	rows, err := cnx_psq.Query(`select
			atuvaloreshistoricogestoraid empresa_mov,
			atuvaloreshistoricoincorporacaoid codigo_pat_mov,
			cast(atuvaloreshistoricodatacorte as varchar) data_mov,
			'R' tipo_mov,
			'S' depreciacao_mov,
			substring(replace(atuvaloreshistoricoclassecodigo,'.',''),1,9),
			-atuvaloreshistoricovalortaxa*atuvaloreshistoricomesesaatualizar valor_mov,
			-atuvaloreshistoricovalortaxa valor_taxa,
			atuvaloreshistoricomesesaatualizar,
			atuvaloreshistoricocompetenciafinal
		from
			atualizacaovaloreshistorico a
		join incorporacao b on
			a.atuvaloreshistoricoincorporacaoid = b.incorporacaoid
		where atuvaloreshistoricogestoraid = $1`, utils.GetEmpresa())
	if err != nil {
		panic(err)
	}

	var count int
	err = cnx_psq.QueryRow(`SELECT COUNT(*) FROM (select
			atuvaloreshistoricogestoraid empresa_mov,
			atuvaloreshistoricoincorporacaoid codigo_pat_mov,
			atuvaloreshistoricodatacorte,
			'R' tipo_mov,
			'S' depreciacao_mov,
			substring(replace(atuvaloreshistoricoclassecodigo,'.',''),1,9),
			-atuvaloreshistoricovalortaxa*atuvaloreshistoricomesesaatualizar valor_mov,
			-atuvaloreshistoricovalortaxa valor_taxa,
			atuvaloreshistoricomesesaatualizar,
			atuvaloreshistoricocompetenciafinal
		from
			atualizacaovaloreshistorico a
		join incorporacao b on
			a.atuvaloreshistoricoincorporacaoid = b.incorporacaoid
		where atuvaloreshistoricogestoraid = $1) as rn`, utils.GetEmpresa()).Scan(&count)
	if err != nil {
		panic(err)
	}

	bar32 := p.AddBar(int64(count), mpb.PrependDecorators(
		decor.Name("PT_MOVBEM: "),
		), mpb.AppendDecorators(
		decor.Percentage(),
		),
	)

	var codigo_mov int
	cnx_fdb.QueryRow("select max(codigo_mov) from pt_movbem").Scan(&codigo_mov)

	for rows.Next() {
		codigo_mov++
		var empresa, codigo_pat_mov, qtd_meses nulls.Int
		var data_mov, tipo_mov, depreciacao_mov, codigo_cpl_mov, historico_mov nulls.String
		var valor_mov, valor_taxa nulls.Float64
		err = rows.Scan(&empresa, &codigo_pat_mov, &data_mov, &tipo_mov, &depreciacao_mov, &codigo_cpl_mov, &valor_mov, &valor_taxa, &qtd_meses, &historico_mov)
		if err != nil {
			panic(err)
		}

		_, err = insert.Exec(empresa, codigo_mov, codigo_pat_mov, data_mov, tipo_mov, depreciacao_mov, codigo_cpl_mov, valor_mov, historico_mov, codigo_mov, valor_taxa, qtd_meses)
		if err != nil {
			panic(err)
		}
		bar32.Increment()
	}
}