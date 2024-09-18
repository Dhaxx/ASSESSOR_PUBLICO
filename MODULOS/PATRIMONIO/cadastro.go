package patrimonio

import (	
	conexao "ASSESSOR_PUBLICO/CONEXAO"
	utils "ASSESSOR_PUBLICO/MODULOS/utils"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"github.com/gobuffalo/nulls"
)

type Key struct {
    First  int
    Second int
}

func PtCadpat(p *mpb.Progress) {
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
	cnx_fdb.Exec("DELETE FROM PT_CADPAT")

	// Prepara Insert
	insert, err := cnx_fdb.Prepare(`insert
                                into
                                pt_cadpat (codigo_pat,
                                            empresa_pat,
                                            codigo_gru_pat,
                                            chapa_pat,
                                            codigo_cpl_pat,
                                            codigo_set_pat,
                                            codigo_set_atu_pat,
                                            orig_pat,
                                            codigo_tip_pat,
                                            codigo_sit_pat,
                                            discr_pat,
                                            obs_pat,
                                            datae_pat,
                                            dtlan_pat,
                                            valaqu_pat,
                                            valatu_pat,
                                            codigo_for_pat,
                                            percenqtd_pat,
                                            dae_pat,
                                            valres_pat,
                                            percentemp_pat,
                                            nota_pat,
											hash_sinc)
                            values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?);`)
	if err != nil {
		panic(err)
	}

	// Query
	rows, err := cnx_psq.Query(`select
			incorporacaoid codigo_pat,
			incorpgestoraid,
			coalesce(incorporacaonatureza,0) grupo,
			to_char(incorporacaoplaquetanumero,
			'fm000000'),
			cast(replace(b.contacontabilcodigoniveltce,
			'.',
			'') as INTEGER) as codigo_cpl_pat,
			incorpundorcid unidade_orc,
			incorporacaodestinoid subunid_ant,
			case  
				when incorporacaooperacao = 1 then 'C'
				--COMPRA
				when incorporacaooperacao = 2 then 'D'
				--DOAÇÃO
				when incorporacaooperacao = 3 then 'P'
				--PERMUTA
				when incorporacaooperacao = 4 then 'O'
				--COMODATO
				when incorporacaooperacao = 5 then 'I'
				--DAÇÃO PRÓPRIA
				when incorporacaooperacao = 6 then 'I'
				--CONSTRUÇÃO
				when incorporacaooperacao = 7 then 'F'
				--FABRICAÇÃO
				when incorporacaooperacao = 8 then 'I'
				--OUTRAS INCORPORAÇÕES
			end orig_pat,
			contacontabilid CODIGO_TIP_PAT,
			incorporacaoestadoconserv,
			incorporacaodescricao,
			incorporacaodiscriminacao obs_pat,
			cast(incorporacaodata as varchar) datae_pat,
			cast(incorporacaodataaquisicao as varchar) dtlan_pat,
			incorporacaovalorincorp valaqu_pat,
			incorporacaovaloratual valatu_pat,
			incorpfornecedorid,
			'N' dae_pat,
			incorporacaovalorresidual valres,
			null percentemp_pat,
			incorporacaonotaentrada
		from
			incorporacao a
		left join contacontabil b on
			a.incorpcontacontabilid = b.contacontabilid
		where
			incorpgestoraid = $1`, utils.GetEmpresa())
	if err != nil {
		panic(err)
	}

	// Conta registros
	var count int
	err = cnx_psq.QueryRow(`select count(*) from (select
			incorporacaoid codigo_pat,
			incorpgestoraid,
			incorporacaonatureza grupo,
			to_char(incorporacaoplaquetanumero,
			'fm000000'),
			cast(replace(b.contacontabilcodigoniveltce,
			'.',
			'') as INTEGER) as codigo_cpl_pat,
			incorpundorcid unidade_orc,
			incorporacaodestinoid subunid_ant,
			case  
				when incorporacaooperacao = 1 then 'C'
				--COMPRA
				when incorporacaooperacao = 2 then 'D'
				--DOAÇÃO
				when incorporacaooperacao = 3 then 'P'
				--PERMUTA
				when incorporacaooperacao = 4 then 'O'
				--COMODATO
				when incorporacaooperacao = 5 then 'I'
				--DAÇÃO PRÓPRIA
				when incorporacaooperacao = 6 then 'I'
				--CONSTRUÇÃO
				when incorporacaooperacao = 7 then 'F'
				--FABRICAÇÃO
				when incorporacaooperacao = 8 then 'I'
				--OUTRAS INCORPORAÇÕES
			end orig_pat,
			contacontabilid CODIGO_TIP_PAT,
			incorporacaoestadoconserv,
			incorporacaodescricao,
			incorporacaodiscriminacao obs_pat,
			cast(incorporacaodata as varchar) datae_pat,
			cast(incorporacaodataaquisicao as varchar) dtlan_pat,
			incorporacaovalorincorp valaqu_pat,
			incorporacaovaloratual valatu_pat,
			incorpfornecedorid,
			'N' dae_pat,
			incorporacaovalorresidual valres,
			null percentemp_pat,
			incorporacaonotaentrada
		from
			incorporacao a
		left join contacontabil b on
			a.incorpcontacontabilid = b.contacontabilid
		where
			incorpgestoraid = $1) as rn`, utils.GetEmpresa()).Scan(&count)
	if err != nil {
		panic(err)
	}

	bar26 := p.AddBar(int64(count), mpb.PrependDecorators(
		decor.Name("PT_CADPAT: "),
	), mpb.AppendDecorators(
		decor.Percentage(),
	))

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

	for rows.Next() {
		var codigo_pat, empresa_pat, codigo_gru_pat, codigo_cpl_pat, unidade_orc, subunid_ant, codigo_tip_pat, codigo_sit_pat, codigo_for_pat, percenqtd_pat nulls.Int
		var chapa_pat, orig_pat, discr_pat, obs_pat, datae_pat, dtlan_pat, dae_pat, percentemp_pat, nota_pat nulls.String
		var valaqu_pat, valatu_pat, valres_pat nulls.Float64
		err = rows.Scan(&codigo_pat, &empresa_pat, &codigo_gru_pat, &chapa_pat, &codigo_cpl_pat, &unidade_orc, &subunid_ant, &orig_pat,
						&codigo_tip_pat, &codigo_sit_pat, &discr_pat, &obs_pat, &datae_pat, &dtlan_pat, &valaqu_pat, &valatu_pat, &codigo_for_pat,
						&dae_pat, &valres_pat, &percentemp_pat, &nota_pat)
		if err != nil {
			panic(err)
		}

		codigo_set_pat := setores[Key{unidade_orc.Int, subunid_ant.Int}]

		_, err = insert.Exec(codigo_pat, empresa_pat, codigo_gru_pat, chapa_pat, codigo_cpl_pat, codigo_set_pat, codigo_set_pat, orig_pat,
						  codigo_tip_pat, codigo_sit_pat, discr_pat, obs_pat, datae_pat, dtlan_pat, valaqu_pat, valatu_pat, codigo_for_pat, 
						  percenqtd_pat, dae_pat, valres_pat, percentemp_pat, nota_pat, codigo_pat)
		if err != nil {
			panic(err)
		}
		bar26.Increment()
	}
}