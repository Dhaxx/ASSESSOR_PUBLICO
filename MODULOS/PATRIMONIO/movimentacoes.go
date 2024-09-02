package patrimonio

import (
	conexao "ASSESSOR_PUBLICO/CONEXAO"
	utils "ASSESSOR_PUBLICO/MODULOS/utils"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	// "github.com/gobuffalo/nulls"
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

	// Limpa Tabela
	cnx_fdb.Exec("DELETE FROM PT_MOVBEM WHERE TIPO_MOV = 'T';")
	
	// Query
	rows, err := cnx_psq.Query(`select
			c.transfbemitemincorporacaoid codigo_mov,
			'TRANSFERÊNCIA DE BENS: '||transferenciabemcodigo||' - '|| transferenciabemdata historico_mov,
			transferenciabemunidorcid unidade,
			transferenciabemdestinoid subunid_ant,
			b.contacontabilcodigoniveltce,
			transferenciabemdata data_mov,
			transferenciabemgestoraid empresa
		from
			transferenciabem a
		join contacontabil b on a.transferenciabemctactbid = b.contacontabilid 
		join transferenciabemitem c on c.transferenciabemid = a.transferenciabemid 
		where
			transferenciabemgestoraid = $1`, utils.GetEmpresa())
	if err != nil {
		panic(err)
	}

	var count int
	err = cnx_psq.QueryRow(`SELECT COUNT(*) FROM (select
			c.transfbemitemincorporacaoid codigo_mov,
			'TRANSFERÊNCIA DE BENS: '||transferenciabemcodigo||' - '|| transferenciabemdata historico_mov,
			transferenciabemunidorcid unidade,
			transferenciabemdestinoid subunid_ant,
			b.contacontabilcodigoniveltce,
			transferenciabemdata data_mov,
			transferenciabemgestoraid empresa
		from
			transferenciabem a
		join contacontabil b on a.transferenciabemctactbid = b.contacontabilid 
		join transferenciabemitem c on c.transferenciabemid = a.transferenciabemid 
		where
			transferenciabemgestoraid = $1)`).Scan(&count)
	if err != nil {
		panic(err)
	}
	bar28 := p.AddBar(int64(count), mpb.PrependDecorators(
		decor.Name("PT_MOVBEM: "),
		), mpb.AppendDecorators(
		decor.Percentage(),
		),
	)

	for rows.Next() {
		var codigo_mov, historico_mov, unidade, subunid_ant, cpl_mov, data_mov, empresa
	}
}