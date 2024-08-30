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

	valores := make(map[string]string)

	valores["A"] = "AQUISIÇÃO"
	valores["B"] = "BAIXA"
	valores["T"] = "TRANSFERÊNCIA"
	valores["R"] = "PR. CONTÁBIL"
	valores["P"] = "TRANS. PLANO"

	bar19 := p.AddSpinner(1,
		mpb.PrependDecorators(
			decor.Name("PT_TIPOMOV: "),
		),
		mpb.AppendDecorators(
			decor.Percentage(),
		),
	)

	for sigla, valor := range valores {
		_, err := cnx_fdb.Exec("INSERT INTO PT_TIPOMOV (SIGLA, VALOR) VALUES (?, ?)", sigla, valor)
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

	cnx_fdb.Exec("INSERT INTO PT_CADAJUSTE (CODIGO_AJU, EMPRESA_AJU, DESCRICAO_AJU) VALUES (1, ?, 'REAVALIAÇÃO (ANTES DO CORTE)')", utils.GetEmpresa())
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
						baixagestoraid empresa_bai,
						case when baixaoperacao = 1 then 'ALIENAÇÃO'
							when baixaoperacao = 2 then 'DOAÇÃO'	
							when baixaoperacao = 3 then 'PERMUTA'	
							when baixaoperacao = 4 then 'COMODATO'	
							when baixaoperacao = 5 then 'FURTO/ROUBO/PERDA'	
							when baixaoperacao = 6 then 'INSERVÍVEL/OBSOLETO'	
							when baixaoperacao = 7 then 'OUTRAS BAIXAS'	
						end descricao_bai
					from
						baixa b 
					where baixagestoraid = $1`, utils.GetEmpresa())
	if err != nil {
		panic(err)
	}
}