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
