package compras

import (
	"ASSESSOR_PUBLICO/CONEXAO"
	"database/sql"
	"fmt"
	"strconv"
	"time"
)

func Cadlic() {
	start := time.Now()
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		fmt.Println(err)
	}
	defer cnx_fdb.Close()

	cnx_pg, err := conexao.ConexaoOrigem()
	if err != nil {
		fmt.Println(err)
	}
	defer cnx_pg.Close()

	// Query
	rows, err := cnx_pg.Query(`select
								a.forprocessonumero numpro,
								forprocessodata datae,
								forprocessoaudienciapublicadata dtpub,
								forprocessodatafimcred dtenc,
								forprocessohorainiciocred horabe,
								forprocessodatainiciocred,
								forprocessojustificativalimitedispensa discr,
								case when forprocessoagruparitens = 'S' then 'Menor Preco Global' else 'Menor Preco Unitario' end discr7,
								case 
									when b.controletipocampo = 40 and controletipoid = 670 then 'IN01'
									when b.controletipocampo = 40 and controletipoid in (671, 681, 678) then 'DI01'
									when b.controletipocampo = 40 and controletipoid = 672 then 'CCO2'
									when b.controletipocampo = 40 and controletipoid = 673 then 'TOM3'
									when b.controletipocampo = 40 and controletipoid in (674,675) then 'CON4'
									when b.controletipocampo = 40 and controletipoid = 676 then 'PE01'
									when b.controletipocampo = 40 and controletipoid = 677 then 'PP01'
									when b.controletipocampo = 40 and controletipoid = 679 then 'LEIL'
									when b.controletipocampo = 40 and controletipoid = 680 then 'CS01'
								end modlic,
								null dthom,
								null dtadj,
								forprocessosituacao comp_ant,
								forprocessonumero,
								forprocessoano,
								a.forprocessoregistropreco,
								'T' ctlance,
								case when forprocessoobraid is null then 'N' else 'S' end obra,
								to_char(forprocessonumero, 'fm000000/')||forprocessoano %2000 proclic,
								a.forprocessoid,
								2 microempresa,
								1 licnova,
								'$' tlance,
								'N' mult_entidade,
								a.forprocessoano,
								'N' lei_invertfasestce,
								a.forprocessovalorestimado,
								forprocessojustificativa detalhe,
								a.forprocessocondicaopagamento,
								a.forprocessoaudespcodigo codtce,
								case when a.forprocessoaudespcodigo is not null then 'S' else 'N' end enviotce
							from
								formalizacaoprocesso a
							left join controletipo b on a.forprocessomodalidadeid = b.controletipoid
							where forprocessougid = 2 
							order by a.forprocessoano desc, a.forprocessonumero `)
	if err != nil {
		fmt.Println(err)
	}

	// Prepara Insert
	insert, err := cnx_fdb.Prepare(`insert into cadlic (numpro,
										datae,
										dtpub,
										dtenc,
										horabe,
										discr,
										discr7,
										modlic,
										dthom,
										dtadj,
										comp_ant,
										numero,
										registropreco,
										ctlance,
										obra,
										proclic,
										numlic,
										microempresa,
										licnova,
										tlance,
										mult_entidade,
										ano,
										lei_invertfasestce,
										valorestimado,
										detalhe,
										condicaopagamento,
										codtce,
										enviotce) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28)`)
	if err != nil {
		fmt.Println(err)
	}
	
	fmt.Println("Cadlic - Tempo de execução: ", time.Since(start))
}