package compras

import (
	"ASSESSOR_PUBLICO/CONEXAO"
	"fmt"
	"time"

	"github.com/gobuffalo/nulls"
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
	rows, err := cnx_pg.Query(`SELECT 
									rn.*, 
									CASE
										WHEN modlic = 'IN01' THEN 5
										WHEN modlic = 'DI01' THEN 1
										WHEN modlic = 'CC02' THEN 2
										WHEN modlic = 'TOM3' THEN 3
										WHEN modlic = 'CON4' THEN 4
										WHEN modlic = 'PE01' THEN 9
										WHEN modlic = 'PP01' THEN 8
										WHEN modlic = 'LEIL' THEN 6
										WHEN modlic = 'CS01' THEN 7
									END AS codmod
								FROM (
									SELECT
										a.forprocessonumero AS numpro,
										CAST(forprocessodata AS VARCHAR) AS datae,
										CAST(forprocessoaudienciapublicadata AS VARCHAR) AS dtpub,
										CAST(forprocessodatafimcred AS VARCHAR) AS dtenc,
										forprocessohorainiciocred AS horabe,
										SUBSTRING(objetopadraodescricao, 1, 1024) AS discr,
										CASE 
											WHEN forprocessoagruparitens = 'S' THEN 'Menor Preco Global'
											ELSE 'Menor Preco Unitario'
										END AS discr7,
										CASE 
											WHEN b.controletipocampo = 40 AND controletipoid = 670 THEN 'IN01'
											WHEN b.controletipocampo = 40 AND controletipoid IN (671, 681, 678) THEN 'DI01'
											WHEN b.controletipocampo = 40 AND controletipoid = 672 THEN 'CCO2'
											WHEN b.controletipocampo = 40 AND controletipoid = 673 THEN 'TOM3'
											WHEN b.controletipocampo = 40 AND controletipoid IN (674, 675) THEN 'CON4'
											WHEN b.controletipocampo = 40 AND controletipoid = 676 THEN 'PE01'
											WHEN b.controletipocampo = 40 AND controletipoid = 677 THEN 'PP01'
											WHEN b.controletipocampo = 40 AND controletipoid = 679 THEN 'LEIL'
											WHEN b.controletipocampo = 40 AND controletipoid = 680 THEN 'CS01'
										END AS modlic,
										NULL AS dthom,
										NULL AS dtadj,
										COALESCE(forprocessosituacao, 0) AS comp_ant,
										forprocessonumero,
										forprocessoano,
										a.forprocessoregistropreco,
										'T' AS ctlance,
										CASE 
											WHEN forprocessoobraid IS NULL THEN 'N'
											ELSE 'S'
										END AS obra,
										TO_CHAR(a.forprocessoid, 'fm000000/') || forprocessoano % 2000 AS proclic,
										a.forprocessoid,
										2 AS microempresa,
										1 AS licnova,
										'$' AS tlance,
										'N' AS mult_entidade,
										a.forprocessoano,
										'N' AS lei_invertfasestce,
										a.forprocessovalorestimado,
										forprocessojustificativa AS detalhe,
										a.forprocessocondicaopagamento,
										a.forprocessoaudespcodigo AS codtce,
										CASE 
											WHEN a.forprocessoaudespcodigo IS NOT NULL THEN 'S'
											ELSE 'N'
										END AS enviotce,
										to_char(d.cotacaoprecosnumero,'fm00000/')||d.cotacaoprecosano%2000 numorc,
										e.processonumero,
										e.processoano
									FROM
										formalizacaoprocesso a
									LEFT JOIN 
										controletipo b ON a.forprocessomodalidadeid = b.controletipoid
									left join 
										objetopadrao c on c.objetopadraoid = a.forprocessoobjetoid
									left join 
										cotacaoprecos d on d.cotacaoprecosid = a.forprocessocotacaoid and d.cotacaoprecosversao = a.forprocessocotacaoversao 
									left join 
										processo e on e.processoid = d.cotacaoprecosprocessoid
									WHERE 
										forprocessougid = 2 
									ORDER BY 
										a.forprocessoano DESC, 
										a.forprocessonumero
								) AS rn;
								`)
	if err != nil {
		fmt.Println(err)
	}

	tx, err := cnx_fdb.Begin()
	if err != nil {
		fmt.Println(err)
	}
	// Prepara Insert
	insert, err := tx.Prepare(`insert into cadlic (numpro,
										datae,
										dtpub,
										dtenc,
										horabe,
										discr,
										discr7,
										modlic,
										dthom,
										dtadj,
										comp,
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
										valor,
										detalhe,
										discr9,
										codtce,
										enviotce,
										liberacompra,
										numorc,
										empresa,
										processo,
										processo_ano,
										codmod,
										anomod) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		fmt.Println(err)
	}

	// aux1, err := cnx_fdb.Query("select id_cadorc, id_ant from cadorc where flg_cotacao = 'S'")
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// var id_cadorc, id_ant int
	// idsCadorc := make(map[int]int)

	// for aux1.Next() {
	// 	err = aux1.Scan(&id_cadorc, &id_ant)
	// 	if err != nil {
	// 		fmt.Println(err)
	// 	}
	// 	idsCadorc[id_ant] = id_cadorc
	// }

	// Executa Insert
	var datae, dtpub, dtenc, horabe, discr, discr7, modlic, dthom, dtadj, registropreco, ctlance, obra, proclic, tlance, mult_entidade, lei_invertfasestce, detalhe, discr9, codtce, enviotce, numorc nulls.String
	var numpro, numero, numlic, microempresa, licnova, ano, processo, processo_ano, codmod nulls.Int
	var comp_ant int
	var valor nulls.Float64
	empresa := GetEmpresa()
	for rows.Next() {
		err = rows.Scan(&numpro, &datae, &dtpub, &dtenc, &horabe, &discr, &discr7, &modlic, &dthom, &dtadj, &comp_ant, &numero, &processo_ano, &registropreco, &ctlance, &obra, &proclic, &numlic, &microempresa, 
						&licnova, &tlance, &mult_entidade, &ano, &lei_invertfasestce, &valor, &detalhe, &discr9, &codtce, &enviotce, &numorc, &processo, &processo_ano, &codmod)
		if err != nil {
			panic(err)
		}

		liberacompra := `N`
		comp := 0

		if comp_ant == 1 || comp_ant == 14 { // Em formalização
			comp = 0 
		} else if comp_ant == 2 { // Em andamento
			comp = 1
		} else if comp_ant == 3 || comp_ant == 8 { // Ratificada ou Encerrado
			comp = 3
			liberacompra = `S`
		} else if comp_ant == 4 { // Fracassada
			comp = 6
		} else if comp_ant == 5 || comp_ant == 6 { // Cancelada ou Anulada
			comp = 4
		} else if comp_ant == 7 { // Revogada
			comp = 7
		} else if comp_ant == 9 { // Suspenso
			comp = 8
		} else if comp_ant == 10 || comp_ant == 12 || comp_ant == 13 || comp_ant == 15 { // Homologada, adjudicada parcialmente, homologada parcialmente ou ratificada parcialmente
			comp = 2
		} else if comp_ant == 11 { // Deserta
			comp = 5
		} else {
			comp = 0
		}
		_, err = insert.Exec(numpro, datae, dtpub, dtenc, horabe, discr, discr7, modlic, dthom, dtadj, comp, numero, registropreco, ctlance, obra, proclic, numlic, microempresa, licnova, tlance, mult_entidade, ano, lei_invertfasestce, valor, detalhe, discr9, codtce, enviotce, liberacompra, numorc, empresa, processo, processo_ano, codmod, processo_ano)
		if err != nil {
			fmt.Println(err)
		}
	}
	err = tx.Commit()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Cadlic - Tempo de execução: ", time.Since(start))

	start = time.Now()
	println("Atualizando CADORC...")
	cnx_fdb.Exec(`EXECUTE BLOCK AS
					DECLARE VARIABLE NUMLIC INTEGER;
					DECLARE VARIABLE NUMORC VARCHAR(8);
					DECLARE VARIABLE PROCLIC VARCHAR(9);
					BEGIN
						FOR 
							SELECT NUMLIC, PROCLIC, NUMORC FROM CADLIC WHERE NUMORC IS NOT NULL INTO :NUMLIC, :PROCLIC, :NUMORC
						DO
						BEGIN
							UPDATE CADORC SET PROCLIC = :PROCLIC, NUMLIC = :NUMLIC WHERE NUMORC = :NUMORC;
						END
						UPDATE CADLIC SET NUMORC = NULL;
					END`)	

	cnx_fdb.Exec(`EXECUTE BLOCK AS
					DECLARE VARIABLE DESCMOD VARCHAR(1024);
					DECLARE VARIABLE CODMOD INTEGER;
					BEGIN
						FOR
							SELECT CODMOD, DESCMOD FROM MODLIC INTO :CODMOD, :DESCMOD
						DO
						BEGIN
							UPDATE CADLIC SET LICIT = :DESCMOD WHERE CODMOD = :CODMOD;
						END
					END`)
	fmt.Println("Atualização de CADORC - Tempo de execução: ", time.Since(start))
}