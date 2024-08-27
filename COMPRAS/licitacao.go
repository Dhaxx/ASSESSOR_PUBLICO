package compras

import (
	"ASSESSOR_PUBLICO/CONEXAO"
	"database/sql"
	"fmt"
	"time"

	"github.com/gobuffalo/nulls"
)

func Cadlic() {
	start := time.Now()
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar ao banco:" + err.Error())
	}
	defer cnx_fdb.Close()

	cnx_pg, err := conexao.ConexaoOrigem()
	if err != nil {
		panic("Falha ao conectar ao banco:" + err.Error())
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
										/*CASE 
											WHEN forprocessoagruparitens = 'S' THEN 'Menor Preco Global'
											ELSE 'Menor Preco Unitario'
										END AS discr7,*/
										'Menor Preco Unitario' AS discr7,
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
		panic("Erro ao consultar no banco: " + err.Error())
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
		panic("Erro ao preparar insert " + err.Error())
	}

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
			panic("Erro ao scannear variáveis: " + err.Error())
		}

		liberacompra := `N`
		comp := 0

		if comp_ant == 1 || comp_ant == 15 { // Em formalização
			comp = 0 
		} else if comp_ant == 2 { // Em andamento
			comp = 1
		} else if comp_ant == 3 || comp_ant == 8 || comp_ant == 16 || comp_ant == 10 || comp_ant == 11 || comp_ant == 13 || comp_ant == 14 { // Ratificada ou Encerrado
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
		} else if comp_ant == 12 { // Deserta
			comp = 5
		}

		_, err = insert.Exec(numpro, datae, dtpub, dtenc, horabe, discr, discr7, modlic, dthom, dtadj, comp, numero, registropreco, ctlance, obra, proclic, numlic, microempresa, licnova, tlance, mult_entidade, ano, lei_invertfasestce, valor, detalhe, discr9, codtce, enviotce, liberacompra, numorc, empresa, processo, processo_ano, codmod, processo_ano)
		if err != nil {
			panic("Erro ao fazer inserção de dados" + err.Error())
		}
	}
	err = tx.Commit()
	if err != nil {
		panic("Erro ao fechar transaction" + err.Error())
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
	cnx_fdb.Exec(`UPDATE CADLIC SET anomod = ano where anomod is null`) 
	fmt.Println("Atualização de CADORC - Tempo de execução: ", time.Since(start))
}

func Cadprolic() {
	start := time.Now()
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Erro ao conectar no banco: " + err.Error())
	}
	defer cnx_fdb.Close()

	cnx_pg, err := conexao.ConexaoOrigem()
	if err != nil {
		panic("Erro ao conectar no banco: " + err.Error())
	}
	defer cnx_pg.Close()

	// Criando Campo Auxiliar
	cnx_fdb.Exec("ALTER TABLE CADPROLIC ADD ITEM_POR_LOTE INTEGER")

	// Limpando Tabela
	cnx_fdb.Exec("DELETE FROM CADPROLIC")
	cnx_fdb.Exec("DELETE FROM CADLOTELIC")

	// Query
	rows, err := cnx_pg.Query(`SELECT 
								numlic,
								lote,
								item,
								itemorc,
								codreduz,
								MIN(codccusto) AS codccusto,  -- Pega o menor valor de codccusto dentro do agrupamento
								SUM(quan1) AS quan1,          -- Soma as quantidades
								AVG(vamed1) AS vamed1,        -- Considerando que vamed1 é o mesmo valor, usando AVG para pegar um valor representativo
								SUM(vatomed1) AS vatomed1,    -- Soma os valores totais
								MIN(item_lc147) AS item_lc147 -- Pega o menor valor de item_lc147 dentro do agrupamento
							FROM (
								SELECT DISTINCT 
									numlic,
									lote,
									item,
									itemorc, 
									itemcompramaterialid AS codreduz, 
									codccusto, 
									COALESCE(itemcompraquantidade, 0) AS quan1, 
									COALESCE(itemcomprapropvalorunitario, 0) AS vamed1, 
									COALESCE(itemcomprapropvalortotal, 0) AS vatomed1,
									item_lc147
								FROM (
									SELECT
										a.itemcomprapropfornecedorid AS codif,
										1 AS sessao,
										c.pedidocompraforprocessoid AS numlic,
										TO_CHAR(d.loteordem, 'fm00000000') AS lote,
										COALESCE(b.itemcompranumitemseq, b.itemcompraordem) AS item,
										e.item AS itemorc,
										e.codccusto,
										b.itemcompramaterialid,
										b.itemcompraquantidade,
										a.itemcomprapropvalorunitario,
										a.itemcomprapropvalortotal,
										'C' AS status,
										1 AS subem,
										CASE 
											WHEN itemcompratipocota IN (1, 2) THEN NULL 
											ELSE e.item 
										END AS item_lc147
									FROM
										itemcompraproposta a
									JOIN itemcompra b ON
										a.itemcompraid = b.itemcompraid
										AND a.itemcompraversao = b.itemcompraversao
									JOIN pedidocompra c ON 
										c.pedidocompraid = b.itemcomprapedidoid 
										AND c.pedidocompraversao = b.itemcomprapedidoversao 
									LEFT JOIN lote d ON 
										b.itemcompraloteid = d.loteid 
									LEFT JOIN icadorc e ON 
										e.pedidocompraforprocessoid = c.pedidocompraforprocessoid 
										AND e.codreduz = b.itemcompramaterialid
									WHERE
										c.pedidocompraugid = $1 and itemcompraorigem <> 4
								) AS rn 
								--WHERE numlic = 716
							) AS aggregated_data where codreduz is not null 
							GROUP BY 
								numlic, lote, item, itemorc, codreduz;`, GetEmpresa())
	if err != nil {
		panic("Erro ao consultar dados: " + err.Error())
	}

	// Consulta Auxiliar
	cadpros := make(map[int]string)
	aux1, err := cnx_fdb.Query(`select cadpro, codreduz from cadest`)
	if err != nil {
		panic("Erro ao consultar cadpro" + err.Error())
	}
	for aux1.Next() {
		var cadpro string
		var codreduz int
		err = aux1.Scan(&cadpro, &codreduz)
		if err != nil {
			panic("Erro ao scannear cadpro" + err.Error())
		}
		cadpros[codreduz] = cadpro
	}

	// Consulta Auxiliar
	numlics := make(map[int]string)
	aux3, err := cnx_fdb.Query(`SELECT MIN(numorc) AS numorc, numlic FROM cadorc WHERE numlic IS NOT NULL GROUP BY numlic; `)
	if err != nil {
		panic("Erro ao consultar cadorc" + err.Error())
	}
	for aux3.Next() {
		var numorc string
		var numlic int
		err = aux3.Scan(&numorc, &numlic)
		if err != nil {
			panic("Erro ao scannear cadorc" + err.Error())
		}
		numlics[numlic] = numorc
	}

	// Prepara Insert
	insert, err := cnx_fdb.Prepare(`insert into cadprolic (numorc, lotelic, item, item_mask, itemorc, cadpro, codccusto, quan1, vamed1, vatomed1, reduz, microempresa, tlance, item_ag, numlic, id_cadorc, item_lote, item_lc147) 
									values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		panic("Erro ao preparar insert: " + err.Error())
	}

	var numorc, cadpro, reduz, microempresa, tlance string
	var lote nulls.String
	var codreduz, codccusto, item, numlic, id_cadorc int
	var itemorc, ilc147 nulls.Int
	var quan1, vamed1, vatomed1 nulls.Float64
	for rows.Next() {
		err = rows.Scan(&numlic, &lote, &item, &itemorc, &codreduz, &codccusto, &quan1, &vamed1, &vatomed1, &ilc147)
		if err != nil {
			panic("Erro ao scannear variáveis: " + err.Error())
		}

		if lote.Valid {
			existeLote := cnx_fdb.QueryRow(`select 1 from cadlotelic where lotelic = ? and numlic = ?`, lote, numlic).Scan()
			if existeLote == sql.ErrNoRows {
				cnx_aux, err := conexao.ConexaoDestino()
				if err != nil {
					panic("Erro ao conectar no banco: " + err.Error())
				}
				func () {
					descr := "Lote " + lote.String
					_, err = cnx_aux.Exec(`insert into cadlotelic (descr, lotelic, numlic) values (?,?,?)`, descr, lote, numlic)
					if err != nil {
						return
					}
				}()
				cnx_aux.Close()
			}
		}

		cadpro = cadpros[codreduz]
		reduz = `N`
		microempresa = `N`
		tlance = `$`

		_, err = insert.Exec(numorc, lote, item, item, itemorc, cadpro, codccusto, quan1, vamed1, vatomed1, reduz, microempresa, tlance, item, numlic, id_cadorc, item, ilc147)
		if err != nil {
			continue
		}
	}

	// cnx_fdb.Exec(`INSERT INTO cadprolic (NUMORC, lotelic, item, ITEM_MASK, ITEMORC, CADPRO, CODCCUSTO, quan1, VAMED1, VATOMED1, REDUZ, MICROEMPRESA, TLANCE, ITEM_AG, numlic, ID_CADORC)
	// SELECT
	// 	numorc,
	// 	lotelic,
	// 	item,
	// 	item item_mask,
	// 	item itemorc,
	// 	cadpro,
	// 	codccusto,
	// 	quan1,
	// 	vamed1,
	// 	vatomed1,
	// 	reduz,
	// 	microempresa,
	// 	tlance,
	// 	item item_ag,
	// 	numlic,
	// 	NULL id_cadorc
	// FROM
	// 	(
	// 	SELECT
	// 		rn.min_numorc AS numorc,
	// 		-- Usa o menor numorc para cada numlic
	// 		rn.lotelic,
	// 		ROW_NUMBER() OVER (PARTITION BY rn.numlic
	// 	ORDER BY
	// 		(
	// 		SELECT
	// 			a.disc1
	// 		FROM
	// 			cadest a
	// 		WHERE
	// 			a.cadpro = rn.cadpro)) item,
	// 		rn.cadpro,
	// 		rn.codccusto,
	// 		SUM(rn.qtd) AS quan1,
	// 		0 AS vamed1,
	// 		0 AS vatomed1,
	// 		rn.reduz,
	// 		rn.microempresa,
	// 		rn.tlance,
	// 		rn.numlic
	// 	FROM
	// 		(
	// 		SELECT
	// 			a.numorc,
	// 			MIN(a.numorc) OVER (PARTITION BY b.numlic) AS min_numorc,
	// 			-- Calcula o menor numorc para cada grupo de numlic
	// 		NULL AS lotelic,
	// 			a.cadpro,
	// 			a.CODCCUSTO,
	// 			a.qtd,
	// 			'N' AS reduz,
	// 			'N' AS microempresa,
	// 			'$' AS tlance,
	// 			b.numlic,
	// 			COUNT(DISTINCT a.numorc) OVER (PARTITION BY b.numlic) AS numorc_count
	// 		FROM
	// 			ICADORC a
	// 		JOIN CADORC b ON
	// 			a.ID_CADORC = b.ID_CADORC
	// 		WHERE
	// 			b.NUMLIC IS NOT NULL
	// 			AND b.FLG_COTACAO = 'N'
	// ) AS rn
	// 	WHERE
	// 		rn.numorc_count > 1
	// 		-- Filtra apenas os registros onde há diferentes numorc para o mesmo numlic
	// 	GROUP BY
	// 		rn.numlic,
	// 		rn.min_numorc,
	// 		-- Inclui min_numorc no GROUP BY para agrupamento correto
	// 		rn.lotelic,
	// 		rn.cadpro,
	// 		rn.codccusto,
	// 		rn.reduz,
	// 		rn.microempresa,
	// 		rn.tlance)
	// UNION ALL
	// SELECT
	// 	a.numorc,
	// 	NULL AS lotelic,
	// 	a.item,
	// 	a.item AS item_mask,
	// 	a.ITEMORC,
	// 	a.cadpro,
	// 	a.CODCCUSTO,
	// 	a.qtd,
	// 	a.valor AS vamed1,
	// 	a.valor AS vatomed1,
	// 	'N' AS reduz,
	// 	'N' AS microempresa,
	// 	'$' AS tlance,
	// 	a.item AS item_ag,
	// 	b.numlic,
	// 	a.ID_CADORC
	// FROM
	// 	ICADORC a
	// JOIN CADORC b ON
	// 	a.ID_CADORC = b.ID_CADORC
	// WHERE
	// 	b.NUMLIC IS NOT NULL
	// 	AND b.FLG_COTACAO = 'N'
	// 	AND b.numlic IN (
	// 	SELECT
	// 		numlic
	// 	FROM
	// 		ICADORC i
	// 	JOIN CADORC c ON
	// 		i.ID_CADORC = c.ID_CADORC
	// 	WHERE
	// 		c.NUMLIC IS NOT NULL
	// 		AND c.FLG_COTACAO = 'N'
	// 	GROUP BY
	// 		c.numlic
	// 	HAVING
	// 		COUNT(DISTINCT i.numorc) = 1) -- Apenas numlic vinculadas a uma única numorc);`)
	fmt.Println("Cadprolic - Tempo de execução: ", time.Since(start))
}

func CadprolicDetalhe() {
	start := time.Now()
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Erro ao conectar no banco: " + err.Error())
	}

	cnx_fdb.Exec("ALTER TRIGGER TBI_CADPROLIC_DETALHE_BLOQUEIO INACTIVE")
	cnx_fdb.Exec(`INSERT INTO CADPROLIC_DETALHE (NUMLIC,item,CADPRO,quan1,VAMED1,VATOMED1,marca,CODCCUSTO,ITEM_CADPROLIC)
					select numlic, item, cadpro, quan1, vamed1, vatomed1, marca, codccusto, item from cadprolic b where
					not exists (select 1 from cadprolic_detalhe c where b.numlic = c.numlic and b.item = c.item);`)
	cnx_fdb.Exec("ALTER TRIGGER TBI_CADPROLIC_DETALHE_BLOQUEIO ACTIVE;`)")
	fmt.Println("CadprolicDetalhe - Tempo de execução: ", time.Since(start))
}

func ProlicProlics() {
	start := time.Now()
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Erro ao conectar no banco: " + err.Error())
	}
	defer cnx_fdb.Close()

	cnx_pg, err := conexao.ConexaoOrigem()
	if err != nil {
		panic("Erro ao conectar no banco: " + err.Error())
	}
	defer cnx_pg.Close()

	// Limpando Tabela
	cnx_fdb.Exec("DELETE FROM PROLICS")
	cnx_fdb.Exec("DELETE FROM PROLIC")

	// Query
	rows, err := cnx_pg.Query(`select distinct
									b.pessoaid,
									substring(b.pessoanome,1,40) nome,
									--1 credenciado, 2 habilitado	
									'A' status,
									--a.habilitacaolicsituacaofornecedor,
									c.forprocessoid
									--c.forprocessoano
								from
									habilitacaolicitante a
								join pessoa b on
									a.habilitacaolicfornid = b.pessoaid
								join formalizacaoprocesso c on
									c.forprocessoid = a.habilitacaolicforprocessoid
									and a.habilitacaolicforprocessoversao = c.forprocessoversao
								where
									c.forprocessougid = $1 --and c.forprocessoid = 22026`, GetEmpresa())
	if err != nil {
		panic("Erro ao consultar dados: " + err.Error())
	}

	// Prepara Insert
	insertProlic, err := cnx_fdb.Prepare(`insert into prolic (codif, nome, status, numlic) values (?,?,?,?)`)
	if err != nil {
		panic("Erro ao preparar insert: " + err.Error())
	}
	insertProlics, err := cnx_fdb.Prepare(`insert into prolics (sessao, codif, status, representante, numlic, usa_preferencia) values (?,?,?,?,?,?)`)
	if err != nil {
		panic("Erro ao preparar insert: " + err.Error())
	}

	var codif, numlic, sessao int
	var nome, status, usa_preferencia string
	for rows.Next() {
		err = rows.Scan(&codif, &nome, &status, &numlic)
		if err != nil {
			panic("Erro ao scannear variáveis: " + err.Error())
		}
		sessao = 1
		usa_preferencia = `N`
		_, err = insertProlic.Exec(codif, nome, status, numlic)
		if err != nil {
			panic("Erro ao inserir dados: " + err.Error())
		}
		_, err = insertProlics.Exec(sessao, codif, status, nome, numlic, usa_preferencia)
		if err != nil {
			panic("Erro ao inserir dados: " + err.Error())
		}
	}
	cnx_fdb.Exec(`alter trigger TBI_CADPRO_STATUS_BLOQUEIO inactive;
					INSERT INTO cadpro_status (numlic, sessao, itemp, item, telafinal)
					SELECT b.NUMLIC, 1 AS sessao, a.item, a.item, 'I_ENCERRAMENTO'
					FROM CADPROLIC a
					JOIN cadlic b ON a.NUMLIC = b.NUMLIC
					WHERE NOT EXISTS (
						SELECT 1
						FROM cadpro_status c
						WHERE a.numlic = c.numlic);`)
	cnx_fdb.Exec(`INSERT INTO CADLIC_SESSAO (NUMLIC, SESSAO, DTREAL, HORREAL, COMP, DTENC, HORENC, SESSAOPARA, MOTIVO) 
                  SELECT L.NUMLIC, CAST(1 AS INTEGER), L.DTREAL, L.HORREAL, L.COMP, L.DTENC, L.HORENC, CAST('T' AS VARCHAR(1)), CAST('O' AS VARCHAR(1)) FROM CADLIC L 
                  WHERE numlic not in (SELECT FIRST 1 S.NUMLIC FROM CADLIC_SESSAO S WHERE S.NUMLIC = L.NUMLIC)`)
	fmt.Println("ProlicProlics - Tempo de execução: ", time.Since(start))
}

func CadproProposta() {
	start := time.Now()
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Erro ao conectar no banco: " + err.Error())
	}
	defer cnx_fdb.Close()

	cnx_pg, err := conexao.ConexaoOrigem()
	if err != nil {
		panic("Erro ao conectar no banco: " + err.Error())
	}
	defer cnx_pg.Close()

	// Limpando Tabela
	cnx_fdb.Exec("DELETE FROM CADPRO_PROPOSTA")

	// Query
	rows, err := cnx_pg.Query(`SELECT
								codif,
								sessao,
								numlic,
								lote,
								item,
								itemorc,
								SUM(qtd) AS qtd,  -- Agregação por soma
								itemcomprapropvalorunitario,
								SUM(total) AS total,
								status,
								subem
							FROM
								(
									SELECT DISTINCT
										a.itemcomprapropfornecedorid AS codif,
										1 AS sessao,
										c.pedidocompraforprocessoid AS numlic,
										TO_CHAR(d.loteordem, 'fm00000000') AS lote,
										COALESCE(b.itemcompranumitemseq, b.itemcompraordem) AS item,
										e.item AS itemorc,
										b.itemcompraquantidade AS qtd,  -- Incluído no GROUP BY e SUM
										a.itemcomprapropvalorunitario,
										a.itemcomprapropvalortotal AS total,
										'C' AS status,
										1 AS subem
									FROM
										itemcompraproposta a
									JOIN itemcompra b ON
										a.itemcompraid = b.itemcompraid
										AND a.itemcompraversao = b.itemcompraversao
									JOIN pedidocompra c ON
										c.pedidocompraid = b.itemcomprapedidoid
										AND c.pedidocompraversao = b.itemcomprapedidoversao
									LEFT JOIN lote d ON
										b.itemcompraloteid = d.loteid
									LEFT JOIN icadorc e ON
										e.pedidocompraforprocessoid = c.pedidocompraforprocessoid
										AND e.codreduz = b.itemcompramaterialid
									WHERE
										c.pedidocompraugid = $1
										--AND c.pedidocompraforprocessoid = 4678
								) AS subquery
							GROUP BY
								codif,
								sessao,
								numlic,
								lote,
								item,
								itemorc,
								itemcomprapropvalorunitario,
								status,
								subem;`, GetEmpresa())
	if err != nil {
		panic("Erro ao consultar dados: " + err.Error())
	}

	// Insert 
	insert, err := cnx_fdb.Prepare(`insert into cadpro_proposta (codif, sessao, numlic, lotelic, itemp, item, quan1, vaun1, vato1, status, subem) values (?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		panic("Erro ao preparar insert: " + err.Error())
	}

	var codif, sessao, numlic, itemp, subem int
	var quan1, vaun1, vato1 nulls.Float64
	var status, lotelic nulls.String
	var item nulls.Int
	for rows.Next() {
		err = rows.Scan(&codif, &sessao, &numlic, &lotelic, &itemp, &item, &quan1, &vaun1, &vato1, &status, &subem)
		if err != nil {
			panic("Erro ao scannear variáveis: " + err.Error())
		}

		_, err = insert.Exec(codif, sessao, numlic, lotelic, itemp, itemp, quan1, vaun1, vato1, status, subem)
		if err != nil {
			// panic("Erro ao inserir dados: " + err.Error())
			continue
		}
	}
	fmt.Println("CadproProposta - Tempo de execução: ", time.Since(start))
}

func CadlicSessao() {
	start := time.Now()
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Erro ao conectar no banco: " + err.Error())
	}

	cnx_fdb.Exec(`delete from cadlic_sessao`)
	cnx_fdb.Exec(`INSERT INTO CADLIC_SESSAO (NUMLIC, SESSAO, DTREAL, HORREAL, COMP, DTENC, HORENC, SESSAOPARA, MOTIVO) 
                  SELECT L.NUMLIC, CAST(1 AS INTEGER), L.DTREAL, L.HORREAL, L.COMP, L.DTENC, L.HORENC, CAST('T' AS VARCHAR(1)), CAST('O' AS VARCHAR(1)) FROM CADLIC L 
                  WHERE numlic not in (SELECT FIRST 1 S.NUMLIC FROM CADLIC_SESSAO S WHERE S.NUMLIC = L.NUMLIC)`)
	fmt.Println("CadlicSessao - Tempo de execução: ", time.Since(start))
}

func CadproStatus() {
	start := time.Now()
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Erro ao conectar no banco: " + err.Error())
	}

	cnx_fdb.Exec(`delete from cadpro_status`)
	cnx_fdb.Exec(`alter trigger TBI_CADPRO_STATUS_BLOQUEIO inactive`)
	cnx_fdb.Exec(`INSERT INTO cadpro_status (numlic, sessao, itemp, item, telafinal)
					SELECT b.NUMLIC, 1 AS sessao, a.item, a.item, 'I_ENCERRAMENTO'
					FROM CADPROLIC a
					JOIN cadlic b ON a.NUMLIC = b.NUMLIC
					WHERE NOT EXISTS (
						SELECT 1
						FROM cadpro_status c
						WHERE a.numlic = c.numlic)
					AND b.COMP = 3;`)
	cnx_fdb.Exec(`alter trigger TBI_CADPRO_STATUS_BLOQUEIO active`)
	fmt.Println("CadproStatus - Tempo de execução: ", time.Since(start))
}

func CadproLance() {
	start := time.Now()
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Erro ao conectar no banco: " + err.Error())
	}

	cnx_fdb.Exec(`delete from cadpro_lance`)
	cnx_fdb.Exec(`insert into cadpro_lance (sessao, rodada, codif, itemp, vaunl, vatol, status, subem, numlic)
					SELECT sessao, 1 rodada, CODIF, ITEMP, VAUN1, VATO1, 'F' status, SUBEM, numlic FROM CADPRO_PROPOSTA cp where subem = 1 and not exists
					(select 1 from cadpro_lance cl where cp.codif = cl.codif and cl.itemp = cp.itemp and cl.numlic = cp.numlic)`)

	fmt.Println("CadproLance - Tempo de execução: ", time.Since(start))
}

func CadproFinal() {
	start := time.Now()
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Erro ao conectar no banco: " + err.Error())
	}

	cnx_fdb.Exec(`delete from cadpro_final`)
	cnx_fdb.Exec("alter table cadpro_final add CQTDADT double precision")
    cnx_fdb.Exec("alter table cadpro_final add ccadpro varchar(20)")
    cnx_fdb.Exec("alter table cadpro_final add CCODCCUSTO integer;")
	cnx_fdb.Exec(`EXECUTE BLOCK
                        AS
                        BEGIN	
                            INSERT INTO CADPRO_FINAL (NUMLIC, ULT_SESSAO, CODIF, ITEMP, VAUNF, VATOF, STATUS, SUBEM, PERCF)
                                                SELECT A.NUMLIC, A.SESSAO, A.CODIF, A.ITEMP, A.VAUNL, A.VATOL, 'C', 1, NULL 
                                                FROM CADPRO_LANCE A  
                                                WHERE NOT EXISTS(SELECT 1 FROM CADPRO_FINAL B WHERE A.NUMLIC = B.NUMLIC AND A.SESSAO = B.ULT_SESSAO AND A.CODIF = B.CODIF AND A.ITEMP = B.ITEMP)  
                                                AND A.STATUS = 'F' AND A.NUMLIC IN (SELECT NUMLIC FROM CADLIC);                            
                            INSERT INTO CADPRO_FINAL (NUMLIC, ULT_SESSAO, CODIF, ITEMP, VAUNF, VATOF, STATUS, SUBEM, PERCF) 
                                                SELECT A.NUMLIC, A.SESSAO, A.CODIF, A.ITEMP, A.VAUN1, A.VATO1, 'C', 1, NULL  
                                                FROM CADPRO_PROPOSTA A 
                                                WHERE NOT EXISTS(SELECT 1 FROM CADPRO_FINAL B WHERE A.NUMLIC = B.NUMLIC AND A.SESSAO = B.ULT_SESSAO AND A.ITEMP = B.ITEMP) 
                                                AND A.STATUS = 'C' AND A.SUBEM = 1 AND A.NUMLIC IN (SELECT NUMLIC FROM CADLIC);
                            UPDATE CADPRO_FINAL A SET A.CQTDADT = (SELECT B.QUAN1 FROM CADPROLIC B WHERE A.NUMLIC = B.NUMLIC AND A.ITEMP = B.ITEM) 
                                            WHERE A.NUMLIC IN (SELECT C.NUMLIC FROM CADLIC C);                              
                            UPDATE CADPRO_FINAL A SET A.CCADPRO = (SELECT B.CADPRO FROM CADPROLIC B WHERE A.NUMLIC = B.NUMLIC AND A.ITEMP = B.ITEM) 
                                            WHERE A.NUMLIC IN (SELECT C.NUMLIC FROM CADLIC C);                              
                            UPDATE CADPRO_FINAL A SET A.CCODCCUSTO = (SELECT B.CODCCUSTO FROM CADPROLIC B WHERE A.NUMLIC = B.NUMLIC AND A.ITEMP = B.ITEM) 
                                            WHERE A.NUMLIC IN (SELECT C.NUMLIC FROM CADLIC C);        
                        END`)
	fmt.Println("CadproFinal - Tempo de execução: ", time.Since(start))
}

func Cadpro() {
	start := time.Now()
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Erro ao conectar no banco: " + err.Error())
	}

	cnx_fdb.Exec(`delete from cadpro`)
	cnx_fdb.Exec(`INSERT INTO CADPRO (
					CODIF,
					CADPRO,
					QUAN1,
					VAUN1,
					VATO1,
					SUBEM,
					STATUS,
					ITEM,
					NUMORC,
					ITEMORCPED,
					CODCCUSTO,
					FICHA,
					ELEMENTO,
					DESDOBRO,
					NUMLIC,
					ULT_SESSAO,
					ITEMP,
					QTDADT,
					QTDPED,
					VAUNADT,
					VATOADT,
					PERC,
					QTDSOL,
					ID_CADORC,
					VATOPED,
					VATOSOL,
					TPCONTROLE_SALDO,
					QTDPED_FORNECEDOR_ANT,
					VATOPED_FORNECEDOR_ANT
				)
				SELECT
					a.CODIF,
					c.CADPRO,
					CASE WHEN a.VAUNL <> 0 THEN ROUND((a.vatol / a.VAUNL), 2) ELSE 0 END qtdunit,
					a.VAUNL,
					CASE WHEN a.VAUNL <> 0 THEN ROUND((a.vatol / a.VAUNL), 2) * a.VAUNL ELSE 0 END VATOTAL,
					1,
					'C',
					c.ITEM,
					c.NUMORC,
					c.ITEM,
					c.CODCCUSTO,
					c.FICHA,
					c.ELEMENTO,
					c.DESDOBRO,
					a.NUMLIC,
					1,
					b.ITEMP,
					CASE WHEN a.VAUNL <> 0 THEN ROUND((a.vatol / a.VAUNL), 2) ELSE 0 END qtdunit_adit,
					0,
					a.VAUNL,
					CASE WHEN a.VAUNL <> 0 THEN ROUND((a.vatol / a.VAUNL), 2) * a.VAUNL ELSE 0 END VATOTAL,
					0,
					0,
					c.ID_CADORC,
					0,
					0,
					'Q',
					0,
					0
				FROM
					CADPRO_LANCE a
				INNER JOIN CADPRO_STATUS b ON
					b.NUMLIC = a.NUMLIC AND a.ITEMP = b.ITEMP AND a.SESSAO = b.SESSAO
				INNER JOIN CADPROLIC_DETALHE c ON
					c.NUMLIC = a.NUMLIC AND b.ITEM = c.ITEM_CADPROLIC
				INNER JOIN CADLIC D ON
					D.NUMLIC = A.NUMLIC
				WHERE
					a.SUBEM = 1 AND a.STATUS = 'F'
					AND NOT EXISTS (
						SELECT 1 
						FROM CADPRO cp
						WHERE cp.NUMLIC = a.NUMLIC 
						AND cp.ITEM = c.ITEM 
						AND cp.CODIF = a.CODIF
					);`)
	cnx_fdb.Exec(`insert into cadprolic_detalhe_fic (numlic, item, codigo, qtd, valor, qtdadt, valoradt, codccusto, qtdmed, valormed, tipo) 
                    select numlic, item, '0', quan1, vato1, qtdadt, vatoadt, codccusto, quan1, vato1, 'C' from cadpro where numlic in 
                    (select numlic from cadlic where registropreco='N' and liberacompra='S') and subem=1;`)
	fmt.Println("Cadpro - Tempo de execução: ", time.Since(start))
}

func Regpreco() {
	start := time.Now()
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Erro ao conectar no banco: " + err.Error())
	}

	cnx_fdb.Exec(`delete from regpreco`)
	cnx_fdb.Exec(`delete from regprecohis`)
	cnx_fdb.Exec(`delete from regprecodoc`)
	cnx_fdb.Exec(`EXECUTE BLOCK AS  
                        BEGIN  
                        INSERT INTO REGPRECODOC (NUMLIC, CODATUALIZACAO, DTPRAZO, ULTIMA)  
                        SELECT DISTINCT A.NUMLIC, 0, DATEADD(1 YEAR TO A.DTHOM), 'S'  
                        FROM CADLIC A WHERE A.REGISTROPRECO = 'S'
                        AND NOT EXISTS(SELECT 1 FROM REGPRECODOC X  
                        WHERE X.NUMLIC = A.NUMLIC);  

                        INSERT INTO REGPRECO (COD, DTPRAZO, NUMLIC, CODIF, CADPRO, CODCCUSTO, ITEM, CODATUALIZACAO, QUAN1, VAUN1, VATO1, QTDENT, SUBEM, STATUS, ULTIMA)  
                        SELECT B.ITEM, DATEADD(1 YEAR TO A.DTHOM), B.NUMLIC, B.CODIF, B.CADPRO, B.CODCCUSTO, B.ITEM, 0, B.QUAN1, B.VAUN1, B.VATO1, 0, B.SUBEM, B.STATUS, 'S'  
                        FROM CADLIC A INNER JOIN CADPRO B ON (A.NUMLIC = B.NUMLIC) WHERE A.REGISTROPRECO = 'S' AND NOT EXISTS(SELECT 1 FROM REGPRECO X  
                        WHERE X.NUMLIC = B.NUMLIC AND X.CODIF = B.CODIF AND X.CADPRO = B.CADPRO AND X.CODCCUSTO = B.CODCCUSTO AND X.ITEM = B.ITEM);  

                        INSERT INTO REGPRECOHIS (NUMLIC, CODIF, CADPRO, CODCCUSTO, ITEM, CODATUALIZACAO, QUAN1, VAUN1, VATO1, SUBEM, STATUS, MOTIVO, MARCA, NUMORC, ULTIMA)  
                        SELECT B.NUMLIC, B.CODIF, B.CADPRO, B.CODCCUSTO, B.ITEM, 0, B.QUAN1, B.VAUN1, B.VATO1, B.SUBEM, B.STATUS, B.MOTIVO, B.MARCA, B.NUMORC, 'S'  
                        FROM CADLIC A INNER JOIN CADPRO B ON (A.NUMLIC = B.NUMLIC) WHERE A.REGISTROPRECO = 'S' 
                        AND NOT EXISTS(SELECT 1 FROM REGPRECOHIS X  
                        WHERE X.NUMLIC = B.NUMLIC AND X.CODIF = B.CODIF AND X.CADPRO = B.CADPRO AND X.CODCCUSTO = B.CODCCUSTO AND X.ITEM = B.ITEM);  
                    
                        insert into cadprolic_detalhe_fic (numlic, item, codigo, qtd, valor, qtdadt, valoradt, codccusto, qtdmed, valormed, tipo)
                        select numlic, item, '0', quan1, vato1, quan1, vato1, codccusto, quan1, vato1, 'C' from regpreco where numlic in 
                        (select numlic from cadlic where registropreco='S' and liberacompra='S') and subem=1;
                        END;`)
	fmt.Println("Regpreco - Tempo de execução: ", time.Since(start))
}

func Aditivo() {
	start := time.Now()
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Erro ao conectar no banco: " + err.Error())
	}
	defer cnx_fdb.Close()

	cnx_pg, err := conexao.ConexaoOrigem()
	if err != nil {
		panic("Erro ao conectar no banco: " + err.Error())
	}
	defer cnx_pg.Close()

	// Limpando Tabela
	cnx_fdb.Exec("update cadpro set qtdadt = quan1, vaunadt = vaun1, vatoadt = vato1")
	cnx_fdb.Exec("update cadprolic_detalhe_fic set qtdadt = qtd, valoradt = valor")

	// Query
	rows, err := cnx_pg.Query(`select
						aditivonumero,
						aditivoid,
						c.pedidocompraforprocessoid,
						b.itemcompramaterialid,
						b.itemcompraaditivoqtde,
						b.itemcompraaditivovalorunitario,
						b.itemcompraaditivovalorunitario * b.itemcompraaditivoqtde totaladt
					from
						aditivo a
					join itemcompra b on
						a.aditivoid = b.itemcompraaditivoid
					join pedidocompra c on
						c.pedidocompraid = b.itemcomprapedidoid
						and c.pedidocompraversao = b.itemcomprapedidoversao
					where
						aditivougid = $1 
						and pedidocompraforprocessoid IS NOT NULL
						--and c.pedidocompraforprocessoid = 1452
					order by pedidocompraforprocessoid, aditivonumero, aditivoano `, GetEmpresa())
	if err != nil {
		panic("Erro ao consultar dados: " + err.Error())
	}

	// Prepara o update
	updtCadpro, err := cnx_fdb.Prepare(`UPDATE CADPRO SET QTDADT = QTDADT + ?, VAUNADT = ?, VATOADT = VATOADT + ? WHERE NUMLIC = ? AND CADPRO = ?`)
	if err != nil {
		panic("Erro ao preparar update: " + err.Error())
	}
	updtDetalheFic, err := cnx_fdb.Prepare(`UPDATE CADPROLIC_DETALHE_FIC SET QTDADT = QTDADT + ?, VALORADT = VALORADT + ? WHERE NUMLIC = ? AND ITEM = ?`)
	if err != nil {
		panic("Erro ao preparar update: " + err.Error())
	}

	// Consulta cadpro
	cadpros := make(map[int]string)
	aux1, err := cnx_fdb.Query(`select cadpro, codreduz from cadest`)
	if err != nil {
		panic("Erro ao consultar cadpro" + err.Error())
	}
	for aux1.Next() {
		var cadpro string
		var codreduz int
		err = aux1.Scan(&cadpro, &codreduz)
		if err != nil {
			panic("Erro ao scannear cadpro" + err.Error())
		}
		cadpros[codreduz] = cadpro
	}

	// Executa o update
	for rows.Next() {
		var aditivonumero, aditivoid, numlic, codreduz int
		var qtdadt, vaunadt, vatoadt float64
		var item nulls.Int
		err = rows.Scan(&aditivonumero, &aditivoid, &numlic, &codreduz, &qtdadt, &vaunadt, &vatoadt)
		if err != nil {
			panic("Erro ao scannear variáveis: " + err.Error())
		}

		cadpro := cadpros[codreduz]
		cnx_fdb.QueryRow(`select item from cadprolic_detalhe where numlic = ? and cadpro = ?`, numlic, cadpro).Scan(&item)

		if item.Valid {
			updtCadpro.Exec(qtdadt, vaunadt, vatoadt, numlic, cadpro)
			updtDetalheFic.Exec(qtdadt, vatoadt, numlic, item)
		} else {
			continue
		}
	}
	fmt.Println("Aditivo - Tempo de execução: ", time.Since(start))
}