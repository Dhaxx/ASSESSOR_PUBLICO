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
		} else if comp_ant == 3 || comp_ant == 8 || comp_ant == 16 { // Ratificada ou Encerrado
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
		} else if comp_ant == 10 || comp_ant == 11 || comp_ant == 13 || comp_ant == 14 { // Adjudicada, Homologada, adjudicada parcialmente, homologada parcialmente ou ratificada parcialmente
			comp = 2
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
	rows, err := cnx_pg.Query(`select
								to_char(c.cotacaoprecosnumero, 'fm00000/')||cotacaoprecosano%2000 numorc,
								to_char(d.loteordem, 'fm00000000') lote,
								row_number() over (partition by c.cotacaoprecosid order by estimativaitemid) as item,
								case 
									when d.loteordem is not null then row_number() over (partition by c.cotacaoprecosid, loteordem order by c.cotacaoprecosid, loteordem, estimativaitemid)
									else estimativaitemid
								end as item_por_lote,
								a.estimativaitemid itemorc,
								a.estimativaitemmaterialid,
								a.estimativaitemqtde,
								a.estimativaitemmenorvalor,
								a.estimativaitemmenorvalortotal
							from
								estimativaitem a
							join estimativa b on
								a.estimativaid = b.estimativaid
							join cotacaoprecos c on
								c.cotacaoprecosid = b.estimativacotacaoid
								and c.cotacaoprecosversao = b.estimativacotacaoversao
							left join lote d on d.loteid = a.estimativaitemloteid and d.loteversao = a.estimativaitemloteversao 
							join formalizacaoprocesso e on e.forprocessocotacaoid = b.estimativacotacaoid and e.forprocessocotacaoversao = b.estimativacotacaoversao 
							where estimativaitemmenorvalor is not null and cotacaoprecosugid = $1`, GetEmpresa())
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
	codccustos := make(map[string]map[int]int) 
	aux2, err := cnx_fdb.Query(`select a.numorc, a.item, a.codccusto from icadorc a JOIN cadorc b ON a.NUMORC = b.NUMORC WHERE b.NUMLIC IS NOT null`)
	if err != nil {
		panic("Erro ao consultar icadorc" + err.Error())
	}
	for aux2.Next() {
		var numorc string
		var item, codccusto int
		err = aux2.Scan(&numorc, &item, &codccusto)
		if err != nil {
			panic("Erro ao scannear icadorc" + err.Error())
		}
		if _, ok := codccustos[numorc]; !ok {
			codccustos[numorc] = make(map[int]int)
		}
		codccustos[numorc][item] = codccusto
	}

	// Consulta Auxiliar
	numlics := make(map[string]int)
	aux3, err := cnx_fdb.Query(`select numorc, numlic from cadorc where numlic is not null`)
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
		numlics[numorc] = numlic
	}

	// Consulta Auxiliar
	idcadors := make(map[string]int)
	aux4, err := cnx_fdb.Query(`select numorc, id_cadorc from cadorc where numlic is not null`)
	if err != nil {
		panic("Erro ao consultar cadorc" + err.Error())
	}
	for aux4.Next() {
		var numorc string
		var id_cadorc int
		err = aux4.Scan(&numorc, &id_cadorc)
		if err != nil {
			panic("Erro ao scannear cadorc" + err.Error())
		}
		idcadors[numorc] = id_cadorc
	}

	// Prepara Insert
	insert, err := cnx_fdb.Prepare(`insert into cadprolic (numorc, lotelic, item, item_mask, itemorc, cadpro, codccusto, quan1, vamed1, vatomed1, reduz, microempresa, tlance, item_ag, numlic, id_cadorc, item_lote, item_por_lote) 
									values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		panic("Erro ao preparar insert: " + err.Error())
	}

	var numorc, cadpro, reduz, microempresa, tlance string
	var lote nulls.String
	var codreduz, codccusto, item, numlic, id_cadorc int
	var itemorc, item_por_lote nulls.Int
	var quan1, vamed1, vatomed1 nulls.Float64
	for rows.Next() {
		err = rows.Scan(&numorc, &lote, &item, &item_por_lote, &itemorc, &codreduz, &quan1, &vamed1, &vatomed1)
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
		codccusto = codccustos[numorc][item]
		reduz = `N`
		microempresa = `N`
		tlance = `$`
		numlic = numlics[numorc]
		id_cadorc = idcadors[numorc]

		_, err = insert.Exec(numorc, lote, item, item, itemorc, cadpro, codccusto, quan1, vamed1, vatomed1, reduz, microempresa, tlance, item, numlic, id_cadorc, item, item_por_lote)
		if err != nil {
			panic("Erro ao inserir dados: " + err.Error())
		}
	}
	fmt.Println("Cadprolic - Tempo de execução: ", time.Since(start))
}

func CadprolicDetalhe() {
	start := time.Now()
	cnx_fdb, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Erro ao conectar no banco: " + err.Error())
	}

	cnx_fdb.Exec(`ALTER TRIGGER TBI_CADPROLIC_DETALHE_BLOQUEIO INACTIVE;
					INSERT INTO CADPROLIC_DETALHE (NUMLIC,item,CADPRO,quan1,VAMED1,VATOMED1,marca,CODCCUSTO,ITEM_CADPROLIC)
					select numlic, item, cadpro, quan1, vamed1, vatomed1, marca, codccusto, item from cadprolic b where
					not exists (select 1 from cadprolic_detalhe c where b.numlic = c.numlic and b.item = c.item);
					ALTER TRIGGER TBI_CADPROLIC_DETALHE_BLOQUEIO ACTIVE;`)
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
									c.forprocessougid = $1`, GetEmpresa())
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
	rows, err := cnx_pg.Query(`select
									a.itemcomprapropfornecedorid codif,
									1 sessao,
									c.pedidocompraforprocessoid,
									to_char(d.loteordem, 'fm00000000') lote,
									b.itemcompranumitemseq item,
									b.itemcompraquantidade,
									a.itemcomprapropvalorunitario,
									a.itemcomprapropvalortotal,
									'C' status,
									1 subem
								from
									itemcompraproposta a
								join itemcompra b on
									a.itemcompraid = b.itemcompraid
									and a.itemcompraversao = b.itemcompraversao
								join pedidocompra c on c.pedidocompraid = b.itemcomprapedidoid and c.pedidocompraversao = b.itemcomprapedidoversao 
								left join lote d on b.itemcompraloteid = d.loteid 
								where
									c.pedidocompraugid = $1 and c.pedidocompracotacaoid is not null --and c.pedidocompraforprocessoid = 29715`, GetEmpresa())
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
	for rows.Next() {
		err = rows.Scan(&codif, &sessao, &numlic, &lotelic, &itemp, &quan1, &vaun1, &vato1, &status, &subem)
		if err != nil {
			panic("Erro ao scannear variáveis: " + err.Error())
		}

		_, err = insert.Exec(codif, sessao, numlic, lotelic, itemp, itemp, quan1, vaun1, vato1, status, subem)
		if err != nil {
			panic("Erro ao inserir dados: " + err.Error())
		}
	}
	fmt.Println("CadproProposta - Tempo de execução: ", time.Since(start))
}