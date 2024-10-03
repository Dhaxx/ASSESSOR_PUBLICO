package compras

import (
	conexao "ASSESSOR_PUBLICO/CONEXAO"
	utils "ASSESSOR_PUBLICO/MODULOS/utils"
	"fmt"

	"github.com/gobuffalo/nulls"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

func Cadped(p *mpb.Progress) {
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

	// Limpando
	cnx_fdb.Exec("DELETE from ICADPED")
	cnx_fdb.Exec("DELETE from CADPED")

	// Query
	rows, err := cnx_pg.Query(`select
			to_char(autforid,
			'fm00000/')|| autforano%2000 numped,
			to_char(autfornumero,
			'fm00000') num,
			autforano,
			cast(autfordataemissao as varchar) data,
			autforfornecedorid codif,
			'N' entrou,
			autforid id_cadped,
			autforugid empresa,
			autforforprocessoid numlic,
			'AF - '||to_char(autfornumero,
			'fm00000/')|| autforano%2000 obs,
			to_char(autfornumero,
			'fm00000/')|| autforano%2000 numpedant,
			min(codccusto) codccusto
		from
			autorizacaofornecimento a
		left join icadorc_pref b on a.autforforprocessoid = b.pedidocompraforprocessoid 
		where
			autforugid = $1
		group by autforid, autforano, autfornumero, autfordataemissao, autforfornecedorid, entrou, autforugid, autforforprocessoid`, utils.GetEmpresa())
	if err != nil {
		panic("Erro ao consultar dados: " + err.Error())
	}

	var count int
	err = cnx_pg.QueryRow(`select count(*) from (select
			to_char(autforid,
			'fm00000/')|| autforano%2000 numped,
			to_char(autfornumero,
			'fm00000') num,
			autforano,
			cast(autfordataemissao as varchar) data,
			autforfornecedorid codif,
			'N' entrou,
			autforid id_cadped,
			autforugid empresa,
			autforforprocessoid numlic,
			'AF - '||to_char(autfornumero,
			'fm00000/')|| autforano%2000 obs,
			to_char(autfornumero,
			'fm00000/')|| autforano%2000 numpedant,
			coalesce(min(codccusto),0) codccusto
		from
			autorizacaofornecimento a
		left join icadorc_pref b on a.autforforprocessoid = b.pedidocompraforprocessoid 
		where
			autforugid = $1
		group by autforid, autforano, autfornumero, autfordataemissao, autforfornecedorid, entrou, autforugid, autforforprocessoid) as rn`, utils.GetEmpresa()).Scan(&count)
	if err != nil {
		panic(`Erro ao contar registros` + err.Error())
	}
	bar15 := p.AddBar(int64(count),
		mpb.PrependDecorators(
			decor.Name("Cadped - "),
			decor.CountersNoUnit("%d/%d", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(
			decor.Percentage(decor.WCSyncSpace),
		),
	)

	// Prepara Insert
	insert, err := cnx_fdb.Prepare(`insert into cadped (numped, num, ano, datped, codif, entrou, id_cadped, empresa, numlic, obs, numpedant, codccusto, codatualizacao_rp) values (?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		panic("Erro ao inserir dados: " + err.Error())
	}

	for rows.Next() {
		var numped, num, ano, datped, entrou, numpedant, obs nulls.String
		var codif, id_cadped, empresa, numlic, codccusto nulls.Int
		err = rows.Scan(&numped, &num, &ano, &datped, &codif, &entrou, &id_cadped, &empresa, &numlic, &obs, &numpedant, &codccusto)
		if err != nil {
			panic("Erro ao ler dados: " + err.Error())
		}

		_, err = insert.Exec(numped, num, ano, datped, codif, entrou, id_cadped, empresa, numlic, obs, numpedant, codccusto, 0)
		if err != nil {
			panic("Erro ao inserir dados: " + err.Error())
		}
		bar15.Increment()
	}
}

func Icadped(p *mpb.Progress) {
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

	// Limpando
	cnx_fdb.Exec("DELETE from ICADPED")
	cnx_fdb.Exec(`EXECUTE BLOCK AS 
		BEGIN
			UPDATE CADPRO SET QTDPED = 0, VATOPED = 0;
			UPDATE REGPRECO SET QTDENT = 0, VATOENT = 0;
			UPDATE CADPROLIC_DETALHE_FIC SET QTD = 0, VALORPED = 0;
		END`)

	// Query
	rows, err := cnx_pg.Query(`select
			to_char(a.autforid,
			'fm00000/')|| autforano%2000 numped,
			autforitemid,
			c.itemcompramaterialid,
			a.autforitemqtdeaut,
			a.autforitemvalorunitario,
			a.autforitemqtdeaut * a.autforitemvalorunitario total,
			a.autforid,
			coalesce(min(d.codccusto),0) codccusto
		from
			autorizacaofornecimentoitem a
		join autorizacaofornecimento b on
			a.autforid = b.autforid
		join itemcompra c on
			a.autforitemcompraid = c.itemcompraid
		left join icadorc_pref d on d.pedidocompraforprocessoid = autforforprocessoid and d.codreduz = c.itemcompramaterialid
		--where a.autforid in (1457, 11864, 12251, 12823, 14903, 18231, 18762, 18999, 19000, 19615, 20127)
		group by a.autforid, b.autforano, autforitemid, c.itemcompramaterialid, a.autforitemqtdetotal, a.autforitemvalorunitario, a.autforid`)
	if err != nil {
		panic("Erro ao consultar dados: " + err.Error())
	}

	var count int
	err = cnx_pg.QueryRow(`select count(*) from (select
			to_char(a.autforid,
			'fm00000/')|| autforano%2000 numped,
			autforitemid,
			c.itemcompramaterialid,
			a.autforitemqtdeaut,
			a.autforitemvalorunitario,
			a.autforitemqtdeaut * a.autforitemvalorunitario total,
			a.autforid,
			min(d.codccusto) codccusto
		from
			autorizacaofornecimentoitem a
		join autorizacaofornecimento b on
			a.autforid = b.autforid
		join itemcompra c on
			a.autforitemcompraid = c.itemcompraid
		join icadorc d on d.pedidocompraforprocessoid = autforforprocessoid and d.codreduz = c.itemcompramaterialid
		--where a.autforid in (1457, 11864, 12251, 12823, 14903, 18231, 18762, 18999, 19000, 19615, 20127)
		group by a.autforid, b.autforano, autforitemid, c.itemcompramaterialid, a.autforitemqtdetotal, a.autforitemvalorunitario, a.autforid) as rn`).Scan(&count)
	if err != nil {
		panic(`Erro ao contar registros` + err.Error())
	}
	bar16 := p.AddBar(int64(count),
		mpb.PrependDecorators(
			decor.Name("Icadped - "),
			decor.CountersNoUnit("%d/%d", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(
			decor.Percentage(decor.WCSyncSpace),
		),
	)

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

	// Prepara Insert
	insert, err := cnx_fdb.Prepare(`insert into icadped (numped, item, cadpro, qtd, prcunt, prctot, codccusto, id_cadped) values (?,?,?,?,?,?,?,?)`)
	if err != nil {
		panic("Erro ao inserir dados: " + err.Error())
	}

	for rows.Next() {
		var numped nulls.String
		var item, codccusto, id_cadped, codreduz nulls.Int
		var qtd, prcunt, prctot nulls.Float64
		var cadpro string
		err = rows.Scan(&numped, &item, &codreduz, &qtd, &prcunt, &prctot, &id_cadped, &codccusto)
		if err != nil {
			panic("Erro ao ler dados: " + err.Error())
		}

		cadpro = cadpros[codreduz.Int]

		_, err = insert.Exec(numped, item, cadpro, qtd, prcunt, prctot, codccusto, id_cadped)
		if err != nil {
			panic("Erro ao inserir dados: " + err.Error())
		}
		bar16.Increment()
	}
	cnx_fdb.Exec(`ALTER TRIGGER TBU_CADPRO INACTIVE;
        ALTER TRIGGER TBU_REGPRECO INACTIVE;
        EXECUTE BLOCK AS
        DECLARE VARIABLE NUMLIC INTEGER;
        DECLARE VARIABLE ITEM INTEGER;
        DECLARE VARIABLE CADPRO VARCHAR(255);
        DECLARE VARIABLE QTDTOTPED DOUBLE PRECISION;
        DECLARE VARIABLE VTOTPED DOUBLE PRECISION;

        BEGIN
			UPDATE A.ICADPED SET CODCCUSTO A = (SELECT CODCCUSTO FROM CADPED B WHERE A.ID_CADPED = B.ID_CADPED) WHERE A.CENTROCUSTO = 0;
            FOR
                SELECT numlic, cadpro, sum(a.QTD), sum(a.PRCTOT)
                FROM icadped a
                JOIN cadped b ON a.ID_CADPED = b.ID_CADPED 
                --WHERE numlic = 13954
                GROUP BY numlic, cadpro
                INTO :NUMLIC, :CADPRO, :QTDTOTPED, :VTOTPED
            DO
                BEGIN
                    UPDATE CADPRO 
                    SET QTDPED = :QTDTOTPED, VATOPED = :VTOTPED 
                    WHERE NUMLIC = :NUMLIC AND CADPRO = :CADPRO; --AND ITEM = :ITEM;

                    UPDATE REGPRECO 
                    SET QTDENT = :QTDTOTPED, VATOENT = :VTOTPED 
                    WHERE NUMLIC = :NUMLIC AND cadpro = :CADPRO; --AND cod = :ITEM;
                END
        END;
        ALTER TRIGGER TBU_CADPRO ACTIVE;
        ALTER TRIGGER TBU_REGPRECO ACTIVE;`)
	cnx_fdb.Exec(`MERGE INTO CADPROLIC_DETALHE_FIC d
		USING (
			SELECT numlic, item, sum(qtdped) qtdped, sum(vatoped) vatoped
			FROM cadpro WHERE qtdped <> 0 GROUP BY 1, 2
		) o
		ON (d.numlic = o.numlic AND d.item = o.item)
		WHEN MATCHED THEN 
			UPDATE SET 
				d.qtdped = o.qtdped,
				d.valorped = o.vatoped;`)
}

func Requi(p *mpb.Progress) {
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

	// Cria Campo
	cnx_fdb.Exec(`ALTER TABLE REQUI ADD tipo_req varchar(2)`)

	// Limpando
	cnx_fdb.Exec("DELETE from ICADREQ")
	cnx_fdb.Exec("DELETE from REQUI")

	// Query
	rows, err := cnx_pg.Query(`select
			empresa,
			id_requi,
			requi,
			numero,
			ano,
			almoxarifado,
			codccusto,
			movdocdata datae,
			movdocdata dtlan,
			tipo_req,
			comp,
			tiposaida,
			tprequi,
			case when tipo_req in ('DE','DS') then '[DEVOLUÇÃO] '||obs else obs end as obs,
			codif, 
			docum
		from
			(
			select
				movdocgestoraid empresa,
				movdocid id_requi,
				to_char(movdocid,
				'fm000000/')|| movdocano%2000 requi,
				to_char(movdocid,
				'fm000000') numero,
				movdocano ano,
				to_char(movdocalmoxarifadoid,
				'fm000000000') almoxarifado,
				coalesce(movdocunidadeorcamentariaid,
				0) codccusto,
				movdocdata,
				case
					when movdoctipodoc = 2 then 'E'
					--Entrada
					when movdoctipodoc = 3 then 'S'
					--Saída
					when movdoctipodoc = 8 then 'DE'
					--Devolução Entrada
					when movdoctipodoc = 9 then 'DS'
					--Devolução Saída
				end tipo_req,
				3 comp,
				'P' tiposaida,
				'OUTRA' TPREQUI,
				'Seq: '||movdocnumero||' - '||
				case
					when movdocobservacao = ''
					or movdocobservacao is null then coalesce(historicopadraodesc,'')
					else coalesce(movdocobservacao,'')
				end obs,
				movdocfornecedorid codif,
				movdocnumerone docum
			from
				movdoc a
			left join historicopadrao b on
				a.movdochistoricopadraoid = b.historicopadraoid
			where
				movdocgestoraid = $1) as rn`, utils.GetEmpresa())
	if err != nil {
		panic("Erro ao consultar dados: " + err.Error())
	}

	var count int
	err = cnx_pg.QueryRow(`select count(*) from (select
			empresa,
			id_requi,
			requi,
			numero,
			ano,
			almoxarifado,
			codccusto,
			movdocdata datae,
			movdocdata dtlan,
			tipo_req,
			comp,
			tiposaida,
			tprequi,
			case when tipo_req in ('DE','DS') then '[DEVOLUÇÃO] '||obs else obs end as obs,
			codif, 
			docum
		from
			(
			select
				movdocgestoraid empresa,
				movdocid id_requi,
				to_char(movdocid,
				'fm000000/')|| movdocano%2000 requi,
				to_char(movdocid,
				'fm000000') numero,
				movdocano ano,
				to_char(movdocalmoxarifadoid,
				'fm000000000') almoxarifado,
				coalesce(movdocunidadeorcamentariaid,
				0) codccusto,
				movdocdata,
				case
					when movdoctipodoc = 2 then 'E'
					--Entrada
					when movdoctipodoc = 3 then 'S'
					--Saída
					when movdoctipodoc = 8 then 'DE'
					--Devolução Entrada
					when movdoctipodoc = 9 then 'DS'
					--Devolução Saída
				end tipo_req,
				3 comp,
				'P' tiposaida,
				'OUTRA' TPREQUI,
				'Seq: '||movdocnumero||' - '||
				case
					when movdocobservacao = ''
					or movdocobservacao is null then coalesce(historicopadraodesc,'')
					else coalesce(movdocobservacao,'')
				end obs,
				movdocfornecedorid codif,
				movdocnumerone docum
			from
				movdoc a
			left join historicopadrao b on
				a.movdochistoricopadraoid = b.historicopadraoid
			where
				movdocgestoraid = $1) as rn) as rn`, utils.GetEmpresa()).Scan(&count)
	if err != nil {
		panic(`Erro ao contar registros` + err.Error())
	}

	bar17 := p.AddBar(int64(count),
		mpb.PrependDecorators(
			decor.Name("Requi - "),
			decor.CountersNoUnit("%d/%d", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(
			decor.Percentage(decor.WCSyncSpace),
		),
	)

	// Prepara Insert
	insert, err := cnx_fdb.Prepare(`INSERT
			INTO
			requi (empresa,
			id_requi,
			requi,
			num,
			ano,
			destino,
			codccusto,
			datae,
			dtlan,
			entr,
			said,
			comp,
			tiposaida,
			tprequi,
			obs,
			codif,
			docum,
			tipo_req,
			dtpag)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		panic("Erro ao inserir dados: " + err.Error())
	}

	for rows.Next() {
		var empresa, id_requi, ano, codccusto, comp, codif, docum nulls.Int
		var requi, num, almoxarifado, tipo_req, tiposaida, tprequi, obs, entr, said nulls.String
		var datae, dtlan, dtpag nulls.Time
		err = rows.Scan(&empresa, &id_requi, &requi, &num, &ano, &almoxarifado, &codccusto, &datae, &dtlan, &tipo_req, &comp, &tiposaida, &tprequi, &obs, &codif, &docum)
		if err != nil {
			panic("Erro ao ler dados: " + err.Error())
		}

		if tipo_req.String == "DE" || tipo_req.String == "E" {
			entr = nulls.NewString("S")
			datae = dtlan
		} else if tipo_req.String == "DS" || tipo_req.String == "S" {
			said = nulls.NewString("S")
			dtpag = dtlan
		}

		_, err = insert.Exec(empresa, id_requi, requi, num, ano, almoxarifado, codccusto, datae, dtlan, entr, said, comp, tiposaida, tprequi, obs, codif, docum, tipo_req, dtpag)
		if err != nil {
			panic("Erro ao inserir dados: " + err.Error())
		}
		bar17.Increment()
	}
}

func Icadreq(p *mpb.Progress) {
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

	// Limpando
	cnx_fdb.Exec("DELETE from ICADREQ")

	// DESLIGA TRIGGER
	cnx_fdb.Exec(`ALTER TRIGGER TI_ICADREQ INACTIVE;`)

	// Query
	rows, err := cnx_pg.Query(`select
			a.movdocid id_requi,
			to_char(b.movdocid, 'fm000000/')|| movdocano%2000 requi,
			coalesce(coalesce(movdocunidadeorcamentariaid,
			movdocitemunidorcid),
			0) codccusto,
			movdocitemgestoraid empresa,
			movdocitemid,
			case
				when movdocitemqtde = 0 then 1
				else movdocitemqtde
			end qtd, 
			movdocitemvalorunitario,
			movdocitemmaterialid codreduz,
			to_char(movdocitemalmoxarifadoid,
			'fm000000000') destino,
			case
				when movdoctipodoc = 2 then 'E'
				--Entrada
				when movdoctipodoc = 3 then 'S'
				--Saída
				when movdoctipodoc = 8 then 'DE'
				--Devolução Entrada
				when movdoctipodoc = 9 then 'DS'
				--Devolução Saída
			end tipo_req
		from
			movdocitem a
		join movdoc b on
			a.movdocid = b.movdocid
		where
			movdocitemgestoraid = $1`, utils.GetEmpresa())
	if err != nil {
		panic("Erro ao consultar dados: " + err.Error())
	}

	var count int
	err = cnx_pg.QueryRow(`select count(*) from (select
			a.movdocid id_requi,
			to_char(b.movdocid, 'fm000000/')|| movdocano%2000 requi,
			coalesce(coalesce(movdocunidadeorcamentariaid,
			movdocitemunidorcid),
			0) codccusto,
			movdocitemgestoraid empresa,
			movdocitemid,
			case
				when movdocitemqtde = 0 then 1
				else movdocitemqtde
			end qtd, 
			movdocitemvalorunitario,
			movdocitemmaterialid codreduz,
			to_char(movdocitemalmoxarifadoid,
			'fm000000000') destino,
			case
				when movdoctipodoc = 2 then 'E'
				--Entrada
				when movdoctipodoc = 3 then 'S'
				--Saída
				when movdoctipodoc = 8 then 'DE'
				--Devolução Entrada
				when movdoctipodoc = 9 then 'DS'
				--Devolução Saída
			end tipo_req
		from
			movdocitem a
		join movdoc b on
			a.movdocid = b.movdocid
		where
			movdocitemgestoraid = $1) as rn`, utils.GetEmpresa()).Scan(&count)
	if err != nil {
		panic(`Erro ao contar registros` + err.Error())
	}

	bar18 := p.AddBar(int64(count),
		mpb.PrependDecorators(
			decor.Name("Icadreq - "),
			decor.CountersNoUnit("%d/%d", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(
			decor.Percentage(decor.WCSyncSpace),
		),
	)

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

	// Prepara Insert
	insert, err := cnx_fdb.Prepare(`insert into icadreq (id_requi, requi, codccusto, empresa, item, quan1, quan2, vaun1, vaun2, vato1, vato2, cadpro, destino) values (?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		panic("Erro ao Preparar inserção: " + err.Error())
	}

	for rows.Next() {
		var id_requi, codccusto, empresa, item nulls.Int
		var qtd, prcunt, quan1, quan2, vaun1, vaun2, vato1, vato2 float64
		var destino, tipo_req nulls.String
		var codreduz int
		var cadpro, requi string
		err = rows.Scan(&id_requi, &requi, &codccusto, &empresa, &item, &qtd, &prcunt, &codreduz, &destino, &tipo_req)
		if err != nil {
			panic("Erro ao ler dados: " + err.Error())
		}

		cadpro = cadpros[codreduz]

		if tipo_req.String == "DE" || tipo_req.String == "E" {
			quan1 = qtd
			vaun1 = prcunt
			vato1 = quan1 * vaun1
		} else {
			quan2 = qtd
			vaun2 = prcunt
			vato2 = quan2 * vaun2
		}

		_, err = insert.Exec(id_requi, requi, codccusto, empresa, item, quan1, quan2, vaun1, vaun2, vato1, vato2, cadpro, destino)
		if err != nil {
			panic("Erro ao inserir dados: " + err.Error())
		}
		bar18.Increment()
	}
}
