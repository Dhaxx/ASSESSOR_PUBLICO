package utils

import (
	"ASSESSOR_PUBLICO/CONEXAO"
	"database/sql"
	"fmt"
	"strconv"
)

func EstourouSubgr(codigo int, subgrupo string, grupo string, con *sql.DB) []string {
	var subgrupoNovo string
	var codigoStr string

	if codigo < 1000 {
		codigoStr = zfill(strconv.Itoa(codigo),3)
		return []string{subgrupo, codigoStr}		
	} else if codigo < 10000 {
		codigoStr = strconv.Itoa(codigo)
		subgrupoNovo = codigoStr[:1]
		subgrupoNovo = `9`+subgrupoNovo+string(subgrupo[2])
		codigoStr = codigoStr[1:]
	} else if codigo >= 10000 {
		codigoStr = strconv.Itoa(codigo)
		subgrupoNovo = codigoStr[:2]
		subgrupoNovo = subgrupoNovo+string(subgrupo[2])
		codigoStr = codigoStr[2:]
	}

	tx, err := con.Begin()
	if err != nil {
		_ = err.Error()
	}

	_, err = tx.Exec(`INSERT INTO CADSUBGR (GRUPO, SUBGRUPO, NOME, OCULTAR) select grupo, ?, nome, 'N' from cadsubgr where grupo = ? and subgrupo = ?`, subgrupoNovo, grupo, subgrupo)
	if err != nil {
		_ = err.Error()
	}

	// Comita a transação
	if err := tx.Commit(); err != nil {
		panic("Falha ao comitar transação em EstourouSubgr: " + err.Error())
	}

	return []string{subgrupoNovo, codigoStr}
}

func zfill(s string, length int) string {
    return fmt.Sprintf("%0*s", length, s)
}

func GetEmpresa() int {
	cnx_aux, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnx_aux.Close()

	var empresa int

	err = cnx_aux.QueryRow(`select empresa from cadcli`).Scan(&empresa)
	if err != nil {
		panic("Falha ao executar select: " + err.Error())
	}

	return empresa
}

func CriaFornConversao() {
	cnx_aux, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnx_aux.Close()

	_, err = cnx_aux.Exec(`insert into desfor (codif, nome) select max(codif)+1, 'CONVERSÃO' from DESFOR`)
	if err != nil {
		panic("Falha ao executar insert: " + err.Error())
	}
}

func Contains(slice []int, value int) bool {
    for _, v := range slice {
        if v == value {
            return true
        }
    }
    return false
}

func AtualizaCadpat() {
	cnx_aux, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}

	_, err = cnx_aux.Exec(`MERGE INTO pt_movbem d USING (SELECT
			codigo_pat_mov,
			m.unid_ant,
			m.subunid_ant,
			b.codigo_set
		FROM (
			SELECT
				codigo_pat_mov,
				unid_ant,
				subunid_ant,
				ROW_NUMBER() OVER (PARTITION BY codigo_pat_mov ORDER BY codigo_mov asc) AS rn
			FROM
				PT_MOVBEM
			WHERE
				tipo_mov in ('P','T')
				AND (unid_ant IS NOT NULL AND subunid_ant IS NOT NULL)
		) AS m
		JOIN pt_cadpats b ON b.codigo_des_set = m.unid_ant AND b.subunid_ant = m.subunid_ant
		WHERE
			rn = 1) o ON (o.codigo_pat_mov = d.codigo_pat_mov AND d.tipo_mov = 'A')
		WHEN MATCHED THEN 
			UPDATE SET d.codigo_set_mov = o.codigo_set`)
	if err != nil {
		panic("Falha ao executar update: " + err.Error())
	}

	_, err = cnx_aux.Exec(`MERGE INTO pt_movbem d USING (SELECT
			codigo_pat_mov,
			codigo_cpl_ant_mov
		FROM (
			SELECT
				codigo_pat_mov,
				codigo_cpl_ant_mov,
				ROW_NUMBER() OVER (PARTITION BY codigo_pat_mov ORDER BY codigo_mov asc) AS rn
			FROM
				PT_MOVBEM
			WHERE
				tipo_mov in ('P','T')
				AND codigo_cpl_ant_mov IS NOT null
		) AS m
		WHERE
			rn = 1) o ON (o.codigo_pat_mov = d.codigo_pat_mov AND d.tipo_mov = 'A')
		WHEN MATCHED THEN 
			UPDATE SET d.codigo_cpl_mov = o.codigo_cpl_ant_mov`)
	if err != nil {
		panic("Falha ao executar update: " + err.Error())
	}

	_, err = cnx_aux.Exec(`MERGE into pt_cadpat destino USING (SELECT codigo_cpl_mov, codigo_set_mov, codigo_pat_mov FROM PT_MOVBEM WHERE tipo_mov = 'A') origem 
		ON (destino.codigo_pat = origem.codigo_pat_mov)
		WHEN MATCHED THEN 
			UPDATE SET destino.codigo_set_pat = origem.codigo_set_mov, 
			destino.codigo_cpl_pat = origem.codigo_cpl_mov `)
	if err != nil {
		panic("Falha ao executar merge: " + err.Error())
	}

	_, err = cnx_aux.Exec(`MERGE INTO PT_CADPAT d USING (SELECT codigo_pat_mov, data_mov, codigo_bai_mov FROM PT_MOVBEM WHERE tipo_mov = 'B') o
		ON (d.codigo_pat = o.codigo_pat_mov)
		WHEN MATCHED THEN 
			UPDATE SET d.dtpag_pat = o.data_mov, d.codigo_bai_pat = o.codigo_bai_mov`)
	if err != nil {
		panic("Falha ao executar merge: " + err.Error())
	}

	_, err = cnx_aux.Exec(`MERGE INTO PT_CADPAT d USING (SELECT
			m.codigo_pat_mov,
			data_mov
		FROM (
			SELECT
				codigo_pat_mov,
				data_mov,
				ROW_NUMBER() OVER (PARTITION BY codigo_pat_mov ORDER BY codigo_mov DESC) AS rn
			FROM
				PT_MOVBEM
			WHERE
				tipo_mov IN ('R') AND depreciacao_mov <> 'S'
		) AS m
		WHERE m.rn = 1) o
		ON (d.codigo_pat = o.codigo_pat_mov)
		WHEN MATCHED THEN 
			UPDATE SET d.dtlan_pat = o.data_mov--, d.valatu_pat = o.valor_mov`)
	if err != nil {
		panic("Falha ao executar merge: " + err.Error())
	}

	_, err = cnx_aux.Exec(`MERGE INTO PT_CADPAT d USING (SELECT codigo_pat_mov, sum(valor_mov) valor_mov FROM PT_MOVBEM GROUP BY 1) o
		ON (d.codigo_pat = o.codigo_pat_mov)
		WHEN MATCHED THEN 
			UPDATE SET d.valatu_pat = o.valor_mov`)
	if err != nil {
		panic("Falha ao executar merge: " + err.Error())
	}

	_, err = cnx_aux.Exec(`MERGE INTO PT_CADPAT d USING (SELECT
			m.codigo_pat_mov,
			codigo_cpl_mov
		FROM (
			SELECT
				codigo_pat_mov,
				codigo_cpl_mov,
				ROW_NUMBER() OVER (PARTITION BY codigo_pat_mov ORDER BY codigo_mov DESC) AS rn
			FROM
				PT_MOVBEM
			WHERE
				tipo_mov IN ('P')
		) AS m
		WHERE m.rn = 1) o
		ON (d.codigo_pat = o.codigo_pat_mov)
		WHEN MATCHED THEN 
			UPDATE SET d.codigo_cpl_pat = o.codigo_cpl_mov`)
	if err != nil {
		panic("Falha ao executar merge: " + err.Error())
	}

	_,err = cnx_aux.Exec(`UPDATE PARAMPATRI SET CORRELACAO_PCASP_OK = 'S', CORRELACAO_PLANOCONTAS='S'`)
	if err != nil {
		panic("Falha ao executar update: " + err.Error())
	}
	cnx_aux.Close()
}

func AtualizaNumeroAta() {
	cnx_aux, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnx_aux.Close()

	cnx_psq, err := conexao.ConexaoOrigem()
	if err != nil {
		panic("Falha ao conectar com o banco de origem: " + err.Error())
	}
	defer cnx_psq.Close()

	rows, err := cnx_psq.Query(`select
			'update prolic set controle_ata_rp = ''' || ataregprecoata || '/' || ataregprecoano || ''' where codif = ' || ataregprecofornecedorid || ' and numlic = ' || ataregprecoforprocessoid || ';',
			'update regprecodoc set dtprazo = ''' || ataregprecodatatermino || ''' where numlic = ' || ataregprecoforprocessoid || ';',
			'update regpreco set dtprazo = ''' || ataregprecodatatermino || ''' where numlic = ' || ataregprecoforprocessoid || ' and codif = '||ataregprecofornecedorid||';'
		from
			ataregistropreco a
		where
			ataregprecougid = $1`, GetEmpresa())	
	if err != nil {
		panic("Falha ao executar select: " + err.Error())
	}

	var prolic string
	var regprecodoc string
	var regpreco string
	for rows.Next() {
		rows.Scan(&prolic, &regprecodoc, &regpreco)
		_, err = cnx_aux.Exec(prolic)
		if err != nil {
			panic("Falha ao executar update: " + err.Error())
		}
		_, err = cnx_aux.Exec(regprecodoc)
		if err != nil {
			panic("Falha ao executar update: " + err.Error())
		}
		_, err = cnx_aux.Exec(regpreco)
		if err != nil {
			panic("Falha ao executar update: " + err.Error())
		}
	}
}

func LimpaPatrimonio() {
	cnx_aux, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnx_aux.Close()

	_, err = cnx_aux.Exec(`DELETE FROM PT_MOVBEM`)
	if err != nil {
		panic("Falha ao executar delete: " + err.Error())
	}
	_, err = cnx_aux.Exec(`DELETE FROM PT_CADPAT`)
	if err != nil {
		panic("Falha ao executar delete: " + err.Error())
	}
	_, err = cnx_aux.Exec(`DELETE FROM PT_CADPATS`)
	if err != nil {
		panic("Falha ao executar delete: " + err.Error())
	}
	_, err = cnx_aux.Exec(`DELETE FROM PT_CADPATD`)
	if err != nil {
		panic("Falha ao executar delete: " + err.Error())
	}
	_, err = cnx_aux.Exec(`DELETE FROM PT_CADPATG`)
	if err != nil {
		panic("Falha ao executar delete: " + err.Error())
	}
	_, err = cnx_aux.Exec(`DELETE FROM PT_CADBAI`)
	if err != nil {
		panic("Falha ao executar delete: " + err.Error())
	}
	_, err = cnx_aux.Exec(`DELETE FROM PT_CADTIP`)
	if err != nil {
		panic("Falha ao executar delete: " + err.Error())
	}
}

func DesativaAtivaTriggers(state string) {
	cnx_aux, err := conexao.ConexaoDestino()
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}
	defer cnx_aux.Close()

	query := fmt.Sprintf(`execute block
        as
            declare variable alter_trigger varchar(1024);
        begin
            for select 'alter trigger ' || trim(rdb$trigger_name) || ' %s;' 
            from RDB$TRIGGERS
            where (rdb$trigger_sequence = 200 OR (trim(rdb$trigger_name) STARTING WITH 'TBI_') OR (trim(rdb$trigger_name) STARTING WITH 'TBU_'))
            AND rdb$relation_name IN (
                'CADUNIMEDIDA',
                'CADGRUPO',
                'CADSUBGR',
                'CADEST',
                'DESTINO',
                'CENTROCUSTO',
                'CADORC',
                'ICADORC',
                'FCADORC',
                'VCADORC',
                'CADLIC',
                'CADPROLIC',
                'CADPROLIC_DETALHE',
                'CADPRO_STATUS',
                'CADLIC_SESSAO',
                'PROLIC',
                'PROLICS',
                'CADPRO_PROPOSTA',
                'CADPRO_LANCE',
                'CADPRO_FINAL',
                'CADPRO',
                'CADPROLIC_DETALHE_FIC',
                'REGPRECODOC',
                'REGPRECO',
                'REGPRECOHIS',
                'CADPED',
                'ICADPED',
                'REQUI',
                'ICADREQ',
                'PT_CADTIP',
                'PT_CADPATD',
                'PT_CADPATS',
                'PT_CADPATG',
                'PT_CADPAT',
                'PT_MOVBEM'
            )
            into :alter_trigger
            do
                execute statement :alter_trigger;
        end`, state)

    _, err = cnx_aux.Exec(query)
    if err != nil {
        panic("Falha ao executar execute block: " + err.Error())
    }
}