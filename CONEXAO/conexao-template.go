package conexao

import (
	"database/sql"
	_ "github.com/nakagami/firebirdsql"
	_ "github.com/lib/pq"
)

func ConexaoOrigemTemplate() (*sql.DB, error) {
	db, err := sql.Open("postgres", "user=xxxx dbname=xxx password=xxx host=xxx sslmode=xxx")
	if err != nil {
		panic("Falha ao conectar com o banco de origem: " + err.Error())
	}
	return db, nil
}

func ConexaoDestinoTemplate() (*sql.DB, error) {
	db, err := sql.Open("firebirdsql", "USER:pass@host:port/direct?charset=encode&auth_plugin_name=Legacy_Auth&timezone=America/Sao_Paulo")
	if err != nil {
		panic("Falha ao conectar com o banco de destino: " + err.Error())
	}
	return db, nil
}