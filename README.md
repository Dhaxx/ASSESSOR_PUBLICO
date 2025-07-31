# Migração de Dados – Prefeitura Municipal de Cerqueira César/SP
**PostgreSQL → Firebird**

## ✅ Descrição Geral  
Este repositório documenta o processo de migração dos dados dos módulos **Compras** e **Patrimônio** do sistema legado da Prefeitura Municipal de Cerqueira César/SP. A migração envolveu a extração dos dados de um banco **PostgreSQL**, transformação e carga em um banco **Firebird**, conforme os requisitos da nova solução adotada.

## 🛠️ Tecnologias Utilizadas  
- **Golang**: Desenvolvimento da aplicação de migração, responsável pelo processo de ETL (Extract, Transform, Load)  
- **SQL**: Consultas, manipulação e transformação de dados  
- **PostgreSQL**: Banco de dados de origem  
- **Firebird**: Banco de dados de destino  
- **Windows-1252 / UTF-8**: Tratamento de codificação de caracteres para compatibilidade entre sistemas

## 📦 Escopo da Migração  
- Conversão de dados dos seguintes módulos:
  - ✅ Compras  
  - ✅ Patrimônio  
- Transformação e adaptação da estrutura de dados para atender ao novo modelo  
- Correção e padronização de dados inconsistentes  
- Geração de relatórios de conferência e logs de auditoria  
- Execução de scripts SQL auxiliares de pós-processamento  

## 📈 Resultados  
- Migração executada com sucesso e dentro do prazo estabelecido  
- Dados homologados pelos usuários responsáveis da prefeitura  
- Aplicação modular reutilizável em futuras migrações com ajustes mínimos
