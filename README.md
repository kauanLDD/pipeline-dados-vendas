# 📊 Pipeline de Dados - Análise de Vendas

Projeto acadêmico de pipeline de dados implementando a arquitetura **Medallion** (Bronze, Silver, Gold) para análise de dados de vendas online.

## 🎯 Objetivo

Desenvolver um pipeline completo de processamento de dados seguindo as melhores práticas de engenharia de dados, desde a ingestão bruta até análises agregadas e relatórios de qualidade.

## 🏗️ Arquitetura

### Camadas do Pipeline

```
data.csv (dados brutos)
    ↓
📦 Bronze Layer (Ingestão)
    ↓
🔧 Silver Layer (Limpeza e Transformação)
    ↓
🏆 Gold Layer (Agregações e Análises)
    ↓
💾 Database (SQLite)
    ↓
📊 Análises e Relatórios
```

### Estrutura de Pastas

```
pipiline_projeto/
│
├── data/
│   ├── bronze/          # Dados brutos
│   ├── silver/          # Dados limpos
│   ├── gold/            # Dados agregados
│   └── pipeline.db      # Banco de dados SQLite
│
├── 01_bronze_layer.ipynb      # Camada Bronze (Ingestão)
├── 02_silver_layer.ipynb      # Camada Silver (Limpeza)
├── 03_gold_layer.ipynb        # Camada Gold (Agregações)
├── 04_load_database.ipynb     # Carga no Banco de Dados
├── 05_sql_queries.ipynb       # Consultas SQL
├── 06_quality_report.ipynb    # Relatório de Qualidade
│
└── data.csv                   # Arquivo de dados original
```

## 📓 Notebooks

### 1️⃣ Bronze Layer - Ingestão de Dados
- Leitura dos dados brutos do arquivo CSV
- Adição de metadados (data de ingestão, fonte)
- Salvamento na camada Bronze sem transformações

### 2️⃣ Silver Layer - Limpeza e Transformação
- Remoção de valores nulos e duplicados
- Tratamento de tipos de dados
- Padronização de formatos
- Criação de colunas calculadas
- Identificação de devoluções

### 3️⃣ Gold Layer - Agregações e Análises
- **Métricas Temporais**: Análises diárias e mensais
- **Análise de Produtos**: Desempenho e ranking
- **Análise de Clientes**: Comportamento e segmentação
- **Análise Geográfica**: Receita por país
- **Análise de Devoluções**: Taxa e motivos

### 4️⃣ Load Database - Carga no Banco
- Criação do banco de dados SQLite
- Carga dos dados das camadas Silver e Gold
- Criação de índices para otimização

### 5️⃣ SQL Queries - Consultas Analíticas
- Top produtos e países
- Análise de clientes VIP
- Métricas de crescimento
- Análise de devoluções
- Tendências temporais

### 6️⃣ Quality Report - Relatório de Qualidade
- **Completude**: Análise de dados preenchidos
- **Unicidade**: Detecção de duplicatas
- **Valores Ausentes**: Identificação de gaps
- **Estatísticas**: Métricas básicas
- **Score de Qualidade**: Avaliação geral

## 🛠️ Tecnologias Utilizadas

- **Python 3.x**
- **Pandas** - Manipulação de dados
- **SQLite3** - Banco de dados
- **Matplotlib** - Visualizações
- **Jupyter Notebook** - Ambiente de desenvolvimento

## 📊 Dataset

O projeto utiliza dados de vendas online contendo:
- Número da fatura (InvoiceNo)
- Código do produto (StockCode)
- Descrição do produto (Description)
- Quantidade (Quantity)
- Data da fatura (InvoiceDate)
- Preço unitário (UnitPrice)
- ID do cliente (CustomerID)
- País (Country)

## 🚀 Como Executar

### Pré-requisitos

```bash
pip install pandas matplotlib notebook
```

### Executar o Pipeline

1. **Clone o repositório**:
```bash
git clone <seu-repositorio>
cd pipiline_projeto
```

2. **Execute os notebooks na ordem**:
   - `01_bronze_layer.ipynb`
   - `02_silver_layer.ipynb`
   - `03_gold_layer.ipynb`
   - `04_load_database.ipynb`
   - `05_sql_queries.ipynb`
   - `06_quality_report.ipynb`

3. **Abra o Jupyter Notebook**:
```bash
jupyter notebook
```

## 📈 Resultados

O pipeline gera:
- ✅ Dados limpos e padronizados (Silver)
- ✅ Análises agregadas (Gold)
- ✅ Banco de dados estruturado
- ✅ Relatórios de qualidade
- ✅ Insights de negócio

## 📝 Métricas de Qualidade

- **Completude**: >99%
- **Unicidade**: >99%
- **Registros processados**: 536.639
- **Score de Qualidade**: Excelente ⭐⭐⭐⭐⭐

## 🎓 Aprendizados

- Implementação de arquitetura Medallion
- Boas práticas em ETL/ELT
- Qualidade e governança de dados
- SQL para análise de dados
- Documentação de projetos de dados

## 👨‍💻 Autor

Projeto desenvolvido como parte do curso de Engenharia/Ciência de Dados.

## 📄 Licença

Este projeto é de código aberto e está disponível sob a licença MIT.

---

⭐ Se este projeto te ajudou, considere dar uma estrela!


