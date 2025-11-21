# importando bibliotecas
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import pandas as pd
import numpy as np
import sqlite3

# configurando caminhos
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
BASE_DATA_PATH = os.path.join(PROJECT_ROOT, 'data')

# criando estrutura de diretorios
def criar_estrutura_diretorios():
    diretorios = [
        os.path.join(BASE_DATA_PATH, 'bronze'),
        os.path.join(BASE_DATA_PATH, 'silver'),
        os.path.join(BASE_DATA_PATH, 'gold'),
        BASE_DATA_PATH
    ]
    
    for diretorio in diretorios:
        os.makedirs(diretorio, exist_ok=True)
        print(f"Diretorio criado/verificado: {diretorio}")

try:
    criar_estrutura_diretorios()
except Exception as e:
    print(f"Nao foi possivel criar diretorios na inicializacao: {e}")

# funcao bronze layer - extracao de dados brutos
def extract_bronze(**context):
    print("BRONZE LAYER - Iniciando ingestao de dados")
    
    try:
        criar_estrutura_diretorios()
        
        url_github = "https://raw.githubusercontent.com/kauanLDD/pipeline-dados-vendas/main/data.csv"
        
        print(f"Baixando dados do GitHub: {url_github}")
        df = pd.read_csv(url_github, sep=',', encoding='ISO-8859-1', low_memory=False)
        
        df['data_ingestao'] = datetime.now()
        df['fonte_arquivo'] = 'github/pipeline-dados-vendas/data.csv'
        
        print(f"Dados carregados: {df.shape[0]:,} linhas x {df.shape[1]} colunas")
        
        caminho_bronze = os.path.join(BASE_DATA_PATH, 'bronze', 'dados_brutos.csv')
        df.to_csv(caminho_bronze, index=False)
        
        print(f"Dados salvos na camada Bronze: {caminho_bronze}")
        print(f"Total de registros: {len(df):,}")
        
        return "Bronze concluido"
        
    except Exception as e:
        print(f"ERRO na camada Bronze: {str(e)}")
        raise

# funcao silver layer - limpeza e transformacao dos dados
def transform_silver(**context):
    print("SILVER LAYER - Iniciando limpeza e transformacao")
    
    try:
        caminho_bronze = os.path.join(BASE_DATA_PATH, 'bronze', 'dados_brutos.csv')
        print(f"Carregando dados de: {caminho_bronze}")
        
        df = pd.read_csv(caminho_bronze)
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], format='%m/%d/%Y %H:%M')
        
        print(f"Registros antes da limpeza: {len(df):,}")
        
        df = df.drop_duplicates()
        print(f"Apos remocao de duplicatas: {len(df):,}")
        
        df['CustomerID'] = df['CustomerID'].fillna(0).astype(int)
        df['Description'] = df['Description'].fillna('PRODUTO SEM DESCRICAO')
        
        df['Country'] = df['Country'].astype('category')
        df['StockCode'] = df['StockCode'].astype('category')
        
        df['Description'] = df['Description'].str.strip().str.upper()
        df['Country'] = df['Country'].astype(str).str.strip()
        
        df = df[df['UnitPrice'] >= 0]
        df['eh_devolucao'] = df['Quantity'] < 0
        df['valor_total'] = df['Quantity'] * df['UnitPrice']
        
        df['data_processamento'] = datetime.now()
        
        print(f"Registros apos transformacao: {len(df):,}")
        
        caminho_silver = os.path.join(BASE_DATA_PATH, 'silver', 'dados_limpos.csv')
        df.to_csv(caminho_silver, index=False)
        
        print(f"Dados salvos na camada Silver: {caminho_silver}")
        print(f"Total de registros: {len(df):,} linhas x {df.shape[1]} colunas")
        
        return "Silver concluido"
        
    except Exception as e:
        print(f"ERRO na camada Silver: {str(e)}")
        raise

# funcao gold layer - criacao de metricas e analises
def aggregate_gold(**context):
    print("GOLD LAYER - Iniciando criacao de metricas e analises")
    
    try:
        caminho_silver = os.path.join(BASE_DATA_PATH, 'silver', 'dados_limpos.csv')
        print(f"Carregando dados de: {caminho_silver}")
        
        df = pd.read_csv(caminho_silver)
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
        df['data_compra'] = df['InvoiceDate'].dt.date
        
        print(f"Dados carregados: {len(df):,} registros")
        
        os.makedirs(os.path.join(BASE_DATA_PATH, 'gold'), exist_ok=True)
        
        print("\n1. Criando metricas diarias")
        metricas_diarias = df.groupby('data_compra').agg({
            'InvoiceNo': 'nunique',
            'valor_total': 'sum',
            'CustomerID': 'nunique'
        }).reset_index()
        
        metricas_diarias.columns = ['data', 'total_pedidos', 'receita_total', 'clientes_unicos']
        metricas_diarias['ticket_medio'] = (metricas_diarias['receita_total'] / metricas_diarias['total_pedidos']).round(2)
        metricas_diarias = metricas_diarias.sort_values('data')
        metricas_diarias.to_csv(os.path.join(BASE_DATA_PATH, 'gold', 'metricas_diarias.csv'), index=False)
        print(f"Metricas diarias: {len(metricas_diarias)} dias")
        
        print("\n2. Criando analise RFM de clientes")
        data_referencia = df['InvoiceDate'].max()
        
        analise_clientes = df.groupby('CustomerID').agg({
            'InvoiceDate': 'max',
            'InvoiceNo': 'nunique',
            'valor_total': 'sum'
        }).reset_index()
        
        analise_clientes.columns = ['id_cliente', 'ultima_compra', 'frequencia_pedidos', 'valor_total_gasto']
        analise_clientes['dias_ultima_compra'] = (data_referencia - analise_clientes['ultima_compra']).dt.days
        
        def calcular_score_rfm(serie, labels):
            try:
                result = pd.qcut(serie, q=5, labels=labels, duplicates='drop')
                return result
            except ValueError:
                try:
                    _, bins = pd.qcut(serie, q=5, duplicates='drop', retbins=True)
                    n_bins = len(bins) - 1
                    return pd.qcut(serie, q=n_bins, labels=labels[:n_bins], duplicates='drop')
                except ValueError:
                    return pd.cut(serie, bins=5, labels=labels, duplicates='drop')
        
        analise_clientes['score_recencia'] = calcular_score_rfm(analise_clientes['dias_ultima_compra'], [5,4,3,2,1])
        analise_clientes['score_frequencia'] = calcular_score_rfm(analise_clientes['frequencia_pedidos'], [1,2,3,4,5])
        analise_clientes['score_monetario'] = calcular_score_rfm(analise_clientes['valor_total_gasto'], [1,2,3,4,5])
        analise_clientes['score_rfm_total'] = analise_clientes[['score_recencia', 'score_frequencia', 'score_monetario']].astype(float).sum(axis=1)
        analise_clientes.to_csv(os.path.join(BASE_DATA_PATH, 'gold', 'analise_clientes.csv'), index=False)
        print(f"Analise de clientes: {len(analise_clientes)} clientes")
        
        print("\n3. Criando analise de desempenho de produtos")
        desempenho_produtos = df.groupby(['StockCode', 'Description']).agg({
            'Quantity': 'sum',
            'valor_total': 'sum',
            'InvoiceNo': 'nunique'
        }).reset_index()
        
        desempenho_produtos.columns = ['codigo_produto', 'nome_produto', 'quantidade_vendida', 'receita_total', 'num_pedidos']
        desempenho_produtos = desempenho_produtos.sort_values('receita_total', ascending=False)
        desempenho_produtos.to_csv(os.path.join(BASE_DATA_PATH, 'gold', 'desempenho_produtos.csv'), index=False)
        print(f"Desempenho de produtos: {len(desempenho_produtos)} produtos")
        
        print("\n4. Criando analise de vendas por pais")
        analise_paises = df.groupby('Country').agg({
            'InvoiceNo': 'nunique',
            'valor_total': 'sum',
            'CustomerID': 'nunique'
        }).reset_index()
        
        analise_paises.columns = ['pais', 'total_pedidos', 'receita_total', 'clientes_unicos']
        analise_paises = analise_paises.sort_values('receita_total', ascending=False)
        analise_paises.to_csv(os.path.join(BASE_DATA_PATH, 'gold', 'analise_paises.csv'), index=False)
        print(f"Analise de paises: {len(analise_paises)} paises")
        
        print("\n5. Criando analise de devolucoes")
        devolucoes = df[df['eh_devolucao'] == True]
        
        if len(devolucoes) > 0:
            analise_devolucoes = devolucoes.groupby(['StockCode', 'Description']).agg({
                'Quantity': 'sum',
                'valor_total': 'sum',
                'InvoiceNo': 'nunique'
            }).reset_index()
            
            analise_devolucoes.columns = ['codigo_produto', 'nome_produto', 'quantidade_devolvida', 'valor_devolvido', 'num_devolucoes']
            analise_devolucoes = analise_devolucoes.sort_values('valor_devolvido', ascending=True)
            analise_devolucoes.to_csv(os.path.join(BASE_DATA_PATH, 'gold', 'analise_devolucoes.csv'), index=False)
            print(f"Analise de devolucoes: {len(analise_devolucoes)} produtos")
        else:
            print("Nao ha devolucoes nos dados")
        
        print("\n6. Criando metricas mensais")
        df['ano_mes'] = df['InvoiceDate'].dt.to_period('M').astype(str)
        
        metricas_mensais = df.groupby('ano_mes').agg({
            'InvoiceNo': 'nunique',
            'valor_total': 'sum',
            'CustomerID': 'nunique'
        }).reset_index()
        
        metricas_mensais.columns = ['ano_mes', 'total_pedidos', 'receita_total', 'clientes_unicos']
        metricas_mensais['ticket_medio'] = (metricas_mensais['receita_total'] / metricas_mensais['total_pedidos']).round(2)
        metricas_mensais.to_csv(os.path.join(BASE_DATA_PATH, 'gold', 'metricas_mensais.csv'), index=False)
        print(f"Metricas mensais: {len(metricas_mensais)} meses")
        
        print("GOLD LAYER concluido com sucesso")
        
        return "Gold concluido"
        
    except Exception as e:
        print(f"ERRO na camada Gold: {str(e)}")
        raise

# funcao load database - carregar dados no banco sqlite
def load_database(**context):
    print("LOAD DATABASE - Iniciando carga no banco de dados")
    
    try:
        os.makedirs(BASE_DATA_PATH, exist_ok=True)
        
        db_path = os.path.join(BASE_DATA_PATH, 'pipeline.db')
        conn = sqlite3.connect(db_path)
        print(f"Conexao estabelecida: {db_path}")
        
        print("\n1. Carregando dados da camada Silver")
        df_limpo = pd.read_csv(os.path.join(BASE_DATA_PATH, 'silver', 'dados_limpos.csv'))
        df_limpo['InvoiceDate'] = pd.to_datetime(df_limpo['InvoiceDate'])
        df_limpo.to_sql('vendas', conn, if_exists='replace', index=False)
        print(f"Tabela 'vendas': {len(df_limpo):,} registros")
        
        print("\n2. Carregando dados da camada Gold")
        
        metricas_diarias = pd.read_csv(os.path.join(BASE_DATA_PATH, 'gold', 'metricas_diarias.csv'))
        metricas_diarias['data'] = pd.to_datetime(metricas_diarias['data'])
        metricas_diarias.to_sql('metricas_diarias', conn, if_exists='replace', index=False)
        print(f"Tabela 'metricas_diarias': {len(metricas_diarias)} registros")
        
        analise_clientes = pd.read_csv(os.path.join(BASE_DATA_PATH, 'gold', 'analise_clientes.csv'))
        analise_clientes['ultima_compra'] = pd.to_datetime(analise_clientes['ultima_compra'])
        analise_clientes.to_sql('analise_clientes', conn, if_exists='replace', index=False)
        print(f"Tabela 'analise_clientes': {len(analise_clientes)} registros")
        
        desempenho_produtos = pd.read_csv(os.path.join(BASE_DATA_PATH, 'gold', 'desempenho_produtos.csv'))
        desempenho_produtos.to_sql('desempenho_produtos', conn, if_exists='replace', index=False)
        print(f"Tabela 'desempenho_produtos': {len(desempenho_produtos)} registros")
        
        analise_paises = pd.read_csv(os.path.join(BASE_DATA_PATH, 'gold', 'analise_paises.csv'))
        analise_paises.to_sql('analise_paises', conn, if_exists='replace', index=False)
        print(f"Tabela 'analise_paises': {len(analise_paises)} registros")
        
        metricas_mensais = pd.read_csv(os.path.join(BASE_DATA_PATH, 'gold', 'metricas_mensais.csv'))
        metricas_mensais.to_sql('metricas_mensais', conn, if_exists='replace', index=False)
        print(f"Tabela 'metricas_mensais': {len(metricas_mensais)} registros")
        
        caminho_devolucoes = os.path.join(BASE_DATA_PATH, 'gold', 'analise_devolucoes.csv')
        if os.path.exists(caminho_devolucoes):
            analise_devolucoes = pd.read_csv(caminho_devolucoes)
            analise_devolucoes.to_sql('analise_devolucoes', conn, if_exists='replace', index=False)
            print(f"Tabela 'analise_devolucoes': {len(analise_devolucoes)} registros")
        
        print("\n3. Criando tabelas relacionadas")
        
        clientes = df_limpo[['CustomerID', 'Country']].drop_duplicates(subset=['CustomerID'])
        clientes.columns = ['id_cliente', 'pais']
        clientes.to_sql('clientes', conn, if_exists='replace', index=False)
        print(f"Tabela 'clientes': {len(clientes)} registros")
        
        produtos = df_limpo[['StockCode', 'Description', 'UnitPrice']].drop_duplicates(subset=['StockCode'])
        produtos.columns = ['codigo_produto', 'nome_produto', 'preco_unitario']
        produtos.to_sql('produtos', conn, if_exists='replace', index=False)
        print(f"Tabela 'produtos': {len(produtos)} registros")
        
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tabelas = cursor.fetchall()
        
        print("TABELAS NO BANCO DE DADOS:")
        
        for tabela in tabelas:
            cursor.execute(f"SELECT COUNT(*) FROM {tabela[0]};")
            count = cursor.fetchone()[0]
            print(f"  {tabela[0]}: {count:,} registros")
        
        conn.close()
        
        print("LOAD DATABASE concluido com sucesso")
        print(f"Banco de dados: {db_path}")
        
        return "Load concluido"
        
    except Exception as e:
        print(f"ERRO no carregamento do banco: {str(e)}")
        raise

# configurando dag
default_args = {
    'owner': 'equipe-pipeline',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pipeline_etl_completo',
    default_args=default_args,
    description='Pipeline ETL completo: Bronze -> Silver -> Gold -> Database',
    schedule='@daily',
    catchup=False,
    tags=['pipeline', 'etl', 'medallion', 'vendas']
)

# definindo tarefas
task_bronze = PythonOperator(
    task_id='extract_bronze',
    python_callable=extract_bronze,
    dag=dag
)

task_silver = PythonOperator(
    task_id='transform_silver',
    python_callable=transform_silver,
    dag=dag
)

task_gold = PythonOperator(
    task_id='aggregate_gold',
    python_callable=aggregate_gold,
    dag=dag
)

task_load = PythonOperator(
    task_id='load_database',
    python_callable=load_database,
    dag=dag
)

# definindo dependencias entre tarefas
task_bronze >> task_silver >> task_gold >> task_load

