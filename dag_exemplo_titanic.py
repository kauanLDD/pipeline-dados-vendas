# importando bibliotecas
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import sqlite3
import os

# configurando caminhos
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '\opt\airflow')
BASE_DATA_PATH = os.path.join(AIRFLOW_HOME, 'data', 'titanic')

# criando estrutura de diretorios
def criar_estrutura_diretorios():
    diretorios = [
        os.path.join(BASE_DATA_PATH, 'bronze'),
        os.path.join(BASE_DATA_PATH, 'silver'),
        os.path.join(BASE_DATA_PATH, 'gold'),
        BASE_DATA_PATH
    ]
    
    for diretorio in diretorios:
        try:
            os.makedirs(diretorio, exist_ok=True)
            if os.path.exists(diretorio):
                print(f"Diretorio criado/verificado: {diretorio}")
            else:
                print(f"Diretorio nao foi criado: {diretorio}")
        except PermissionError as e:
            print(f"Permissao negada ao criar {diretorio}: {e}")
        except Exception as e:
            print(f"Erro ao criar diretorio {diretorio}: {e}")

try:
    criar_estrutura_diretorios()
except Exception as e:
    print(f"Nao foi possivel criar diretorios na inicializacao: {e}")

# funcao extrair dados - baixar dados do github
def extrair_dados_titanic():
    print("Iniciando extracao dos dados do Titanic")
    
    url = "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"
    df = pd.read_csv(url)
    
    print(f"Dados extraidos com sucesso")
    print(f"Total de registros: {len(df)}")
    print(f"Colunas: {list(df.columns)}")
    
    bronze_dir = os.path.join(BASE_DATA_PATH, 'bronze')
    try:
        os.makedirs(bronze_dir, exist_ok=True)
        print(f"Diretorio bronze criado/verificado: {bronze_dir}")
    except Exception as e:
        print(f"Nao foi possivel criar diretorio bronze: {e}")
        raise
    
    caminho_bronze = os.path.join(bronze_dir, 'titanic_bronze.csv')
    try:
        df.to_csv(caminho_bronze, index=False)
        print(f"Dados salvos em: {caminho_bronze}")
    except Exception as e:
        print(f"Nao foi possivel salvar arquivo: {e}")
        raise
    
    return caminho_bronze

# funcao analisar qualidade - verificar qualidade dos dados
def analisar_qualidade():
    print("Analisando qualidade dos dados")
    
    df = pd.read_csv(os.path.join(BASE_DATA_PATH, 'bronze', 'titanic_bronze.csv'))
    
    print(f"Total de registros: {len(df)}")
    print(f"Total de colunas: {len(df.columns)}")
    
    print("Valores nulos por coluna:")
    nulos = df.isnull().sum()
    for coluna, qtd in nulos.items():
        if qtd > 0:
            percentual = (qtd / len(df)) * 100
            print(f"  {coluna}: {qtd} ({percentual:.1f}%)")
    
    print("Tipos de dados:")
    print(df.dtypes)
    
    print("Estatisticas basicas:")
    print(df.describe())
    
    return "Analise concluida"

# funcao limpar dados - remover nulos e colunas desnecessarias
def limpar_dados():
    print("Iniciando limpeza dos dados")
    
    df = pd.read_csv(os.path.join(BASE_DATA_PATH, 'bronze', 'titanic_bronze.csv'))
    
    print(f"Registros antes da limpeza: {len(df)}")
    
    colunas_remover = ['PassengerId', 'Name', 'Ticket', 'Cabin']
    df = df.drop(columns=colunas_remover, errors='ignore')
    print(f"Colunas removidas: {colunas_remover}")
    
    if 'Age' in df.columns:
        mediana_age = df['Age'].median()
        df['Age'].fillna(mediana_age, inplace=True)
        print(f"Age: valores nulos preenchidos com mediana {mediana_age:.1f}")
    
    if 'Embarked' in df.columns:
        moda_embarked = df['Embarked'].mode()[0]
        df['Embarked'].fillna(moda_embarked, inplace=True)
        print(f"Embarked: valores nulos preenchidos com moda '{moda_embarked}'")
    
    df = df.dropna()
    print(f"Registros apos limpeza: {len(df)}")
    
    silver_dir = os.path.join(BASE_DATA_PATH, 'silver')
    os.makedirs(silver_dir, exist_ok=True)
    
    caminho_silver = os.path.join(silver_dir, 'titanic_silver.csv')
    df.to_csv(caminho_silver, index=False)
    print(f"Dados limpos salvos em: {caminho_silver}")
    
    return caminho_silver

# funcao transformar dados - criar novas colunas e features
def transformar_dados():
    print("Transformando dados")
    
    df = pd.read_csv(os.path.join(BASE_DATA_PATH, 'silver', 'titanic_silver.csv'))
    
    df['Faixa_Etaria'] = pd.cut(df['Age'], 
                                  bins=[0, 18, 35, 60, 100],
                                  labels=['Crianca', 'Jovem', 'Adulto', 'Idoso'])
    print("Coluna 'Faixa_Etaria' criada")
    
    df['Categoria_Tarifa'] = pd.cut(df['Fare'],
                                      bins=[0, 10, 30, 100, 1000],
                                      labels=['Baixa', 'Media', 'Alta', 'Premium'])
    print("Coluna 'Categoria_Tarifa' criada")
    
    df['Tamanho_Familia'] = df['SibSp'] + df['Parch'] + 1
    print("Coluna 'Tamanho_Familia' criada")
    
    df['Viajando_Sozinho'] = (df['Tamanho_Familia'] == 1).astype(int)
    print("Coluna 'Viajando_Sozinho' criada")
    
    print(f"Total de colunas apos transformacao: {len(df.columns)}")
    print(f"Colunas: {list(df.columns)}")
    
    gold_dir = os.path.join(BASE_DATA_PATH, 'gold')
    os.makedirs(gold_dir, exist_ok=True)
    
    caminho_transformado = os.path.join(gold_dir, 'titanic_transformado.csv')
    df.to_csv(caminho_transformado, index=False)
    print(f"Dados transformados salvos em: {caminho_transformado}")
    
    return caminho_transformado

# funcao calcular estatisticas - calcular metricas de sobrevivencia
def calcular_estatisticas_sobrevivencia():
    print("Calculando estatisticas de sobrevivencia")
    
    df = pd.read_csv(os.path.join(BASE_DATA_PATH, 'gold', 'titanic_transformado.csv'))
    
    taxa_sobrevivencia = df['Survived'].mean() * 100
    print(f"Taxa geral de sobrevivencia: {taxa_sobrevivencia:.2f}%")
    
    print("Sobrevivencia por Classe:")
    sobrev_classe = df.groupby('Pclass')['Survived'].agg(['mean', 'count'])
    sobrev_classe['mean'] = sobrev_classe['mean'] * 100
    sobrev_classe.columns = ['Taxa_Sobrevivencia_%', 'Total_Passageiros']
    print(sobrev_classe)
    
    print("Sobrevivencia por Sexo:")
    sobrev_sexo = df.groupby('Sex')['Survived'].agg(['mean', 'count'])
    sobrev_sexo['mean'] = sobrev_sexo['mean'] * 100
    sobrev_sexo.columns = ['Taxa_Sobrevivencia_%', 'Total_Passageiros']
    print(sobrev_sexo)
    
    print("Sobrevivencia por Faixa Etaria:")
    sobrev_idade = df.groupby('Faixa_Etaria')['Survived'].agg(['mean', 'count'])
    sobrev_idade['mean'] = sobrev_idade['mean'] * 100
    sobrev_idade.columns = ['Taxa_Sobrevivencia_%', 'Total_Passageiros']
    print(sobrev_idade)
    
    gold_dir = os.path.join(BASE_DATA_PATH, 'gold')
    os.makedirs(gold_dir, exist_ok=True)
    
    caminho_stats = os.path.join(gold_dir, 'estatisticas_sobrevivencia.csv')
    
    stats_dict = {
        'Metrica': ['Taxa_Geral', 'Sobreviventes', 'Nao_Sobreviventes'],
        'Valor': [
            taxa_sobrevivencia,
            df['Survived'].sum(),
            len(df) - df['Survived'].sum()
        ]
    }
    df_stats = pd.DataFrame(stats_dict)
    df_stats.to_csv(caminho_stats, index=False)
    
    print(f"Estatisticas salvas em: {caminho_stats}")
    
    return caminho_stats

# funcao criar agregacoes - criar agregacoes para analise
def criar_agregacoes():
    print("Criando agregacoes")
    
    df = pd.read_csv(os.path.join(BASE_DATA_PATH, 'gold', 'titanic_transformado.csv'))
    
    agg_classe_sexo = df.groupby(['Pclass', 'Sex']).agg({
        'Survived': ['mean', 'count'],
        'Age': 'mean',
        'Fare': 'mean'
    }).reset_index()
    agg_classe_sexo.columns = ['Classe', 'Sexo', 'Taxa_Sobrevivencia', 
                                'Total', 'Idade_Media', 'Tarifa_Media']
    
    print("Agregacao por Classe e Sexo:")
    print(agg_classe_sexo)
    
    agg_embarque = df.groupby('Embarked').agg({
        'Survived': ['mean', 'count'],
        'Fare': 'mean'
    }).reset_index()
    agg_embarque.columns = ['Porto_Embarque', 'Taxa_Sobrevivencia', 
                            'Total_Passageiros', 'Tarifa_Media']
    
    print("Agregacao por Porto de Embarque:")
    print(agg_embarque)
    
    gold_dir = os.path.join(BASE_DATA_PATH, 'gold')
    os.makedirs(gold_dir, exist_ok=True)
    
    caminho_agg1 = os.path.join(gold_dir, 'agg_classe_sexo.csv')
    caminho_agg2 = os.path.join(gold_dir, 'agg_embarque.csv')
    
    agg_classe_sexo.to_csv(caminho_agg1, index=False)
    agg_embarque.to_csv(caminho_agg2, index=False)
    
    print(f"Agregacoes salvas:")
    print(f"  - {caminho_agg1}")
    print(f"  - {caminho_agg2}")
    
    return "Agregacoes criadas"

# funcao criar banco dados - criar tabelas no sqlite
def criar_banco_dados():
    print("Criando banco de dados SQLite")
    
    os.makedirs(BASE_DATA_PATH, exist_ok=True)
    
    db_path = os.path.join(BASE_DATA_PATH, 'titanic.db')
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS passageiros (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            survived INTEGER,
            pclass INTEGER,
            sex TEXT,
            age REAL,
            sibsp INTEGER,
            parch INTEGER,
            fare REAL,
            embarked TEXT,
            faixa_etaria TEXT,
            categoria_tarifa TEXT,
            tamanho_familia INTEGER,
            viajando_sozinho INTEGER
        )
    ''')
    
    print("Tabela 'passageiros' criada")
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS estatisticas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            metrica TEXT,
            valor REAL
        )
    ''')
    
    print("Tabela 'estatisticas' criada")
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS agregacao_classe_sexo (
            classe INTEGER,
            sexo TEXT,
            taxa_sobrevivencia REAL,
            total INTEGER,
            idade_media REAL,
            tarifa_media REAL
        )
    ''')
    
    print("Tabela 'agregacao_classe_sexo' criada")
    
    conn.commit()
    conn.close()
    
    print(f"Banco de dados criado em: {db_path}")
    
    return db_path

# funcao carregar dados banco - inserir dados nas tabelas
def carregar_dados_banco():
    print("Carregando dados no banco")
    
    os.makedirs(BASE_DATA_PATH, exist_ok=True)
    
    db_path = os.path.join(BASE_DATA_PATH, 'titanic.db')
    conn = sqlite3.connect(db_path)
    
    caminho_transformado = os.path.join(BASE_DATA_PATH, 'gold', 'titanic_transformado.csv')
    df_main = pd.read_csv(caminho_transformado)
    df_main.to_sql('passageiros', conn, if_exists='replace', index=False)
    print(f"Tabela 'passageiros' carregada: {len(df_main)} registros")
    
    caminho_stats = os.path.join(BASE_DATA_PATH, 'gold', 'estatisticas_sobrevivencia.csv')
    df_stats = pd.read_csv(caminho_stats)
    df_stats.to_sql('estatisticas', conn, if_exists='replace', index=False)
    print(f"Tabela 'estatisticas' carregada: {len(df_stats)} registros")
    
    caminho_agg = os.path.join(BASE_DATA_PATH, 'gold', 'agg_classe_sexo.csv')
    df_agg = pd.read_csv(caminho_agg)
    df_agg.to_sql('agregacao_classe_sexo', conn, if_exists='replace', index=False)
    print(f"Tabela 'agregacao_classe_sexo' carregada: {len(df_agg)} registros")
    
    conn.close()
    
    print(f"Banco de dados: {db_path}")
    print("Tabelas criadas:")
    print("  - passageiros")
    print("  - estatisticas")
    print("  - agregacao_classe_sexo")
    
    return "Dados carregados com sucesso"

# funcao validar carga - verificar se dados foram carregados corretamente
def validar_carga():
    print("Validando carga no banco de dados")
    
    db_path = os.path.join(BASE_DATA_PATH, 'titanic.db')
    conn = sqlite3.connect(db_path)
    
    query1 = "SELECT COUNT(*) as total FROM passageiros"
    result1 = pd.read_sql_query(query1, conn)
    print(f"Tabela passageiros: {result1['total'].values[0]} registros")
    
    query2 = "SELECT * FROM estatisticas"
    result2 = pd.read_sql_query(query2, conn)
    print(f"Tabela estatisticas: {len(result2)} registros")
    print(result2)
    
    query3 = "SELECT COUNT(*) as total FROM agregacao_classe_sexo"
    result3 = pd.read_sql_query(query3, conn)
    print(f"Tabela agregacao_classe_sexo: {result3['total'].values[0]} registros")
    
    query4 = """
        SELECT 
            pclass,
            sex,
            COUNT(*) as total,
            AVG(age) as idade_media,
            SUM(survived) as sobreviventes
        FROM passageiros
        GROUP BY pclass, sex
        ORDER BY pclass, sex
    """
    result4 = pd.read_sql_query(query4, conn)
    print("Consulta analitica - Resumo por classe e sexo:")
    print(result4)
    
    conn.close()
    
    print("Validacao concluida")
    print("Todos os dados foram carregados corretamente")
    
    return "Validacao OK"

# configurando dag
default_args = {
    'owner': 'professor',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

dag = DAG(
    'etl_titanic_completo',
    default_args=default_args,
    description='Pipeline ETL completo do dataset Titanic: GitHub -> SQLite',
    schedule='@daily',
    catchup=False,
    tags=['exemplo', 'etl', 'titanic', 'completo']
)

# definindo tarefas
task_1_extrair = PythonOperator(
    task_id='extrair_dados_titanic',
    python_callable=extrair_dados_titanic,
    dag=dag
)

task_2_analisar = PythonOperator(
    task_id='analisar_qualidade',
    python_callable=analisar_qualidade,
    dag=dag
)

task_3_limpar = PythonOperator(
    task_id='limpar_dados',
    python_callable=limpar_dados,
    dag=dag
)

task_4_transformar = PythonOperator(
    task_id='transformar_dados',
    python_callable=transformar_dados,
    dag=dag
)

task_5_calcular_stats = PythonOperator(
    task_id='calcular_estatisticas',
    python_callable=calcular_estatisticas_sobrevivencia,
    dag=dag
)

task_6_agregacoes = PythonOperator(
    task_id='criar_agregacoes',
    python_callable=criar_agregacoes,
    dag=dag
)

task_7_criar_db = PythonOperator(
    task_id='criar_banco_dados',
    python_callable=criar_banco_dados,
    dag=dag
)

task_8_carregar = PythonOperator(
    task_id='carregar_dados_banco',
    python_callable=carregar_dados_banco,
    dag=dag
)

task_9_validar = PythonOperator(
    task_id='validar_carga',
    python_callable=validar_carga,
    dag=dag
)

# definindo dependencias entre tarefas
task_1_extrair >> task_2_analisar >> task_3_limpar >> task_4_transformar
task_4_transformar >> [task_5_calcular_stats, task_6_agregacoes] >> task_7_criar_db
task_7_criar_db >> task_8_carregar >> task_9_validar

