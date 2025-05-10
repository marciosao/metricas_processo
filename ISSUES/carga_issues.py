# Script: cargaDados-V3.py
# Objetivo: Carregar dados tratados do Redmine (em CSV) para uma base MySQL

import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
import unicodedata

# =====================
# 1. CONFIGURAÇÃO INICIAL
# =====================
DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = "1234"
DB_NAME = "bd_metricas"
FILE_PATH = "./TRATAMENTO/issue.csv"

# =====================
# 2. CONECTAR AO MYSQL
# =====================
try:
    print("🔄 Conectando ao banco de dados...")
    connection = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    if connection.is_connected():
        print("✅ Conexão estabelecida com sucesso!")
except Error as e:
    print(f"❌ Erro ao conectar ao banco de dados: {e}")
    exit()

# =====================
# 3. VERIFICAR ARQUIVO CSV
# =====================
if not os.path.exists(FILE_PATH):
    print(f"❌ Arquivo '{FILE_PATH}' não encontrado.")
    exit()

# =====================
# 4. CARREGAR E PREPARAR DADOS
# =====================
try:
    print("🔄 Carregando arquivo CSV...")
    df = pd.read_csv(FILE_PATH)
    # Normaliza nomes das colunas para evitar conflitos com MySQL
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .map(lambda x: ''.join(c for c in unicodedata.normalize('NFKD', x) if not unicodedata.combining(c)))
    )
    print("✅ Arquivo carregado com sucesso!")

    print("\n📌 Colunas detectadas no DataFrame:")
    for col in df.columns:
        print(f"- {repr(col)}")


    # Remove colunas sem nome (NaN literal ou colunas Unnamed)
    df = df.loc[:, df.columns.notna()]
    df = df.loc[:, ~df.columns.astype(str).str.lower().isin(['nan', 'unnamed: 0'])]

    # Padroniza nomes das colunas
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .map(lambda x: ''.join(c for c in unicodedata.normalize('NFKD', x) if not unicodedata.combining(c)))
    )

    # Converte datas (coerentemente com o schema do MySQL)
    for col in df.columns:
        if 'data' in col:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Converte NaNs para None
    df = df.where(pd.notnull(df), None)

except Exception as e:
    print(f"❌ Erro ao carregar ou preparar o arquivo: {e}")
    exit()

if df.empty:
    print("⚠️ O arquivo está vazio. Nada a carregar.")
    exit()

# =====================
# 5. INSERIR DADOS NO MYSQL
# =====================
try:

    print("🧹 Limpando dados existentes da tabela 'issues'...")
    cursor = connection.cursor()
    cursor.execute("TRUNCATE TABLE issues")
    connection.commit()
    print("✅ Tabela 'issues' truncada com sucesso!")

    print("🔄 Inserindo dados na tabela 'issues'...")
    # cursor = connection.cursor()

    colunas = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    query_insert = f"INSERT INTO issues ({colunas}) VALUES ({placeholders})"

    valores = [tuple(None if pd.isna(cell) else cell for cell in row) for row in df.to_numpy()]
    cursor.executemany(query_insert, valores)

    connection.commit()
    print(f"✅ {cursor.rowcount} registros inseridos com sucesso!")

except Error as e:
    print(f"❌ Erro ao inserir dados: {e}")
    connection.rollback()
    exit()

# =====================
# 6. PREVIEW E FINALIZAÇÃO
# =====================
try:
    df_preview = pd.read_sql("SELECT * FROM issues LIMIT 5", con=connection)
    print("\n📊 Preview dos dados:")
    print(df_preview)

    cursor.execute("SELECT COUNT(*) FROM issues")
    total = cursor.fetchone()
    print(f"\n📌 Total de registros na tabela: {total[0]}")
except Error as e:
    print(f"❌ Erro ao consultar dados: {e}")

print("🔌 Conexão encerrada.")
