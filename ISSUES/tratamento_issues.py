# Script: tratamento.py (versao corrigida)
# Objetivo: Ler, tratar e exportar dados de issues extraídos do Redmine

import os
import pandas as pd
import json
import re
import numpy as np

def normalize_key(key):
    """Normaliza os nomes das chaves para melhor correspondência."""
    return re.sub(r'\s+', ' ', key.strip()).lower()

def format_json_string(json_string):
    try:
        json_string = json_string.replace("'", "\"")
        parsed_json = json.loads(json_string)
        if isinstance(parsed_json, list):
            result = {}
            for item in parsed_json:
                if isinstance(item, dict) and "name" in item and "value" in item:
                    result[item["name"]] = item["value"]
            return result
        return parsed_json if isinstance(parsed_json, dict) else {}
    except json.JSONDecodeError:
        return {}

def process_file(input_path, output_path):
    print("Iniciando o processo de leitura do arquivo...")
    df = pd.read_csv(input_path, dtype=str)
    if df.empty:
        print("⚠️ O arquivo CSV está vazio. Nenhum dado para inserir no banco.")
        exit()

    print("Arquivo lido com sucesso. Iniciando o tratamento dos dados...")

    if "custom_fields" not in df.columns:
        raise KeyError("A coluna 'custom_fields' não foi encontrada no arquivo.")

    new_columns = set(df.columns)

    for index, row in df.iterrows():
        if pd.notna(row["custom_fields"]):
            json_data = format_json_string(row["custom_fields"])
            if isinstance(json_data, dict):
                for key, value in json_data.items():
                    if not key or not isinstance(key, str) or key.strip().lower() in ['id', '', 'nan']:
                        continue
                    normalized_key = key.strip()
                    if normalized_key not in new_columns:
                        new_columns.add(normalized_key)
                        df[normalized_key] = None
                    df.at[index, normalized_key] = value

    df.columns = [
        col.replace(" ", "_").replace(".", "_").replace(" 2.0", "_20")
           .replace(" de ", "_").replace("çã", "ca").replace("ú", "u")
           .replace("2_0", "20").replace("_do_", "_").replace("_de_", "_")
           .replace("_da_", "_").replace("ço", "co").replace("_to_", "_")
           .replace("ç", "c").replace("ã", "a") for col in df.columns
    ]

    colunas_numericas = [
        'status_id', 'parent_id', 'fiplan_20', 'legado',
        'pf_estimado', 'backlog', 'esforco', 'projeto', 'numero_rds'
    ]

    colunas_data = [
        'created_on', 'updated_on', 'Data_Entrega', 'Data_Prevista_Desenvolvimento', 
        'Data_Desejada', 'Data_Entrega_Homologacao', 'Data_Prevista_Implantacao', 
        'Data_Prevista_Teste'
    ]

    for coluna in colunas_numericas:
        if coluna in df.columns:
            df[coluna] = df[coluna].replace(['', 'null', None, np.nan], 0)
            df[coluna] = pd.to_numeric(df[coluna], errors='coerce').fillna(0).astype(int)

    for coluna in colunas_data:
        if coluna in df.columns:
            df[coluna] = pd.to_datetime(df[coluna], errors='coerce', utc=True)
            df[coluna] = df[coluna].dt.tz_localize(None)
            df[coluna] = df[coluna].dt.strftime('%Y-%m-%d %H:%M:%S')

    df = df.where(pd.notnull(df), None)

    print("Dados processados com sucesso. Criando o arquivo de saída...")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    if os.path.exists(output_path):
        os.remove(output_path)
    # if os.path.exists(output_file_json):
    #     os.remove(output_file_json)

    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    # df.to_json(output_file_json, index=False)

    print(f"Arquivo '{output_file}' gerado com sucesso! ({len(df)} tickets)")
    print("Processo concluído. Arquivo salvo em:", output_path)
    # print("Processo concluído. Arquivo salvo em:", output_file_json)

# Caminhos de entrada e saída
# input_file = "./EXTRACAO/EXTRACAO_ISSUES.CSV"
input_file = "./DADOS/1-EXTRACAO/EXTRACAO_ISSUES.CSV"
# treatment_folder = "TRATAMENTO"
treatment_folder = "./DADOS/2-TRATAMENTO"
output_file = os.path.join(treatment_folder, "issue.CSV")
# output_file_json = os.path.join(treatment_folder, "issue.json")

# Executar
process_file(input_file, output_file)
