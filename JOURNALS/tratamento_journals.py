
# Objetivo: Ler, tratar e exportar dados de journals do Redmine, expandindo campos de "details" corretamente
# tratamento_journals_expandido

import os
import pandas as pd
import json
import re

def corrigir_json_malformado(texto):
    if not isinstance(texto, str):
        return None
    try:
        return json.loads(texto)
    except:
        pass
    texto_corrigido = texto.strip()
    texto_corrigido = texto_corrigido.replace("'", '"')
    texto_corrigido = texto_corrigido.replace("\\", "")
    texto_corrigido = texto_corrigido.replace("\\", "")
    texto_corrigido = re.sub(r",\s*}", "}", texto_corrigido)
    texto_corrigido = re.sub(r",\s*]", "]", texto_corrigido)
    texto_corrigido = texto_corrigido if texto_corrigido.startswith("[") else f"[{texto_corrigido}]"
    try:
        return json.loads(texto_corrigido)
    except:
        return None

def process_file(input_path, output_path):
    print("Iniciando o processo de leitura do arquivo...")
    df = pd.read_csv(input_path, dtype=str)
    if df.empty:
        print("⚠️ O arquivo CSV está vazio. Nenhum dado para processar.")
        exit()

    print("Arquivo lido com sucesso. Iniciando o tratamento dos dados...")

    for col in ["property", "name", "old_value", "new_value"]:
        if col not in df.columns:
            df[col] = None

    linhas_expandida = []

    for _, row in df.iterrows():
        if pd.notna(row["details"]):
            details_raw = row["details"].replace("'", '"').replace('\\', '').replace('\\', '')
            try:
                details_json = json.loads(details_raw)
            except json.JSONDecodeError:
                details_json = corrigir_json_malformado(row["details"])

            if isinstance(details_json, list):
                for detail in details_json:
                    nova_linha = row.copy()
                    nova_linha["property"] = detail.get("property")
                    nova_linha["name"] = detail.get("name")
                    nova_linha["old_value"] = detail.get("old_value")
                    nova_linha["new_value"] = detail.get("new_value")
                    linhas_expandida.append(nova_linha)
            else:
                linhas_expandida.append(row)
        else:
            linhas_expandida.append(row)

    df = pd.DataFrame(linhas_expandida)

    if "created_on" in df.columns:
        df["created_on"] = pd.to_datetime(df["created_on"], errors='coerce', utc=True)
        df["created_on"] = df["created_on"].dt.tz_localize(None)
        df["created_on"] = df["created_on"].dt.strftime('%Y-%m-%d %H:%M:%S')

    df = df.where(pd.notnull(df), None)

    print("Dados processados com sucesso. Criando o arquivo de saída...")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    if os.path.exists(output_path):
        os.remove(output_path)

    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"📁 Arquivo gerado com sucesso: {output_path} ({len(df)} registros)")

# Caminhos de entrada e saída
# input_file = "./EXTRACAO/EXTRACAO_JOURNALS.CSV"
# treatment_folder = "TRATAMENTO"
# output_file = os.path.join(treatment_folder, "JOURNALS_EXPANDIDO.CSV")
input_file = "./DADOS/1-EXTRACAO/EXTRACAO_JOURNALS.CSV"
treatment_folder = "./DADOS/2-TRATAMENTO"
output_file = os.path.join(treatment_folder, "JOURNALS_EXPANDIDO.CSV")

# Executar
process_file(input_file, output_file)

