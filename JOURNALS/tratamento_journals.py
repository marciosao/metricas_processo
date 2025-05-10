# Objetivo: Ler, tratar e exportar dados de journals do Redmine, extraindo campos de "details" com tentativa de correção automática

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
    texto_corrigido = texto_corrigido.replace("\\\\", "")
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

    erros = []
    for index, row in df.iterrows():
        if pd.notna(row["details"]):
            details_raw = row["details"].replace("'", '"').replace('\\\\', '').replace('\\', '')
            try:
                details_json = json.loads(details_raw)
                if isinstance(details_json, list) and len(details_json) > 0:
                    detail = details_json[0]
                    df.at[index, "property"] = detail.get("property")
                    df.at[index, "name"] = detail.get("name")
                    df.at[index, "old_value"] = detail.get("old_value")
                    df.at[index, "new_value"] = detail.get("new_value")
            except json.JSONDecodeError:
                erros.append(index)

    # Tentativa de recuperação
    recuperados = 0
    for index in erros:
        row = df.loc[index]
        details_corrigido = corrigir_json_malformado(row["details"])
        if isinstance(details_corrigido, list) and len(details_corrigido) > 0:
            detail = details_corrigido[0]
            df.at[index, "property"] = detail.get("property")
            df.at[index, "name"] = detail.get("name")
            df.at[index, "old_value"] = detail.get("old_value")
            df.at[index, "new_value"] = detail.get("new_value")
            recuperados += 1

    erros_restantes = []
    for index in erros:
        if not df.at[index, "property"]:
            erros_restantes.append(index)

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

    # Exibir resumo
    print("\n✅ RESUMO FINAL DO PROCESSAMENTO")
    print(f"Total de registros processados: {len(df)}")
    print(f"Registros com erro inicial de parsing no campo 'details': {len(erros)}")
    print(f"Registros recuperados automaticamente: {recuperados}")
    print(f"Registros ainda com erro após tentativa de recuperação: {len(erros_restantes)}")
    print("Índices com erro remanescente:", erros_restantes[:20] if erros_restantes else "Nenhum")

# Caminhos de entrada e saída
input_file = "./EXTRACAO/EXTRACAO_JOURNALS.CSV"
treatment_folder = "TRATAMENTO"
output_file = os.path.join(treatment_folder, "JOURNALS.CSV")

# Executar
process_file(input_file, output_file)