import asyncio
import aiohttp
import pandas as pd
import os
import mysql.connector
from aiohttp import ClientSession
from datetime import datetime


# Configuração do caminho de saída
OUTPUT_DIR = "EXTRACAO"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "EXTRACAO_JOURNALS.csv")

# ==============================
# CONFIGURAÇÕES DO REDMINE
# ==============================
API_KEY = "fc4613361394fdd0c77c9a7c1cb4312bb8f410a4"
URL_BASE = "http://redmine.prodeb.ba.gov.br/"
PROJECT_ID = "projeto-fiplan" 
HEADERS = {"X-Redmine-API-Key": API_KEY}

# ==============================
# CONFIGURAÇÃO DO BANCO
# ==============================
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '1234',
    'database': 'bd_metricas'
}

# ==============================
# FUNÇÃO PARA CONECTAR E BUSCAR ISSUE_IDS
# ==============================
def carregar_issue_ids():
    print("🔍 Conectando ao banco de dados e buscando issues...")
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM issues")
    issue_ids = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    print(f"✅ {len(issue_ids)} issues carregados.")
    return issue_ids

# ==============================
# FUNÇÃO ASSÍNCRONA PARA OBTER JOURNALS
# ==============================
async def buscar_journals(session: ClientSession, issue_id: int, sem: asyncio.Semaphore):
    url = f"{URL_BASE}/issues/{issue_id}.json?include=journals"
    async with sem:
        try:
            async with session.get(url, headers=HEADERS) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    journals = data.get("issue", {}).get("journals", [])
                    return [
                        {
                            "issue_id": issue_id,
                            "journal_id": j.get("id"),
                            "user": j.get("user", {}).get("name"),
                            "notes": j.get("notes"),
                            "created_on": j.get("created_on"),
                            "details": j.get("details")
                        } for j in journals
                    ]
                else:
                    print(f"⚠️ Falha no issue {issue_id} | Status: {resp.status}")
        except Exception as e:
            print(f"❌ Erro no issue {issue_id}: {e}")
    return []

# ==============================
# ORQUESTRAÇÃO ASSÍNCRONA
# ==============================
async def extrair_todos_journals(issue_ids):
    sem = asyncio.Semaphore(20)  # limite de concorrência
    async with aiohttp.ClientSession() as session:
        tasks = [buscar_journals(session, issue_id, sem) for issue_id in issue_ids]
        results = await asyncio.gather(*tasks)
        # Flatten da lista de listas
        all_journals = [journal for sublist in results for journal in sublist]
        return all_journals

# ==============================
# EXECUÇÃO PRINCIPAL
# ==============================
def main():
    issue_ids = carregar_issue_ids()
    all_journals = asyncio.run(extrair_todos_journals(issue_ids))

    # Converter para DataFrame e salvar
    df = pd.DataFrame(all_journals)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # df.to_csv(f"journals_extraidos_{timestamp}.csv", index=False, encoding="utf-8-sig")
    # print(f"📁 CSV gerado com {len(df)} journals.")
    # Salva no CSV
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print(f"Arquivo '{OUTPUT_FILE}' gerado com sucesso! ({len(df)} tickets)")

if __name__ == "__main__":
    main()
