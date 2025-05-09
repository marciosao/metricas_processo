import requests
import pandas as pd
import time

# Configurações do Redmine
REDMINE_URL = "http://redmine.prodeb.ba.gov.br/"
API_KEY = "fc4613361394fdd0c77c9a7c1cb4312bb8f410a4"
HEADERS = {"X-Redmine-API-Key": API_KEY, "Content-Type": "application/json"}

# Configurações do filtro
PROJECT_ID = "projeto-fiplan"  # ID do projeto específico
START_DATE = "2025-02-01"  # Filtrar apenas tickets criados a partir dessa data

# Listas para armazenar os dados
issues_data = []
journals_data = []
journal_details_data = []

# Parâmetros iniciais
limit = 100  # Número máximo de registros por página
offset = 0   # Posição inicial
total_count = None  # Contador total das issues

print("🔄 Iniciando a extração de issues...")

# Etapa 1: Buscar todas as issues do projeto dentro do período desejado
while total_count is None or offset < total_count:
    print(f"📡 Buscando issues do {PROJECT_ID} (offset {offset})...")
    url = f"{REDMINE_URL}/issues.json?project_id={PROJECT_ID}&created_on=%3E%3D{START_DATE}&limit={limit}&offset={offset}"
    response = requests.get(url, headers=HEADERS)

    if response.status_code != 200:
        print(f"❌ Erro ao buscar issues: {response.status_code}")
        break

    data = response.json()
    total_count = data["total_count"]  # Atualiza o total de issues

    for issue in data.get("issues", []):
        issues_data.append({
            "id": issue["id"],
            "project_id": issue["project"]["id"] if "project" in issue else None,
            "subject": issue["subject"],
            "status": issue["status"]["name"],
            "author_id": issue["author"]["id"],
            "created_on": issue["created_on"],
            "updated_on": issue["updated_on"]
        })

    offset += limit  # Atualiza o offset para a próxima página
    time.sleep(1)  # Pequeno delay para evitar sobrecarga na API

print(f"✅ Issues extraídas: {len(issues_data)}")
df_issues = pd.DataFrame(issues_data)
df_issues.to_csv("./testes/issues.csv", index=False, encoding="utf-8")

# # Etapa 2: Buscar os journals de cada issue
# for issue in issues_data:
#     issue_id = issue["id"]
#     print(f"📡 Buscando journals para a issue {issue_id}...")

#     url = f"{REDMINE_URL}/issues/{issue_id}.json?include=journals"
#     response = requests.get(url, headers=HEADERS)

#     if response.status_code != 200:
#         print(f"❌ Erro ao buscar journals da issue {issue_id}: {response.status_code}")
#         continue

#     issue_data = response.json()
#     for journal in issue_data.get("issue", {}).get("journals", []):
#         journals_data.append({
#             "journal_id": journal["id"],
#             "issue_id": issue_id,
#             "user_id": journal["user"]["id"],
#             "notes": journal.get("notes", ""),
#             "created_on": journal["created_on"]
#         })

#     time.sleep(1)  # Delay para evitar sobrecarga

# print(f"✅ Journals extraídos: {len(journals_data)}")
# df_journals = pd.DataFrame(journals_data)
# df_journals.to_csv("./testes/journals.csv", index=False, encoding="utf-8")

# # Etapa 3: Buscar os detalhes de cada journal
# for journal in journals_data:
#     journal_id = journal["journal_id"]
#     issue_id = journal["issue_id"]
#     print(f"📡 Detalhes do journal {journal_id} da issue {issue_id}...")

#     url = f"{REDMINE_URL}/issues/{issue_id}.json?include=journals"
#     response = requests.get(url, headers=HEADERS)

#     if response.status_code != 200:
#         print(f"❌ Erro ao buscar detalhes do journal {journal_id}: {response.status_code}")
#         continue

#     issue_data = response.json()
#     for journal_entry in issue_data.get("issue", {}).get("journals", []):
#         if journal_entry["id"] == journal_id:
#             for detail in journal_entry.get("details", []):
#                 journal_details_data.append({
#                     "journal_id": journal_id,
#                     "property": detail["property"],
#                     "name": detail.get("name", ""),
#                     "old_value": detail.get("old_value", ""),
#                     "new_value": detail.get("new_value", "")
#                 })

#     time.sleep(1)  # Delay para evitar sobrecarga

# print(f"✅ Detalhes dos journals extraídos: {len(journal_details_data)}")

# Criar DataFrames
##df_issues = pd.DataFrame(issues_data)
#df_journals = pd.DataFrame(journals_data)
df_journal_details = pd.DataFrame(journal_details_data)

# Exibir os primeiros registros
print(df_issues.head())
# print(df_journals.head())
# print(df_journal_details.head())

# Opcional: Salvar em CSV
#df_issues.to_csv("./testes/issues.csv", index=False, encoding="utf-8")
#df_journals.to_csv("./testes/journals.csv", index=False, encoding="utf-8")
# df_journal_details.to_csv("./testes/journal_details.csv", index=False, encoding="utf-8")

print("✅ Extração finalizada! Arquivos salvos.")  
