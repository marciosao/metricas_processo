import requests
import pandas as pd
import os
import time

# Configurações do Redmine
API_KEY = "fc4613361394fdd0c77c9a7c1cb4312bb8f410a4"
URL_BASE = "http://redmine.prodeb.ba.gov.br/"
PROJECT_ID = "projeto-fiplan" 
START_DATE = "2025-02-01"  # Data mínima para extração

# Configuração do caminho de saída
OUTPUT_DIR = "TESTES"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "EXTRACAO.CSV")

# Listas para armazenar os dados
##issues_data = []
journals_data = []
journal_details_data = []

# Cabeçalhos da requisição
HEADERS = {
    "X-Redmine-API-Key": API_KEY,
    "Content-Type": "application/json"
}

def get_issues(project_id, start_date):
    """Extrai todos os tickets de um projeto do Redmine a partir de uma data específica, com status a cada 10 segundos."""
    issues = []
    offset = 0
    limit = 100  # Máximo permitido por requisição
    last_report_time = time.time()  # Marca o tempo inicial

    print("Iniciando extração de tickets...")

    while True:
        params = {
            "project_id": project_id,
            "created_on": f"><{start_date}",  # Filtra tickets criados após a data especificada
            "offset": offset,
            "limit": limit,
            "status_id": "*",  # Pega todos os status
        }

        response = requests.get(f"{URL_BASE}/issues.json", headers=HEADERS, params=params)

        if response.status_code != 200:
            print(f"Erro {response.status_code}: {response.text}")
            break

        data = response.json()
        issues.extend(data.get("issues", []))

        # Exibe status da extração a cada 10 segundos
        if time.time() - last_report_time >= 10:
            print(f"Tickets extraídos até agora: {len(issues)}. Extração continua...")
            last_report_time = time.time()  # Atualiza o tempo de referência

        if len(data.get("issues", [])) < limit:
            break  # Sai do loop se não houver mais tickets

        offset += limit

    print(f"Extração finalizada. Total de tickets extraídos: {len(issues)}")
    return issues

def get_journals(tickets):
    # Etapa 2: Buscar os journals de cada issue
    for issue in tickets:
        issue_id = issue["id"]
        print(f"📡 Buscando journals para a issue {issue_id}...")

        url = f"{URL_BASE}/issues/{issue_id}.json?include=journals"
        response = requests.get(url, headers=HEADERS)

        if response.status_code != 200:
            print(f"❌ Erro ao buscar journals da issue {issue_id}: {response.status_code}")
            continue

        issue_data = response.json()
        for journal in issue_data.get("issue", {}).get("journals", []):
            journals_data.append({
                "journal_id": journal["id"],
                "issue_id": issue_id,
                "user_id": journal["user"]["id"],
                "notes": journal.get("notes", ""),
                "created_on": journal["created_on"]
            })

        #time.sleep(1)  # Delay para evitar sobrecarga

    print(f"✅ Journals extraídos: {len(journals_data)}")
    df_journals = pd.DataFrame(journals_data)
    df_journals.to_csv("./testes/journals.csv", index=False, encoding="utf-8")

    return df_journals

def get_journals_datils(journals):
    # Etapa 3: Buscar os detalhes de cada journal
    for journal in journals_data:
        journal_id = journal["journal_id"]
        issue_id = journal["issue_id"]
        print(f"📡 Detalhes do journal {journal_id} da issue {issue_id}...")

        url = f"{URL_BASE}/issues/{issue_id}.json?include=journals"
        response = requests.get(url, headers=HEADERS)

        if response.status_code != 200:
            print(f"❌ Erro ao buscar detalhes do journal {journal_id}: {response.status_code}")
            continue

        issue_data = response.json()
        for journal_entry in issue_data.get("issue", {}).get("journals", []):
            if journal_entry["id"] == journal_id:
                for detail in journal_entry.get("details", []):
                    journal_details_data.append({
                        "journal_id": journal_id,
                        "property": detail["property"],
                        "name": detail.get("name", ""),
                        "old_value": detail.get("old_value", ""),
                        "new_value": detail.get("new_value", "")
                    })

        #time.sleep(1)  # Delay para evitar sobrecarga

    print(f"✅ Detalhes dos journals extraídos: {len(journal_details_data)}")
    df_journal_details = pd.DataFrame(journal_details_data)
    df_journal_details.to_csv("./testes/journal_details.csv", index=False, encoding="utf-8")

def save_to_csv(issues):
    """Salva os tickets extraídos em um arquivo CSV na pasta 'EXTRAÇÃO'."""
    if not issues:
        print("Nenhum ticket encontrado.")
        return

    # Cria a pasta se não existir
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Exclui o arquivo se já existir
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    # Converte os dados para DataFrame do Pandas
    ####df = pd.DataFrame(issues, columns=["id", "subject", "created_on","custom_fields","created_on","updated_on","tracker.name","status.name","priority.name","author.name","assigned_to.name","category.name","closed_on"])
        
    ####df = pd.DataFrame(issues)

    df = pd.json_normalize(issues)
    # Lista de colunas a serem removidas no segundo arquivo
    colunas_remover = [
        'description',
        'done_ratio',
        'project.id',
        'project.name',
        'tracker.id',
        'status.id',
        'priority.id',
        'author.id',
        'assigned_to.id',
        'category.id',
        'fixed_version.id',
        'fixed_version.name',
        'closed_on',
        'start date',
        'estimated_hours',
        'due_date',
        'start_date'
    ]

    # Criar um novo DataFrame removendo as colunas especificadas
    df_filtrado = df.drop(columns=[col for col in colunas_remover if col in df.columns], errors='ignore')

    # Salva no CSV
    df_filtrado.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print(f"Arquivo '{OUTPUT_FILE}' gerado com sucesso! ({len(df)} tickets)")

# Chamada das funções
tickets = get_issues(PROJECT_ID, START_DATE)

journals = get_journals(tickets)

get_journals_datils(journals)

save_to_csv(tickets)
