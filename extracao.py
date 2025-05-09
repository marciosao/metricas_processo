import requests
import pandas as pd
import os
import time

# Configurações do Redmine
API_KEY = "fc4613361394fdd0c77c9a7c1cb4312bb8f410a4"
URL_BASE = "http://redmine.prodeb.ba.gov.br/"
PROJECT_ID = "projeto-fiplan" 
START_DATE = "2025-03-28"  # Data mínima para extração

# Configuração do caminho de saída
OUTPUT_DIR = "EXTRACAO"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "EXTRACAO.CSV")

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
save_to_csv(tickets)
