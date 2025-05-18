from redminelib import Redmine
import pandas as pd
import os

# === CONFIGURAÇÕES ===
REDMINE_URL = 'http://redmine.prodeb.ba.gov.br/'
API_KEY = 'fc4613361394fdd0c77c9a7c1cb4312bb8f410a4'
PROJECT_IDENTIFIER = 'projeto-fiplan'  # Exemplo: 'projeto-teste'
PASTA_SAIDA = f"saida_{PROJECT_IDENTIFIER}"
ARQUIVO_EXCEL = os.path.join(PASTA_SAIDA, f"{PROJECT_IDENTIFIER}.xlsx")

# === GARANTE A PASTA DE SAÍDA ===
os.makedirs(PASTA_SAIDA, exist_ok=True)

# === CONECTANDO AO REDMINE ===
redmine = Redmine(REDMINE_URL, key=API_KEY)
print(f"\n📌 Coletando dados do projeto: {PROJECT_IDENTIFIER.upper()}")

# === 1. TRACKERS DO PROJETO ===
project = redmine.project.get(PROJECT_IDENTIFIER, include='trackers')
df_trackers = pd.DataFrame([{'id': t.id, 'nome': t.name} for t in project.trackers])
print("✔️ Trackers obtidos.")

# === 2. USUÁRIOS E PERFIS (MEMBERSHIPS) DO PROJETO ===
usuarios_data = []
for membership in project.memberships:
    if hasattr(membership, 'user'):
        user = membership.user
        for role in membership.roles:
            usuarios_data.append({
                'id_usuario': user['id'],
                'nome_usuario': user['name'],
                'perfil': role['name']
            })
df_usuarios = pd.DataFrame(usuarios_data)
print("✔️ Usuários e Perfis obtidos.")

# === 3. TODOS OS PERFIS DO SISTEMA (ROLES) ===
df_roles = pd.DataFrame([{'id': r.id, 'nome': r.name} for r in redmine.role.all()])
print("✔️ Perfis do sistema obtidos.")

# === 4. STATUS DE TAREFAS (GLOBAIS) ===
df_status = pd.DataFrame([{'id': s.id, 'nome': s.name} for s in redmine.issue_status.all()])
print("✔️ Status de tarefa obtidos.")

# === 5. EXPORTANDO PARA EXCEL ===
with pd.ExcelWriter(ARQUIVO_EXCEL, engine='openpyxl') as writer:
    df_trackers.to_excel(writer, sheet_name='Trackers', index=False)
    df_usuarios.to_excel(writer, sheet_name='Usuarios_Perfis', index=False)
    df_roles.to_excel(writer, sheet_name='Perfis_Sistema', index=False)
    df_status.to_excel(writer, sheet_name='Status_Tarefa', index=False)

# === 6. EXPORTANDO PARA CSV ===
df_trackers.to_csv(os.path.join(PASTA_SAIDA, "trackers.csv"), index=False)
df_usuarios.to_csv(os.path.join(PASTA_SAIDA, "usuarios_perfis.csv"), index=False)
df_roles.to_csv(os.path.join(PASTA_SAIDA, "perfis_sistema.csv"), index=False)
df_status.to_csv(os.path.join(PASTA_SAIDA, "status_tarefa.csv"), index=False)

# === 7. EXPORTANDO PARA JSON ===
df_trackers.to_json(os.path.join(PASTA_SAIDA, "trackers.json"), orient="records", indent=4)
df_usuarios.to_json(os.path.join(PASTA_SAIDA, "usuarios_perfis.json"), orient="records", indent=4)
df_roles.to_json(os.path.join(PASTA_SAIDA, "perfis_sistema.json"), orient="records", indent=4)
df_status.to_json(os.path.join(PASTA_SAIDA, "status_tarefa.json"), orient="records", indent=4)

# === RESUMO FINAL ===
print(f"\n✅ Exportações concluídas com sucesso em: {PASTA_SAIDA}")
print("Arquivos gerados:")
print(f" - {PROJECT_IDENTIFIER}.xlsx (Excel)")
print(" - trackers.csv / .json")
print(" - usuarios_perfis.csv / .json")
print(" - perfis_sistema.csv / .json")
print(" - status_tarefa.csv / .json")
