import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
import os
import time

# Configuração da API do Redmine
REDMINE_URL = "http://redmine.prodeb.ba.gov.br/"
API_KEY = "fc4613361394fdd0c77c9a7c1cb4312bb8f410a4"
PROJECT_ID = "projeto-fiplan"
START_DATE = "2024-01-01"  # Data inicial para extração


# Configuração do Banco de Dados PostgreSQL
DATABASE_URL = "postgresql://postgres:1234@localhost:5432/cards"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

# Definição das tabelas
class Issue(Base):
    __tablename__ = "issues"
    id = Column(Integer, primary_key=True)
    subject = Column(String)
    created_on = Column(DateTime)
    updated_on = Column(DateTime)
    tracker_name = Column(String)
    status_name = Column(String)
    priority_name = Column(String)
    author_name = Column(String)
    assigned_to_name = Column(String, nullable=True)
    category_name = Column(String, nullable=True)
    parent_id = Column(Integer, nullable=True)
    journals = relationship("Journal", back_populates="issue")

class Journal(Base):
    __tablename__ = "journals"
    id = Column(Integer, primary_key=True)
    issue_id = Column(Integer, ForeignKey("issues.id"))
    created_on = Column(DateTime)
    issue = relationship("Issue", back_populates="journals")
    journal_details = relationship("JournalDetail", back_populates="journal")

class JournalDetail(Base):
    __tablename__ = "journal_details"
    id = Column(Integer, primary_key=True)
    journal_id = Column(Integer, ForeignKey("journals.id"))
    property = Column(String)
    name = Column(String)
    old_value = Column(String, nullable=True)
    new_value = Column(String, nullable=True)
    journal = relationship("Journal", back_populates="journal_details")

# Criar tabelas se não existirem
Base.metadata.create_all(engine)

# Cabeçalhos para autenticação
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

        response = requests.get(f"{REDMINE_URL}/issues.json", headers=HEADERS, params=params)

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

def get_journals(issue_id):
    """Obtém os journals de uma issue específica."""
    response = requests.get(f"{REDMINE_URL}/issues/{issue_id}.json", headers=HEADERS, params={"include": "journals"})
    if response.status_code == 200:
        return response.json().get("issue", {}).get("journals", [])
    return []

def save_issues_to_db(issues):
    """Salva as issues e seus históricos no banco de dados."""
    session.query(JournalDetail).delete()
    session.query(Journal).delete()
    session.query(Issue).delete()
    session.commit()
    
    for issue_data in issues:
        issue = Issue(
            id=issue_data["id"],
            subject=issue_data["subject"],
            created_on=datetime.fromisoformat(issue_data["created_on"]),
            updated_on=datetime.fromisoformat(issue_data["updated_on"]),
            tracker_name=issue_data["tracker"]["name"],
            status_name=issue_data["status"]["name"],
            priority_name=issue_data["priority"]["name"],
            author_name=issue_data["author"]["name"],
            assigned_to_name=issue_data.get("assigned_to", {}).get("name"),
            category_name=issue_data.get("category", {}).get("name"),
            parent_id=issue_data.get("parent", {}).get("id")
        )
        session.merge(issue)
        
        journals = get_journals(issue.id)
        for journal_data in journals:
            journal = Journal(
                id=journal_data["id"],
                issue_id=issue.id,
                created_on=datetime.fromisoformat(journal_data["created_on"])
            )
            session.merge(journal)
            
            for detail in journal_data.get("details", []):
                journal_detail = JournalDetail(
                    journal_id=journal.id,
                    property=detail["property"],
                    name=detail["name"],
                    old_value=detail.get("old_value"),
                    new_value=detail.get("new_value")
                )
                session.merge(journal_detail)
    
    session.commit()

# Executando a extração
dados_issues = get_issues(PROJECT_ID, START_DATE)
save_issues_to_db(dados_issues)

print("Processo concluído com sucesso!")
