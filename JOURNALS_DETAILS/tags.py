from redminelib import Redmine


# ENVIROMENT VARIABLES
redmineUrlProdeb = 'http://redmine.prodeb.ba.gov.br/'
redmineProdeb_key = 'fc4613361394fdd0c77c9a7c1cb4312bb8f410a4' #sua key
issue_id = 208490   #id da issue

def redmineConnection(url, usuario, senha, key):
    if key:
        try:
            user = Redmine(url, key=key).auth()
        except Exception as e:
            print(f"Falha ao conectar no Redmine: {str(e)}")
        else:
            return Redmine(url, key=key)
    else:
        try:
            user = Redmine(url, username=usuario, password=senha).auth()
        except Exception as e:
            print(f"Falha ao conectar no Redmine: {str(e)}")
        else:
            return Redmine(url, username=usuario, password=senha)


def extrairJournal(journal, issue_id):
    j = journal
    # print (issue_id)
    nota = {
        'Nota ID': j.id,
        'Issue ID': issue_id,
        # 'Nota':j.notes.replace(',', ' ') if hasattr(j, 'notes') else '',
        'Usuário ID': j.user.id,
        'Usuário': j.user.name,
        'Criado em': j.created_on
    }
    return nota, j.details


def extrairDetalhesJournal(detalhesJournal, journal_id, issue_id):
    d = detalhesJournal

    if 'old_value' in d and d['old_value']:
        valor_antigo = d['old_value'].replace(',', ' ')
    else:
        valor_antigo = ''
    if 'new_value' in d and d['new_value']:
        valor_novo = d['new_value'].replace(',', ' ')
    else:
        valor_novo = ''

    if d['name'] == 'description':
        valor_antigo = 'REMOVIDO NA EXTRACAO'
        valor_novo = 'REMOVIDO NA EXTRACAO'

    detalhes_nota = {
        'Nota ID': journal_id,
        'Issue ID': issue_id,
        'Propriedade': d['property'],
        'Nome': d['name'].replace(',', ' '),
        'Valor Antigo': valor_antigo,
        'Valor Novo': valor_novo,
    }
    return detalhes_nota

if __name__ == '__main__':
    # redmineProdeb = redmineConnection(redmineUrlProdeb, key=redmineProdeb_key, usuario='', senha='')
    redmineProdeb = redmineConnection(redmineUrlProdeb, key=redmineProdeb_key, usuario='marcio.oliveira@prodeb.ba.gov.br', senha='Senh@@c3ss0!')

    issue = redmineProdeb.issue.get(issue_id, include=['journals'])

    journals = issue.journals

    if journals:
        for journal in journals:
            nota, details = extrairJournal(journal, issue.id)
            #journal_list.append(nota)
            print (f'\n---\nNOTA:\n{nota}\n')
            if details:
                for detail in details:
                    detalhes = extrairDetalhesJournal(detail, journal.id, issue.id)
                    print (f'DETALHES:\n{detalhes}')


    print (issue)
