#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sex Dez 17 16:06:00 2019
Realiza a captura das notícias do site do Agazeta Minuto a Minuto e insere no banco de dados
MySQL
http://dcalixtoblog.wordpress.com
@author: calixto
"""
#importações
from bs4 import BeautifulSoup as soup
from urllib.request import urlopen as uReq, Request
import MySQLdb as mySQL
from datetime import datetime
from pytz import timezone

#quebra-galho pra injection
def limparStr(campo):
    if (campo != None and len(campo) > 0):
        campo = campo.replace('\\', '-')
        campo = campo.replace('\\\\', '-')
        campo = campo.replace('\\\\\\\\', '-')        
        campo = campo.replace('\"', '\"')        
        campo = campo.replace('\'', '\"') 
        campo = campo.replace('´', '\"')            
        return campo
    else:
        return '-XXX-'

#url alvo inicial
my_url = 'https://www.agazeta.com.br/minuto-a-minuto'
#classe noticia onde irei guardar temporariamente as notícias
class Noticia:
    def __init__(self):
        self.title = ''
        self.abstract = ''
        self.content = ''
        self.link = ''
        self.tags = ''
        self.data = datetime.now()

#site da agazeta barra se a requisição não se identificar como um browser 'conhecido'
req = Request(my_url, headers={'User-Agent': 'Mozilla/5.0'})
web_byte = uReq(req).read()
page_html = web_byte.decode('utf-8')

# fazendo parsing da info capturada para HTML
page_soup = soup(page_html, "html.parser")

#capturando a div com todas notícia
containers = page_soup.findAll("div", {"class":"minuto-item"})

#(TODO) implementar try/catch

#lista de notícias já tratadas
clean_notices = list()
#itero todos containers que adquiri no meu request à página
for container in containers:
    #pego título da notícia
    notice_title = container.find("h1", {"class":"titulo"})
    #pego o resumo
    abstract_container = []
    abstract_container = container.findAll("p", {"class":"linha-fina"})
    abstract_notice = ""
    for abstract in abstract_container:
        abstract_notice = abstract.text
        abstract_notice = abstract_notice.replace("'", "\\'")

    #tags
    tag_notice = container.find('label', {'class':'kicker'})

    #capturo o link da notícia
    link_notice = container.a['href']    

    #pegando hora e minuto bem específico do site da agazeta
    # hora = 00
    # minuto = 00
    # tempo = container.find('h4', {'class':'tempo'})
    # if (tempo != None):
    #     hora = tempo.text[0:2]
    #     minuto = tempo.text[-2:]

    #preencho meu objeto notícia
    n = Noticia()
    n.title = notice_title.text
    n.abstract = abstract_notice
    n.content = 'Não implementado nesse site'
    n.link = link_notice
    n.tags = tag_notice.text if tag_notice != None else 'Não consegui obter tag'

    #pego o dia como se fosse o de hj através do now no inicio do código e altero os minutos e hora
    #pois o site minuto a a minuto não oferece a data, então tenho q supor que é a do dia
    #o que pode dar um erro na virada do dia, mas....
    #n.data = n.data.replace(hour=int(hora), minute=int(minuto))
    dt = datetime.now()
    fuso = timezone('America/Bahia')
    n.data = dt.astimezone(fuso)
    
    #adiciono a lista das noticia tratadas
    clean_notices.append(n)
    #print(n.data)
#(TODO) checar erros
if (True):
    con = mySQL.connect(host ='10.0.10.3', user='noticia', passwd="noticia1234, db='bd_noticia')
    try:
        con.select_db("sipom")

        #instancia o cursor para execulçao de cmd's
        cursor = con.cursor()
        #força a conexão encodar utf8, por default o connector ira força para latin-1 (8859-1) (instalação)
        #meu banco esta em UTF-8 e a página em latin-1
        con.set_character_set('latin1')
        cursor.execute('SET NAMES latin1;')
        cursor.execute('SET CHARACTER SET latin1;')
        cursor.execute('SET character_set_connection=latin1;')

        #usando o for de outra maneira, somente para demonstração
        for i in range(len(clean_notices)):                                    

            #verifico se a notícia foi inserida no banco, o link como PK
            sql = (
                    "SELECT 1 FROM noticia WHERE titulo LIKE \'" + limparStr(clean_notices[i].title)
                    +"\' AND interesse = 0 AND link LIKE \'" +  (clean_notices[i].link if clean_notices[i].link != None else "NÃO DISPONIVEL") +"\'"
            )
            cursor.execute(sql)
            num_rows = int(cursor.rowcount)
            #print("ROWS: %d" % num_rows)
            #se noticia não existe na base insere.
            if (num_rows == 0):
                sql = (
                    "INSERT INTO noticia(titulo, conteudo, link, tags, resumo, data, fonte, bot) "
                    "VALUES (%s, %s, %s, %s, %s, %s, 'AGAZETA MIN_A_MIN', 1)"
                )
                data = ( limparStr(clean_notices[i].title), limparStr(clean_notices[i].content), limparStr(clean_notices[i].link), limparStr(clean_notices[i].tags), limparStr(clean_notices[i].abstract), clean_notices[i].data)
                cursor.execute(sql, data)
                #commita a transação de inserção
                con.commit()
    except mySQL.Error:
        print("Erro ao interagir com banco. Erro")
        raise
    finally:
        if con:
            con.close()

