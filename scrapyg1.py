#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 24 16:52:20 2018
Realiza a captura das notícias do site do G1/ES e insere no banco de dados
MySQL 
http://dcalixtoblog.wordpress.com
@author: calixto
"""
#importações
from bs4 import BeautifulSoup as soup
from urllib.request import urlopen as uReq
import MySQLdb as mySQL
from datetime import datetime

#url alvo inicial
my_url = 'http://g1.globo.com/espirito-santo/'
#classe noticia onde irei guardar temporariamente as notícias
class Noticia:
    def __init__(self):
        self.title = ''
        self.abstract = ''
        self.content = ''
        self.link = ''
        self.tags = ''
        self.data = datetime.now()

#abrindo conexão, e capturando a página
uClient = uReq(my_url)
page_html = uClient.read()
uClient.close()

# fazendo parsing da info capturada para HTML
page_soup = soup(page_html, "html.parser")

#capturando a div com todas notícia
containers = page_soup.findAll("div", {"class":"bastian-feed-item"})
###############################################################################
#Testes diversos sem iteração:
#print(containers[0])
#print(containers[0].findAll("p", {"class":"feed-post-body-title"}))
#testando sem iterar no for para o 1º Objeto do ResultSet
#title_container = containers[0].findAll("p", {"class":"feed-post-body-title"})
#title_notice = title_container[0].text
#capturo o link da notícia
#link_container = containers[0].findAll("a" ,{"class":"feed-post-link"})
#retiro o conteúdo de href
#link = link_container[0].get('href') 
#print(link)
#tags_container = containers[0].findAll("span", {"class":"feed-post-header-chapeu"})
#print(tags_container[0].prettify())
###############################################################################

#(TODO) implementar try/catch

#lista de notícias já tratadas
clean_notices = list()
#itero todos containers que adquiri no meu request a página do G1-ES
for container in containers:
    #pego título da notícia
    title_container = container.findAll("p", {"class":"feed-post-body-title"})    
    title_notice = title_container[0].text
    
    #pego o resumo
    abstract_container = []
    abstract_container = container.findAll("p", {"class":"feed-post-body-resumo"})
    for abstract in abstract_container:    
        abstract_notice = abstract.text
    
    #capturo o link da notícia
    link_container = container.findAll("a", {"class":"feed-post-link"})    
    #retiro o conteúdo de href, sempre tenho somente um link nessa classe :P
    link_notice = link_container[0].get('href')
    
    #(TODO)Adicionar função de entrar dentro do link e coletar notícia completa.
    #pego a url da notícia e leio sua página
    my_url=link_notice
    uClient = uReq(my_url)
    page_html = uClient.read()
    uClient.close()
    
    #TODO(Torna tudo isso uma função...)
    #fazendo o parse agora da página do conteúdo
    page_soup = soup(page_html, "html.parser")
    #pego o conteúdo da notícia da página recente que acabamos de visitar
    contents_containers = page_soup.findAll("p", {"class":"content-text__container"})
    
    #pego a data da notícia que não tinha na primeira página
    data_containers = page_soup.find_all("div", {"class":"content-publication-data"})    
    try:        
        for data_container in data_containers:
            #print(data_container.prettify())
            #print(data_container.time.get('datetime'))
            #converto a string da data para datetime depois uso o strftime pra torna o datetime em uma string do formato do datetime do meu banco
            data_notice = datetime.strptime(data_container.time.get('datetime'), '%Y-%m-%dT%H:%M:%S.%fZ').strftime('\'%Y/%m/%d %I:%M:%S\'')            
    except (RuntimeError, TypeError):
        #qualque 'zica' tomo a data como a atual
        data_notice = datetime.now().strftime('\'%Y/%m/%d %I:%M:%S\'')
    #se a minha string está vazia
    if not data_notice:
        data_notice.now().strftime('\'%Y/%m/%d %I:%M:%S\'')    
    #vou iterar todos contents, (são vários por o G1 Divide a informação em vários
    #o editor deles deve trabalhar dividindo partes do conteúdo em classes CSS)
    content_notice = ''
    for content_container in contents_containers:
        #content_notice.join(content_container.text)
        content_notice = content_notice + content_container.text
    
    
    #tag é a classificação da notícia segundo site do G1 (Chapéu o.O :P)
    tags_container = container.findAll("span", {"class":"feed-post-header-chapeu"})    
    tags_notice = tags_container[0].text
        
    print(title_notice)#,'\n', abstract_notice, '\n', link_notice, '\n ', tags_notice, '\n', content_notice)
    #preencho meu objeto notícia
    n = Noticia()
    n.title = title_notice
    n.abstract = abstract_notice
    n.content = content_notice
    n.link = link_notice
    n.tags = tags_notice
    #adiciono a lista das noticia tratadas
    clean_notices.append(n)

#(TODO) checar erros    
if (True):
    con = mySQL.connect(host ='127.0.0.1', user='root', passwd="root1234", db='bd_noticia')
    try:        
        con.select_db("bd_noticia")
        
        #instancia o cursor para execulçao de cmd's
        cursor = con.cursor()
        #força a conexão encodar utf8, por default o connector ira força para latin-1 (8859-1) (instalação)
        #meu banco esta em UTF-8 e a página em latin-1
        con.set_character_set('utf8')
        cursor.execute('SET NAMES utf8;')
        cursor.execute('SET CHARACTER SET utf8;')
        cursor.execute('SET character_set_connection=utf8;')
        
        ############################################
        #testar a conexao com o banco
        #ver = cursor.execute("SELECT VERSION()")
        #ver = cursor.fetchone()
        #print("Versão do MySQL: %s" % ver)
        ############################################
        
        #usando o for de outra maneira, somente para demonstração
        for i in range(len(clean_notices)):                        
            #verifico se a notícia foi inserida no banco, o link como PK
            sql = (
                    "SELECT 1 FROM noticia WHERE titulo LIKE \'" + clean_notices[i].title 
                    +"\' AND link LIKE \'" + clean_notices[i].link +"\'" 
            )
            cursor.execute(sql)
            num_rows = int(cursor.rowcount)        
            #print("ROWS: %d" % num_rows)
            #se noticia não existe na base insere.
            if (num_rows == 0):                
                sql = (
                    "INSERT INTO noticia(titulo, conteudo, link, tags, resumo, data) " 
                    "VALUES (%s, %s, %s, %s, %s, %s)"
                )
                data = (clean_notices[i].title, clean_notices[i].content, clean_notices[i].link, clean_notices[i].tags, clean_notices[i].abstract, clean_notices[i].data)
                cursor.execute(sql, data)
                #cursor.execute(sql, ("teste", "teste", "teste", "teste", "teste"))
                #commita a transação de inserção
                con.commit()         
    except mySQL.Error:
        print("Erro ao interagir com banco. Erro")        
        raise
    finally:
        if con:
            con.close()
