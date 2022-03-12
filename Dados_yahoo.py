#lxml ← para parsear(fazer a análise sintática)
#  o html e extrair as informações desejadas;

import lxml.html as parser

#requests ← para realizar nossas requisições,
# pode ser substituída por qualquer pacote similar;

import requests
from time import process_time, perf_counter, struct_time, localtime
from time import time as tm
from time import sleep
import csv
import pandas as pd
import pandas_market_calendars as mark
import os.path
import numpy as np
import sys
from datetime import date, time, datetime, timedelta
import monthdelta
import threading
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchWindowException
import B3_Reader as b3

class WebScraping(threading.Thread):

    def __init__(self, descartados, path):#,mutex):

        self.path_now = path
        self.descartados = descartados
        #self.mutex = mutex
        threading.Thread.__init__(self)

    # OBTENDO AS LISTAS DE FUNDOS IMOBILIÁRIOS

    # Acessando o site/endereço
    def listaFundos(self):
        self.listaAcao = []
        # Carregando banco de dados: parse_dataset pertence à biblioteca B3_Reader
        self.dadosAcao = b3.parse_dataset(self.path_now)
        # CRIANDO AS DATAFRAME COM OS BANCOS DE DADOS
        df_acao = pd.DataFrame(b3.translate_bdi(self.dadosAcao))
        # FILTRANDO OS DATAFRAMES
        # df_acao_filtro = df_acao[(df_acao.iloc[0:, 6] == 'ON') & (df_acao.iloc[0:, 4] == 'VISTA')]# & (df_acao.iloc[0:, 1] == '20190102')]
        # print(df_acao_filtro.iloc[0, 1:9])
        # Adicionando ações e fundos imobiliários (troquei df_acao.iloc[0:, 3] por df_acao.iloc[::-1, 3] para começar pelo final dos dados
        for i, x in enumerate(df_acao.iloc[::-1, 3]):  # .sort_values(ascending=True):
            try:
                hoje = date.today()
                # Condição para interromper coleta dos papéis
                if x in self.listaAcao and df_acao.iloc[-(i + 1), 1].month != hoje.month:
                    break
                # Condição para adicionar as ações ON
                if x not in self.listaAcao and x[4] == '3' and len(x) == 5 and x not in self.descartados and\
                        df_acao.iloc[i, 6][0] == 'O' and df_acao.iloc[i, 6][1] == 'N':
                    self.listaAcao.append(x)
                # Condição para adicionar as ações PN
                if x not in self.listaAcao and x[4] == '4' and len(x) == 5 and x not in self.descartados and\
                        df_acao.iloc[i, 6][0] == 'P' and df_acao.iloc[i, 6][1] == 'N':
                    self.listaAcao.append(x)
                # Condição para adicionar os FII's (Troquei o df_acao.iloc[i, 5][0] por df_acao.iloc[-i, 5][0] pq inverti a coleta dos dados
                if x not in self.listaAcao and x[4] == '1' and x[5] == '1' and len(
                        x) == 6 and x not in self.descartados and \
                        df_acao.iloc[-(i + 1), 5][0] == 'F' and df_acao.iloc[-(i + 1), 5][1] == 'I' and \
                        df_acao.iloc[-(i + 1), 5][2] == 'I':
                    self.listaAcao.append(x)
            except IndexError:
                pass

        # print(self.listaAcao)
        print(len(self.listaAcao))
        print(self.listaAcao)

        return (self.listaAcao)

    # Selecionando as cotações
    def dadosCotacoes(self): #antigo run

        self.dados_fundos= []

        # DIVIDINDO O BANCO DE DADOS POR PERÍODO DE TEMPO
        #self.listaAcao = ['VISC11']

        for fundo in self.listaAcao:
            #with self.mutex:
            #Verificando se o Arquivo existe:
            if os.path.exists(f'F:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_yahoo/{fundo}.SA.csv'):
                pass
            else:
                self.options = FirefoxOptions()
                #options = webdriver.FirefoxProfile()
                # OCULTANDO O FIREFOX
                self.options.add_argument("--headless")
                # Configurando salvamento automático
                self.options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/csv,text/csv,text/plain")
                self.options.set_preference("browser.download.manager.showWhenStarting", False)
                self.options.set_preference("browser.download.dir", "F:\OneDrive\Cursos Python\Antifragil_Bolsa\Dados_yahoo")
                self.options.set_preference("browser.download.folderList", 2)
                self.options.add_argument("--disable-extensions")

                # INICIANDO
                # Verificando se há conteúdo no site
                try:
                    browser = webdriver.Firefox(executable_path='F:/OneDrive/Cursos Python/Antifragil_Bolsa/geckodriver.exe',
                                                options=self.options)
                    sleep(5)
                    browser.get(f"https://br.financas.yahoo.com/quote/{fundo}.SA/history?p={fundo}.SA")#ancr11-sa/")
                    browser.maximize_window()
                    '''
                    https://www.quora.com/How-do-I-check-if-a-page-is-active-and-no-404-error-is-shown-in-selenium
                    driver.getPageSource().contains("404");
                    or
                    driver.getPageSource().contains("not found")
                    '''
                    sleep(5)
                    self.button_tabela = WebDriverWait(browser, 3).until(EC.presence_of_element_located((By.XPATH,
                                                                                                    '/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[2]/div/div/section/div[1]/div[1]/div[1]/div/div/div/span')))
                    # button_tabela = browser.find_element_by_xpath('/html/body/div[5]/article/div[2]/div/div[1]/div/div/div[1]/div[3]/div/div/div/div[3]/div[1]/div[1]/div/span[2]')
                    self.button_tabela.click()

                    self.button_max = WebDriverWait(browser, 3).until(EC.presence_of_element_located((By.XPATH,
                                                                                                         '/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[2]/div/div/section/div[1]/div[1]/div[1]/div/div/div/div/div/ul[2]/li[4]/button')))
                    # button_tabela = browser.find_element_by_xpath('/html/body/div[5]/article/div[2]/div/div[1]/div/div/div[1]/div[3]/div/div/div/div[3]/div[1]/div[1]/div/span[2]')
                    self.button_max.click()

                    #sleep(5)
                    button_aplicar = WebDriverWait(browser, 3).until(EC.presence_of_element_located((By.XPATH,
                                                                                                        '/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[2]/div/div/section/div[1]/div[1]/button')))
                    # button_historico = browser.find_element_by_xpath('/html/body/div[5]/article/div[2]/div/div[1]/div/div/div[1]/div[3]/div/div/div/div[3]/div[1]/div[2]/div/span[2]')
                    button_aplicar.click()
                    #sleep(5)
                    #button_download = WebDriverWait(browser, 3).until(EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[2]/div/div/section/div[1]/div[2]/span[2]/a[2]/text()")))
                    #button_download = browser.find_element_by_xpath("/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[2]/div/div/section/div[1]/div[2]/span[2]/a[@href]")
                    #browser.find_elements_by_link_text(("Fazer download dos dados")


                    for a in browser.find_elements_by_link_text('Fazer download dos dados'):
                        self.elem = a.get_attribute('href')
                        print(self.elem)
                    browser.close()
                    self.download_window()

                except (TimeoutException, NoSuchWindowException):
                    browser.close()
                    pass

    def download_window(self):

        try:
            # UTILIZANDO O CHROME PORQUE O FIREFOX NÃO FECHA A PÁGINA DE DOWNLOAD (obs: usa "\" para descrever caminho da pasta de download)
            self.op = ChromeOptions()
            self.op.gpu = False
            self.op.headless = True
            #self.op.add_argument("--disable-extensions")
            self.op.add_experimental_option("prefs", {"download.default_directory": "F:\OneDrive\Cursos Python\Antifragil_Bolsa\Dados_yahoo",
                                              "download.prompt_for_download": False,
                                              "download.directory_upgrade": True,
                                              "safebrowsing.enabled": True,
                                              'profile.default_content_setting_values.automatic_downloads': 2})

            desired = self.op.to_capabilities()
            desired['loggingPrefs'] = {'performance': 'ALL'}
            self.down = webdriver.Chrome(executable_path='F:/OneDrive/Cursos Python/Antifragil_Bolsa/chromedriver.exe', options=self.op,
                                         desired_capabilities=desired)

            self.down.get(f'{self.elem}')
            sleep(6)
            self.down.close()

        except (TimeoutException, NoSuchWindowException):
            self.down.close()
            pass


if __name__ == '__main__':
    path = 'F:/OneDrive/Cursos Python/AntiFragil/COTAHIST/COTAHIST_A2020.TXT'
    #stdoutmutex = threading.Lock()
    #threads = []
    descartados = ['']
    obj = WebScraping(descartados, path)#, stdoutmutex)
    #obj.time()
    obj.listaFundos()
    obj.dadosCotacoes()
    duracao = round((perf_counter()), 0)
    horas = int(duracao//3600)
    minutos = int(round((duracao/3600 - duracao//3600)*60, 0))
    segundos = int(round((duracao%60), 0))
    print(f'Tempo de execução:{horas}:{minutos}:{segundos}')
