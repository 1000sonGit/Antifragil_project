# -*- coding: utf-8 -*-
import scipy.stats as s
import scipy.integrate as sint
import scipy.interpolate as interpol
import numpy as np
import matplotlib.pyplot as plt
import B3_Reader as b3
import pandas as pd
import nolds
from datetime import date
from time import perf_counter
import csv
import threading
import os.path
import datetime

class Converter(threading.Thread):

    def __init__(self, path, mutex):

        self.path_now = path
        self.mutex = mutex
        threading.Thread.__init__(self)

    def listaAcoes(self):
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
                if x not in self.listaAcao and x[4] == '3' and len(x) == 5 and\
                        df_acao.iloc[i, 6][0] == 'O' and df_acao.iloc[i, 6][1] == 'N':
                    self.listaAcao.append(x)
                # Condição para adicionar as ações PN
                if x not in self.listaAcao and x[4] == '4' and len(x) == 5 and\
                        df_acao.iloc[i, 6][0] == 'P' and df_acao.iloc[i, 6][1] == 'N':
                    self.listaAcao.append(x)
                # Condição para adicionar os FII's (Troquei o df_acao.iloc[i, 5][0] por df_acao.iloc[-i, 5][0] pq inverti a coleta dos dados
                if x not in self.listaAcao and x[4] == '1' and x[5] == '1' and len(
                        x) == 6 and \
                        df_acao.iloc[-(i + 1), 5][0] == 'F' and df_acao.iloc[-(i + 1), 5][1] == 'I' and \
                        df_acao.iloc[-(i + 1), 5][2] == 'I':
                    self.listaAcao.append(x)
            except IndexError:
                pass

        # print(self.listaAcao)
        print(len(self.listaAcao))
        print(self.listaAcao)

        return (self.listaAcao)

    def dadosB3(self):
        #self.listaAcao = ['BBDC3']
        with self.mutex:
            for papel in self.listaAcao:
                Date = []
                Open = []
                High = []
                low = []
                close = []
                variacao = []
                volume = []
                with open(f'F:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_B3_CSV_fast/{papel}.csv', 'a',
                          newline='') as arquivo:
                    writer = csv.writer(arquivo)
                    # Inserindo cabeçalho
                    writer.writerow(('Date', 'Open', 'High', 'Low', 'Close', 'Variation', 'Volume'))
                    for i in range(1998, 2021):
                        # path_year = (f'F:/OneDrive/Investimento/COTAHIST/COTAHIST_A{i}.TXT')
                        dadosAcao = b3.parse_dataset(f'F:/OneDrive/Investimento/COTAHIST/COTAHIST_A{i}.TXT')  # path_year)
                        df_acao = pd.DataFrame(b3.translate_bdi(dadosAcao))
                        # Convertendo a data para datetime
                        #df_acao['Date'] = df_acao['Date'].astype('datetime64[ns]')
                        df_filtered = df_acao[df_acao.iloc[0:, 3] == papel]#.sort_values(by='Date')  # 'PETR3','JBSS3', 'VALE3', 'MGLU3']
                        # lista = [x for x in (df_filtered.ClosePrice)]
                        # Convertendo a data para String
                        #df_filtered['Date'] = df_filtered['Date'].astype(str)
                        lista = [x for x in (df_filtered.iloc[0:10, 13])]
                        #datas = [w[:11] for w in (df_acao.iloc[0:, 1])]
                        # print(df_filtered['Date'])
                        # Obtendo variação, preço
                        if len(lista) != 0:
                            for v in range(0, len(df_filtered.iloc[0:, 0])):
                                Date.append(df_filtered.iloc[v, 1])
                                Open.append(df_filtered.iloc[v, 9])
                                High.append(df_filtered.iloc[v, 10])
                                low.append(df_filtered.iloc[v, 11])
                                close.append(df_filtered.iloc[v, 13])
                                variacao.append(((df_filtered.iloc[v, 13] / df_filtered.iloc[v, 9]) - 1) * 100)
                                volume.append(df_filtered.iloc[v, 18])

                        else:
                            pass


                    # Dados organizados em data crescente!
                    for x in range(0, len(variacao)):
                        # Date, open, High, low, close, variação, volume
                        writer.writerow((Date[x],
                                         Open[x],
                                         High[x],
                                         low[x],
                                         close[x],
                                         variacao[x],
                                         volume[x]))
    '''
    def dadosB3(self):
        #self.listaAcao = ['BBDC3']
        with self.mutex:
            for papel in self.listaAcao:
                if os.path.exists(f'F:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_B3_CSV/{papel}.csv'):
                    pass
                else:
                    with open(f'F:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_B3_CSV/{papel}.csv', 'a',
                              newline='') as arquivo:
                        writer = csv.writer(arquivo)
                        # Inserindo cabeçalho
                        writer.writerow(('Date', 'Open', 'High', 'Low', 'Close', 'Variation', 'Volume'))
            for i in range(1998, 2021):
                # path_year = (f'F:/OneDrive/Investimento/COTAHIST/COTAHIST_A{i}.TXT')
                dadosAcao = b3.parse_dataset(f'F:/OneDrive/Investimento/COTAHIST/COTAHIST_A{i}.TXT')  # path_year)
                df_acao = pd.DataFrame(b3.translate_bdi(dadosAcao))
                for papel in self.listaAcao:
                    Date = []
                    Open = []
                    High = []
                    low = []
                    close = []
                    variacao = []
                    volume = []
                    mtime = datetime.datetime.fromtimestamp(os.path.getmtime(f'F:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_B3_CSV/{papel}.csv'))
                    #print(mtime.date())  # Imprime ano/mês/dia
                    hoje = date.today()

                    if os.path.exists(f'F:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_B3_CSV/{papel}.csv') and mtime.date() != hoje:
                        pass
                    else:
                        with open(f'F:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_B3_CSV/{papel}.csv', 'a',
                                  newline='') as arquivo:
                            writer = csv.writer(arquivo)
                            # Inserindo cabeçalho
                            #writer.writerow(('Date', 'Open', 'High', 'Low', 'Close', 'Variation', 'Volume'))

                            # Convertendo a data para datetime
                            #df_acao['Date'] = df_acao['Date'].astype('datetime64[ns]')
                            df_filtered = df_acao[df_acao.iloc[0:, 3] == papel]#.sort_values(by='Date')  # 'PETR3','JBSS3', 'VALE3', 'MGLU3']
                            # lista = [x for x in (df_filtered.ClosePrice)]
                            # Convertendo a data para String
                            #df_filtered['Date'] = df_filtered['Date'].astype(str)
                            lista = [x for x in (df_filtered.iloc[0:10, 13])]
                            #datas = [w[:11] for w in (df_acao.iloc[0:, 1])]
                            # print(df_filtered['Date'])
                            # Obtendo variação, preço
                            if len(lista) != 0:
                                for v in range(0, len(df_filtered.iloc[0:, 0])):
                                    Date.append(df_filtered.iloc[v, 1])
                                    Open.append(df_filtered.iloc[v, 9])
                                    High.append(df_filtered.iloc[v, 10])
                                    low.append(df_filtered.iloc[v, 11])
                                    close.append(df_filtered.iloc[v, 13])
                                    variacao.append(((df_filtered.iloc[v, 13] / df_filtered.iloc[v, 9]) - 1) * 100)
                                    volume.append(df_filtered.iloc[v, 18])

                            else:
                                pass


                            # Dados organizados em data crescente!
                            try:
                                for x in range(0, len(variacao)):
                                    # Date, open, High, low, close, variação, volume
                                    writer.writerow((Date[x],
                                                     Open[x],
                                                     High[x],
                                                     low[x],
                                                     close[x],
                                                     variacao[x],
                                                     volume[x]))
                            except: pass
    '''
if __name__ == '__main__':
    path = 'F:/OneDrive/Investimento/COTAHIST/COTAHIST_A2020.TXT'
    stdoutmutex = threading.Lock()
    obj = Converter(path, stdoutmutex)
    obj.listaAcoes()
    obj.dadosB3()
    duracao = round((perf_counter()), 0)
    horas = int(duracao // 3600)
    minutos = int(round((duracao / 3600 - duracao // 3600) * 60, 0))
    segundos = int(round((duracao % 60), 0))
    print(f'Tempo de execução:{horas}:{minutos}:{segundos}')
