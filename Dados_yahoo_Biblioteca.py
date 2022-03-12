import yfinance as yf
from datetime import date
import B3_Reader as b3
import pandas as pd
import csv
import threading
import os.path
from time import perf_counter
#from pandas_datareader import data as pdr
from time import sleep

class Coleta(threading.Thread):

    def __init__(self, path, mutex, descartados):
        self.path_now = path
        self.mutex = mutex
        self.descartados = descartados
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
                if x in self.listaAcao:# and df_acao.iloc[-(i + 1), 1].month != hoje.month:
                    break
                # Condição para adicionar as ações ON
                if x not in self.listaAcao and x[4] == '3' and len(x) == 5 and x not in self.descartados and \
                        df_acao.iloc[i, 6][0] == 'O' and df_acao.iloc[i, 6][1] == 'N':
                    self.listaAcao.append(x)
                # Condição para adicionar as ações PN
                if x not in self.listaAcao and x[4] == '4' and len(x) == 5 and x not in self.descartados and \
                        df_acao.iloc[i, 6][0] == 'P' and df_acao.iloc[i, 6][1] == 'N':
                    self.listaAcao.append(x)
                # Condição para adicionar os FII's (Troquei o df_acao.iloc[i, 5][0] por df_acao.iloc[-i, 5][0] pq inverti a coleta dos dados
                if x not in self.listaAcao and x[4] == '1' and x[5] == '1' and len(
                        x) == 6 and x not in self.descartados and \
                        df_acao.iloc[-(i + 1), 5][0] == 'F' and df_acao.iloc[-(i + 1), 5][1] == 'I' and \
                        df_acao.iloc[-(i + 1), 5][2] == 'I':
                    self.listaAcao.append(x)
            except:# IndexError:
                pass

        # print(self.listaAcao)
        print(len(self.listaAcao))
        print(self.listaAcao)

        return (self.listaAcao)

    def getting(self):
        #with self.mutex:
        #Utilizando os nomes dos arquivos já baixados
        lista = os.listdir(f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_yahoo_CSV')
        self.listaAcao = [x.replace('.csv', '') for x in lista]
        print(self.listaAcao)
        self.listaAcao = ['BRFS3.SA', 'SBPS3.SA', 'EMBR3.SA', 'SUZB3.SA', 'MRFG3.SA', 'JBSS3.SA', 'WEGE3.SA', 'MGLU3.SA', 'VIIA3.SA',
                          'B3SA3.SA', 'BPAC11.SA', 'BBSE3.SA', 'CMIG4.SA']
        for name in self.listaAcao:
            try:
                yf.pdr_override()
                dados = yf.download(tickers=f'{name}', period='max', group_by='ticker')#, interval='1mo') #interval default: 1d
                #dados = pdr.get_data_yahoo(f"{name}", period='max')
                sleep(0.05)
                #print(dados)
                if len(dados.iloc[0:, 0]) > 0:
                    if os.path.exists(f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_yahoo_CSV/{name}.csv'):
                        os.remove(f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_yahoo_CSV/{name}.csv')
                    with open(f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_yahoo_CSV/{name}.csv', 'w', newline='') as arquivo:
                        writer = csv.writer(arquivo)
                        # Inserindo cabeçalho
                        writer.writerow(('Date', 'Open', 'High', 'Low', 'Close', 'Variation', 'Volume'))
                        date = list(dados.Open.index.astype(str))
                        for i in range(0, len(dados.Open)):
                            writer.writerow((date[i], dados.iloc[i, 0], dados.iloc[i, 1], dados.iloc[i, 2], dados.iloc[i, 3], ((dados.iloc[i, 3] / dados.iloc[i, 0]) - 1) * 100, dados.iloc[i, 5]))
                else:
                    pass
            except SystemError:
                pass


if __name__ == '__main__':
    path = 'E:/OneDrive/Cursos Python/AntiFragil/COTAHIST/COTAHIST_A2020.TXT'
    stdoutmutex = threading.Lock()
    descartados = ''
    obj = Coleta(path, stdoutmutex, descartados)
    #obj.listaFundos()
    obj.getting()
    duracao = round((perf_counter()), 0)
    horas = int(duracao // 3600)
    minutos = int(round((duracao / 3600 - duracao // 3600) * 60, 0))
    segundos = int(round((duracao % 60), 0))
    print(f'Tempo de execução:{horas}:{minutos}:{segundos}')