import csv
import B3_Reader as b3
import pandas as pd
from datetime import date
import threading
import os.path

class Atulizar(threading.Thread):

    def __init__(self, path, descartados, mutex):
        self.path_now = path
        self.descartados = descartados
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

    def comparar_dados(self):

        # COLETANDO OS DADOS DO ARQUIVO YAHOO
        #papel = self.listaAcao[0]
        #papel = 'MGLU3'
        with self.mutex:
            for papel in self.listaAcao:
                # Verificando se o arquivo existe
                if os.path.exists(f'F:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_B3_CSV/{papel}.csv'):
                    self.dadosB3_CSV = pd.read_csv(f'F:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_B3_CSV/{papel}.csv', sep=',', engine='python')
                    # dadosAcao = pd.read_csv(f'F:/OneDrive/Cursos Python/AntiFragil/Blue Chips/{self.papel}.csv', sep=',', engine='python')  # ECOR3.SA
                    # Removendo os dados nulos (NaN)
                    try:
                        self.dadosB3_CSV.dropna(inplace=True)
                    except ValueError:
                        pass
                    pass
                    # Convertendo a Data para datetime
                    self.dadosB3_CSV['Date'] = self.dadosB3_CSV['Date'].astype('datetime64[ns]')

                    #COLETANDO OS DADOS DO ARQUIVO DA B3
                    # Carregando banco de dados: parse_dataset pertence à biblioteca B3_Reader
                    df_acao = b3.parse_dataset(self.path_now)
                    # CRIANDO AS DATAFRAME COM OS BANCOS DE DADOS
                    self.dadosB3 = pd.DataFrame(b3.translate_bdi(df_acao))

                    # FILTRANDO OS DATAFRAMES
                    # Verificando se contém o fundo no banco de dados
                    passa = 0
                    try:
                        data_filtro_B3 = self.dadosB3[self.dadosB3.iloc[0:, 3] == papel]

                        if data_filtro_B3.iloc[1, 3] == '':
                            raise ValueError
                        else:
                            pass

                    except:
                        passa = 1
                        pass
                    #print(type(data_filtro_B3.iloc[-1, 1]))
                    #print(type(self.dadosB3_CSV.iloc[-1, 0]))
                    #print(data_filtro_B3.iloc[-1, 1])
                    #print(self.dadosB3_CSV.iloc[-1, 0])
                    if passa != 1:
                        self.count = 0
                        # Verificando as datas diferentes
                        for x in data_filtro_B3.iloc[::-1, 1]:
                            if x != self.dadosB3_CSV.iloc[-1, 0]:
                                self.count += 1
                            elif x == self.dadosB3_CSV.iloc[-1, 0]:
                                break
                            else: pass
                        print(self.count)

                    # Convertendo a Data para String
                    self.dadosB3_CSV['Date'] = self.dadosB3_CSV['Date'].astype(str)

                    with open(f'F:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_B3_CSV/{papel}.csv', 'a', newline='') as arquivo:
                        writer = csv.writer(arquivo)
                        for x in range(0, self.count):
                            # Date, open, High, low, close, adj close, volume
                            writer.writerow((data_filtro_B3.iloc[-(self.count-x), 1], data_filtro_B3.iloc[-(self.count-x), 9], data_filtro_B3.iloc[-(self.count-x), 10],
                                             data_filtro_B3.iloc[-(self.count-x), 11], data_filtro_B3.iloc[-(self.count-x), 13], ((data_filtro_B3.iloc[-(self.count-x), 13] / data_filtro_B3.iloc[-(self.count-x), 9]) - 1) * 100,
                                             data_filtro_B3.iloc[-(self.count-x), 18]))
                else:
                    pass


if __name__ == '__main__':
    path = 'F:/OneDrive/Investimento/COTAHIST/COTAHIST_A2020.TXT'
    descartados = ''
    stdoutmutex = threading.Lock()
    obj = Atulizar(path, descartados, stdoutmutex)
    obj.listaAcoes()
    obj.comparar_dados()
