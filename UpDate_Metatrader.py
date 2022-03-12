import csv
import B3_Reader as b3
import pandas as pd
import threading
import os.path
import yfinance as yf
from datetime import date, timedelta
import monthdelta
from time import perf_counter
import MetaTrader5 as mt5

class Atualizar(threading.Thread):

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
        #self.listaAcao = ['AALR3']
        # Utilizando os nomes dos arquivos já baixados
        ##lista = os.listdir(f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_yahoo_CSV')
        ##self.listaAcao = [x.replace('.csv', '') for x in lista]
        # Utilizando somente a lista das opções com liquidez
        self.listaAcao = ['BOVA11.SA', 'ABEV3.SA', 'BBDC4.SA', 'BOVA11.SA', 'CIEL3.SA', 'CSNA3.SA', 'GGBR4.SA',
                          'ITUB4.SA', 'ITSA4.SA', 'PETR4.SA', 'USIM5.SA', 'VALE3.SA',
                          'BRFS3.SA', 'SBPS3.SA', 'EMBR3.SA', 'SUZB3.SA', 'MRFG3.SA', 'JBSS3.SA', 'WEGE3.SA', 'MGLU3.SA', 'VIIA3.SA',
                          'B3SA3.SA', 'BPAC11.SA', 'BBSE3.SA', 'CMIG4.SA']
        #f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_yahoo_CSV/{papel}.csv'
        with self.mutex:

            for papel in self.listaAcao:
                print(papel)
                # Verificando se o arquivo existe
                if os.path.exists(f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_yahoo_CSV/{papel}.csv'):
                    self.dadosY_CSV = pd.read_csv(f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_yahoo_CSV/{papel}.csv', sep=',', engine='python')
                    # dadosAcao = pd.read_csv(f'E:/OneDrive/Cursos Python/AntiFragil/Blue Chips/{self.papel}.csv', sep=',', engine='python')  # ECOR3.SA
                    # Removendo os dados nulos (NaN)
                    # sleep(0.05)
                    try:
                        self.dadosY_CSV.dropna(inplace=True)
                    except ValueError:
                        pass
                    pass
                    # Convertendo a Data para datetime
                    # self.dadosY_CSV['Date'] = self.dadosY_CSV['Date'].astype('datetime64[ns]')

                    # COLETANDO OS DADOS DO ARQUIVO DA YAHOO
                    # Coletando o período de 1 mês
                    # Obtendo a data de hoje
                    # hoje = date.today()

                    # Subtraindo os meses para obter a data inicial
                    # data_i = hoje - monthdelta.monthdelta(1)

                    try:
                        # conecte-se ao MetaTrader 5
                        if not mt5.initialize():
                            print("initialize() failed, error code =", mt5.last_error())
                            quit()

                        # consultamos o estado e os parâmetros de conexão
                        print(mt5.terminal_info())
                        # obtemos informações sobre a versão do MetaTrader 5
                        print(mt5.version())
                        # yf.pdr_override()
                        # dados = wb.DataReader(f'{papel}', data_source='yahoo', start=f'{data_i}', end=f'{hoje}')
                        '''
                        copy_rates_from_pos(
                        symbol,       // nome do símbolo
                        timeframe,    // período gráfico
                        start_pos,    // número da barra inicial (vai do presente para o passado, portanto posição 0 é a data de hoje)
                        count         // número de barras
                           )
                        '''
                        # solicitamos n barras do papel D1 do dia atual
                        dados = mt5.copy_rates_from_pos(f"{papel.replace('.SA', '')}", mt5.TIMEFRAME_D1, 1, 900)
                        # concluímos a conexão ao terminal MetaTrader 5
                        mt5.shutdown()
                        # exibimos cada elemento de dados recebidos numa nova linha
                        print("Exibimos os dados recebidos como estão")
                        for rate in dados:
                            print(rate)
                        # a partir dos dados recebidos criamos o DataFrame
                        rates_frame = pd.DataFrame(dados)
                        # convertemos o tempo em segundos no formato datetime
                        rates_frame['time'] = pd.to_datetime(rates_frame['time'], unit='s')
                        # exibimos dados
                        print("\nExibimos o dataframe com dados")
                        print(rates_frame)
                    except (SystemError, TypeError):
                        pass

                    # Condição para verificar se a última data é igual a de hoje, se sim deletar do banco de dados e atualizar somente esta
                    # Obtendo a data de hoje
                    hoje = date.today()
                    # hoje = date.today() - timedelta(1)
                    # Condição para verificar se a última data do banco de dados é diferente de hoje e os dados são diferente de ZERO
                    if (self.dadosY_CSV.iloc[-1, 0] != str(hoje)) and (self.dadosY_CSV.iloc[-1, 1] != 0):
                        # Coletando as datas e convertendo para String
                        dias = [date.strftime(x, '%Y-%m-%d') for x in rates_frame.iloc[0:, 0]]
                        self.count = 0
                        # Verificando as datas diferentes
                        for x in dias[::-1]:
                            if x != self.dadosY_CSV.iloc[-1, 0]:
                                self.count += 1
                            elif x == self.dadosY_CSV.iloc[-1, 0]:
                                break
                            else:
                                pass
                        print(self.count)

                        with open(f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_yahoo_CSV/{papel}.csv', 'a', newline='') as arquivo:
                            writer = csv.writer(arquivo)
                            for x in range(0, self.count):
                                # 'Date', 'Open', 'High', 'Low', 'Close', 'Variation', 'Volume'
                                writer.writerow((dias[-(self.count - x)], rates_frame.iloc[-(self.count - x), 1],
                                                 rates_frame.iloc[-(self.count - x), 2],
                                                 rates_frame.iloc[-(self.count - x), 3],
                                                 rates_frame.iloc[-(self.count - x), 4], ((rates_frame.iloc[
                                                                                               -(self.count - x), 4] /
                                                                                           rates_frame.iloc[-(
                                                                                                       self.count - x), 1]) - 1) * 100,
                                                 rates_frame.iloc[-(self.count - x), 7]))
                    # Condição para atualizar o dado do mesmo dia
                    else:
                        # Copiando os dados do arquivo e deletando
                        with open(f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_yahoo_CSV/{papel}.csv', 'r', newline='') as arquivo:
                            read = list(csv.reader(arquivo))
                            # Deletando a última linha da lista
                            read.pop()
                        if os.path.exists(f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_yahoo_CSV/{papel}.csv'):
                            os.remove(f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_yahoo_CSV/{papel}.csv')
                        '''
                        # Caso ocorra problema de valor inválido na última atualização e não fizer o download antes do horário do pregão do dia seguinte
                        with open(f'E:/OneDrive/Cursos Python/Farofa do Mercado/Dados_yahoo_CSV/{papel}.csv', 'w',
                                  newline='') as arquivo:
                            writer = csv.writer(arquivo)
                            for x in read:
                                # 'Date', 'Open', 'High', 'Low', 'Close', 'Variation', 'Volume'
                                writer.writerow(x)

                        '''
                        # Reescrevendo e acrescentando a atualização
                        with open(f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_yahoo_CSV/{papel}.csv', 'w', newline='') as arquivo:
                            writer = csv.writer(arquivo)
                            for x in read:
                                # 'Date', 'Open', 'High', 'Low', 'Close', 'Variation', 'Volume'
                                writer.writerow(x)
                            # 'Date', 'Open', 'High', 'Low', 'Close', 'Variation', 'Volume'
                            # Coletando as datas e convertendo para String
                            dias = [date.strftime(x, '%Y-%m-%d') for x in rates_frame.iloc[0:, 0]]
                            writer.writerow((dias[-1], rates_frame.iloc[-1, 1],
                                             rates_frame.iloc[-1, 2], rates_frame.iloc[-1, 3],
                                             rates_frame.iloc[-1, 4],
                                             ((rates_frame.iloc[-1, 4] / rates_frame.iloc[-1, 1]) - 1) * 100,
                                             rates_frame.iloc[-1, 7]))

                else:
                    pass


if __name__ == '__main__':
    path = 'E:/OneDrive/Investimento/COTAHIST/COTAHIST_A2020.TXT'
    descartados = ''
    stdoutmutex = threading.Lock()
    threads = []
    obj = Atualizar(path, descartados, stdoutmutex)
    #obj.listaAcoes()
    threadCOMP = threading.Thread(target=obj.comparar_dados())
    threadCOMP.daemon = True
    threadCOMP.start()
    threads.append(threadCOMP)
    # obj.comparar_dados()
    for thread in threads:
        thread.join()
    duracao = round((perf_counter()), 0)
    horas = int(duracao // 3600)
    minutos = int(round((duracao / 3600 - duracao // 3600) * 60, 0))
    segundos = int(round((duracao % 60), 0))
    print(f'Tempo de execução:{horas}:{minutos}:{segundos}')