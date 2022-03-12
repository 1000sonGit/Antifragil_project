from numpy import *
import Dados_B3 as afr
import os.path
import csv
import pandas as pd
import threading
import numpy as np

class Calculo(threading.Thread):

    def __init__(self, papeis, dim, An, mutex):
        self.papeis = papeis
        self.dim = dim
        self.An = An
        self.mutex = mutex
        threading.Thread.__init__(self)

    # Gera os números aleatórios
    def geraAi(self, numbers):
        #fonte: https://stackoverflow.com/questions/18659858/generating-a-list-of-random-numbers-summing-to-1
        self.b = random.dirichlet(ones(numbers), size=1)
        #print(self.b, self.b.sum())
        return self.b

    # Gera as matrizes de fragilidade e probabilidade
    def variaveis(self):
        fragilidade = []
        probabilidade = []
        for v in self.colecao:
            fragilidade.append(v[0])
            probabilidade.append(v[1])
        self.frag = array(fragilidade)
        self.prob = array(probabilidade)

        return self.frag, self.prob

    # Calcula e coleta os dados dos papéis
    def retornoAF(self):
        path = 'E:/OneDrive/Cursos Python/AntiFragil/COTAHIST/COTAHIST_A2020.TXT'
        descartados = ''
        inf = 2
        sup = np.inf
        self.colecao = []
        for i in self.papeis:

            obj = afr.Antifragil(i, path, descartados, inf, sup, threading.Lock())

            if i != 'bcdata_IPCA' and i != 'USD_BRL Dados Históricos_MEN' and i != 'USD_BRL Dados Históricos_SEM' and i != 'USD_BRL Dados Históricos':
                # Renda variável
                # Yahoo
                retorno = obj.dadosCSV(i)[1]
                variacao = obj.dadosCSV(i)[0]

                # B3
                #retorno = obj.dadosB3()[1]
                #variacao = obj.dadosB3()[0]

            elif i == 'USD_BRL Dados Históricos_MEN' or i == 'USD_BRL Dados Históricos_SEM' or i == 'USD_BRL Dados Históricos':
                # Moeda Dólar
                retorno = obj.dadosCSVMoeda()[1]
                variacao = obj.dadosCSVMoeda()[0]

            else:
                # Renda fixa tesouro direto
                retorno = obj.dadosXL()[1]
                variacao = obj.dadosXL()[0]

            obj.calc(variacao, retorno)
            print(obj.fragilidade())
            print(obj.probability())
            # Armazenando em dicionário para usar a função com número variável de argumentos
            dic = [obj.fragilidade(), obj.probability()[0]]
            self.colecao.append(dic)
            # dic.clear()
            print(self.colecao)
        return self.colecao

    def run(self):

        # Armazenando os resultados
        # Verificando se o arquivo existe
        if os.path.exists(f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Resultados/Dados_Resultados_AF_{self.dim}papeis.csv'):
            os.remove(f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Resultados/Dados_Resultados_AF_{self.dim}papeis.csv')
        with open(f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Resultados/Dados_Resultados_AF_{self.dim}papeis.csv', 'w', newline='') as dados:
            writer = csv.writer(dados)

            # Criando lista para renomear as colunas
            self.columns_name = []
            '''
            c = 0
            for i in range(0, (self.An // 10) + 1):
                for v in range(0, 10):
                    if i == 0 and v == 0:
                        c += 1
                        pass
                    else:
                        self.columns_name.append(f'A{i}{v}')
                        c += 1
                        if c == self.An + 1: break
            '''
            self.columns_name.append('Papéis')
            self.columns_name.append('Porcentagem')
            self.columns_name.append('Retorno')
            self.columns_name.append('Risco')
            self.columns_name.append('Ret./Risco')
            writer.writerow(self.columns_name)

            unilist = []
            maximum = []
            retorno = []
            risco = []
            for i in range(0, self.dim):
                with self.mutex:

                    # Retorno
                    # retorno = Ain.dot(matrix_best_mean.T)
                    retorn = self.variaveis()[1][i]
                    retorno.append(retorn)

                    # print('-' * 20, 'Retorno', '-' * 20)
                    # print(retorno)

                    # EQUAÇÃO DO RISCO
                    # antifra = array(self.best_AF_value)
                    # risco = Ain.dot(antifra.T) #Risco antifrágil
                    risk = self.variaveis()[0][i]
                    risco.append(risk)  # Resulta na somatória das fragilidades
                    # print('-' * 20, 'Risco', '-' * 20)
                    # print(risco)

                    # EQUAÇÃO DE MAXIMIZAÇÃO
                    # maximum = retorno + (1/risco)
                    maximum.append(retorn / risk)
                    # print('-' * 20, 'Maximum retorno', '-' * 20)
                    # print(maximum)
            soma_max = sum(maximum)
            for g in range(self.An):
                unilist.append(self.papeis[g])
                unilist.append(round(100*maximum[g]/soma_max, 2))
                unilist.append(retorno[g])
                unilist.append(risco[g])
                unilist.append(maximum[g])
                writer.writerow(unilist)
                unilist.clear()
            #unilist.append(retorno[0])#, 0])
            #unilist.append(risco[0])#, 0])
            #unilist.append(maximum[0])#, 0])
            # print(unilist)



        file_name = f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Resultados/Dados_Resultados_AF_{self.dim}papeis.csv'
        df = pd.read_csv(file_name, engine='python')

        # df.columns = ['A1', 'A2', 'A3', 'Retorno', 'Risco', 'Maximum']
        # df.columns = self.columns_name
        print('-' * 20, 'Dataframe dos resultados', '-' * 20)
        print(df.head())
        # Ordenando os valores de maximum e pegando o melhor
        # ord_max = df.iloc[0:, 5].sort_values(ascending=False)
        #ord_max = df.sort_values(['Maximum'], ascending=False)
        #print('-' * 20, 'Dataframe dos resultados Ordenada', '-' * 20)
        #print(ord_max.head())
        # best_max = ord_max.index[0]
        # print(best_max)

if __name__ == '__main__':
    #papeis = ['VISC11.SA_SEM', 'PETR3.SA_MEN', 'bcdata_IPCA'] #'USD_BRL Dados Históricos_SEM', 'bcdata_IPCA', 'ABEV3.SA_MEN', 'AZUL4.SA', 'B3SA3.SA', 'BBAS3.SA', 'BBDC3.SA', 'BBDC4.SA', 'BBSE3.SA', 'BPAC11.SA', 'BRAP4.SA', 'BRFS3.SA', 'BRKM5.SA',
    #          #'BRML3.SA', 'BTOW3.SA', 'CCRO3.SA', 'CIEL3.SA', 'CMIG4.SA', 'COGN3.SA', 'CRFB3.SA', 'CSAN3.SA', 'CSNA3.SA', 'CVCB3.SA', 'CYRE3.SA', 'ECOR3.SA']
    papeis = ['ABEV3', 'BBDC4', 'BOVA11', 'CIEL3', 'CSNA3', 'GGBR4', 'ITUB4', 'ITSA4', 'PETR4', 'USIM5', 'VALE3']
    dim = len(papeis)
    An = dim
    stdoutmutex = threading.Lock()
    threads = []
    af = Calculo(papeis, dim, An, stdoutmutex)
    af.retornoAF()
    af.start()
    threads.append(af)
    for thread in threads:
        thread.join()
    '''
    Ideias
    Selecionar o número desejado de papéis pela relação prob/fragilidade e depois fazer as demais etapas de filtragem
    '''