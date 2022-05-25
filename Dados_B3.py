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
import os.path
import csv
import threading

class Antifragil(threading.Thread):

    def __init__(self, papel, path, descartados, inf, sup, mutex):
        self.papel = papel
        self.path_now = path
        self.descartados = descartados
        self.limite_inf = inf
        self.limite_sup = sup
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

    # Métodos para Renda Variável

    # B3
    def dadosB3(self):
        variacao = []
        self.ano = []
        for i in range(2000, 2021):
            # path_year = (f'E:/OneDrive/Investimento/COTAHIST/COTAHIST_A{i}.TXT')
            dadosAcao = b3.parse_dataset(f'E:/OneDrive/Investimento/COTAHIST/COTAHIST_A{i}.TXT')  # path_year)
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
                for v in range(1, len(df_filtered.iloc[0:, 0]) + 1):
                    variacao.append(((df_filtered.iloc[-v, 13] / df_filtered.iloc[-v, 9]) - 1) * 100)
                self.ano.append(df_filtered.iloc[0, 1])
            else:
                pass

        retorno = []
        capital = [1]
        for u, r in enumerate(variacao):
            if u == 0:
                capital.append(capital[u] * (1 + r / 100))
                retorno.append(capital[u + 1] - capital[u])
            else:
                capital.append(capital[u] * (1 + r / 100))
                retorno.append(capital[u + 1] - capital[u])

        # Transformando os dados em vetor
        # data = np.array(price)
        var = np.array(variacao)
        retor = np.array(retorno)

        # Obtendo a médiados dados
        # dados_med = np.mean(data)
        # Obtendo o desvio padrão
        # dados_desvpad = st.variance(data)

        # Calculando o Hurst
        # h = nolds.hurst_rs(data=data)
        h_var = nolds.hurst_rs(data=var)
        h_retor = nolds.hurst_rs(data=retor)

        print(f'Hurst = variação:{h_var}\nretorno:{h_retor}')

        # print(data.size)
        print(var.size)

        return var, retor

        # B3_CSV
    def dadosB3CSV(self, papel):
        dadosAcao = pd.read_csv(f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_B3_CSV/{papel}.csv', sep=',',
                                engine='python')
        # dadosAcao = pd.read_csv(f'E:/OneDrive/Cursos Python/AntiFragil/Blue Chips/{self.papel}.csv', sep=',', engine='python')  # ECOR3.SA
        # Convertendo a data para datetime
        dadosAcao['Date'] = dadosAcao['Date'].astype('datetime64[ns]')
        # Removendo os dados nulos (NaN)
        try:
            dadosAcao.dropna(inplace=True)
        except ValueError:
            pass
        # price = [round(x/100, 2) for x in dadosAcao.iloc[0:, 4]]

        # Dados CSV
        self.ano = [dadosAcao.iloc[0, 0], dadosAcao.iloc[-1, 0]]
        '''
        variacao = []
        for v in range(1, len(dadosAcao.iloc[0:, 0]) + 1):
            # Condição para verificar denominador igual a zero
            if dadosAcao.iloc[-v, 1] != 0:
                variacao.append(round(((dadosAcao.iloc[-v, 4] / dadosAcao.iloc[-v, 1]) - 1) * 100, 2))
            else:
                # variacao.append(round(dadosAcao.iloc[-v, 4], 2))
                pass
        '''
        variacao = [x for x in dadosAcao.iloc[0:, 5]]
        retorno = []
        volume = [x for x in dadosAcao.iloc[0:, 6]]
        tam = []
        #capital = [1000.0]
        capital = [0.1]
        # variacao = np.random.choice(a=variacao, size=700, replace=True)
        # Laço para obter retorno e o tamanho dos candles
        for u, r in enumerate(variacao):
            if u == 0:
                capital.append(capital[u] * (1 + r / 100))
                retorno.append(capital[u + 1] - capital[u])
                # Fazendo o módulo do tamanho do candle
                tam.append(((dadosAcao.iloc[u, 4] - dadosAcao.iloc[u, 1]) ** 2) ** 1/2)
            else:
                capital.append(capital[u] * (1 + r / 100))
                retorno.append(capital[u + 1] - capital[u])
                # Fazendo o módulo do tamanho do candle
                tam.append(((dadosAcao.iloc[u, 4] - dadosAcao.iloc[u, 1]) ** 2) ** 1 / 2)

        # Transformando os dados em vetor
        # data = np.array(price)
        var = np.array(variacao)
        retor = np.array(retorno)
        vol = np.array(volume)
        vol_med = vol.mean()
        tamanho = np.array(tam)
        tam_med = tamanho.mean()
        #capita = np.array(capital)

        #print(f'capital mínimo:{capita.min()}')
        #print(f'capital máximo:{capita.max()}')

        print(f'variação mínima:{var.min()}')
        print(f'variação máxima:{var.max()}')
        #print(f'retorno mínimo:{retor.min()}')
        #print(f'retorno máximo:{retor.max()}')

        # Obtendo a médiados dados
        # dados_med = np.mean(data)
        # Obtendo o desvio padrão
        # dados_desvpad = st.variance(data)

        # Calculando o Hurst
        # h = nolds.hurst_rs(data=data)
        h_var = nolds.hurst_rs(data=var)  # var_desloc)
        h_retor = nolds.hurst_rs(data=retor)

        print(f'Hurst = variação:{h_var}\nretorno:{h_retor}')

        # print(data.size)
        print(var.size)

        return var, retor, tam_med, vol_med

    # Yahoo
    def dadosCSV(self, papel):
        print(papel)
        dadosAcao = pd.read_csv(f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_yahoo_CSV/{papel}.SA.csv', sep=',', engine='python')
        # dadosAcao = pd.read_csv(f'E:/OneDrive/Cursos Python/AntiFragil/Blue Chips/{self.papel}.csv', sep=',', engine='python')  # ECOR3.SA
        # Removendo os dados nulos (NaN)
        try:
            dadosAcao.dropna(inplace=True)
        except ValueError:
            pass
        # price = [round(x/100, 2) for x in dadosAcao.iloc[0:, 4]]
        # Convertendo a data para datetime
        dadosAcao['Date'] = dadosAcao['Date'].astype('datetime64[ns]')
        # Dados CSV
        self.ano = [dadosAcao.iloc[0, 0], dadosAcao.iloc[-1, 0]]
        # MÉTODO PARA REMOVER OUTLIERS
        df_q = dadosAcao['Variation']
        removed_outliers = df_q.between(df_q.quantile(.003), df_q.quantile(.997))
        variacao = [x for x in df_q[removed_outliers]]
        '''
        for v in range(1, len(dadosAcao.iloc[0:, 0]) + 1):
            # Condição para verificar denominador igual a zero
            if dadosAcao.iloc[-v, 1] != 0:
                variacao.append(round(((dadosAcao.iloc[-v, 4] / dadosAcao.iloc[-v, 1]) - 1) * 100, 2))
            else:
                # variacao.append(round(dadosAcao.iloc[-v, 4], 2))
                pass
        '''
        retorno = []
        capital = [0.1]
        dp = np.std(variacao)
        volume = [x for x in dadosAcao.iloc[0:, 6]]# if x > 0]
        tam = []
        # variacao = np.random.choice(a=variacao, size=700, replace=True)
        for u, r in enumerate(variacao):
            if u == 0:
                capital.append(capital[u] * (1 + r / 100))
                retorno.append(capital[u + 1] - capital[u])
                # Fazendo o módulo do tamanho do candle
                tam.append(((dadosAcao.iloc[u, 4] - dadosAcao.iloc[u, 1]) ** 2) ** 1 / 2)
            else:
                capital.append(capital[u] * (1 + r / 100))
                retorno.append(capital[u + 1] - capital[u])
                # Fazendo o módulo do tamanho do candle
                tam.append(((dadosAcao.iloc[u, 4] - dadosAcao.iloc[u, 1]) ** 2) ** 1 / 2)

        # Transformando os dados em vetor
        # data = np.array(price)
        var = np.array(variacao)
        retor = np.array(retorno)
        vol = np.array(volume)
        vol_med = vol.mean()
        print(f'Volume médio: {vol_med}')
        tamanho = np.array(tam)
        tam_med = tamanho.mean()

        # Obtendo a médiados dados
        # dados_med = np.mean(data)
        # Obtendo o desvio padrão
        # dados_desvpad = st.variance(data)

        # Calculando o Hurst
        # h = nolds.hurst_rs(data=data)
        h_var = nolds.hurst_rs(data=var)  # var_desloc)
        h_retor = nolds.hurst_rs(data=retor)

        print(f'Hurst = variação:{h_var}\nretorno:{h_retor}')

        # print(data.size)
        print(var.size)

        return var, retor, tam_med, vol_med, dp

    # Módulo para o Dólar
    def dadosCSVMoeda(self):
        dadosAcao = pd.read_csv(f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/{self.papel}.csv', sep=',',
                                engine='python')  # ECOR3.SA
        # dadosAcao = pd.read_csv(f'E:/OneDrive/Cursos Python/AntiFragil/Blue Chips/{self.papel}.csv', sep=',', engine='python')  # ECOR3.SA
        # Removendo os dados nulos (NaN)
        try:
            dadosAcao.dropna(inplace=True)
        except ValueError:
            pass
        # price = [round(x/100, 2) for x in dadosAcao.iloc[0:, 4]]

        # Dados CSV
        variacao = [float(x.replace('%', '').replace(',', '.')) for x in dadosAcao['Var%']]
        self.ano = [dadosAcao.iloc[-1, 0][-4:], dadosAcao.iloc[0, 0][-4:]]
        '''
        for v in range(1, len(dadosAcao.iloc[0:, 0]) + 1):
            # Condição para verificar denominador igual a zero
            if dadosAcao.iloc[-v, 1] != 0:
                variacao.append(round(((dadosAcao.iloc[-v, 4] / dadosAcao.iloc[-v, 1]) - 1) * 100, 2))
            else:
                # variacao.append(round(dadosAcao.iloc[-v, 4], 2))
                pass
        '''
        retorno = []
        capital = [0.1]
        # variacao = np.random.choice(a=variacao, size=700, replace=True)
        for u, r in enumerate(variacao):
            if u == 0:
                capital.append(capital[u] * (1 + r / 100))
                retorno.append(capital[u + 1] - capital[u])
            else:
                capital.append(capital[u] * (1 + r / 100))
                retorno.append(capital[u + 1] - capital[u])

        # Transformando os dados em vetor
        # data = np.array(price)
        var = np.array(variacao)
        retor = np.array(retorno)

        # Obtendo a médiados dados
        # dados_med = np.mean(data)
        # Obtendo o desvio padrão
        # dados_desvpad = st.variance(data)

        # Calculando o Hurst
        # h = nolds.hurst_rs(data=data)
        h_var = nolds.hurst_rs(data=var)  # var_desloc)
        h_retor = nolds.hurst_rs(data=retor)

        print(f'Hurst = variação:{h_var}\nretorno:{h_retor}')

        # print(data.size)
        print(var.size)

        return var, retor

    # Módelo para o IPCA
    def dadosXL(self):
        dadosAcao = pd.ExcelFile(f'E:/OneDrive/Cursos Python/AntiFragil/{self.papel}.xlsx')  # , engine='python')
        df = pd.read_excel(dadosAcao, 'bcdata_IPCA')
        variacao = [x for x in df.iloc[0:, 1]]
        retorno = []
        capital = [1000]
        for u, r in enumerate(variacao):
            if u == 0:
                capital.append(capital[u] * (1 + r / 100))
                retorno.append(capital[u + 1] - capital[u])
            else:
                capital.append(capital[u] * (1 + r / 100))
                retorno.append(capital[u + 1] - capital[u])

        # Transformando os dados em vetor
        # data = np.array(price)
        var = np.array(variacao)
        retor = np.array(retorno)

        # Obtendo a médiados dados
        # dados_med = np.mean(data)
        # Obtendo o desvio padrão
        # dados_desvpad = st.variance(data)

        # Calculando o Hurst
        # h = nolds.hurst_rs(data=data)
        h_var = nolds.hurst_rs(data=var)  # var_desloc)
        h_retor = nolds.hurst_rs(data=retor)

        print(f'Hurst = variação:{h_var}\nretorno:{h_retor}')

        # print(data.size)
        print(var.size)

        return var, retor

    def calc(self, var, retor):
        # Obtenção da PDF
        # pdf = s.gaussian_kde(data)
        # pdf_var = s.gaussian_kde(var)
        self.pdf_var = s.gaussian_kde(var)  # var_desloc
        self.pdf_retor = s.gaussian_kde(retor)
        # set_bandwidth: método utilizado para determinar a largura de banda (Facultativo)
        # pdf_var.set_bandwidth(bw_method='silverman') #This can be ‘scott’, ‘silverman’

        print(f'variação mínima:{var.min()}')
        print(f'variação máxima:{var.max()}')
        print(f'retorno mínimo:{retor.min()}')
        print(f'retorno máximo:{retor.max()}')

        # Obtendo o valor do parâmetro de largura de banda
        larg_band = self.pdf_retor.covariance_factor()
        print(larg_band)
        # Obtenção dos eixos do retorno
        # x_var = np.linspace(var_desloc.min(), var_desloc.max(), len(var_desloc))
        # Multipliquei por 10 o último termo do linspace para aumentar o intervalo, melhora a precisão da interpolação
        self.x_retor = np.linspace(retor.min(), retor.max(), 10 * len(retor))
        self.y0_retor = np.reshape(self.pdf_retor(self.x_retor).T, self.x_retor.shape)

        # Fazendo o Delta s
        # Alterando a largura de banda para cima (aumentando)
        self.pdf_retor = s.gaussian_kde(retor)
        self.pdf_retor.set_bandwidth(bw_method=self.pdf_retor.factor + 1)
        # Obtendo o valor da largura de banda plus
        larg_plus = self.pdf_retor.covariance_factor()
        # Obtendo a função alterada plus
        self.y0_plus = np.reshape(self.pdf_retor(self.x_retor).T, self.x_retor.shape)
        # Alterando a largura de banda para baixo (diminuindo)
        self.pdf_retor.set_bandwidth(bw_method=self.pdf_retor.factor / (1.5 * np.pi))
        # Obtendo o valor da largura de banda minus
        larg_minus = self.pdf_retor.covariance_factor()
        # Obtendo a função alterada minus
        self.y0_minus = np.reshape(self.pdf_retor(self.x_retor).T, self.x_retor.shape)

        # calculando o Delta s
        self.delta = (larg_plus - larg_minus) * 2

        # Calculando o centro da distribuição, nível de estresse
        # Calculando o omega
        # self.omega = np.median(self.y0_retor)
        max_y0_plus = self.y0_plus.max()
        # Calculando o valor de x para o max_y0_plus
        max_y0_plus_reduced = self.y0_plus - max_y0_plus
        self.max_y0_plus_x = interpol.UnivariateSpline(self.x_retor, max_y0_plus_reduced, s=0)

        # Calculando a intersecção entre a curva do retorno e do retorno plus para Fragilidade
        intersection = []
        limite = self.max_y0_plus_x.roots()[0]
        for z, a in enumerate(self.y0_plus):
            if a > self.y0_retor[z] and self.x_retor[z] < limite:
                intersection.append(a)
            if self.x_retor[z] > limite:
                break
        inter = np.array(intersection)
        self.omega = inter.max()

        # Calculando o valor de x para o omega(centro da distribuição) encontrado
        omega_reduced = self.y0_retor - self.omega
        self.omega_x = interpol.UnivariateSpline(self.x_retor, omega_reduced, s=0)

        # Há duas raízes porque a primeira representa o lado esquerdo da curva e a outra o lado direito
        # Imprimindo a(s) raiz(es)
        # print(self.omega_x.roots())
        # Confirmando o resultado
        # print(f'Valor de omega_x: {self.pdf_retor(self.omega_x.roots()[0])}')
        #print(f'Valor de omega_x na função: {self.pdf_retor(self.omega_x.roots()[0])} y:{self.omega}')
        #print(f'Valor de omega_x: {self.omega_x.roots()[0]}')

        # Calculando o nível de estresse (k)
        # Eixo x mediano entre omega e o começo
        x0_k = np.linspace(retor.min(), self.omega_x.roots()[0], 10 * len(retor))
        y0_k = np.reshape(self.pdf_retor(x0_k).T, x0_k.shape)
        # self.k = np.median(y0_k)
        self.k = np.mean(y0_k)*2
        # Calculando o valor de x para o k encontrado
        k_reduced = y0_k - self.k
        self.k_x = interpol.UnivariateSpline(x0_k, k_reduced,
                                             s=0)  # s: é o número de nós utilizado para encontrar o valor
        #self.k_x = self.omega_x.roots()[0]/0.4
        # Há duas raízes porque a primeira representa o lado esquerdo da curva e a outra o lado direito
        # Imprimindo a(s) raiz(es)
        # print(self.k_x.roots())
        # Confirmando o resultado
        #print(self.k_x.roots())
        #print(f'Raiz de k_x: {self.pdf_retor(self.k_x.roots()[0])} y:{self.k}')
        #print(f'Centro da Distribuição: {self.omega}')

        # Obtenção dos eixos do retorno
        # x_retor = np.linspace(retor.min(), retor.max(), len(retor))
        # y0_retor = np.reshape(self.pdf_retor(x_retor).T, x_retor.shape)

        # Calculando a intersecção entre a curva do retorno e do retorno plus para AntiFragilidade
        intersection_AF = []
        limite_AF = self.max_y0_plus_x.roots()[0]
        for z, a in enumerate(self.y0_plus[::-1]):
            if a > self.y0_retor[-(z+1)] and self.x_retor[-(z+1)] > limite_AF:
                intersection_AF.append(a)
            if self.x_retor[-(z+1)] < limite_AF:
                break
        inter_AF = np.array(intersection_AF)
        try:
            self.omega_AF = inter_AF.max()
        except ValueError:
            self.omega_AF = self.x_retor[-1]

        # Calculando o valor de x para o omega(centro da distribuição) encontrado
        omega_reduced_AF = self.y0_retor - self.omega_AF
        self.omega_AF_x = interpol.UnivariateSpline(self.x_retor, omega_reduced_AF, s=0)

        # Calculando os Parâmetros L e H
        # Eixo x mediano entre omega e o final
        x0_k = np.linspace(self.omega_x.roots()[0], retor.max(), 10 * len(retor))
        y0_k = np.reshape(self.pdf_retor(x0_k).T, x0_k.shape)
        # self.k = np.median(y0_k)
        #self.k = np.mean(y0_k)
        self.L = self.omega_AF / 1.5
        self.H = self.omega_AF / 2.5
        # Calculando o valor de x para o L e H encontrado
        L_reduced = y0_k - self.L
        self.L_x = interpol.UnivariateSpline(x0_k, L_reduced,
                                             s=0)  # s: é o número de nós utilizado para encontrar o valor
        H_reduced = y0_k - self.H
        self.H_x = interpol.UnivariateSpline(x0_k, H_reduced,
                                             s=0)  # s: é o número de nós utilizado para encontrar o valor

        # Média da função
        mean_function = self.y0_retor.mean()

        # Função da média
        func_mean = self.pdf_var(self.x_retor.mean())

        # Verificando o perfil do Papel (Obs: tem que analisar o comprimento da cauda também)
        if mean_function > func_mean:
            print('Anti-frágil')
        else:
            print('Frágil')
        # result_pdf = s.exponweib.fit_loc_scale(y0_var, 1, 1)
        # print(f'alfa e beta: {result_pdf[0]}, {result_pdf[1]}')

        # return x_var, y0_minus, y0_plus, k_x, delta, omega_x

    def pdf_retor_plus(self, value):
        self.pdf_retor.set_bandwidth(bw_method=self.pdf_retor.factor + 1)
        return self.pdf_retor(value)

    def pdf_retor_minus(self, value):
        self.pdf_retor.set_bandwidth(bw_method=self.pdf_retor.factor / (1.5 * np.pi))
        return self.pdf_retor(value)

    # Função da integral
    def integrate(self, x, omega, delta):
        integ = (omega - x) * ((self.pdf_retor_plus(x)) - (self.pdf_retor_minus(x))) / delta #delta já está multiplicado por 2
        return integ

    def fragilidade(self):
        sensivity = sint.quad(self.integrate, -np.inf, self.k_x.roots()[0], args=(self.omega_x.roots()[0], self.delta))
        resultado = sensivity[0]
        return resultado

    # Métodos para calcular a Antifragilidade
    def integrate_AF_LH(self, x, omega, delta):
        integ = (x - omega) * ((self.pdf_retor_plus(x)) - (self.pdf_retor_minus(x))) / delta #self.pdf_retor(x)
        return integ

    def integrate_AF_Omega_inf(self, x, omega, delta):
        integ = (x - omega) * ((self.pdf_retor_plus(x)) - (self.pdf_retor_minus(x))) / delta #self.pdf_retor(x)
        return integ

    def antifragilidade(self):
        sensivity_LH = sint.quad(self.integrate_AF_LH, self.L_x.roots()[0], self.H_x.roots()[0], args=(self.omega_x.roots()[0], self.delta))
        #sensivity_Omega_inf = sint.quad(self.integrate_AF_Omega_inf, self.omega_x.roots()[0], np.inf, args=(self.omega_x.roots()[0], self.delta))
        #print(f'LH: {sensivity_LH[0]}')
        #print(f'Omega até inf: {sensivity_Omega_inf[0]}')
        resultado = sensivity_LH[0] #/ sensivity_Omega_inf[0]
        return resultado

    # Integral para calcular o intervalo de probabilidade de ganho(usa a variação. ex: de 2 a 4%)
    def integrate_prob(self, x):
        integ_prob = self.pdf_var(x)
        return integ_prob

    def probability(self):
        prob_var = sint.quad(self.integrate_prob, self.limite_inf, self.limite_sup)
        return prob_var

    def plot(self):
        fig, ax = plt.subplots()
        # Plotando o ponto de estresse (k)
        ax.scatter(self.k_x.roots()[0], self.k, alpha=0.5, label='k: nível de estresse', color='r')  # c=self.k_x.roots()[0], 

        # Plotando o parâmetro L
        ax.scatter(self.L_x.roots()[0], self.L, alpha=0.5, label='L', color='m')  # c=self.k_x.roots()[0],

        # Plotando o parâmetro H
        ax.scatter(self.H_x.roots()[0], self.H, alpha=0.5, label='H', color='orange')  # c=self.k_x.roots()[0],

        # Plotando o centro da distribuição (omega)
        # Omega negativo
        ax.scatter(self.omega_x.roots()[0], self.omega, alpha=0.5, label='omega: centro da distribuição -', color='y')  # c=self.omega_x.roots()[0],
        # Omega positivo
        ax.scatter(self.omega_AF_x.roots()[1], self.omega_AF, alpha=0.5, label='omega: centro da distribuição +', color='g')

        # Plotando as curvas
        if papel[-1] == 3 or papel[-1] == 4 or papel[-1] == 1:
            ax.set_title(f'{self.papel}\n[Retorno/ Densidade de Probabilidade] de {self.ano[0].year} a {self.ano[-1].year}', ha='center')
        elif papel != 'bcdata_IPCA':
            ax.set_title(f'{self.papel}\n[Retorno/ Densidade de Probabilidade] de {self.ano[0]} a {self.ano[1]}', ha='center')
        else:
            ax.set_title(f'{self.papel}\n[Retorno/ Densidade de Probabilidade]', ha='center')
        ax.plot(self.x_retor, self.y0_retor, label='Curva original')
        ax.plot(self.x_retor, self.y0_plus, label='Curva s+')
        ax.plot(self.x_retor, self.y0_minus, label='Curva s-')
        plt.xlabel('Retorno')
        plt.ylabel('Densidade de Probabilidade')
        fig.text(0.5, 0.2, f'Fragilidade: {round(obj.fragilidade(), 4)}\n'
        f'Probabilidade de Retorno entre {inf}% e {sup}%: {round(obj.probability()[0]*100, 2)}%\n'
        f'Antifragilidade: {round(obj.antifragilidade(), 4)}', ha='center')

        # ax.legend(f'{self.fragilidade()[0]}')
        ax.legend()
        plt.show()

    # GRAVANDO DADOS IMPORTANTES SOBRE OS ARQUIVOS
    def dadosSelect(self):

        # Utilizando os nomes dos arquivos já baixados
        ##lista = os.listdir(f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_yahoo_CSV')
        ##self.listaAcao = [x.replace('.SA.csv', '') for x in lista]
        # Utilizando somente a lista das opções com liquidez
        lista = ['ABEV3.SA', 'BBDC4.SA', 'BOVA11.SA', 'CIEL3.SA', 'CSNA3.SA', 'GGBR4.SA',
                          'ITUB4.SA', 'ITSA4.SA', 'PETR4.SA', 'USIM5.SA', 'VALE3.SA',
                          'BRFS3.SA', 'SBPS3.SA', 'EMBR3.SA', 'SUZB3.SA', 'MRFG3.SA', 'JBSS3.SA', 'WEGE3.SA',
                          'MGLU3.SA', 'VIIA3.SA',
                          'B3SA3.SA', 'BPAC11.SA', 'BBSE3.SA', 'CMIG4.SA']
        self.listaAcao = [x.replace('.SA', '') for x in lista]
        # Verificando se o arquivo existe
        if os.path.exists(f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_calculados.csv'):
            os.remove(f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_calculados.csv')
        with open(f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_calculados.csv', 'w', newline='') as dados:
            writer = csv.writer(dados)
            # Inserindo cabeçalho
            writer.writerow(('Papel', 'Tam. médio candle', 'Volume médio', 'Fragilidade', 'Antifragilidade', 'Risco'))
            with self.mutex:
                for d in self.listaAcao:  # obj.dadosCotacoes():

                    # Verificando se o arquivo existe

                    if os.path.exists(f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_yahoo_CSV/{d}.SA.csv'):
                        Acao = pd.read_csv(f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_yahoo_CSV/{d}.SA.csv', sep=',',
                                                engine='python')
                        # Condição para a quantidade de dias. OBS: 5000 equivale +/- à 20 anos (mudei para 2000 para enquadrar o BOVA11 e outros)
                        if (len(Acao.iloc[0:, 0]) > 2000):# and (self.dadosCSV(d)[0].max() < 100.0 or self.dadosCSV(d)[0].min() > -100.0):

                            # Condição para o volume e desvio padrão (Ação e FII)
                            if (d[-1] == '4' or d[-1] == '3' or d[-1] == '5' or d[-1] == '1') and (self.dadosCSV(d)[3] >= 1000000.0) and (self.dadosCSV(d)[4] <= 3.0):
                                
                                retorno = self.dadosCSV(d)[1]
                                variacao = self.dadosCSV(d)[0]
                                self.calc(variacao, retorno)
                                writer.writerow((f'{d}', self.dadosCSV(d)[2], self.dadosCSV(d)[3], self.fragilidade(), self.antifragilidade(), self.fragilidade()/self.antifragilidade()))

                            elif (d[-1] == '1') and (self.dadosCSV(d)[3] >= 100000.0) and (self.dadosCSV(d)[4] <= 3.0):

                                retorno = self.dadosCSV(d)[1]
                                variacao = self.dadosCSV(d)[0]
                                self.calc(variacao, retorno)
                                writer.writerow((f'{d}', self.dadosCSV(d)[2], self.dadosCSV(d)[3],
                                                 self.fragilidade(), self.antifragilidade(),
                                                 self.fragilidade() / self.antifragilidade()))

                            else:
                                pass
                        else:
                            pass
                    else:
                        pass

        # Lendo a tabela com PANDAS Dataframe
        # df = pd.read_table('/Users/milso/Desktop/MONETA/Dados_Papeis.csv', 'utf-8')
        df = pd.read_csv(f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_calculados.csv', engine='python', encoding='latin1')
        print(df)

    def best(self):
        An = 5
        df = pd.read_csv(f'E:/OneDrive/Cursos Python/Antifragil_Bolsa/Dados_calculados.csv', engine='python', encoding='latin1')
        
        '''
        # ANÁLISE POR ANTI-FRAGILIDADE
        # Ordenando os melhores
        ordenado = df.iloc[0:, 4].sort_values(ascending=False)
        print(ordenado[0:An])
        best_AF_index = list(ordenado[0:An].index)
        print('#' * 10, 'Os melhores', '#' * 10)
        for i in best_AF_index:
            print(df.iloc[i, 0:])
        # Ordenando os piores
        df_filtered = df[(df.Antifragilidade > 0) | (df.Antifragilidade < 0)]
        ordenado_best = df_filtered.iloc[0:, 4].sort_values(ascending=True)
        #print(ordenado_worst[0:An])
        worst_AF_index = list(ordenado_best[0:An].index)
        print('#' * 10, 'Os piores', '#' * 10)
        for i in worst_AF_index:
            print(df.iloc[i, 0:])
        '''

        
        # ANÁLISE POR FRAGILIDADE
        # Ordenando os piores
        ordenado = df.iloc[0:, 3].sort_values(ascending=False)
        print(ordenado[0:An])
        best_AF_index = list(ordenado[0:An].index)
        print('#' * 10, 'Os piores', '#' * 10)
        for i in best_AF_index:
            print(df.iloc[i, 0:])
        # Ordenando os melhores
        ordenado_best = df.iloc[0:, 3].sort_values(ascending=True)
        #print(ordenado_worst[0:An])
        worst_AF_index = list(ordenado_best[0:An].index)
        print('#' * 10, 'Os melhores', '#' * 10)
        for i in worst_AF_index:
            print(df.iloc[i, 0:])      
        
        '''
        # ANÁLISE POR RISCO
        # Ordenando os piores
        ordenado = df.iloc[0:, 5].sort_values(ascending=False)
        best_AF_index = list(ordenado[0:An].index)
        print('#'*10, 'Os piores', '#'*10)
        for i in best_AF_index:
            print(df.iloc[i, 0:])
        # Ordenando os melhores
        #df_filtered = df[(df.Risco > 0) | (df.Risco < 0)]
        #ordenado_best = df_filtered.iloc[0:, 5].sort_values(ascending=True)
        ordenado_best = df.iloc[0:, 5].sort_values(ascending=True)
        worst_AF_index = list(ordenado_best[0:An].index)
        print('#' * 10, 'Os melhores', '#' * 10)
        for i in worst_AF_index:
            print(df.iloc[i, 0:])
        '''
if __name__ == '__main__':
    papel = 'PETR4'  #BBSE3,BRAP3,ABEV3,BBDC4,BOVA11,CIEL3,CSNA3,GGBR4,ITUB4,ITSA4,PETR4,USIM5,VALE3# DTEX3 CVCB3 CSNA3 OIBR3 TIET3 PETR4,POSI3 BPAN4 BRAP4 ENEV3 SHOW3'Piores MGLU3 RENT3 CSAN3 HYPE3 TIMP3,LAME3 CGAS3 RPMG3 RSID3 SAPR4 JBSS3.SA_MEN'#'PETR3.SA_MEN'#'bcdata_IPCA'os dados do IPCA são mensais #USD_BRL Dados Históricos_MEN #IRBR3 exemplo de Peru!
    path = 'E:/OneDrive/Investimento/COTAHIST/COTAHIST_A2020.TXT' #WEGE3 MGLU3
    descartados = ['TELB4', 'ENGI3', 'VVAR3', 'UGPA3']
    inf = 2
    sup = 4
    stdoutmutex = threading.Lock()
    threads = []
    obj = Antifragil(papel, path, descartados, inf, sup, stdoutmutex)

    # Gráfico do papel desejado
    if papel != 'bcdata_IPCA' and papel != 'USD_BRL Dados Históricos_SEM' and papel != 'USD_BRL Dados Históricos_MEN' and papel != 'USD_BRL Dados Históricos':
        # Renda variável
        # Yahoo
        retorno = obj.dadosCSV(papel)[1]
        variacao = obj.dadosCSV(papel)[0]

        # B3
        #retorno = obj.dadosB3()[1]
        #variacao = obj.dadosB3()[0]

        # B3 CSV
        #retorno = obj.dadosB3CSV(papel)[1]
        #variacao = obj.dadosB3CSV(papel)[0]

    elif papel == 'USD_BRL Dados Históricos_SEM' or papel == 'USD_BRL Dados Históricos_MEN' or papel == 'USD_BRL Dados Históricos':
        # Moeda Dólar
        retorno = obj.dadosCSVMoeda()[1]
        variacao = obj.dadosCSVMoeda()[0]

    else:
        # Renda fixa tesouro direto
        retorno = obj.dadosXL()[1]
        variacao = obj.dadosXL()[0]

    obj.calc(variacao, retorno)
    print(f'Fragilidade:{obj.fragilidade()}')
    print(f'AntiFragilidade:{obj.antifragilidade()}')
    print(f'Probabilidade de Retorno entre {inf}% e {sup}%:{round(obj.probability()[0]*100, 2)}')
    duracao = round((perf_counter()), 0)
    horas = int(duracao // 3600)
    minutos = int(round((duracao / 3600 - duracao // 3600) * 60, 0))
    segundos = int(round((duracao % 60), 0))
    print(f'Tempo de execução:{horas}:{minutos}:{segundos}')
    obj.plot()
    '''
    # Comparação entre os papéis
    #obj.listaAcoes()
    threadCOMP = threading.Thread(target=obj.dadosSelect())
    threadCOMP.daemon = True
    threadCOMP.start()
    threads.append(threadCOMP)
    for thread in threads:
        thread.join()
    #obj.dadosSelect()
    obj.best()
    duracao = round((perf_counter()), 0)
    horas = int(duracao // 3600)
    minutos = int(round((duracao / 3600 - duracao // 3600) * 60, 0))
    segundos = int(round((duracao % 60), 0))
    print(f'Tempo de execução:{horas}:{minutos}:{segundos}')
    '''
