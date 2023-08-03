import pandas as pd
import numpy as np
import mysql.connector
import os
import glob
from datetime import datetime

df1 = pd.read_csv('./AC1.csv', sep = ';', low_memory=False)

pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)

#Colocando colunas com nome Maiusculo
df1.columns = df1.columns.str.upper()

#Colocando 0 a esquerda
df1['CPF'] = df1['CPF'].astype(str).str.zfill(11)

#Convertendo para o tipo Date e especificando o padrão americano
df1['DT-NASC'] = pd.to_datetime(df1['DT-NASC'],format='%Y%m%d')
df1['AVERBACAO'] = pd.to_datetime(df1['AVERBACAO'],format='%Y%m%d')

#Convertendo colunas para String
df1 = df1.astype({'INICIO': str, 'FIM': str})

#Separando os 4 primeiros caracteres, concatenando com um hífen e adicionando os ultimos 4 digitos
df1['INICIO'] = df1['INICIO'].str[:4] + '-' + df1['INICIO'].str[4:] 
df1['FIM'] = df1['FIM'].str[:4] + '-' + df1['FIM'].str[4:] 

#Função para formatação do telefone
def formatar_telefone(numero):
    # Converte o número para string e remove a notação científica
    numero_str = '{:.0f}'.format(numero)
    # Formata o número para o padrão de números sequenciados
    return '{}{}{}'.format(numero_str[:2], numero_str[2:7], numero_str[7:])

#Aplicando as função de formatar_telefone
df1['FONE1'] = df1['FONE1'].apply(formatar_telefone)
df1['FONE2'] = df1['FONE2'].apply(formatar_telefone)
df1['FONE3'] = df1['FONE3'].apply(formatar_telefone)

#Convertendo os valores que estão como String para NaN
df1['FONE1'] = df1['FONE1'].replace('nan', np.nan)
df1['FONE2'] = df1['FONE2'].replace('nan', np.nan)
df1['FONE3'] = df1['FONE3'].replace('nan', np.nan)

#Higienizando os telefones com if, elif e else, verificando se o valor do FONE1 for null, e o do FONE2 não for, retornar para o FONE1 o valor do FONE2
#e segue esse sequenciamento até o FONE 3, caso tenha algum número em alguma das colunas que não tiver na primeira, retornar para a FONE1.
def preencher_fone(row):
    if pd.isnull(row['FONE1']) and not pd.isnull(row['FONE2']):
        return row['FONE2']
    elif pd.isnull(row['FONE1']) and not pd.isnull(row['FONE3']):
        return row['FONE3']
    else:
        return row['FONE1']
#Aplicando a função preencher_fone
df1['FONE1'] = df1.apply(preencher_fone, axis=1)

#Deletando as colunas FONE2 e FONE3
df1.drop(['FONE2', 'FONE3'], axis=1, inplace=True)

#Dropando colunas que não são interessantes para dar seguimento
df1.drop(['IDADE','CPF.1'], axis=1, inplace=True)

#Convertendo a coluna de FONE1 para String
df1['FONE1'] = df1['FONE1'].astype(str) 

#Função para remover .0 da coluna FONE1 (A coluna estava retornando alguns valores como 11233226688.0)
def remover_ponto_zero(numero_str):
    return numero_str.rstrip(".0")

#Aplicando a função remover_ponto_zero
df1['FONE1'] = df1['FONE1'].apply(remover_ponto_zero)

#Removendo valores duplicados por CPF e adicionando os valores unicos em um novo DataFrame (df)
df = df1.drop_duplicates(subset=['CPF'])

#Renomeando as colunas para o padrão da Tabela dentro do SGBD, as demais colunas ja estão com os mesmos nomes que a Tabela em si.
df.rename(columns={
    'NOME':'NOME_CLIENTE',
    'DT-NASC':'DT_NASCIMENTO',
    'BANCO EMP': 'BANCO_EMP',
    'VL BENEFICIO':'VL_BENEFICIO',
    'AGENCIA PGTO':'AGENCIA_PGTO',
    'FONE1': 'TELEFONE_DDD'
}, inplace=True)

#Salvando o DataFrame em .csv pronto para subir no BD.
df.to_csv('AC1_ETL_TESTE.csv', sep=';', index=False)
