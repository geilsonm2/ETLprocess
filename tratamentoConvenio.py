import pandas as pd
import numpy as np
import mysql.connector
import os
import glob
from datetime import datetime
from dotenv import load_dotenv

#Carregando as variáveis de dentro do arquivo .env
load_dotenv()

#Atribuindo a variável o valor do arquivo .env (caminho_csv)
caminho_csv = os.getenv('caminho_csv')

#Exibindo todas as colunas, evitando quebra desnecessária de linhas
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)


# Caminho para a pasta onde estão os arquivos CSV
caminho_csv = 'C:/Users/geils/inssMacica/teste/'

# Lista de todos os arquivos CSV no caminho especificado
arquivos_csv = glob.glob(caminho_csv + '*.csv')

# Loop para ler cada arquivo CSV e exibir seus nomes
for arquivo in arquivos_csv:
    # Lê o arquivo CSV
    df_sujo = pd.read_csv(arquivo, sep=';', low_memory=False)

    # Exibe o nome do arquivo lido
    print(f'Arquivo que esta sendo lido: {arquivo}\n\n')
   
    #Removendo valores duplicados por CPF e adicionando os valores unicos em um novo DataFrame (df)
    df1 = df_sujo.drop_duplicates(subset=['cpf'])
    
    #Colocando colunas com nome Maiusculo
    df1.columns = df1.columns.str.upper()

    #Colocando 0 a esquerda
    df1['CPF'] = df1['CPF'].astype(str).str.zfill(11)

    #Verificando se o valor da data é valido, caso contrario transformando-a em NaT
    def convertendo_para_data(data):
        if data == '0':
            return None
        try:
            return pd.to_datetime(data, format='%Y%m%d')
        except ValueError:
            return None
    
    #Convertendo para o tipo Date e especificando o padrão americano

    df1.loc[:,'DT-NASC'] = df1['DT-NASC'].apply(convertendo_para_data)
    df1.loc[:,'AVERBACAO'] = df1['AVERBACAO'].apply(convertendo_para_data)

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

    #Copiando o dataFrame para realizar um rename de forma mais otimizada
    df = df1.copy()

        #Renomeando as colunas para o padrão da Tabela dentro do SGBD, as demais colunas ja estão com os mesmos nomes que a Tabela em si.
    df.rename(columns={
        'NOME':'NOME_CLIENTE',
        'DT-NASC':'DT_NASCIMENTO',
        'BANCO EMP': 'BANCO_EMP',
        'VL BENEFICIO':'VL_BENEFICIO',
        'AGENCIA PGTO':'AGENCIA_PGTO',
        'FONE1': 'TELEFONE_DDD'
    }, inplace=True)

    #Pegando o nome do arquivo original
    nome_arquivo_original = os.path.basename(arquivo)    

    #Concatenando nome original com _ETL para ficar mais evidente e legal
    nome_novo_arquivo = nome_arquivo_original.replace('.csv', '_ETL.csv')

    #Salvando o DataFrame em .csv pronto para subir no BD.
    df.to_csv(nome_novo_arquivo, sep=';', index=False)

    print (f'\nTratamento do {arquivo} finalizado.\n')
