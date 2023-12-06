# Blibiotecas

# Modelation
import numpy as np
import pandas as pd
from functions import get_files_name, upload_sheets

########### Extraindo e manipulando os dados de Agua e Esgoto
df_base = pd.read_excel('base_de_dados/2Planilhas_AE2021/Planilhas_AE2021/Planilha_AE2021_Completa_Regionais/Planilha_AE_Indicadores_CAESB-53001000.xls', skiprows=7)

# Filtrando somente a linha desejada
df_base = df_base.loc[(df_base['Município'] != '') & (df_base['Município'].notna()) & (df_base['Município'] != '-')]
# dropando colunas indesejadas
columns = ['Liquidez corrente','Liquidez geral','Grau de endividamento','Margem operacional com depreciação','Margem operacional sem depreciação',
          'Margem líquida com depreciação','Margem líquida sem depreciação', 'Retorno sobre o patrimônio líquido', 'Composição de exigibilidades',
          'Nome do prestador de serviços','Sigla', 'Abrangência','Natureza jurídica do prestador de serviços','Código do prestador de serviços',
          'Código da região']
df_filtered = df_base.drop(columns=columns)

# Separando os dataframes por tipo de informação
df_ind_agua = df_filtered.iloc[:,36:58]
df_ind_esgoto = df_filtered.iloc[:,58:66]
df_ind_qualidade = df_filtered.iloc[:,66:78]
df_base = df_base.iloc[:, :3]

# Criando um dataframe com as informacoes da respectiva cidade indicada
df_ind_agua = pd.merge(df_base, df_ind_agua, left_index=True, right_index=True, how='outer')
df_ind_esgoto = pd.merge(df_base, df_ind_esgoto, left_index=True, right_index=True, how='outer')
df_ind_qualidade = pd.merge(df_base, df_ind_qualidade, left_index=True, right_index=True, how='outer')

folder = "base_de_dados/2Planilhas_AE2021/Planilhas_AE2021/Planilha_AE2021_Completa_Regionais"
columns = ['Liquidez corrente','Liquidez geral','Grau de endividamento','Margem operacional com depreciação','Margem operacional sem depreciação',
        'Margem líquida com depreciação','Margem líquida sem depreciação', 'Retorno sobre o patrimônio líquido', 'Composição de exigibilidades',
        'Nome do prestador de serviços','Sigla', 'Abrangência','Natureza jurídica do prestador de serviços','Código do prestador de serviços',
        'Código da região']
df_final_ind_agua = pd.DataFrame()
df_final_ind_esgoto = pd.DataFrame()
df_final_ind_qualidade = pd.DataFrame()

files_name = get_files_name(folder)
# Loop for para mapeamento das planilhas e concatenação dos dados
for file in files_name:
    df_states = pd.read_excel(f"{folder}/{file}", skiprows=7)
    df_states = df_states.loc[(df_states['Município'] != '') & (df_states['Município'].notna()) & (df_states['Município'] != '-')]
    # dropando colunas indesejadas
    df_filtered = df_states.drop(columns=columns)
    # Separando os dataframes por tipo de informação
    df_ind_agua = df_filtered.iloc[:,36:58]
    df_ind_esgoto = df_filtered.iloc[:,58:66]
    df_ind_qualidade = df_filtered.iloc[:,66:78]
    df_filtered = df_filtered.iloc[:, :3]
    # Criando um dataframe com as informacoes da respectiva cidade indicada
    df_ind_agua = pd.merge(df_filtered, df_ind_agua, left_index=True, right_index=True, how='outer')
    df_ind_esgoto = pd.merge(df_filtered, df_ind_esgoto, left_index=True, right_index=True, how='outer')
    df_ind_qualidade = pd.merge(df_filtered, df_ind_qualidade, left_index=True, right_index=True, how='outer')
    # realizando merge dos dataframes
    df_final_ind_agua = pd.concat([df_final_ind_agua,df_ind_agua])
    df_final_ind_esgoto = pd.concat([df_final_ind_esgoto,df_ind_esgoto])
    df_final_ind_qualidade = pd.concat([df_final_ind_qualidade,df_ind_qualidade])

# Excluindo os dados nan dos dataframes
df_final_ind_agua = df_final_ind_agua.dropna()
df_final_ind_esgoto = df_final_ind_esgoto.dropna()
df_final_ind_qualidade = df_final_ind_qualidade.dropna()

# informações para a função de upload
sheet_id = '1psQ_UB3RW70DPeTZ1No-Q9QQxcj6Aa4jB1xPIYHPwVo'
credentials_folder = 'Credentials/credentials_google.json'
sheet_page_agua = 'AE_IND_AGUA'
sheet_page_esg = 'AE_IND_ESG'
sheet_page_qua = 'AE_IND_QUA'
sheet_range = 'A1:AJ1000000'

# Enviando os dados Indicadores de agua    
result = upload_sheets(df_final_ind_agua, sheet_id, sheet_page_agua, sheet_range, credentials_folder)
# # Enviando os dados Indicadores de esgoto    
result = upload_sheets(df_final_ind_esgoto, sheet_id, sheet_page_esg, sheet_range, credentials_folder)
# Enviando os dados Indicadores de qualidade    
result = upload_sheets(df_final_ind_qualidade, sheet_id, sheet_page_qua, sheet_range, credentials_folder)

########## Extraindo e manipulando os dados de Aguas Pluviais

df_base = pd.read_excel('base_de_dados/Planilhas_AP2021/Tabela de Indicadores_AP2021.xls', sheet_name='Indicadores por município',skiprows=7)
df_ind_ap = df_base.drop([0,1,2]) # Dropando linhas indesejadas
df_ind_ap = df_ind_ap.rename(columns={'Nome':'Município'})

# informações para a função de upload
sheet_id = '1psQ_UB3RW70DPeTZ1No-Q9QQxcj6Aa4jB1xPIYHPwVo'
credentials_folder = 'Credentials/credentials_google.json'
sheet_page_ind = 'AP_IND'
sheet_range = 'A1:AJ1000000'

# Enviando os dados de Indicadores
result = upload_sheets(df_ind_ap, sheet_id, sheet_page_ind, sheet_range, credentials_folder)

df_base = pd.read_excel('base_de_dados/Planilhas_AP2021/Tabela de Informaçoes_AP2021.xlsx', sheet_name='Informações',skiprows=7)
df_base = df_base.drop([0,1,2,3]) # Dropando linhas indesejadas
columns = {'Unnamed: 0': 'Codigo IBGE','Unnamed: 1': 'Município', 'Unnamed: 2':'UF', 'Unnamed: 3':'Região', 'Unnamed: 4':'Capital'}
df_base = df_base.rename(columns=columns)

# Pegando somente as colunas desejadas
df_inf_ap = df_base.iloc[:,:18]
df_inf_ap.replace([np.inf, -np.inf], np.nan, inplace=True)
df_inf_ap.fillna(0, inplace=True)  # Substitua 0 pelo valor desejado para NaN, se necessário
df_inf_ap['Codigo IBGE'].fillna('', inplace=True)  # Substitua '' pelo valor desejado, se necessário

# informações para a função de upload
sheet_id = '1psQ_UB3RW70DPeTZ1No-Q9QQxcj6Aa4jB1xPIYHPwVo'
credentials_folder = 'Credentials/credentials_google.json'
sheet_page_inf = 'AP_INF'
sheet_range = 'A1:AJ1000000'

# Enviando os dados de Informações 
result = upload_sheets(df_inf_ap, sheet_id, sheet_page_inf, sheet_range, credentials_folder)
