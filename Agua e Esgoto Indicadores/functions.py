# Blibiotecas utilizadas
import os
import gspread

# função para acessar a pasta e retornar o nome de todos os arquivos
def get_files_name(folder):
    try:
        # Lista todos os arquivos na pasta
        arquivos = os.listdir(folder)
        arquivos = list(filter(lambda x: 'xls' in x, arquivos))
        
        # Retorna a lista de nomes de arquivos
        return arquivos
    except OSError as e:
        # Trata possíveis erros ao acessar a pasta
        print(f"Erro ao listar arquivos em {folder}: {e}")
        return None

# Função para realizar o upload de dados na plataforma Google Sheets   
def upload_sheets(dataframe, sheet_id, sheet_page, sheet_range, credentials_folder):
    try:
        # Formatando os dados para subir na planilha Sheets
        columns = dataframe.columns.to_list()
        dataframe = dataframe.astype(str)
        registros = [columns] + dataframe.to_numpy().tolist()
        # Acessando a planilha
        gc = gspread.service_account(filename=credentials_folder)
        wb = gc.open_by_key(sheet_id)
        ws = wb.worksheet(sheet_page)
        # Realizando o upload
        ws.update(sheet_range, registros)
        return 'Upload na planilha Sheets concluído'
    except Exception as e:
        print(f"Erro ao subir os dados no Sheets{sheet_page}: {e}")
        return None