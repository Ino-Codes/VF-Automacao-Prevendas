import pandas as pd
import glob
import warnings

# --- NOVAS IMPORTAÇÕES PARA O UPLOAD NO GOOGLE DRIVE ---
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Ignora o aviso de estilo dos arquivos Excel da PGFN
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# --- NOVA FUNÇÃO DE UPLOAD (COPIADA DO OUTRO SCRIPT) ---
def upload_para_drive_compartilhado(nome_arquivo_local, nome_arquivo_nuvem, id_drive_compartilhado, creds_file):
    """Faz o upload de um arquivo para um DRIVE COMPARTILHADO específico."""
    try:
        print(f"  Iniciando upload para o Drive Compartilhado: {nome_arquivo_nuvem}")
        
        SCOPES = ['https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file(creds_file, scopes=SCOPES)
        service = build('drive', 'v3', credentials=creds)

        file_metadata = {
            'name': nome_arquivo_nuvem,
            'parents': [id_drive_compartilhado]
        }
        
        media = MediaFileUpload(nome_arquivo_local, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id',
            supportsAllDrives=True
        ).execute()

        print(f"  -> SUCESSO! Arquivo carregado com ID: {file.get('id')}")
        return True
    except Exception as e:
        print(f"  -> ERRO no upload para o Google Drive: {e}")
        return False

# --- ETAPA 0: CONFIGURAÇÃO ---

ARQUIVO_LEADS = "relatorio_prospeccao_previdenciario.xlsx"
PADRAO_ARQUIVOS_PARCELAMENTO = "painel do *.xlsx"
ARQUIVO_FINAL = "relatorio_detalhado_previdenciario.xlsx"

# --- NOVAS VARIÁVEIS DE CONFIGURAÇÃO PARA O DRIVE ---
ID_DRIVE_COMPARTILHADO = "0ACoS77f0zpMFUk9PVA"
CREDS_FILE = './vf-automacoes-67e45a498e41.json'

# Lista com as colunas que desejamos manter da planilha de parcelamentos
COLUNAS_PARA_MANTER = [
    "Tipo de Negociação",
    "Modalidade da Negociação",
    "Situação da Negociação",
    "Qtde de Parcelas Concedidas",
    "Qtde de Parcelas em Atraso",
    "Valor Consolidado",
    "Valor do Principal",
    "Valor da Multa",
    "Valor dos Juros",
    "Valor do Encargo Legal"
]

# --- SCRIPT PRINCIPAL DE CRUZAMENTO DE DADOS ---
if __name__ == "__main__":
    print("--- INICIANDO CRUZAMENTO DE DADOS COM MÚLTIPLAS PLANILHAS ---")

    # --- ETAPAS 1, 2, 3 e 4 (SEM ALTERAÇÕES) ---
    try:
        # Etapa 1
        print(f"Lendo o arquivo de leads: {ARQUIVO_LEADS}")
        df_leads = pd.read_excel(ARQUIVO_LEADS)
        print(f"Arquivo de leads carregado com {len(df_leads)} empresas.")

        # Etapa 2
        print(f"Procurando por arquivos de parcelamento com o padrão: '{PADRAO_ARQUIVOS_PARCELAMENTO}'")
        lista_nomes_arquivos = glob.glob(PADRAO_ARQUIVOS_PARCELAMENTO)
        if not lista_nomes_arquivos:
            raise FileNotFoundError(f"Nenhum arquivo de parcelamento encontrado.")
        print(f"Encontrados {len(lista_nomes_arquivos)} arquivos de parcelamento.")
        lista_dataframes_parcelamento = []
        for arquivo in lista_nomes_arquivos:
            print(f"  Lendo o arquivo: {arquivo}...")
            df_temp = pd.read_excel(arquivo, header=2)
            lista_dataframes_parcelamento.append(df_temp)
        print("Consolidando todos os dados de parcelamento...")
        df_parcelamentos = pd.concat(lista_dataframes_parcelamento, ignore_index=True)
        print(f"Base de parcelamentos consolidada com {len(df_parcelamentos)} registros.")

    except Exception as e:
        print(f"\nERRO durante o carregamento dos dados: {e}")
        exit()
    
    try:
        # Etapa 3
        print("Preparando dados para o cruzamento...")
        NOME_DA_COLUNA_CNPJ_NO_ARQUIVO = "CPF/CNPJ do Optante"
        df_parcelamentos = df_parcelamentos.rename(columns={NOME_DA_COLUNA_CNPJ_NO_ARQUIVO: 'CPF_CNPJ'})
        df_leads['CPF_CNPJ'] = df_leads['CPF_CNPJ'].astype(str)
        df_parcelamentos['CPF_CNPJ'] = df_parcelamentos['CPF_CNPJ'].astype(str)
        colunas_essenciais = ['CPF_CNPJ'] + COLUNAS_PARA_MANTER
        df_parcelamentos_filtrado = df_parcelamentos[colunas_essenciais]
        
    except Exception as e:
        print(f"\nERRO na etapa de preparação dos dados: {e}")
        exit()

    # Etapa 4
    print("Realizando o cruzamento das bases de dados pelo CNPJ...")
    df_com_parcelamento = pd.merge(df_leads, df_parcelamentos_filtrado, on='CPF_CNPJ', how='inner')
    cnpjs_com_parcelamento = df_com_parcelamento['CPF_CNPJ'].unique()
    df_sem_parcelamento = df_leads[~df_leads['CPF_CNPJ'].isin(cnpjs_com_parcelamento)]
    print("Cruzamento de dados concluído.")

    # --- ETAPA 5: Salvar os relatórios em abas E FAZER UPLOAD ---
    try:
        print(f"\nSalvando o arquivo final localmente: '{ARQUIVO_FINAL}'")
        
        print(f"-> Encontrados {len(df_com_parcelamento['CPF_CNPJ'].unique())} CNPJs únicos COM parcelamento, totalizando {len(df_com_parcelamento)} linhas de negociação.")
        print(f"-> Encontrados {len(df_sem_parcelamento)} CNPJs SEM parcelamento.")

        with pd.ExcelWriter(ARQUIVO_FINAL, engine='openpyxl') as writer:
            df_com_parcelamento.to_excel(writer, sheet_name='Com Parcelamento (Detalhado)', index=False)
            df_sem_parcelamento.to_excel(writer, sheet_name='Sem Parcelamento', index=False)
        
        print(f"Arquivo local '{ARQUIVO_FINAL}' salvo com sucesso.")

        # --- NOVA ETAPA: UPLOAD DO ARQUIVO FINAL ---
        print("\nIniciando upload do relatório final para o Google Drive...")
        upload_para_drive_compartilhado(ARQUIVO_FINAL, ARQUIVO_FINAL, ID_DRIVE_COMPARTILHADO, CREDS_FILE)
        
        print(f"\n--- PROCESSO CONCLUÍDO! ---")
        
    except Exception as e:
        print(f"ERRO ao salvar ou fazer upload do arquivo final: {e}")