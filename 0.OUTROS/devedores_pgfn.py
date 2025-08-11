import pandas as pd
import requests
import io
import zipfile
# As importações do Google foram movidas para dentro da função para manter o script principal limpo
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# --- Função de Upload para o Drive (ATUALIZADA) ---
def upload_para_drive_compartilhado(nome_arquivo_local, nome_arquivo_nuvem, id_drive_compartilhado):
    """Faz o upload de um arquivo para um DRIVE COMPARTILHADO específico."""
    try:
        print(f"  Iniciando upload para o Drive Compartilhado: {nome_arquivo_nuvem}")
        
        SCOPES = ['https://www.googleapis.com/auth/drive']
        CREDS_FILE = './vf-automacoes-67e45a498e41.json' # <-- Coloque o caminho do seu arquivo de credencial

        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
        service = build('drive', 'v3', credentials=creds)

        # A pasta pai agora é o próprio Drive Compartilhado
        file_metadata = {
            'name': nome_arquivo_nuvem,
            'parents': [id_drive_compartilhado]
        }
        
        media = MediaFileUpload(nome_arquivo_local, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

        # MUDANÇA CRÍTICA: Adicionados parâmetros para suportar Drives Compartilhados
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id',
            supportsAllDrives=True # <-- ESSENCIAL para contas de serviço
        ).execute()

        print(f"  -> SUCESSO! Arquivo carregado com ID: {file.get('id')}")
        return True
    except Exception as e:
        print(f"  -> ERRO no upload para o Google Drive: {e}")
        return False

# --- ETAPA 0: CONFIGURAÇÃO ---
URL_DADOS_PGFN = "https://dadosabertos.pgfn.gov.br/2025_trimestre_02/Dados_abertos_Previdenciario.zip"
SUBPASTA_DENTRO_DO_ZIP = '' 
NOMES_ARQUIVOS_CSV = ['arquivo_lai_PREV_1_202506.csv', 'arquivo_lai_PREV_2_202506.csv', 'arquivo_lai_PREV_3_202506.csv', 'arquivo_lai_PREV_4_202506.csv', 'arquivo_lai_PREV_5_202506.csv', 'arquivo_lai_PREV_6_202506.csv']
UF_DESEJADA = 'RS'
VALOR_MINIMO_DIVIDA = 100000
TERMOS_EXCLUIR = ['MUNICIPIO', 'MUNICÍPIO', 'CONTABILIDADE', 'CONTÁBIL', 'CONTABIL', 'CONTADOR', 'CONTADORA', 'CONTADORES', 'FALENCIA', 'FALÊNCIA', 'MASSA FALIDA', 'FALIDA', 'FALIDO', 'FILIAL', 'RECUPERACAO JUDICIAL', 'RECUPERAÇÃO JUDICIAL', 'EM LIQUIDACAO', 'EM LIQUIDAÇÃO']
COLUNAS_PARA_REMOVER = ['TIPO_PESSOA', 'TIPO_DEVEDOR', 'UNIDADE_RESPONSAVEL', 'NUMERO_INSCRICAO', 'TIPO_CREDITO', 'DATA_INSCRICAO', 'INDICADOR_AJUIZADO']

# --- SCRIPT PRINCIPAL ---
if __name__ == "__main__":
    # ... (ETAPA 1 e 2 permanecem as mesmas) ...
    # --- ETAPA 1: EXTRAÇÃO E CONSOLIDAÇÃO DOS DADOS ---
    print("--- INICIANDO SCRIPT DE GERAÇÃO DE RELATÓRIOS ---")
    try:
        print(f"\n[ETAPA 1/3] Baixando e consolidando dados...")
        response = requests.get(URL_DADOS_PGFN, stream=True)
        response.raise_for_status()
        lista_de_dataframes = []
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            for nome_arquivo in NOMES_ARQUIVOS_CSV:
                caminho_completo = SUBPASTA_DENTRO_DO_ZIP + nome_arquivo
                with z.open(caminho_completo) as f:
                    df_temp = pd.read_csv(f, sep=';', encoding='latin-1', low_memory=False, dtype={'CPF_CNPJ': str}, on_bad_lines='warn')
                    lista_de_dataframes.append(df_temp)
        df = pd.concat(lista_de_dataframes, ignore_index=True)
        del lista_de_dataframes
        print(f"Dados carregados. Total de {len(df)} registros.")
    except Exception as e:
        print(f"ERRO CRÍTICO na Etapa 1: {e}")
        exit()

    # --- ETAPA 2: TRATAMENTO DE DADOS ---
    try:
        print(f"\n[ETAPA 2/3] Iniciando tratamento e filtragem dos dados...")
        registros_antes = len(df)
        df = df[df['CPF_CNPJ'].str.contains('/0001-', na=False)]
        df = df[df['UF_DEVEDOR'] == UF_DESEJADA]
        mascara_exclusao = df['NOME_DEVEDOR'].str.contains('|'.join(TERMOS_EXCLUIR), case=False, na=False)
        df = df[~mascara_exclusao]
        df = df[df['VALOR_CONSOLIDADO'] > VALOR_MINIMO_DIVIDA]
        df_tratado = df.drop(columns=COLUNAS_PARA_REMOVER, errors='ignore')
        del df 
        print(f"Tratamento concluído. De {registros_antes} registros, restaram {len(df_tratado)} débitos individuais.")
    except Exception as e:
        print(f"ERRO CRÍTICO na Etapa 2: {e}")
        exit()

    # --- ETAPA 3: GERAR, SALVAR E FAZER UPLOAD DOS RELATÓRIOS ---
    print(f"\n[ETAPA 3/3] Gerando e salvando os relatórios finais no Drive...")

    ID_DRIVE_COMPARTILHADO = "0ACoS77f0zpMFUk9PVA" 

    # 1. Relatório DETALHADO
    nome_arquivo_detalhado = "relatorio_prospeccao_previdenciario_por_divida.xlsx"
    df_tratado.to_excel(nome_arquivo_detalhado, index=False)
    print(f"-> Relatório detalhado salvo localmente como '{nome_arquivo_detalhado}'")
    upload_para_drive_compartilhado(nome_arquivo_detalhado, nome_arquivo_detalhado, ID_DRIVE_COMPARTILHADO)

    # 2. Relatório TOTALIZADO
    try:
        print("Agrupando e somando dívidas por CNPJ...")
        
        # CORREÇÃO: Substituído o '...' pela lógica de agregação correta
        df_totalizado = df_tratado.groupby('CPF_CNPJ').agg(
            NOME_DEVEDOR=('NOME_DEVEDOR', 'first'),
            UF_DEVEDOR=('UF_DEVEDOR', 'first'),
            VALOR_TOTAL_DIVIDA=('VALOR_CONSOLIDADO', 'sum')
        ).reset_index()

        df_totalizado['VALOR_TOTAL_DIVIDA'] = df_totalizado['VALOR_TOTAL_DIVIDA'].round(2)

        nome_arquivo_totalizado = "relatorio_prospeccao_previdenciario.xlsx"
        df_totalizado.to_excel(nome_arquivo_totalizado, index=False)
        print(f"-> Relatório totalizado salvo localmente como '{nome_arquivo_totalizado}'")
        upload_para_drive_compartilhado(nome_arquivo_totalizado, nome_arquivo_totalizado, ID_DRIVE_COMPARTILHADO)
    except Exception as e:
        print(f"-> ERRO ao gerar ou salvar o relatório totalizado: {e}")
        
    print("\n--- PROCESSO CONCLUÍDO ---")