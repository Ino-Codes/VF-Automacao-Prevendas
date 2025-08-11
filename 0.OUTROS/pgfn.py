import pandas as pd
import requests
import io
import zipfile
import glob
import warnings
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Ignora avisos de estilo do openpyxl
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# --- FUNÇÃO DE UPLOAD PARA O GOOGLE DRIVE ---
def upload_para_drive_compartilhado(nome_arquivo_local, nome_arquivo_nuvem, id_pasta_pai, creds_file):
    """Faz o upload de um arquivo para uma pasta/drive específico."""
    try:
        print(f"  Iniciando upload de '{nome_arquivo_nuvem}' para o Drive...")
        
        SCOPES = ['https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file(creds_file, scopes=SCOPES)
        service = build('drive', 'v3', credentials=creds)

        file_metadata = {
            'name': nome_arquivo_nuvem,
            'parents': [id_pasta_pai] # Usa o ID da pasta de destino
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

# --- ETAPA 0: CONFIGURAÇÃO GERAL ---

# -- Configs da Fase 1 (Extração de Devedores) --
URL_DADOS_PGFN = "https://dadosabertos.pgfn.gov.br/2025_trimestre_02/Dados_abertos_Previdenciario.zip"
SUBPASTA_DENTRO_DO_ZIP = '' 
NOMES_ARQUIVOS_CSV = ['arquivo_lai_PREV_1_202506.csv', 'arquivo_lai_PREV_2_202506.csv', 'arquivo_lai_PREV_3_202506.csv', 'arquivo_lai_PREV_4_202506.csv', 'arquivo_lai_PREV_5_202506.csv', 'arquivo_lai_PREV_6_202506.csv']
UF_DESEJADA = 'RS'
TERMOS_EXCLUIR = ['MUNICIPIO', 'MUNICÍPIO', 'CONTABILIDADE', 'CONTÁBIL', 'CONTABIL', 'CONTADOR', 'CONTADORA', 'CONTADORES', 'FALENCIA', 'FALÊNCIA', 'MASSA FALIDA', 'FALIDA', 'FALIDO', 'FILIAL', 'RECUPERACAO JUDICIAL', 'RECUPERAÇÃO JUDICIAL', 'EM LIQUIDACAO', 'EM LIQUIDAÇÃO']
COLUNAS_PARA_REMOVER_FASE1 = ['TIPO_PESSOA', 'TIPO_DEVEDOR', 'UNIDADE_RESPONSAVEL', 'NUMERO_INSCRICAO', 'TIPO_CREDITO', 'DATA_INSCRICAO', 'INDICADOR_AJUIZADO']
NOME_ARQUIVO_DEVEDORES_DETALHADO = "relatorio_prospeccao_previdenciario_por_divida.xlsx"
NOME_ARQUIVO_DEVEDORES_AGREGADO = "relatorio_prospeccao_previdenciario.xlsx"

# -- Configs da Fase 2 (Cruzamento com Parcelamentos) --
PADRAO_ARQUIVOS_PARCELAMENTO = "painel do *.xlsx"
COLUNAS_PARA_MANTER_FASE2 = ["Tipo de Negociação", "Modalidade da Negociação", "Situação da Negociação", "Qtde de Parcelas Concedidas", "Qtde de Parcelas em Atraso", "Valor Consolidado", "Valor do Principal", "Valor da Multa", "Valor dos Juros", "Valor do Encargo Legal"]
NOME_ARQUIVO_FINAL_COM_PARCELAMENTOS = "relatorio_detalhado_previdenciario.xlsx"

# -- Configs do Google Drive --
CREDS_FILE = './vf-automacoes-67e45a498e41.json'
# ** NOVO: Insira o ID da PASTA DE DESTINO dentro do Drive Compartilhado **
ID_PASTA_ALVO_NO_DRIVE = "1pq4p9CS388dcxiZgyJ8rfG_0U1wFJ3tp"


# --- SCRIPT PRINCIPAL UNIFICADO ---
if __name__ == "__main__":
    
    VALOR_MINIMO_DIVIDA = 0
    while True:
        try:
            valor_input = input(">>> Digite o valor mínimo da dívida para o filtro (desconsidere casas decimais, por exemplo para R$100.000,00 digite: 100000) e pressione Enter: ")
            VALOR_MINIMO_DIVIDA = float(valor_input)
            if VALOR_MINIMO_DIVIDA > 0:
                print(f"Valor mínimo definido como: R$ {VALOR_MINIMO_DIVIDA:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
                break
            else:
                print("ERRO: Por favor, digite um número positivo maior que zero.")
        except ValueError:
            print("ERRO: Entrada inválida. Por favor, digite apenas números.")

    # =================================================================================
    # FASE 1: GERAÇÃO DA LISTA DE LEADS A PARTIR DOS DADOS BRUTOS DA PGFN
    # =================================================================================
    print("\n--- FASE 1: INICIANDO EXTRAÇÃO E TRATAMENTO DOS DEVEDORES DA PGFN ---")
    
    try:
        # ... (código da Fase 1, download e tratamento) ...
        print(f"\n[FASE 1 - ETAPA 1/3] Baixando e consolidando dados...")
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

        print(f"\n[FASE 1 - ETAPA 2/3] Iniciando tratamento e filtragem dos dados...")
        registros_antes = len(df)
        df = df[df['CPF_CNPJ'].str.contains('/0001-', na=False)]
        df = df[df['UF_DEVEDOR'] == UF_DESEJADA]
        mascara_exclusao = df['NOME_DEVEDOR'].str.contains('|'.join(TERMOS_EXCLUIR), case=False, na=False)
        df = df[~mascara_exclusao]
        df = df[df['VALOR_CONSOLIDADO'] > VALOR_MINIMO_DIVIDA]
        df_tratado = df.drop(columns=COLUNAS_PARA_REMOVER_FASE1, errors='ignore')
        del df 
        print(f"Tratamento concluído. De {registros_antes} registros, restaram {len(df_tratado)} débitos individuais.")

        print(f"\n[FASE 1 - ETAPA 3/3] Gerando, salvando e fazendo upload dos relatórios de prospecção...")
        df_tratado.to_excel(NOME_ARQUIVO_DEVEDORES_DETALHADO, index=False)
        print(f"-> Relatório detalhado salvo localmente como '{NOME_ARQUIVO_DEVEDORES_DETALHADO}'")
        # ALTERAÇÃO: Passando o ID da pasta de destino para a função de upload
        upload_para_drive_compartilhado(NOME_ARQUIVO_DEVEDORES_DETALHADO, NOME_ARQUIVO_DEVEDORES_DETALHADO, ID_PASTA_ALVO_NO_DRIVE, CREDS_FILE)
        
        print("Agrupando e somando dívidas por CNPJ...")
        df_totalizado = df_tratado.groupby('CPF_CNPJ').agg(NOME_DEVEDOR=('NOME_DEVEDOR', 'first'), UF_DEVEDOR=('UF_DEVEDOR', 'first'), VALOR_TOTAL_DIVIDA=('VALOR_CONSOLIDADO', 'sum')).reset_index()
        df_totalizado['VALOR_TOTAL_DIVIDA'] = df_totalizado['VALOR_TOTAL_DIVIDA'].round(2)
        df_totalizado.to_excel(NOME_ARQUIVO_DEVEDORES_AGREGADO, index=False)
        print(f"-> Relatório totalizado salvo localmente como '{NOME_ARQUIVO_DEVEDORES_AGREGADO}'")
        # ALTERAÇÃO: Passando o ID da pasta de destino para a função de upload
        upload_para_drive_compartilhado(NOME_ARQUIVO_DEVEDORES_AGREGADO, NOME_ARQUIVO_DEVEDORES_AGREGADO, ID_PASTA_ALVO_NO_DRIVE, CREDS_FILE)

    except Exception as e:
        print(f"ERRO CRÍTICO NA FASE 1: O processo foi interrompido. Detalhe: {e}")
        exit()
        
    print("\n--- FASE 1 CONCLUÍDA COM SUCESSO! ---")

    # =================================================================================
    # FASE 2: CRUZAMENTO DA LISTA DE LEADS COM OS DADOS DE PARCELAMENTO
    # =================================================================================
    print("\n--- FASE 2: INICIANDO CRUZAMENTO COM DADOS DE PARCELAMENTO ---")

    try:
        # ... (código da Fase 2, carregamento e cruzamento) ...
        print(f"Lendo o arquivo de leads gerado: {NOME_ARQUIVO_DEVEDORES_AGREGADO}")
        df_leads = pd.read_excel(NOME_ARQUIVO_DEVEDORES_AGREGADO)

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

        print("Preparando dados para o cruzamento...")
        NOME_DA_COLUNA_CNPJ_NO_ARQUIVO = "CPF/CNPJ do Optante"
        df_parcelamentos = df_parcelamentos.rename(columns={NOME_DA_COLUNA_CNPJ_NO_ARQUIVO: 'CPF_CNPJ'})
        df_leads['CPF_CNPJ'] = df_leads['CPF_CNPJ'].astype(str)
        df_parcelamentos['CPF_CNPJ'] = df_parcelamentos['CPF_CNPJ'].astype(str)
        colunas_essenciais = ['CPF_CNPJ'] + COLUNAS_PARA_MANTER_FASE2
        df_parcelamentos_filtrado = df_parcelamentos[colunas_essenciais]
        
        print("Realizando o cruzamento das bases de dados pelo CNPJ...")
        df_com_parcelamento = pd.merge(df_leads, df_parcelamentos_filtrado, on='CPF_CNPJ', how='inner')
        cnpjs_com_parcelamento = df_com_parcelamento['CPF_CNPJ'].unique()
        df_sem_parcelamento = df_leads[~df_leads['CPF_CNPJ'].isin(cnpjs_com_parcelamento)]
        print("Cruzamento de dados concluído.")

        print(f"\nSalvando e fazendo upload do arquivo final: '{NOME_ARQUIVO_FINAL_COM_PARCELAMENTOS}'")
        print(f"-> Encontrados {len(df_com_parcelamento['CPF_CNPJ'].unique())} CNPJs únicos COM parcelamento, totalizando {len(df_com_parcelamento)} linhas de negociação.")
        print(f"-> Encontrados {len(df_sem_parcelamento)} CNPJs SEM parcelamento.")

        with pd.ExcelWriter(NOME_ARQUIVO_FINAL_COM_PARCELAMENTOS, engine='openpyxl') as writer:
            df_com_parcelamento.to_excel(writer, sheet_name='Com Parcelamento (Detalhado)', index=False)
            df_sem_parcelamento.to_excel(writer, sheet_name='Sem Parcelamento', index=False)
        
        print(f"Arquivo local '{NOME_ARQUIVO_FINAL_COM_PARCELAMENTOS}' salvo com sucesso.")
        # ALTERAÇÃO: Passando o ID da pasta de destino para a função de upload
        upload_para_drive_compartilhado(NOME_ARQUIVO_FINAL_COM_PARCELAMENTOS, NOME_ARQUIVO_FINAL_COM_PARCELAMENTOS, ID_PASTA_ALVO_NO_DRIVE, CREDS_FILE)
        
        print(f"\n--- AUTOMAÇÃO COMPLETA CONCLUÍDA! ---")

    except Exception as e:
        print(f"ERRO CRÍTICO NA FASE 2: O processo foi interrompido. Detalhe: {e}")
        exit()