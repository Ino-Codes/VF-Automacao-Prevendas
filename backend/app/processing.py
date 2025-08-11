import pandas as pd
import requests
import io
import zipfile
import warnings

# Ignora avisos de estilo do openpyxl
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# --- CONSTANTES DE CONFIGURAÇÃO ---
# OBS: Em uma aplicação real, considere tornar a URL e os nomes dos arquivos dinâmicos ou configuráveis.
URL_DADOS_PGFN = "https://dadosabertos.pgfn.gov.br/2025_trimestre_02/Dados_abertos_Previdenciario.zip"
SUBPASTA_DENTRO_DO_ZIP = '' 
NOMES_ARQUIVOS_CSV = ['arquivo_lai_PREV_1_202506.csv', 'arquivo_lai_PREV_2_202506.csv', 'arquivo_lai_PREV_3_202506.csv', 'arquivo_lai_PREV_4_202506.csv', 'arquivo_lai_PREV_5_202506.csv', 'arquivo_lai_PREV_6_202506.csv']
UF_DESEJADA = 'RS'
TERMOS_EXCLUIR = ['MUNICIPIO', 'MUNICÍPIO', 'CONTABILIDADE', 'CONTÁBIL', 'CONTABIL', 'CONTADOR', 'CONTADORA', 'CONTADORES', 'FALENCIA', 'FALÊNCIA', 'MASSA FALIDA', 'FALIDA', 'FALIDO', 'FILIAL', 'RECUPERACAO JUDICIAL', 'RECUPERAÇÃO JUDICIAL', 'EM LIQUIDACAO', 'EM LIQUIDAÇÃO']
COLUNAS_PARA_REMOVER_FASE1 = ['TIPO_PESSOA', 'TIPO_DEVEDOR', 'UNIDADE_RESPONSAVEL', 'NUMERO_INSCRICAO', 'TIPO_CREDITO', 'DATA_INSCRICAO', 'INDICADOR_AJUIZADO']
COLUNAS_PARA_MANTER_FASE2 = ["Tipo de Negociação", "Modalidade da Negociação", "Situação da Negociação", "Qtde de Parcelas Concedidas", "Qtde de Parcelas em Atraso", "Valor Consolidado", "Valor do Principal", "Valor da Multa", "Valor dos Juros", "Valor do Encargo Legal"]
NOME_DA_COLUNA_CNPJ_NO_ARQUIVO = "CPF/CNPJ do Optante"

def processar_dados(valor_minimo: float, arquivo_parcelamento_bytes: bytes):
    """
    Função principal que executa toda a lógica de download, filtragem e cruzamento de dados.
    Recebe o valor mínimo e os bytes do arquivo de parcelamento enviado pelo usuário.
    """
    try:
        # =================================================================================
        # FASE 1: GERAÇÃO DA LISTA DE LEADS A PARTIR DOS DADOS BRUTOS DA PGFN
        # =================================================================================
        print("[FASE 1] Baixando e consolidando dados da PGFN...")
        response = requests.get(URL_DADOS_PGFN, stream=True)
        response.raise_for_status()
        lista_de_dataframes = []
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            # Pega apenas os nomes de arquivo que existem no ZIP
            nomes_validos = [nome for nome in NOMES_ARQUIVOS_CSV if nome in z.namelist()]
            for nome_arquivo in nomes_validos:
                with z.open(nome_arquivo) as f:
                    df_temp = pd.read_csv(f, sep=';', encoding='latin-1', low_memory=False, dtype={'CPF_CNPJ': str}, on_bad_lines='warn')
                    lista_de_dataframes.append(df_temp)

        if not lista_de_dataframes:
            raise ValueError("Nenhum dos arquivos CSV especificados foi encontrado no ZIP da PGFN.")

        df = pd.concat(lista_de_dataframes, ignore_index=True)
        del lista_de_dataframes
        print(f"[FASE 1] Dados carregados. Total de {len(df)} registros.")

        print("[FASE 1] Iniciando tratamento e filtragem dos dados...")
        df = df[df['CPF_CNPJ'].str.contains('/0001-', na=False)]
        df = df[df['UF_DEVEDOR'] == UF_DESEJADA]
        mascara_exclusao = df['NOME_DEVEDOR'].str.contains('|'.join(TERMOS_EXCLUIR), case=False, na=False)
        df = df[~mascara_exclusao]
        df = df[df['VALOR_CONSOLIDADO'] > valor_minimo]
        df_tratado = df.drop(columns=COLUNAS_PARA_REMOVER_FASE1, errors='ignore')
        del df 

        df_totalizado = df_tratado.groupby('CPF_CNPJ').agg(NOME_DEVEDOR=('NOME_DEVEDOR', 'first'), UF_DEVEDOR=('UF_DEVEDOR', 'first'), VALOR_TOTAL_DIVIDA=('VALOR_CONSOLIDADO', 'sum')).reset_index()
        df_totalizado['VALOR_TOTAL_DIVIDA'] = df_totalizado['VALOR_TOTAL_DIVIDA'].round(2)
        print(f"[FASE 1] Concluída. {len(df_totalizado)} leads gerados.")
        
        # =================================================================================
        # FASE 2: CRUZAMENTO DA LISTA DE LEADS COM OS DADOS DE PARCELAMENTO
        # =================================================================================
        print("[FASE 2] Iniciando cruzamento com dados de parcelamento...")
        df_leads = df_totalizado
        
        # Lê o arquivo de parcelamento que veio da requisição web (em bytes)
        df_parcelamentos = pd.read_excel(io.BytesIO(arquivo_parcelamento_bytes), header=2)
        print(f"[FASE 2] Base de parcelamentos carregada com {len(df_parcelamentos)} registros.")
        
        print("[FASE 2] Preparando dados para o cruzamento...")
        df_parcelamentos = df_parcelamentos.rename(columns={NOME_DA_COLUNA_CNPJ_NO_ARQUIVO: 'CPF_CNPJ'})
        df_leads['CPF_CNPJ'] = df_leads['CPF_CNPJ'].astype(str)
        df_parcelamentos['CPF_CNPJ'] = df_parcelamentos['CPF_CNPJ'].astype(str)
        
        colunas_essenciais = ['CPF_CNPJ'] + COLUNAS_PARA_MANTER_FASE2
        df_parcelamentos_filtrado = df_parcelamentos[colunas_essenciais]
        
        print("[FASE 2] Realizando o cruzamento das bases de dados pelo CNPJ...")
        df_com_parcelamento = pd.merge(df_leads, df_parcelamentos_filtrado, on='CPF_CNPJ', how='inner')
        cnpjs_com_parcelamento = df_com_parcelamento['CPF_CNPJ'].unique()
        df_sem_parcelamento = df_leads[~df_leads['CPF_CNPJ'].isin(cnpjs_com_parcelamento)]
        print("[FASE 2] Cruzamento concluído.")

        # Converte os dataframes para listas de dicionários para serem enviados como JSON
        resultado_com_parcelamento = df_com_parcelamento.to_dict(orient='records')
        resultado_sem_parcelamento = df_sem_parcelamento.to_dict(orient='records')

        return {
            "com_parcelamento": resultado_com_parcelamento,
            "sem_parcelamento": resultado_sem_parcelamento
        }

    except Exception as e:
        print(f"ERRO CRÍTICO no processamento: {e}")
        # Propaga o erro para que a API possa tratá-lo
        raise