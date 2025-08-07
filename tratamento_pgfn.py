import pandas as pd
import requests
import io
import zipfile

# --- ETAPA 0: CONFIGURAÇÃO ---
URL_DADOS_PGFN = "https://dadosabertos.pgfn.gov.br/2025_trimestre_02/Dados_abertos_Previdenciario.zip"
SUBPASTA_DENTRO_DO_ZIP = '' 
NOMES_ARQUIVOS_CSV = [
    'arquivo_lai_PREV_1_202506.csv',
    'arquivo_lai_PREV_2_202506.csv',
    'arquivo_lai_PREV_3_202506.csv',
    'arquivo_lai_PREV_4_202506.csv',
    'arquivo_lai_PREV_5_202506.csv',
    'arquivo_lai_PREV_6_202506.csv'
]
UF_DESEJADA = 'RS'
VALOR_MINIMO_DIVIDA = 100000
TERMOS_EXCLUIR = ['MUNICIPIO', 'MUNICÍPIO', 'CONTABILIDADE', 'CONTÁBIL', 'CONTABIL', 'CONTADOR', 'CONTADORA', 'CONTADORES', 'FALENCIA', 'FALÊNCIA', 'MASSA FALIDA', 'FALIDA', 'FALIDO', 'FILIAL', 'RECUPERACAO JUDICIAL', 'RECUPERAÇÃO JUDICIAL', 'EM LIQUIDACAO', 'EM LIQUIDAÇÃO']
COLUNAS_PARA_REMOVER = [
    'TIPO_PESSOA',
    'TIPO_DEVEDOR',
    'UNIDADE_RESPONSAVEL',
    'NUMERO_INSCRICAO',
    'TIPO_CREDITO',
    'DATA_INSCRICAO',
    'INDICADOR_AJUIZADO'
]

# --- SCRIPT PRINCIPAL ---
if __name__ == "__main__":
    print("--- INICIANDO SCRIPT DE GERAÇÃO DE RELATÓRIOS (DETALHADO E TOTALIZADO) ---")
    
    # --- ETAPA 1: EXTRAÇÃO E CONSOLIDAÇÃO DOS DADOS ---
    try:
        # ... (O código da Etapa 1 permanece o mesmo) ...
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
        # ... (O código da Etapa 2 permanece o mesmo) ...
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
        print(f"ERRO CRÍTICO na Etapa 2: Ocorreu um erro durante o tratamento dos dados. Verifique os nomes das colunas.")
        print(f"Detalhe do erro: {e}")
        exit()

    # --- ETAPA 3: GERAR E SALVAR RELATÓRIOS ---
    print(f"\n[ETAPA 3/3] Gerando e salvando os relatórios finais...")
    
    # 1. Salvar o relatório DETALHADO (igual ao anterior)
    try:
        nome_arquivo_detalhado = "relatorio_detalhado_previdenciario.xlsx"
        df_tratado.to_excel(nome_arquivo_detalhado, index=False)
        print(f"-> SUCESSO! Relatório detalhado salvo como '{nome_arquivo_detalhado}'")
    except Exception as e:
        print(f"-> ERRO ao salvar o relatório detalhado: {e}")

    # 2. Gerar e salvar o relatório TOTALIZADO
    try:
        print("Agrupando e somando dívidas por CNPJ...")
        
        # Agrupa pelo CNPJ e define quais operações fazer em cada coluna
        df_totalizado = df_tratado.groupby('CPF_CNPJ').agg(
            NOME_DEVEDOR=('NOME_DEVEDOR', 'first'), # Pega o primeiro nome (serão todos iguais)
            UF_DEVEDOR=('UF_DEVEDOR', 'first'),     # Pega a primeira UF (serão todas iguais)
            VALOR_TOTAL_DIVIDA=('VALOR_CONSOLIDADO', 'sum') # SOMA os valores
        ).reset_index() # Transforma o agrupamento de volta em um DataFrame padrão

        # Arredonda o valor total para 2 casas decimais
        df_totalizado['VALOR_TOTAL_DIVIDA'] = df_totalizado['VALOR_TOTAL_DIVIDA'].round(2)

        # Salva o novo DataFrame totalizado
        nome_arquivo_totalizado = "relatorio_total_previdenciario.xlsx"
        df_totalizado.to_excel(nome_arquivo_totalizado, index=False)
        print(f"-> SUCESSO! Relatório totalizado salvo como '{nome_arquivo_totalizado}'")
    except Exception as e:
        print(f"-> ERRO ao gerar ou salvar o relatório totalizado: {e}")
        
    print("\n--- PROCESSO CONCLUÍDO ---")