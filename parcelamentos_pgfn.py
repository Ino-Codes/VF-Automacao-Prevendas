import pandas as pd
import glob
import warnings

# Ignora o aviso de estilo dos arquivos Excel da PGFN
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# --- ETAPA 0: CONFIGURAÇÃO ---

ARQUIVO_LEADS = "relatorio_total_previdenciario.xlsx"
PADRAO_ARQUIVOS_PARCELAMENTO = "painel_parcelamento_*.xlsx"
ARQUIVO_FINAL = "relatorio_final_com_negociacoes_consolidadas.xlsx"

# Lista com as colunas que desejamos consolidar
COLUNAS_PARA_CONSOLIDAR = [
    "Mês/Ano do Requerimento da Negociação",
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

    # --- ETAPA 1 e 2 (SEM ALTERAÇÕES) ---
    try:
        print(f"Lendo o arquivo de leads: {ARQUIVO_LEADS}")
        df_leads = pd.read_excel(ARQUIVO_LEADS)
        print(f"Arquivo de leads carregado com {len(df_leads)} empresas.")

        print(f"Procurando por arquivos de parcelamento com o padrão: '{PADRAO_ARQUIVOS_PARCELAMENTO}'")
        lista_nomes_arquivos = glob.glob(PADRAO_ARQUIVOS_PARCELAMENTO)
        
        if not lista_nomes_arquivos:
            raise FileNotFoundError(f"Nenhum arquivo de parcelamento encontrado com o padrão '{PADRAO_ARQUIVOS_PARCELAMENTO}'.")
            
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
        print(f"\nERRO ao ler os arquivos: {e}")
        exit()

    # --- ETAPA 3: Preparar e agregar dados de parcelamento (LÓGICA CORRIGIDA) ---
    try:
        print("Preparando e agregando dados de parcelamento por CNPJ...")
        NOME_DA_COLUNA_CNPJ_NO_ARQUIVO = "CPF/CNPJ do Optante"
        df_parcelamentos = df_parcelamentos.rename(columns={NOME_DA_COLUNA_CNPJ_NO_ARQUIVO: 'CPF_CNPJ'})
        
        df_leads['CPF_CNPJ'] = df_leads['CPF_CNPJ'].astype(str)
        df_parcelamentos['CPF_CNPJ'] = df_parcelamentos['CPF_CNPJ'].astype(str)
        
        # Passo 1: Criar o dicionário de agregação apenas para as colunas de texto
        agg_funcs_texto = {col: lambda x: ' | '.join(x.astype(str).unique()) for col in COLUNAS_PARA_CONSOLIDAR}
        
        # Passo 2: Executar a agregação do texto
        df_agregado_texto = df_parcelamentos.groupby('CPF_CNPJ').agg(agg_funcs_texto).reset_index()
        
        # Passo 3: Calcular a contagem de negociações separadamente
        df_contagem = df_parcelamentos.groupby('CPF_CNPJ').size().reset_index(name='Qtde. Negociações')
        
        # Passo 4: Juntar os dois resultados (texto agregado e contagem)
        df_parcelamentos_agregado = pd.merge(df_agregado_texto, df_contagem, on='CPF_CNPJ')
        
        print("Dados de parcelamento agregados com sucesso.")

    except Exception as e:
        print(f"\nERRO na etapa de preparação dos dados: {e}")
        exit()

    # --- ETAPA 4: Cruzar as duas bases de dados ---
    print("Realizando o cruzamento das bases de dados pelo CNPJ...")
    df_final = pd.merge(df_leads, df_parcelamentos_agregado, on='CPF_CNPJ', how='left')
    
    # Adiciona a coluna "POSSUI_PARCELAMENTO"
    df_final['POSSUI_PARCELAMENTO'] = df_final['Qtde. Negociações'].notna().map({True: 'Sim', False: 'Não'})
    
    print("Cruzamento de dados concluído.")

    # --- ETAPA 5: Salvar o relatório final enriquecido ---
    try:
        df_final.to_excel(ARQUIVO_FINAL, index=False)
        print(f"\n--- PROCESSO CONCLUÍDO! ---")
        print(f"Relatório final salvo como '{ARQUIVO_FINAL}'")
        print("\nResumo do Resultado:")
        print(df_final['POSSUI_PARCELAMENTO'].value_counts())
    except Exception as e:
        print(f"ERRO ao salvar o arquivo final: {e}")