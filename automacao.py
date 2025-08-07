import pandas as pd
import io
import requests
import zipfile

# --- Etapa 1: Extração de Dados (A Nova Forma) ---

# 1. Obtenha o link direto do arquivo CSV que você quer baixar.
#    Para obter o link, vá na página de Recursos, clique no botão "Ir para o recurso" 
#    e copie o URL que aparecer.
#
# Exemplo de URL (este é um exemplo, verifique o link mais recente no portal):
url_dados_pj = "https://dados.gov.br/dados/recursos/a7125330-496a-48cf-9c4c-ac9c716183c5/situacao_devedores_pj.csv"

print(f"Baixando dados de: {url_dados_pj}")

try:
    # 2. Use o Pandas para ler o CSV diretamente da URL para um DataFrame.
    #    Isso é extremamente eficiente, pois não salva o arquivo no disco primeiro.
    #    O separador (sep) e a codificação (encoding) podem precisar de ajuste.
    #    'latin-1' e 'utf-8' são codificações comuns para dados do governo brasileiro.
    df = pd.read_csv(url_dados_pj, sep=';', encoding='latin-1', on_bad_lines='warn', low_memory=False)

    print("Download e carregamento dos dados concluídos com sucesso!")
    print("Amostra dos dados carregados:")
    print(df.head())

    # --- Etapa 2: Tratamento de Dados (Seu processo continua daqui) ---

    # A lógica de tratamento que você já planejou pode ser aplicada diretamente no DataFrame 'df'.
    # Exemplo: Remover colunas
    # colunas_para_remover = ['Coluna Inútil 1', 'Coluna Inútil 2'] 
    # df_tratado = df.drop(columns=colunas_para_remover)
    # print("\nColunas removidas.")

    # Exemplo: Filtrar por UF (se a coluna UF existir)
    # df_filtrado_rs = df_tratado[df_tratado['UF'] == 'RS']
    # print(f"Encontrados {len(df_filtrado_rs)} registros para o RS.")


except Exception as e:
    print(f"Ocorreu um erro: {e}")
    print("Verifique se o link está correto e se o separador e a codificação estão certos.")