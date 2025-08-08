import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
import time
import datetime

# --- ETAPA 0: CONFIGURAÇÃO ---
ARQUIVO_ENTRADA = "./relatorio_total_previdenciario.xlsx"
URL_PAINEL_PARCELAMENTOS = "https://dw.pgfn.fazenda.gov.br/dwsigpgfn/servlet/mstrWeb?evt=3140&documentID=6C86F338847E71AE8ACC292A5E08B3B43&..."

# --- SCRIPT PRINCIPAL ---
if __name__ == "__main__":
    print("--- INICIANDO CRUZAMENTO COM O PAINEL DE PARCELAMENTOS ---")
    
    # --- ETAPA 1: Carregar a lista de CNPJs ---
    try:
        df_principal = pd.read_excel(ARQUIVO_ENTRADA)
        print(f"Arquivo '{ARQUIVO_ENTRADA}' carregado. {len(df_principal)} CNPJs para consultar.")
    except FileNotFoundError:
        print(f"ERRO: O arquivo '{ARQUIVO_ENTRADA}' não foi encontrado.")
        exit()

    # --- ETAPA 2: Iniciar o Selenium ---
    print("Iniciando o navegador...")
    options = webdriver.ChromeOptions()
    options.add_argument("start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    driver = webdriver.Chrome(options=options)
    
    resultados_parcelamentos = []

    try:
        # --- ETAPA 3: Acessar o painel e aplicar filtros iniciais ---
        print(f"Acessando o painel de parcelamentos...")
        driver.get(URL_PAINEL_PARCELAMENTOS)
        
        print("Aguardando carregamento inicial do painel (pode levar um tempo)...")
        time.sleep(25) # Espera inicial mais longa para garantir que todos os scripts do painel carreguem
        
        wait = WebDriverWait(driver, 30)

        # 1. Selecionar o ESTADO (apenas uma vez)
        try:
            print("Aplicando filtro de Estado (UF)...")
            # ** VOCÊ PRECISA ENCONTRAR O SELETOR CORRETO E SUBSTITUIR ABAIXO **
            seletor_uf_dropdown = '//*[@id="mstr518"]'
            uf_dropdown_element = wait.until(EC.presence_of_element_located((By.XPATH, seletor_uf_dropdown)))
            select_uf = Select(uf_dropdown_element)
            select_uf.select_by_visible_text("RIO GRANDE DO SUL") # Ou select_by_value("RS")
            time.sleep(2) # Pausa após seleção para permitir atualização
        except Exception as e:
            print(f"ERRO: Não foi possível aplicar o filtro de Estado. Verifique o seletor. Erro: {e}")
            raise # Levanta o erro para parar a execução, pois o filtro é essencial

        # 2. Selecionar o ANO (apenas uma vez)
        try:
            print("Aplicando filtro de Ano...")
            ano_atual = str(datetime.datetime.now().year)
            # ** VOCÊ PRECISA ENCONTRAR O SELETOR CORRETO E SUBSTITUIR ABAIXO **
            seletor_ano_dropdown = '//*[@id="mstr504"]'
            ano_dropdown_element = wait.until(EC.presence_of_element_located((By.XPATH, seletor_ano_dropdown)))
            select_ano = Select(ano_dropdown_element)
            select_ano.select_by_value(ano_atual) # Seleciona o ano atual
            print(f"Filtros de Estado e Ano ({ano_atual}) aplicados com sucesso.")
            time.sleep(5) # Pausa maior após os filtros iniciais
        except Exception as e:
            print(f"ERRO: Não foi possível aplicar o filtro de Ano. Verifique o seletor. Erro: {e}")
            raise

        # --- ETAPA 4: Iterar e consultar cada CNPJ ---
        for index, row in df_principal.iterrows():
            cnpj = row['CPF_CNPJ']
            # Remove formatação do CNPJ para inserção no campo, se necessário
            cnpj_sem_formatacao = cnpj.replace('.', '').replace('/', '').replace('-', '')
            print(f"\nConsultando CNPJ {index + 1}/{len(df_principal)}: {cnpj}")

            try:
                # 1. Encontrar e preencher o campo do CNPJ
                # ** VOCÊ PRECISA ENCONTRAR O SELETOR CORRETO E SUBSTITUIR ABAIXO **
                seletor_campo_cnpj = '//*[@id="mstr560"]/div[1]/div'
                campo_cnpj = wait.until(EC.element_to_be_clickable((By.XPATH, seletor_campo_cnpj)))
                
                campo_cnpj.clear()
                campo_cnpj.send_keys(cnpj_sem_formatacao)
                
                # 2. ESPERA INTELIGENTE: Aguarda a atualização automática
                print("  Aguardando atualização em tempo real...")
                # ** VOCÊ PRECISA ENCONTRAR O SELETOR DO INDICADOR DE CARREGAMENTO E SUBSTITUIR ABAIXO **
                seletor_do_spinner = '/*[@id="waitBox"]/div[1]/div[3]'
                # Espera o spinner desaparecer (se torna invisível)
                wait.until(EC.invisibility_of_element_located((By.XPATH, seletor_do_spinner)))
                time.sleep(0.5) # Pequena pausa para garantir a renderização final

                # 3. Extrair os dados do resultado
                # ** VOCÊ PRECISA ENCONTRAR O SELETOR DA ÁREA DE RESULTADO E SUBSTITUIR ABAIXO **
                seletor_resultado = '//*[@id="mstr492"]/div[1]/div[1]'
                area_resultado = wait.until(EC.presence_of_element_located((By.XPATH, seletor_resultado)))
                
                texto_resultado = area_resultado.text.upper()
                if "ATIVO" in texto_resultado:
                    status_parcelamento = "Possui Parcelamento Ativo"
                elif "NÃO HÁ DADOS" in texto_resultado or "NENHUM DADO" in texto_resultado:
                    status_parcelamento = "Sem Parcelamento"
                else:
                    # Se não encontrar nenhuma das mensagens, pode extrair o texto para análise posterior
                    status_parcelamento = f"Status Desconhecido ({texto_resultado[:50]})"
                
                print(f"  -> Status: {status_parcelamento}")
                resultados_parcelamentos.append({'CPF_CNPJ': cnpj, 'STATUS_PARCELAMENTO': status_parcelamento})

            except Exception as e_inner:
                print(f"  -> ERRO ao consultar o CNPJ {cnpj}: {e_inner}")
                resultados_parcelamentos.append({'CPF_CNPJ': cnpj, 'STATUS_PARCELAMENTO': 'Erro na Consulta'})

    finally:
        print("\nFechando o navegador...")
        driver.quit()

    # --- ETAPA 5: Unir os dados e salvar o relatório final ---
    print("\nUnindo dados de parcelamento com a planilha original...")
    df_resultados = pd.DataFrame(resultados_parcelamentos)
    df_final = pd.merge(df_principal, df_resultados, on='CPF_CNPJ', how='left')
    nome_arquivo_final = "relatorio_final_com_parcelamentos.xlsx"
    df_final.to_excel(nome_arquivo_final, index=False)

    print(f"\n--- PROCESSO CONCLUÍDO! ---")
    print(f"Relatório final salvo como '{nome_arquivo_final}'")