import requests
import time
import json

CHAVE_API = "NDYzMjAyNS0wOC0wNyAxODoxODo0NmF1bmZocXM4"

CNPJS_PARA_TESTE = [
    "93011211000100",
    "33100920000130",
    "87711453000104"
]

URL_GERAR_TOKEN = "https://paineljob.com.br/api/gerar_token.php"
URL_ENVIAR_LOTE_PJ = "https://paineljob.com.br/api/V2/api_dados_bulk.php"
URL_CAPTURAR_RESULTADOS_PJ = "https://paineljob.com.br/api/V2/api_dados_bulk_out_pj.php"

if __name__ == "__main__":

    print("--- ETAPA 1 de 3: Gerando token de autenticação...")
    try:
        headers_token = {'Authorization': f'Bearer {CHAVE_API}'}
        
        response_token = requests.get(URL_GERAR_TOKEN, headers=headers_token)
        
        response_token.raise_for_status()

        token_gerado = response_token.json().get('token')
        if not token_gerado:
            raise ValueError("A resposta da API de token não continha um 'token'.")
        
        print("-> SUCESSO! Token de autenticação gerado.")
    except Exception as e:
        print(f"ERRO na Etapa 1: Não foi possível gerar o token.")
        print(f"Detalhe: {e}")
        print(f"Resposta da API: {response_token.text if 'response_token' in locals() else 'N/A'}")
        exit()

    print("\n--- ETAPA 2 de 3: Enviando lote de CNPJs para consulta...")
    try:
        payload_envio = {
            "bulk": [
                {"dado": cnpj, "tipo": "PJ"} for cnpj in CNPJS_PARA_TESTE
            ]
        }

        headers_envio = {
            'Authorization': f'Bearer {token_gerado}',
            'Content-Type': 'application/json'
        }

        response_envio = requests.post(URL_ENVIAR_LOTE_PJ, headers=headers_envio, json=payload_envio)
        response_envio.raise_for_status()

        id_lote = response_envio.json().get('RETORNO', {}).get('id_lote_consulta')
        if not id_lote:
             raise ValueError("A resposta da API de envio não continha um 'id_lote_consulta'.")

        print(f"-> SUCESSO! Lote enviado. ID da consulta: {id_lote}")

    except Exception as e:
        print(f"ERRO na Etapa 2: Não foi possível enviar o lote de CNPJs.")
        print(f"Detalhe: {e}")
        print(f"Resposta da API: {response_envio.text if 'response_envio' in locals() else 'N/A'}")
        exit()

    print("\n--- ETAPA 3 de 3: Capturando os resultados...")
    print("Aguardando 15 segundos para o processamento do lote...")
    time.sleep(15)

    try:
        payload_captura = {'id_lote_consulta': id_lote}
        
        headers_captura = {
            'Authorization': f'Bearer {token_gerado}',
            'Content-Type': 'application/json'
        }

        response_captura = requests.post(URL_CAPTURAR_RESULTADOS_PJ, headers=headers_captura, json=payload_captura)
        response_captura.raise_for_status()
        
        resultados = response_captura.json()
        
        print("-> SUCESSO! Resultados da consulta capturados.")
        print("\n--- RESULTADO OBTIDO (JSON) ---")
        print(json.dumps(resultados, indent=4, ensure_ascii=False))

    except Exception as e:
        print(f"ERRO na Etapa 3: Não foi possível capturar os resultados.")
        print(f"Detalhe: {e}")
        print(f"Resposta da API: {response_captura.text if 'response_captura' in locals() else 'N/A'}")