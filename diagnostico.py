import requests
import io
import zipfile

# URL do arquivo para diagnóstico
URL_DADOS_PGFN = "https://dadosabertos.pgfn.gov.br/2025_trimestre_01/Dados_abertos_Previdenciario.zip"

print(f"Baixando arquivo de: {URL_DADOS_PGFN}")
print("Analisando a estrutura do arquivo ZIP...")

try:
    response = requests.get(URL_DADOS_PGFN)
    response.raise_for_status()

    # Abre o ZIP em memória
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        # Pega a lista de TODOS os arquivos e pastas dentro do ZIP
        lista_de_arquivos = z.namelist()

    print("\n--- ESTRUTURA ENCONTRADA DENTRO DO ZIP ---")
    if not lista_de_arquivos:
        print("O arquivo ZIP está vazio ou corrompido.")
    else:
        for item in lista_de_arquivos:
            print(item)
    print("------------------------------------------")
    print("\nANÁLISE: Compare a saída acima com a lista de nomes no seu script. Se houver uma pasta, você precisará adicioná-la ao caminho.")

except Exception as e:
    print(f"\nERRO: Não foi possível analisar o arquivo. Detalhe: {e}")