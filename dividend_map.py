import pandas as pd
import yfinance as yf
from unidecode import unidecode
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

# Caminho do arquivo lista.txt
file_path = "lista.txt"

# Leitura da lista de ativos
try:
    with open(file_path, 'r', encoding='utf-8') as file:
        ativos = [line.strip() for line in file if line.strip()]
    print(f"Lista de ativos carregada com sucesso: {ativos}")
except FileNotFoundError:
    print(f"Arquivo {file_path} não encontrado.")
    exit()
except Exception as e:
    print(f"Erro ao ler o arquivo de lista: {e}")
    exit()

# Função para buscar os dividendos nos últimos 12 meses
def obter_dividendos(ativo):
    try:
        print(f"Processando ativo: {ativo}")
        ticker = yf.Ticker(f"{ativo}")
        historico = ticker.dividends
        
        if historico.empty:
            print(f"{ativo}: sem histórico de dividendos.")
            return None
        
        # Filtrar os últimos 12 meses (removendo o fuso horário)
        data_atual = datetime.now()
        data_inicio = data_atual - timedelta(days=365)
        historico.index = historico.index.tz_localize(None)
        historico = historico[historico.index >= pd.to_datetime(data_inicio)]
        
        if historico.empty:
            print(f"{ativo}: sem dividendos nos últimos 12 meses.")
            return None
        
        # Inicializar o dicionário para armazenar os dividendos (YYYY-MM)
        dividendos_mensais = {}

        for data, valor in historico.items():
            ano_mes = data.strftime("%Y-%m")
            
            if ano_mes not in dividendos_mensais:
                dividendos_mensais[ano_mes] = 0
            dividendos_mensais[ano_mes] += valor
        
        return pd.Series(dividendos_mensais, name=unidecode(ativo).upper())
    except Exception as e:
        print(f"Erro ao buscar dados para {ativo}: {e}")
        return None

# Função para executar a busca em paralelo
def processar_ativos(ativos):
    linhas = []
    with ThreadPoolExecutor() as executor:
        resultados = list(tqdm(executor.map(obter_dividendos, ativos), total=len(ativos), desc="Processando FIIs"))
        for dividendos in resultados:
            if dividendos is not None:
                linhas.append(dividendos)
    return linhas

# Executar o processamento em paralelo para os FIIs
linhas = processar_ativos(ativos)

# Verificar se há dados para concatenar
if linhas:
    dividendos_map = pd.concat(linhas, axis=1).T
    dividendos_map.index = [unidecode(idx).upper() for idx in dividendos_map.index]
    
    # Adicionar a coluna 'ativo' como a primeira coluna
    dividendos_map.reset_index(inplace=True)
    dividendos_map.rename(columns={'index': 'ativo'}, inplace=True)

    # Ordenar as colunas de forma cronológica
    colunas_ordem = ['ativo'] + sorted([col for col in dividendos_map.columns if col != 'ativo'])
    dividendos_map = dividendos_map[colunas_ordem]

    # Exibir o resultado final
    print("Mapa de Dividendos dos Últimos 12 Meses (YYYY-MM) - Ordenado:")
    print(dividendos_map)
    
    # Salvar o resultado em um JSON
    try:
        dividendos_map.to_json("mapa_dividendos_12meses.json", orient="records", force_ascii=False)
        print("Arquivo 'mapa_dividendos_12meses.json' salvo com sucesso.")
    except Exception as e:
        print(f"Erro ao salvar o arquivo: {e}")
else:
    print("Nenhum dado de dividendos encontrado para os FIIs listados.")
