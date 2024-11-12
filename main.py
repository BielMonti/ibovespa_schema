import yfinance as yf
import pandas as pd
import psycopg2
from datetime import datetime

# Configurações de conexão com o PostgreSQL
conn = psycopg2.connect(
    dbname="ibovespa",
    user="admin",
    password="admin",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# Função para inserir dados de uma empresa na tabela de dimensão
def inserir_empresa(ticker, nome):
    cursor.execute(
        "INSERT INTO dim_empresa (ticker, nome_empresa) VALUES (%s, %s) ON CONFLICT (ticker) DO NOTHING",
        (ticker, nome)
    )
    conn.commit()

# Função para popular a tabela de calendário
def popular_calendario(start_year=2022, end_year=2024):
    dates = pd.date_range(start=f"{start_year}-01-01", end=f"{end_year}-12-31")
    for date in dates:
        ano = date.year
        mes = date.month
        trimestre = (date.month - 1) // 3 + 1
        cursor.execute(
            "INSERT INTO dim_calendario (data, ano, mes, trimestre) VALUES (%s, %s, %s, %s) ON CONFLICT (data) DO NOTHING",
            (date, ano, mes, trimestre)
        )
    conn.commit()

# Função para carregar dados das ações
def carregar_dados_acoes(ticker):
    data = yf.download(ticker, start="2022-01-01", end="2024-11-09")
    data.reset_index(inplace=True)
    
    cursor.execute("SELECT id_empresa FROM dim_empresa WHERE ticker = %s", (ticker,))
    id_empresa = cursor.fetchone()[0]
    
    for _, row in data.iterrows():
        cursor.execute(
            "SELECT id_data FROM dim_calendario WHERE data = %s", (row['Date'],)
        )
        id_data = cursor.fetchone()[0]
        
        cursor.execute(
            """
            INSERT INTO fato_acoes (
                id_empresa, id_data, preco_abertura, preco_fechamento, maxima, minima, volume, preco_ajustado
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                id_empresa, id_data, row['Open'], row['Close'], row['High'],
                row['Low'], row['Volume'], row['Adj Close']
            )
        )
    conn.commit()

# Função para calcular indicadores fundamentalistas e inserir na tabela fato_indicadores
def calcular_indicadores(ticker):
    cursor.execute("SELECT id_empresa FROM dim_empresa WHERE ticker = %s", (ticker,))
    id_empresa = cursor.fetchone()[0]

# Coletar dados históricos trimestrais para calcular os indicadores
    data = yf.Ticker(ticker)
    historico = data.quarterly_financials.transpose()  # Obtém os dados trimestrais

    # Iterar pelos anos e trimestres que precisamos (2022 a 2024, 1º a 4º trimestre)
    for ano in range(2022, 2025):
        for trimestre in range(1, 5):
            # Verificar se o ano e trimestre existem no histórico para calcular indicadores
            trimestre_data = historico.loc[
                (historico.index.year == ano) & ((historico.index.month - 1) // 3 + 1 == trimestre)
            ]

            if not trimestre_data.empty:
                # Extrair dados trimestrais se disponíveis
                pe_ratio = data.info.get("trailingPE") #(Preço sobre Lucro)
                pb_ratio = data.info.get("priceToBook") #(Preço sobre Valor Patrimonial)
                gross_margin = data.info.get("grossMargins") * 100 if data.info.get("grossMargins") else None #(Margem Bruta)
                profit_margin = data.info.get("profitMargins") * 100 if data.info.get("profitMargins") else None #(Margem de Lucro)
                roe = data.info.get("returnOnEquity") * 100 if data.info.get("returnOnEquity") else None #(Retorno sobre o Patrimônio Líquido)
                eps = data.info.get("trailingEps") #(Lucro por Ação)

                # Assegure-se de que os valores sejam do tipo 'float' ou 'None'
                pe_ratio = float(pe_ratio) if pe_ratio is not None else None
                pb_ratio = float(pb_ratio) if pb_ratio is not None else None
                gross_margin = float(gross_margin) if gross_margin is not None else None
                profit_margin = float(profit_margin) if profit_margin is not None else None
                roe = float(roe) if roe is not None else None
                eps = float(eps) if eps is not None else None
        
                # Obter o id_data para o ano e trimestre correspondente
                cursor.execute(
                    "SELECT id_data FROM dim_calendario WHERE ano = %s AND trimestre = %s",
                    (ano, trimestre)
                )
                id_data_result = cursor.fetchone()

                # Se id_data for encontrado, prossiga com a inserção
                if id_data_result:
                    id_data = id_data_result[0]

                    # Inserir os indicadores calculados na tabela fato_indicadores com granularidade trimestral
                    cursor.execute(
                        """
                        INSERT INTO fato_indicadores (id_empresa, id_data, pe_ratio, pb_ratio, gross_margin, profit_margin, roe, eps)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id_empresa, id_data) DO NOTHING
                        """,
                        (id_empresa, id_data, pe_ratio, pb_ratio, gross_margin, profit_margin, roe, eps)
                    )

    # Confirmar transação após a inserção dos dados                
    conn.commit()

# Processo de ETL
empresas = [
    {"ticker": "PETR4.SA", "nome": "Petrobras"},
    {"ticker": "VALE3.SA", "nome": "Vale"},
    {"ticker": "ITUB4.SA", "nome": "Itaú Unibanco"},
    {"ticker": "BBDC4.SA", "nome": "Bradesco"},
    {"ticker": "ABEV3.SA", "nome": "Ambev"},
    {"ticker": "BBAS3.SA", "nome": "Banco do Brasil"},
    {"ticker": "ELET3.SA", "nome": "Eletrobras"},
    {"ticker": "WEGE3.SA", "nome": "Weg"},
    {"ticker": "BRKM5.SA", "nome": "Braskem"}
]


# Passo 1: Inserir dados das empresas e preencher a tabela calendário
for empresa in empresas:
    inserir_empresa(empresa["ticker"], empresa["nome"])

popular_calendario()

# Passo 2: Carregar dados das ações e calcular indicadores
for empresa in empresas:
    carregar_dados_acoes(empresa["ticker"])
    calcular_indicadores(empresa["ticker"])

# Feche a conexão
cursor.close()
conn.close()
