import streamlit as st
import pandas as pd
import plotly.express as px
import sqlite3
from datetime import datetime, date, timedelta
import calendar
from streamlit_autorefresh import st_autorefresh

# Configuration
DB_PATH = "database\\vwsomelier.db"

def get_db_connection():
    """Return a connection to the SQLite database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        return conn
    except sqlite3.Error as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        return None

def carregar_dados():
    """Load data directly from the SQLite database."""
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        df = pd.read_sql(f"SELECT DESCRICAO_1, CODPROD, DATA, QT, PVENDA, VLCUSTOFIN,CONDVENDA,NUMPED,CODOPER, DTCANCEL FROM vwsomelier", conn)
        if df.empty:
            st.warning("Nenhum dado encontrado no banco de dados.")
            return pd.DataFrame()

        # Process data
        df['DESCRICAO_1'] = df['DESCRICAO_1'].fillna('').astype(str).str.strip()
        df['CÓDIGO PRODUTO'] = df['CODPROD'].fillna('').astype(str).str.strip()
        df['Data do Pedido'] = pd.to_datetime(df['DATA'], errors='coerce')

        if df['Data do Pedido'].isnull().any():
            st.warning("Existem valores inválidos ou ausentes na coluna 'DATA'.")
            invalid_dates = df[df['Data do Pedido'].isnull()]['DATA'].unique()
            st.write("Valores inválidos encontrados em 'DATA':", invalid_dates)
            df = df.dropna(subset=['Data do Pedido'])

        df['VALOR TOTAL VENDIDO'] = df['PVENDA']
        df['Margem de Lucro'] = (df['PVENDA'] - df['VLCUSTOFIN'])
        df['Ano'] = df['Data do Pedido'].dt.year
        df['Mês'] = df['Data do Pedido'].dt.month

        return df
    except sqlite3.Error as e:
        st.error(f"Erro ao carregar dados do banco de dados: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def exibir_tabela(df_filtrado):
    """Display a summary table of sales data."""
    if df_filtrado.empty:
        st.warning("Nenhum dado disponível para exibir na tabela.")
        return

    df_resumo = df_filtrado.groupby(['CÓDIGO PRODUTO', 'DESCRICAO_1']).agg(
        Total_Vendido=('QT', 'sum'),
        Valor_Total_Vendido=('VALOR TOTAL VENDIDO', 'sum')
    ).reset_index()

    df_resumo.rename(columns={
        'Valor_Total_Vendido': 'VALOR TOTAL VENDIDO',
        'Total_Vendido': 'QUANTIDADE'
    }, inplace=True)

    df_resumo['VALOR TOTAL VENDIDO'] = df_resumo['VALOR TOTAL VENDIDO'].apply(lambda x: f"R$ {x:,.2f}".replace(',', '.'))
    df_resumo['QUANTIDADE'] = df_resumo['QUANTIDADE'].apply(lambda x: f"{x:,.0f}".replace(',', '.'))
    
    st.dataframe(df_resumo, use_container_width=True)

def exibir_grafico_top_produtos(df, periodo_inicial, periodo_final):
    """Display a bar chart of top products by sales value."""
    if df.empty:
        st.warning("Nenhum dado disponível para o gráfico de top produtos.")
        return

    periodo_inicial = pd.to_datetime(periodo_inicial)
    periodo_final = pd.to_datetime(periodo_final)
    df_mes = df.dropna(subset=['Data do Pedido'])
    df_mes = df_mes[(df_mes['Data do Pedido'] >= periodo_inicial) & (df_mes['Data do Pedido'] <= periodo_final)]
    
    if df_mes.empty:
        st.warning("Nenhum dado disponível para o período selecionado.")
        return

    top_produtos = df_mes.groupby('DESCRICAO_1').agg(
        Total_Vendido=('QT', 'sum'),
        Valor_Total_Vendido=('VALOR TOTAL VENDIDO', 'sum')  
    ).reset_index()

    top_produtos = top_produtos.sort_values(by='Valor_Total_Vendido', ascending=False).head(20)
    top_produtos['Valor_Total_Vendido'] = top_produtos['Valor_Total_Vendido'].astype(float)

    fig = px.bar(top_produtos, x='DESCRICAO_1', y='Valor_Total_Vendido',
                 title='Top 20 Produtos Mais Vendidos',
                 labels={'DESCRICAO_1': 'Produto', 'Valor_Total_Vendido': 'Valor Total Vendido (R$)'},
                 color='Valor_Total_Vendido', color_continuous_scale='RdYlGn',
                 hover_data={'DESCRICAO_1': False, 'Valor_Total_Vendido': True, 'Total_Vendido': True})

    fig.update_traces(texttemplate="R$ %{y:,.2f}", textposition="outside", textfont_size=12)
    fig.update_layout(title_font_size=20, xaxis_title_font_size=13, yaxis_title_font_size=13,
                      xaxis_tickfont_size=10, yaxis_tickfont_size=12, xaxis_tickangle=-45)

    st.plotly_chart(fig, key=f"top_produtos_{periodo_inicial}_{periodo_final}")


def main():
    st.title("🍾 Desempenho de Vendas por Produto")

    # Adicionar auto-refresh a cada 3 minutos (180 segundos)
    refresh_interval = 180
    st_autorefresh(interval=refresh_interval * 1000, key="data_refresh")

    # Exibir a próxima atualização
    next_refresh = datetime.now() + timedelta(seconds=refresh_interval)
    st.write(f"Próxima atualização em: {next_refresh.strftime('%H:%M:%S')}")


    df = carregar_dados()

    if df.empty:
        st.error("Nenhum dado carregado. Verifique se o banco de dados contém dados.")
        return

    hoje = datetime.now()
    primeiro_dia_mes = hoje.replace(day=1)
    ultimo_dia_mes = hoje.replace(day=calendar.monthrange(hoje.year, hoje.month)[1])

    st.markdown("""<style> .stTextInput>div>div>input { border: 2px solid #4CAF50; border-radius: 10px; padding: 10px; font-size: 16px; background-color: #1a1a1a; } </style>""", unsafe_allow_html=True)
    produto_pesquisa = st.text_input('🔍 Pesquise por um produto ou código', '', key='search_input')

    if 'Data do Pedido' in df.columns:
        with st.container():
            st.subheader("Tabela de Resumo")
            col1, col2 = st.columns(2)
            with col1:
                periodo_inicio_tabela = st.date_input('Data de Início - Tabela', value=primeiro_dia_mes)
            with col2:
                periodo_fim_tabela = st.date_input('Data de Fim - Tabela', value=ultimo_dia_mes)
        
        df_filtrado = df.dropna(subset=['Data do Pedido'])
        df_filtrado['Data do Pedido'] = pd.to_datetime(df_filtrado['Data do Pedido'], errors='coerce')
        if df_filtrado['Data do Pedido'].isnull().any():
            st.warning("Valores inválidos persistiram na coluna 'Data do Pedido' após filtragem.")
            invalid_dates = df_filtrado[df_filtrado['Data do Pedido'].isnull()]['DATA'].unique()
            st.write("Valores inválidos encontrados em 'DATA':", invalid_dates)
            df_filtrado = df_filtrado.dropna(subset=['Data do Pedido'])

        df_filtrado = df_filtrado[(df_filtrado['Data do Pedido'] >= pd.to_datetime(periodo_inicio_tabela)) & 
                                  (df_filtrado['Data do Pedido'] <= pd.to_datetime(periodo_fim_tabela))]

        if produto_pesquisa:
            produto_pesquisa = ' '.join(produto_pesquisa.split()).strip()
            df_filtrado['DESCRICAO_1'] = df_filtrado['DESCRICAO_1'].apply(lambda x: ' '.join(str(x).split()).strip())
            df_filtrado['CÓDIGO PRODUTO'] = df_filtrado['CÓDIGO PRODUTO'].apply(lambda x: ' '.join(str(x).split()).strip())
            df_filtrado = df_filtrado[
                df_filtrado['DESCRICAO_1'].str.contains(produto_pesquisa, case=False, na=False) |
                df_filtrado['CÓDIGO PRODUTO'].str.contains(produto_pesquisa, case=False, na=False)
            ]

        exibir_tabela(df_filtrado)

    with st.container():
        st.subheader("Top Produtos Mais Vendidos por Valor")
        col1, col2 = st.columns(2)
        with col1:
            periodo_inicio_produtos = st.date_input('Data de Início - Top Produtos', value=primeiro_dia_mes)
        with col2:
            periodo_fim_produtos = st.date_input('Data de Fim - Top Produtos', value=ultimo_dia_mes)
        exibir_grafico_top_produtos(df, periodo_inicio_produtos, periodo_fim_produtos)

if __name__ == "__main__":
    main()