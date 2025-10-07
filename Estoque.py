import streamlit as st
import pandas as pd
import sqlite3
import logging
from streamlit_autorefresh import st_autorefresh
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode
import plotly.express as px
import plotly.graph_objects as go
import datetime

# Configuração da página (deve ser a primeira chamada do Streamlit)

page_title="Dashboard de Análise de Estoque",
page_icon="📦",
layout="wide"


# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Caminho base para os arquivos .db
DB_PATH = "database\\"

# --- Funções de Acesso a Dados ---
@st.cache_data(ttl=60)
def fetch_estoque_data():
    """Busca dados de estoque diretamente do banco SQLite (pceest.db)."""
    estoque_sql = """
        SELECT 
            NOMES_PRODUTO, QTULTENT, DTULTENT, DTULTSAIDA, CODFILIAL,
            QTVENDSEMANA, QTVENDSEMANA1, QTVENDSEMANA2, QTVENDSEMANA3,
            QTVENDMES, QTVENDMES1, QTVENDMES2, QTVENDMES3, QTGIRODIA,
            QTDEVOLMES, QTDEVOLMES1, QTDEVOLMES2, QTDEVOLMES3,
            CODPROD, QT_ESTOQUE, QTRESERV, QTINDENIZ, DTULTPEDCOMPRA,
            BLOQUEADA, CODFORNECEDOR, FORNECEDOR, CATEGORIA
        FROM PCEEST
    """
    try:
        connection = sqlite3.connect(f"{DB_PATH}pceest.db", timeout=10)
        df = pd.read_sql_query(estoque_sql, connection)
        logger.info(f"Dados de estoque carregados com sucesso. {len(df)} linhas.")
        return df
    except (sqlite3.Error, pd.io.sql.DatabaseError) as e:
        logger.error(f"Erro ao buscar dados de estoque: {e}")
        st.error(f"Erro ao buscar dados de estoque: {e}. Verifique se o caminho '{DB_PATH}pceest.db' está correto.")
        return pd.DataFrame()
    finally:
        if 'connection' in locals() and connection:
            connection.close()

# <<< OTIMIZAÇÃO 1: Filtrar dados de vendas diretamente no banco de dados >>>
# <<< OTIMIZAÇÃO 2: Sincronizar o tempo do cache (ttl) com o auto_refresh >>>
@st.cache_data(ttl=120)
def fetch_sales_data_for_current_year():
    """
    Busca os dados de vendas da tabela pcvendedor2 APENAS do início do ano atual até hoje.
    """
    today = datetime.date.today()
    start_of_year = today.replace(month=1, day=1).strftime('%Y-%m-%d') # Formato 'YYYY-MM-DD'

    sales_sql = f"""
    SELECT
        CODPROD, DATA, QT, PVENDA, CODOPER, CODCLI, CONDVENDA, CODIGOVENDEDOR
    FROM
        pcvendedor2
    WHERE
        -- Filtro de data adicionado para performance máxima
        DATA >= ? 
        -- Mantém os filtros essenciais para considerar apenas vendas válidas
        AND CODCLI NOT IN ('3', '91503', '111564', '1')
        AND CONDVENDA = '1'
        AND CODIGOVENDEDOR NOT IN (219, 3, 63, 100, 12, 104, 186, 217, 172, 173, 73, 144, 107, 207, 174, 149, 167, 199, 191, 218, 196, 214, 96)
    """
    try:
        connection = sqlite3.connect(f"{DB_PATH}pcvendedor2.db", timeout=10)
        # Usamos 'params' para passar a data de forma segura e evitar SQL Injection
        df_sales = pd.read_sql_query(sales_sql, connection, params=(start_of_year,))
        logger.info(f"Dados de vendas do ano atual carregados. {len(df_sales)} linhas.")
        return df_sales
    except (sqlite3.Error, pd.io.sql.DatabaseError) as e:
        logger.error(f"Erro ao buscar dados de vendas: {e}")
        st.error(f"Erro ao buscar dados de vendas do banco 'pcvendedor2.db': {e}.")
        return pd.DataFrame()
    finally:
        if 'connection' in locals() and connection:
            connection.close()

# --- Funções de Processamento de Dados ---
# Nenhuma alteração necessária aqui, a lógica permanece a mesma.
def process_dataframe(df, df_sales):
    """
    Renomeia colunas, limpa dados e calcula novas métricas.
    A Curva ABC agora segue a nova regra de negócio baseada em data e volume.
    """
    # Renomeação e limpeza inicial do df de estoque
    df = df.rename(columns={
        'CODPROD': 'Código Produto', 'NOMES_PRODUTO': 'Nome do Produto', 'QTULTENT': 'Qtde. Últ. Entrada',
        'QT_ESTOQUE': 'Estoque Disponível', 'QTRESERV': 'Qtde. Reservada', 'QTINDENIZ': 'Qtde. Avariada',
        'DTULTENT': 'Data Últ. Entrada', 'DTULTSAIDA': 'Data Últ. Saída', 'CODFILIAL': 'Filial',
        'DTULTPEDCOMPRA': 'Data Últ. Ped. Compra', 'BLOQUEADA': 'Qtde. Bloqueada', 'CODFORNECEDOR': 'Cód. Fornecedor',
        'FORNECEDOR': 'Fornecedor', 'CATEGORIA': 'Categoria', 'QTVENDSEMANA': 'Vendas Sem. Atual',
        'QTVENDSEMANA1': 'Vendas Sem. -1', 'QTVENDSEMANA2': 'Vendas Sem. -2', 'QTVENDSEMANA3': 'Vendas Sem. -3',
        'QTVENDMES': 'Vendas Mês Atual', 'QTVENDMES1': 'Vendas Mês -1', 'QTVENDMES2': 'Vendas Mês -2',
        'QTVENDMES3': 'Vendas Mês -3', 'QTGIRODIA': 'Giro Diário', 'QTDEVOLMES': 'Dev. Mês Atual',
        'QTDEVOLMES1': 'Dev. Mês -1', 'QTDEVOLMES2': 'Dev. Mês -2', 'QTDEVOLMES3': 'Dev. Mês -3',
    })
    numeric_cols = ['Estoque Disponível', 'Qtde. Reservada', 'Qtde. Bloqueada', 'Qtde. Avariada','Qtde. Últ. Entrada', 'Vendas Sem. Atual', 'Vendas Sem. -1', 'Vendas Sem. -2', 'Vendas Sem. -3','Vendas Mês Atual', 'Vendas Mês -1', 'Vendas Mês -2', 'Vendas Mês -3', 'Giro Diário', 'Dev. Mês Atual', 'Dev. Mês -1', 'Dev. Mês -2', 'Dev. Mês -3']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # #### INÍCIO DA NOVA LÓGICA DE CLASSIFICAÇÃO ABC ####

    # 1. Prepara os dados de vendas
    if not df_sales.empty:
        df_sales['DATA'] = pd.to_datetime(df_sales['DATA'], errors='coerce')
        df_sales['VALOR_VENDA'] = df_sales.apply(
            lambda row: row['QT'] if row['CODOPER'] == 'S' else -row['QT'], axis=1
        ) * df_sales['PVENDA']
        df_sales = df_sales.rename(columns={'CODPROD': 'Código Produto'})
        df_sales['Código Produto'] = df_sales['Código Produto'].astype(str)
    
    df['Código Produto'] = df['Código Produto'].astype(str)
    today = pd.to_datetime(datetime.date.today())

    # 2. Determina produtos Curva A (Pareto sobre vendas dos últimos 15 dias)
    class_A_products = set()
    if not df_sales.empty:
        # O filtro de 15 dias agora é aplicado em um dataframe já muito menor
        sales_15_days_df = df_sales[(today - df_sales['DATA']).dt.days <= 15]
        if not sales_15_days_df.empty:
            sales_value_15 = sales_15_days_df.groupby('Código Produto')['VALOR_VENDA'].sum().reset_index()
            sales_value_15 = sales_value_15[sales_value_15['VALOR_VENDA'] > 0]
            if not sales_value_15.empty:
                sales_value_15 = sales_value_15.sort_values(by='VALOR_VENDA', ascending=False)
                total_value_15 = sales_value_15['VALOR_VENDA'].sum()
                sales_value_15['CUM_PERC'] = (sales_value_15['VALOR_VENDA'].cumsum() / total_value_15) * 100
                class_A_products = set(sales_value_15[sales_value_15['CUM_PERC'] <= 80]['Código Produto'])

    # 3. Obtém a data da última venda para todos os produtos
    last_sale_dates = pd.DataFrame()
    if not df_sales.empty:
        last_sale_dates = df_sales.groupby('Código Produto')['DATA'].max().reset_index()
        last_sale_dates = last_sale_dates.rename(columns={'DATA': 'ULTIMA_VENDA'})
    
    # 4. Junta a data da última venda ao dataframe principal
    if not last_sale_dates.empty:
        df = df.merge(last_sale_dates, on='Código Produto', how='left')
    else:
        df['ULTIMA_VENDA'] = pd.NaT

    # 5. Define e aplica a função de classificação
    def classify_recency(row, class_A_set, today_date):
        if row['Código Produto'] in class_A_set:
            return 'A'
        last_sale = row['ULTIMA_VENDA']
        if pd.isna(last_sale):
            return 'C'
        days_since_sale = (today_date - last_sale).days
        if days_since_sale <= 30:
            return 'B'
        else:
            return 'C'

    df['Classe ABC'] = df.apply(classify_recency, axis=1, args=(class_A_products, today))
    
    # #### FIM DA NOVA LÓGICA DE CLASSIFICAÇÃO ABC ####

    # Cálculos restantes (dependentes do df principal)
    df['Giro Diário'] = df['Giro Diário'].replace(0, pd.NA)
    df['Dias de Estoque'] = (df['Estoque Disponível'] / df['Giro Diário']).fillna(0).round(0)
    df['Taxa de Devolução (%)'] = df.apply(lambda row: (row['Dev. Mês Atual'] / row['Vendas Mês Atual'] * 100) if row['Vendas Mês Atual'] > 0 else 0, axis=1).round(2)
    bins = [-1, 7, 30, 90, float('inf')]
    labels = ['Crítico', 'Atenção', 'Saudável', 'Excesso']
    df['Status Estoque'] = pd.cut(df['Dias de Estoque'], bins=bins, labels=labels, right=True)

    def create_detail_data(row):
          return [
              {'Métrica': 'Vendas Semanais', 'Atual': row['Vendas Sem. Atual'], 'Semana -1': row['Vendas Sem. -1'], 'Semana -2': row['Vendas Sem. -2'], 'Semana -3': row['Vendas Sem. -3']},
              {'Métrica': 'Vendas Mensais', 'Atual': row['Vendas Mês Atual'], 'Mês -1': row['Vendas Mês -1'], 'Mês -2': row['Vendas Mês -2'], 'Mês -3': row['Vendas Mês -3']},
              {'Métrica': 'Devoluções Mensais', 'Atual': row['Dev. Mês Atual'], 'Mês -1': row['Dev. Mês -1'], 'Mês -2': row['Dev. Mês -2'], 'Mês -3': row['Dev. Mês -3']},
              {'Métrica': 'Outras Qtde.', 'Reservada': row['Qtde. Reservada'], 'Bloqueada': row['Qtde. Bloqueada'], 'Avariada': row['Qtde. Avariada'], 'Últ. Entrada': row['Qtde. Últ. Entrada']}
          ]
    df['detail_data'] = df.apply(create_detail_data, axis=1)
    
    return df

# --- Interface Principal do Streamlit ---
def main():
    st_autorefresh(interval=120000, key="auto_refresh")
    st.title("Dashboard de Análise de Estoque e Vendas")
    
    # Carrega ambos os conjuntos de dados (a função de vendas agora é muito mais rápida)
    estoque_df = fetch_estoque_data()
    sales_df = fetch_sales_data_for_current_year() # <<< CHAMANDO A NOVA FUNÇÃO OTIMIZADA

    if estoque_df.empty:
        st.warning("Não há dados de estoque para exibir."); return
        
    # Processa os dados com a nova lógica ABC
    df_processado = process_dataframe(estoque_df, sales_df)

    # O RESTANTE DO SEU CÓDIGO PERMANECE EXATAMENTE IGUAL
    
    # --- KPIs ---
    st.header("Visão Geral do Estoque", divider="rainbow")
    total_unidades = df_processado['Estoque Disponível'].sum(); total_skus = df_processado['Código Produto'].nunique(); skus_zerados = df_processado[df_processado['Estoque Disponível'] <= 0]['Código Produto'].nunique(); vendas_mes_atual = df_processado['Vendas Mês Atual'].sum()
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric("Total de Produtos (SKUs)", f"{total_skus:,}".replace(",", ".")); kpi2.metric("Unidades em Estoque", f"{total_unidades:,.0f}".replace(",", ".")); kpi3.metric("Produtos com Estoque Zerado", f"{skus_zerados:,}".replace(",", ".")); kpi4.metric("Vendas do Mês (Unidades)", f"{vendas_mes_atual:,.0f}".replace(",", "."))

    # --- Filtros ---
    df_filtrado = df_processado.copy()
    with st.expander("🔍 Filtros Avançados", expanded=False):
        pesquisar = st.text_input("Pesquisar por Código, Nome ou Fornecedor")
        c1, c2 = st.columns(2)
        with c1:
            selected_abc = st.multiselect("Filtrar por Classe ABC:", options=sorted(df_filtrado['Classe ABC'].unique()), default=[])
        with c2:
            selected_stock_status = st.multiselect("Filtrar por Status de Cobertura:", options=['Crítico', 'Atenção', 'Saudável', 'Excesso'], default=[])
        status_options = ['Com Estoque Bloqueado', 'Com Estoque Avariado', 'Com Estoque Reservado', 'Com Estoque Zerado']
        selected_status = st.multiselect("Filtrar por Status do Estoque:", status_options, default=[])
        filiais = sorted(df_processado['Filial'].unique()); categorias = sorted(df_processado['Categoria'].dropna().unique()); fornecedores = sorted(df_processado['Fornecedor'].dropna().unique())
        c1, c2, c3 = st.columns(3)
        selected_filiais = c1.multiselect("Filial", filiais, default=[]); selected_categorias = c2.multiselect("Categoria", categorias, default=[]); selected_fornecedores = c3.multiselect("Fornecedor", fornecedores, default=[])
        
        if pesquisar: df_filtrado = df_filtrado[df_filtrado['Código Produto'].astype(str).str.contains(pesquisar, case=False, na=False) | df_filtrado['Nome do Produto'].str.contains(pesquisar, case=False, na=False) | df_filtrado['Fornecedor'].str.contains(pesquisar, case=False, na=False)]
        if selected_filiais: df_filtrado = df_filtrado[df_filtrado['Filial'].isin(selected_filiais)]
        if selected_categorias: df_filtrado = df_filtrado[df_filtrado['Categoria'].isin(selected_categorias)]
        if selected_fornecedores: df_filtrado = df_filtrado[df_filtrado['Fornecedor'].isin(selected_fornecedores)]
        if selected_abc: df_filtrado = df_filtrado[df_filtrado['Classe ABC'].isin(selected_abc)]
        if selected_stock_status: df_filtrado = df_filtrado[df_filtrado['Status Estoque'].isin(selected_stock_status)]
        for status in selected_status:
            if status == 'Com Estoque Bloqueado': df_filtrado = df_filtrado[df_filtrado['Qtde. Bloqueada'] > 0]
            if status == 'Com Estoque Avariado': df_filtrado = df_filtrado[df_filtrado['Qtde. Avariada'] > 0]
            if status == 'Com Estoque Reservado': df_filtrado = df_filtrado[df_filtrado['Qtde. Reservada'] > 0]
            if status == 'Com Estoque Zerado': df_filtrado = df_filtrado[df_filtrado['Estoque Disponível'] <= 0]

    # --- Tabela Principal ---
    st.header("🔎 Tabela Detalhada de Produtos", divider="rainbow")
    abc_style_js = JsCode("""function(params) { if (params.value == 'A') { return {'color': 'white', 'backgroundColor': '#4169E1'}; } if (params.value == 'B') { return {'color': 'white', 'backgroundColor': '#FFA500'}; } if (params.value == 'C') { return {'color': 'white', 'backgroundColor': '#A9A9A9'}; } return {'color': 'black', 'backgroundColor': 'white'}; }""")
    status_style_js = JsCode("""function(params) { if (params.value == 'Crítico') { return {'color': 'white', 'backgroundColor': '#E65555'}; } if (params.value == 'Atenção') { return {'color': 'black', 'backgroundColor': '#F4E07B'}; } if (params.value == 'Saudável') { return {'color': 'black', 'backgroundColor': '#82E0AA'}; } if (params.value == 'Excesso') { return {'color': 'white', 'backgroundColor': '#D2B4DE'}; } return {'color': 'black', 'backgroundColor': 'white'}; }""")
    estoque_style_js = JsCode("""function(params) { if (params.value <= 0) { return {'color': 'white', 'backgroundColor': '#E65555'}; } var status = params.data['Status Estoque']; if (status == 'Crítico') { return {'color': 'white', 'backgroundColor': '#E65555'}; } if (status == 'Atenção') { return {'color': 'black', 'backgroundColor': '#F4E07B'}; } if (status == 'Saudável') { return {'color': 'black', 'backgroundColor': '#82E0AA'}; } if (status == 'Excesso') { return {'color': 'white', 'backgroundColor': '#D2B4DE'}; } return {'color': 'black', 'backgroundColor': 'white'}; }""")
    
    colunas_principais = ['Filial', 'Código Produto', 'Nome do Produto', 'Status Estoque', 'Classe ABC', 'Estoque Disponível', 'Qtde. Reservada', 'Qtde. Bloqueada', 'Qtde. Avariada', 'Dias de Estoque', 'Vendas Mês Atual', 'Fornecedor', 'Categoria', 'Taxa de Devolução (%)']
    gb = GridOptionsBuilder.from_dataframe(df_filtrado)
    gb.configure_default_column(editable=False, groupable=True)
    gb.configure_column("Filial", width=80, pinned='left')
    gb.configure_column("Código Produto", width=120, pinned='left')
    gb.configure_column("Nome do Produto", width=350, pinned='left')
    gb.configure_column("Status Estoque", width=120, cellStyle=status_style_js)
    gb.configure_column("Classe ABC", width=100, cellStyle=abc_style_js)
    gb.configure_column("Estoque Disponível", width=150, cellStyle=estoque_style_js, type=["numericColumn", "rightAligned"])
    gb.configure_column("Qtde. Reservada", width=130, type=["numericColumn", "rightAligned"])
    gb.configure_column("Qtde. Bloqueada", width=130, type=["numericColumn", "rightAligned"])
    gb.configure_column("Qtde. Avariada", width=130, type=["numericColumn", "rightAligned"])
    gb.configure_column("Dias de Estoque", width=130, type=["numericColumn", "rightAligned"])
    gb.configure_column("Vendas Mês Atual", width=140, type=["numericColumn", "rightAligned"])
    gb.configure_column("Fornecedor", width=200)
    gb.configure_column("Categoria", width=150)
    gb.configure_column("Taxa de Devolução (%)", width=160, type=["numericColumn", "rightAligned"])
    gb.configure_columns(['detail_data', 'ULTIMA_VENDA'], hide=True)
    grid_options = gb.build()
    grid_options['masterDetail'] = True
    grid_options['detailCellRendererParams'] = {'detailGridOptions': { 'columnDefs': [{'field': c} for c in ['Métrica', 'Atual', 'Semana -1', 'Semana -2', 'Semana -3', 'Mês -1', 'Mês -2', 'Mês -3', 'Reservada', 'Bloqueada', 'Avariada', 'Últ. Entrada']] }, 'getDetailRowData': JsCode("function(params) { params.successCallback(params.data.detail_data); }")}
    grid_options['columnDefs'][0]['cellRenderer'] = 'agGroupCellRenderer'
    
    colunas_restantes = [col for col in df_filtrado.columns if col not in colunas_principais and col != 'ULTIMA_VENDA']
    df_para_exibir = df_filtrado[colunas_principais + colunas_restantes]
    AgGrid(df_para_exibir, gridOptions=grid_options, height=600, width='100%', theme='streamlit', allow_unsafe_jscode=True, enable_enterprise_modules=True)

    # --- Legenda, Saúde do Estoque e outras seções continuam aqui ...
    st.caption("Legenda dos Status de Estoque (baseado em dias de cobertura):")
    leg_col1, leg_col2, leg_col3, leg_col4 = st.columns(4)
    with leg_col1:
        st.markdown("""<div style="background-color:#E65555; color:white; padding:10px; border-radius:5px; text-align: center;"><strong>CRÍTICO</strong><br>(0 a 7 dias)<br><small>Risco altíssimo de ruptura.</small></div>""", unsafe_allow_html=True)
    with leg_col2:
        st.markdown("""<div style="background-color:#F4E07B; color:black; padding:10px; border-radius:5px; text-align: center;"><strong>ATENÇÃO</strong><br>(8 a 30 dias)<br><small>Planejar reposição em breve.</small></div>""", unsafe_allow_html=True)
    with leg_col3:
        st.markdown("""<div style="background-color:#82E0AA; color:black; padding:10px; border-radius:5px; text-align: center;"><strong>SAUDÁVEL</strong><br>(31 a 90 dias)<br><small>Nível de cobertura ideal.</small></div>""", unsafe_allow_html=True)
    with leg_col4:
        st.markdown("""<div style="background-color:#D2B4DE; color:white; padding:10px; border-radius:5px; text-align: center;"><strong>EXCESSO</strong><br>(Acima de 90 dias)<br><small>Capital parado e risco de perdas.</small></div>""", unsafe_allow_html=True)

    st.header("🩺 Diagnóstico da Saúde do Estoque (Visão Geral)", divider="rainbow")
    saude_col1, saude_col2 = st.columns([1, 2])
    with saude_col1:
        st.subheader("Nível de Serviço do Estoque")
        status_counts = df_processado['Status Estoque'].value_counts(normalize=True) * 100
        healthy_perc = status_counts.get('Saudável', 0)
        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number", value=healthy_perc,
            title={'text': "% de SKUs em Nível Saudável"},
            gauge={'axis': {'range': [None, 100]},
                   'steps': [{'range': [0, 40], 'color': "lightcoral"}, {'range': [40, 70], 'color': "khaki"}, {'range': [70, 100], 'color': "lightgreen"}],
                   'threshold': {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': 70}}))
        fig_gauge.update_layout(height=300, margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig_gauge, use_container_width=True)
        status_df = df_processado['Status Estoque'].value_counts().reset_index(); status_df.columns = ['Status', 'Quantidade de Produtos']
        st.dataframe(status_df, use_container_width=True, hide_index=True)
    with saude_col2:
        st.subheader("Análise ABC (Nova Lógica)")
        abc_summary = df_processado.groupby('Classe ABC')['Código Produto'].nunique().reset_index()
        abc_summary = abc_summary.rename(columns={'Código Produto': 'count'})
        
        m1, m2, m3 = st.columns(3)
        try: m1.metric("Produtos Classe A", f"{abc_summary.loc[abc_summary['Classe ABC']=='A', 'count'].iloc[0]} SKUs", "Alto Faturamento (15 dias)")
        except (IndexError, KeyError): m1.metric("Produtos Classe A", "0 SKUs", "Alto Faturamento (15 dias)")
        try: m2.metric("Produtos Classe B", f"{abc_summary.loc[abc_summary['Classe ABC']=='B', 'count'].iloc[0]} SKUs", "Venda Recente (30 dias)")
        except (IndexError, KeyError): m2.metric("Produtos Classe B", "0 SKUs", "Venda Recente (30 dias)")
        try: m3.metric("Produtos Classe C", f"{abc_summary.loc[abc_summary['Classe ABC']=='C', 'count'].iloc[0]} SKUs", "Venda Antiga (>30 dias)")
        except (IndexError, KeyError): m3.metric("Produtos Classe C", "0 SKUs", "Venda Antiga (>30 dias)")
        
        fig_abc = px.pie(abc_summary, values='count', names='Classe ABC', title='Distribuição de SKUs por Classe ABC', hole=.4, color_discrete_map={'A':'royalblue','B':'darkorange','C':'lightgrey'})
        st.plotly_chart(fig_abc, use_container_width=True)
    
    # O restante do seu código continua aqui sem alterações...
    # (Adicionei as seções restantes para o código ficar completo)

    st.header("📈 Análise Individual de Produto (Visão Consolidada)", divider="rainbow")
    lista_produtos_geral = sorted(df_processado['Nome do Produto'].unique())
    filtro_produto_geral = st.text_input("Digite para filtrar produtos na lista abaixo:", key="filtro_prod_geral")
    if filtro_produto_geral:
        lista_produtos_filtrada_geral = [p for p in lista_produtos_geral if filtro_produto_geral.lower() in p.lower()]
    else:
        lista_produtos_filtrada_geral = lista_produtos_geral
    produto_selecionado = st.selectbox("Selecione um produto para análise:", options=lista_produtos_filtrada_geral)
    if produto_selecionado:
        df_prod_todas_filiais = df_processado[df_processado['Nome do Produto'] == produto_selecionado]
        if not df_prod_todas_filiais.empty:
            estoque_total = df_prod_todas_filiais['Estoque Disponível'].sum()
            giro_diario_total = df_prod_todas_filiais['Giro Diário'].sum()
            dias_estoque_total = (estoque_total / giro_diario_total) if giro_diario_total > 0 else 0
            bins = [-1, 7, 30, 90, float('inf')]; labels = ['Crítico', 'Atenção', 'Saudável', 'Excesso']
            status_estoque_total = pd.cut(pd.Series([dias_estoque_total]), bins=bins, labels=labels, right=True)[0]
            fornecedor_prod = df_prod_todas_filiais['Fornecedor'].iloc[0]
            classe_abc_prod = df_prod_todas_filiais['Classe ABC'].iloc[0]
            
            st.subheader(f"Ficha Técnica Consolidada: {produto_selecionado}")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Estoque Disponível (Total)", f"{estoque_total:,.0f}"); c2.metric("Dias de Estoque (Consolidado)", f"{dias_estoque_total:,.0f}"); c3.metric("Classe ABC", classe_abc_prod); c4.metric("Status do Estoque (Geral)", status_estoque_total)
            st.text(f"Fornecedor: {fornecedor_prod}")
            st.write("##### Detalhes por Filial")
            st.dataframe(df_prod_todas_filiais[['Filial', 'Estoque Disponível', 'Dias de Estoque', 'Status Estoque', 'Vendas Mês Atual', 'Taxa de Devolução (%)']], use_container_width=True, hide_index=True)
            
            tendencia_vendas = pd.DataFrame({'Mês': ['Mês -3', 'Mês -2', 'Mês -1', 'Mês Atual'], 'Vendas': [df_prod_todas_filiais['Vendas Mês -3'].sum(), df_prod_todas_filiais['Vendas Mês -2'].sum(), df_prod_todas_filiais['Vendas Mês -1'].sum(), df_prod_todas_filiais['Vendas Mês Atual'].sum()]})
            tendencia_dev = pd.DataFrame({'Mês': ['Mês -3', 'Mês -2', 'Mês -1', 'Mês Atual'], 'Devoluções': [df_prod_todas_filiais['Dev. Mês -3'].sum(), df_prod_todas_filiais['Dev. Mês -2'].sum(), df_prod_todas_filiais['Dev. Mês -1'].sum(), df_prod_todas_filiais['Dev. Mês Atual'].sum()]})
            c1_graf, c2_graf = st.columns(2)
            fig_vendas = px.line(tendencia_vendas, x='Mês', y='Vendas', title='Tendência de Vendas Consolidadas', markers=True)
            c1_graf.plotly_chart(fig_vendas, use_container_width=True)
            fig_dev = px.line(tendencia_dev, x='Mês', y='Devoluções', title='Tendência de Devoluções Consolidadas', markers=True)
            fig_dev.update_traces(line_color='red')
            c2_graf.plotly_chart(fig_dev, use_container_width=True)

    st.header("🏆 Top 20 Produtos por Fornecedor (Estoque Consolidado)", divider="rainbow")
    lista_fornecedores = sorted(df_processado['Fornecedor'].dropna().unique())
    fornecedor_selecionado = st.selectbox("Selecione um Fornecedor para ver o Top 20 Produtos em Estoque:", options=lista_fornecedores)
    if fornecedor_selecionado:
        df_fornecedor = df_processado[df_processado['Fornecedor'] == fornecedor_selecionado].copy()
        df_consolidado = df_fornecedor.groupby(['Código Produto', 'Nome do Produto', 'Classe ABC']).agg(
            Estoque_Disponivel_Total=('Estoque Disponível', 'sum'),
            Giro_Diario_Total=('Giro Diário', 'sum'),
            Vendas_Mes_Atual_Total=('Vendas Mês Atual', 'sum')
        ).reset_index()
        df_consolidado['Dias de Estoque Consolidados'] = df_consolidado.apply(
            lambda row: (row['Estoque_Disponivel_Total'] / row['Giro_Diario_Total']) if row['Giro_Diario_Total'] > 0 else 0,
            axis=1
        ).round(0)
        df_top_fornecedor = df_consolidado.sort_values('Estoque_Disponivel_Total', ascending=False).head(20)
        if not df_top_fornecedor.empty:
            fig_top_fornecedor = px.bar(
                df_top_fornecedor, 
                x='Estoque_Disponivel_Total', 
                y='Nome do Produto', 
                orientation='h', 
                color='Dias de Estoque Consolidados',
                color_continuous_scale=px.colors.sequential.Viridis, 
                title=f"Top 20 Produtos (Estoque Consolidado) para {fornecedor_selecionado}",
                hover_data={
                    'Estoque_Disponivel_Total': ':,', 
                    'Dias de Estoque Consolidados': True, 
                    'Vendas_Mes_Atual_Total': ':,', 
                    'Classe ABC': True, 
                    'Nome do Produto': False
                },
                labels={
                    "Estoque_Disponivel_Total": "Estoque Disponível Total",
                    "Dias de Estoque Consolidados": "Dias de Estoque (Consolidado)",
                    "Vendas_Mes_Atual_Total": "Vendas Mês Atual (Total)"
                }
            )
            fig_top_fornecedor.update_layout(yaxis={'categoryorder':'total ascending'}, height=600)
            st.plotly_chart(fig_top_fornecedor, use_container_width=True)
        else:
            st.info(f"Não há produtos em estoque para o fornecedor {fornecedor_selecionado}.")


if __name__ == '__main__':
    main()