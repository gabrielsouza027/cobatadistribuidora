import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import sqlite3
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
import logging
import plotly.express as px
from streamlit_autorefresh import st_autorefresh
import hashlib

# --- Configurações Iniciais ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Caminho para o banco de dados
DB_PATH = "database\\"
DB_FILE = f"{DB_PATH}pcvendedor2.db"

# --- NOVO: Função para Otimizar o Banco de Dados ---
def initialize_database():
    """
    Garante que a tabela de vendas tenha um índice na coluna DATA.
    Isso acelera drasticamente as consultas baseadas em intervalo de datas.
    A operação é segura e só cria o índice se ele não existir.
    """
    try:
        connection = sqlite3.connect(DB_FILE)
        cursor = connection.cursor()
        # Cria um índice na coluna DATA. O 'IF NOT EXISTS' garante que não haverá erro se o índice já existir.
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_data_vendas ON pcvendedor2 (DATA);")
        connection.commit()
        connection.close()
        logger.info("Índice do banco de dados verificado/criado com sucesso.")
    except Exception as e:
        st.error(f"Erro ao inicializar e otimizar o banco de dados: {e}")
        logger.error(f"Erro ao criar índice: {e}")

# --- Funções de Acesso e Processamento de Dados ---
@st.cache_data(ttl=300)
def fetch_vendas_data(data_inicial, data_final):
    """
    Busca dados de vendas e cria colunas explícitas para Venda e Devolução,
    garantindo que os cálculos sejam sempre corretos e isolados.
    Retorna o DataFrame e o timestamp da atualização.
    
    ## OTIMIZAÇÃO DE PERFORMANCE ##
    A consulta SQL foi modificada para usar um índice na coluna 'DATA',
    evitando a função DATE() que causa lentidão (full table scan).
    """
    # MODIFICADO: A consulta SQL agora é "SARGable", permitindo o uso de índices.
    vendas_sql = "SELECT NUMPED, CLIENTE, VENDEDOR, PRODUTO, QT, PVENDA, VLBONIFIC, DATA, FORNECEDOR FROM pcvendedor2 WHERE DATA >= ? AND DATA < ?"
    
    try:
        connection = sqlite3.connect(DB_FILE)
        
        # MODIFICADO: Ajusta a data final para incluir todas as horas do último dia.
        # Ex: Se data_final for '2023-10-25', o filtro será até '2023-10-26 00:00:00'.
        data_final_ajustada = data_final + timedelta(days=1)
        
        df = pd.read_sql_query(vendas_sql, connection, params=(data_inicial.strftime('%Y-%m-%d'), data_final_ajustada.strftime('%Y-%m-%d')))
        connection.close()
        
        update_time = datetime.now() # Captura o momento da busca

        if df.empty: return pd.DataFrame(), update_time

        df['DATA'] = pd.to_datetime(df['DATA'], errors='coerce')
        for col in ['QT', 'PVENDA', 'VLBONIFIC']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        df = df.dropna(subset=['DATA'])

        df['VALOR_TRANSACAO'] = (df['QT'] * df['PVENDA']) - df['VLBONIFIC']
        df['VALOR_VENDA_LIQUIDA'] = df['VALOR_TRANSACAO'].where(df['QT'] >= 0, 0)
        df['VALOR_DEVOLUCAO'] = -df['VALOR_TRANSACAO'].where(df['QT'] < 0, 0)
        df['MES'] = df['DATA'].dt.month
        df['ANO'] = df['DATA'].dt.year
        return df, update_time
    except Exception as e:
        st.error(f"Erro ao buscar dados de vendas: {e}")
        return pd.DataFrame(), datetime.now()

@st.cache_data(ttl=300, hash_funcs={pd.DataFrame: lambda df: hashlib.md5(pd.util.hash_pandas_object(df).values.tobytes()).hexdigest()})
def prepare_tree_data(df: pd.DataFrame, start_date, end_date, show_transactions=True):
    """
    ## LÓGICA CORRIGIDA E OTIMIZADA ##
    Prepara os dados na estrutura de árvore: Fornecedor -> Mês -> Produto -> Vendedor -> (Transação opcional).
    Otimizado com mais operações vetoriais e menos loops para melhorar desempenho.
    """
    if df.empty: return [], []
    month_range = pd.date_range(start=start_date, end=end_date, freq='MS').strftime('%b/%y').unique()
    tree_data = []

    # Pivot para meses (vetorial)
    pivot_meses = df.pivot_table(
        index='FORNECEDOR',
        columns=df['DATA'].dt.strftime('%b/%y'),
        values='VALOR_VENDA_LIQUIDA',
        aggfunc='sum', fill_value=0
    ).reindex(columns=month_range, fill_value=0).sort_index(ascending=True)
    ordered_cols = list(pivot_meses.columns)

    # Nível 1: Fornecedores (vetorial)
    supplier_totals = df.groupby('FORNECEDOR')['VALOR_VENDA_LIQUIDA'].sum().to_dict()
    supplier_nodes = [
        {'dataPath': [forn], 'ENTIDADE': f"🏢 {forn}", 'Total Período': supplier_totals.get(forn, 0), **row.to_dict()}
        for forn, row in pivot_meses.iterrows()
    ]
    tree_data.extend(supplier_nodes)

    # Agrupamentos por fornecedor
    grouped_forn = df.groupby('FORNECEDOR')
    for fornecedor, df_fornecedor in grouped_forn:
        # Agrupamentos por mês dentro do fornecedor
        grouped_mes = df_fornecedor.groupby(['ANO', 'MES'])
        for (ano, mes_num), df_mes in grouped_mes:
            mes_nome_str = datetime(ano, mes_num, 1).strftime('%B/%Y')
            
            # Nível 2: Mês (vetorial onde possível)
            mes_node = {
                'dataPath': [fornecedor, f"{ano}-{mes_num:02d}"], 'ENTIDADE': f"🗓️ {mes_nome_str}",
                'VENDAS_BRUTAS': df_mes['VALOR_VENDA_LIQUIDA'].sum(),
                'TOTAL_DEVOLVIDO': df_mes['VALOR_DEVOLUCAO'].sum(),
                'VENDAS_LIQUIDAS': df_mes['VALOR_VENDA_LIQUIDA'].sum() - df_mes['VALOR_DEVOLUCAO'].sum(),
                'TOTAL_PEDIDOS': df_mes['NUMPED'].nunique(),
                'POSITIVACAO': df_mes[df_mes['VALOR_VENDA_LIQUIDA'] > 0]['CLIENTE'].nunique()
            }
            tree_data.append(mes_node)
            
            # Agrupamentos por produto dentro do mês
            grouped_prod = df_mes.groupby('PRODUTO')
            for produto, df_produto in grouped_prod:
                vendas_brutas_prod = df_produto['VALOR_VENDA_LIQUIDA'].sum()
                total_devolvido_prod = df_produto['VALOR_DEVOLUCAO'].sum()
                
                # Nível 3: Produto
                produto_node = {
                    'dataPath': [fornecedor, f"{ano}-{mes_num:02d}", produto], 'ENTIDADE': f"📦 {produto}",
                    'VENDAS_BRUTAS': vendas_brutas_prod,
                    'TOTAL_DEVOLVIDO': total_devolvido_prod,
                    'VENDAS_LIQUIDAS': vendas_brutas_prod - total_devolvido_prod,
                    'TOTAL_PEDIDOS': df_produto['NUMPED'].nunique(),
                    'POSITIVACAO': df_produto[df_produto['VALOR_VENDA_LIQUIDA'] > 0]['CLIENTE'].nunique(),
                    'QT': df_produto['QT'].sum()
                }
                tree_data.append(produto_node)

                # Agrupamentos por vendedor dentro do produto
                grouped_vend = df_produto.groupby('VENDEDOR')
                for vendedor, df_vendedor in grouped_vend:
                    vendas_brutas_vend = df_vendedor['VALOR_VENDA_LIQUIDA'].sum()
                    total_devolvido_vend = df_vendedor['VALOR_DEVOLUCAO'].sum()

                    # Nível 4: Vendedor
                    vendedor_node = {
                        'dataPath': [fornecedor, f"{ano}-{mes_num:02d}", produto, vendedor], 'ENTIDADE': f"👨‍💼 {vendedor}",
                        'VENDAS_BRUTAS': vendas_brutas_vend,
                        'TOTAL_DEVOLVIDO': total_devolvido_vend,
                        'VENDAS_LIQUIDAS': vendas_brutas_vend - total_devolvido_vend,
                        'TOTAL_PEDIDOS': df_vendedor['NUMPED'].nunique(),
                        'POSITIVACAO': df_vendedor[df_vendedor['VALOR_VENDA_LIQUIDA'] > 0]['CLIENTE'].nunique(),
                        'QT': df_vendedor['QT'].sum()
                    }
                    tree_data.append(vendedor_node)
                    
                    if show_transactions:
                        # Nível 5: Transações (vetorial para criar nodes)
                        transacoes = df_vendedor.reset_index()
                        transacao_nodes = [
                            {
                                'dataPath': [fornecedor, f"{ano}-{mes_num:02d}", produto, vendedor, f"T.{row['index']}"],
                                'ENTIDADE': f"📄 Pedido: {row['NUMPED']}",
                                'NUMPED': row['NUMPED'],
                                'CLIENTE': row['CLIENTE'],
                                'QT': row['QT'],
                                'PVENDA': row['PVENDA'],
                                'TIPO': 'Devolução' if row['QT'] < 0 else 'Venda',
                                'VENDAS_BRUTAS': row['VALOR_VENDA_LIQUIDA'],
                                'TOTAL_DEVOLVIDO': row['VALOR_DEVOLUCAO']
                            }
                            for _, row in transacoes.iterrows()
                        ]
                        tree_data.extend(transacao_nodes)
                        
    return tree_data, ordered_cols

def display_charts(df: pd.DataFrame):
    """Exibe gráficos baseados no DataFrame completo do período selecionado."""
    st.header("Análise Gráfica", divider="rainbow")
    if df.empty: st.warning("Não há dados para gerar gráficos."); return
    
    c1, c2 = st.columns([1.2, 1])
    with c1:
        st.subheader("Top Fornecedores por Venda Líquida")
        periodo = st.radio("Período do Gráfico:", ('Ano', 'Mês Atual', 'Últimos 3 Meses'), horizontal=True, key="periodo_grafico_vendas", index=1)
        
        today = datetime.now().date()
        if periodo == 'Ano':
            df_chart = df[df['DATA'].dt.year == today.year]
        elif periodo == 'Mês Atual':
            df_chart = df[(df['DATA'].dt.year == today.year) & (df['DATA'].dt.month == today.month)]
        else: # Últimos 3 Meses
            start_date_chart = (today - relativedelta(months=2)).replace(day=1)
            df_chart = df[df['DATA'].dt.date >= start_date_chart]

        if not df_chart.empty:
            top_fornecedores = df_chart.groupby('FORNECEDOR')['VALOR_TRANSACAO'].sum().nlargest(10).sort_values()
            fig = px.bar(top_fornecedores, x=top_fornecedores.values, y=top_fornecedores.index, orientation='h', text_auto='.2s', labels={'y': '', 'x': 'Venda Líquida (Sem/Devoluções) (R$)'})
            st.plotly_chart(fig, use_container_width=True)
        else: st.warning(f"Não há dados de vendas para o período: {periodo}")
    with c2:
        st.subheader("Top 10 Fornecedores por Devolução")
        periodo_dev = st.radio("Período do Gráfico:", ('Ano', 'Mês Atual', 'Últimos 3 Meses'), horizontal=True, key="periodo_grafico_dev", index=1)
        
        today_dev = datetime.now().date()
        if periodo_dev == 'Ano':
            df_chart_dev = df[df['DATA'].dt.year == today_dev.year]
        elif periodo_dev == 'Mês Atual':
            df_chart_dev = df[(df['DATA'].dt.year == today_dev.year) & (df['DATA'].dt.month == today_dev.month)]
        else: # Últimos 3 Meses
            start_date_chart_dev = (today_dev - relativedelta(months=2)).replace(day=1)
            df_chart_dev = df[df['DATA'].dt.date >= start_date_chart_dev]
        
        top_devolucoes = df_chart_dev[df_chart_dev['VALOR_DEVOLUCAO'] > 0].groupby('FORNECEDOR')['VALOR_DEVOLUCAO'].sum().nlargest(10).sort_values()
        if not top_devolucoes.empty:
            fig_dev = px.bar(top_devolucoes, x=top_devolucoes.values, y=top_devolucoes.index, orientation='h', text_auto='.2s', labels={'y': '', 'x': 'Valor Devolvido (R$)'})
            fig_dev.update_traces(marker_color='#d62728')
            st.plotly_chart(fig_dev, use_container_width=True)
        else: st.info(f"Nenhuma devolução encontrada para o período: {periodo_dev}")

# --- Função Principal da Aplicação ---
def main():
    st_autorefresh(interval=5 * 60 * 1000, key="data_refresher")
    st.title("Análise Hierárquica de Vendas por Fornecedor")

    today = datetime.now()
    
    # --- NOVO: Carregamento de dados para os GRÁFICOS ---
    # Carrega os dados do ano inteiro para ter uma base consistente para os gráficos.
    # Isso independe dos filtros de data que o usuário selecionar abaixo.
    start_of_year_for_charts = today.replace(month=1, day=1)
    df_vendas_graficos, _ = fetch_vendas_data(start_of_year_for_charts, today)
    
    # --- NÍVEL 1: FILTROS PRINCIPAIS (COM LAYOUT AJUSTADO) ---
    col1, col2, col3, col4 = st.columns([1, 1, 2, 1])
    with col1:
        start_of_year = today.replace(month=1, day=1)
        data_inicial = st.date_input("Data Inicial", value=start_of_year)
    with col2:
        data_final = st.date_input("Data Final", value=today)
    
    if data_inicial > data_final: st.error("A data inicial não pode ser maior que a data final."); return

    # MODIFICADO: Converte os inputs de data para datetime para consistência
    data_inicial = datetime.combine(data_inicial, datetime.min.time())
    data_final = datetime.combine(data_final, datetime.min.time())

    # MODIFICADO: Busca os dados especificamente para a TABELA usando os filtros de data.
    df_vendas_tabela, last_update_time = fetch_vendas_data(data_inicial, data_final)

    with col4:
        st.markdown("<div style='text-align: right;'>&nbsp;</div>", unsafe_allow_html=True)
        st.markdown(f"<div style='text-align: right; font-style: italic;'>Última atualização: {last_update_time.strftime('%H:%M:%S')}</div>", unsafe_allow_html=True)

    # MODIFICADO: A verificação de "vazio" agora é sobre os dados da tabela.
    if df_vendas_tabela.empty: st.warning("Nenhum dado de venda encontrado para o período selecionado na tabela."); 
    
    with col3:
        # MODIFICADO: A lista de fornecedores para o filtro multiselect vem dos dados da tabela.
        lista_fornecedores = sorted(df_vendas_tabela['FORNECEDOR'].unique())
        fornecedores_selecionados = st.multiselect(
            "Filtrar Fornecedores (somente para a tabela):",
            options=lista_fornecedores, default=[]
        )
    
    # --- NÍVEL 2: TABELA DETALHADA ---
    st.header("Análise Detalhada", divider="rainbow")
    
    # MODIFICADO: Garante que a tabela use os dados filtrados por data.
    if not df_vendas_tabela.empty:
        df_para_tabela = df_vendas_tabela[df_vendas_tabela['FORNECEDOR'].isin(fornecedores_selecionados)] if fornecedores_selecionados else df_vendas_tabela
        tree_data, dynamic_month_cols = prepare_tree_data(df_para_tabela, data_inicial, data_final, show_transactions=True)
        
        if not tree_data: 
            st.warning("Nenhum dado para exibir na tabela com os filtros atuais.")
        else:
            gb = GridOptionsBuilder()
            cell_style_js = JsCode(""" function(params) { if (params.node.level > 0) { return {backgroundColor: 'rgba(255, 255, 255, 0.05)'}; } return null; } """)
            for mes_ano in dynamic_month_cols: gb.configure_column(mes_ano, headerName=mes_ano, type=["numericColumn"], valueFormatter="x > 0 ? x.toLocaleString('pt-BR', {style: 'currency', currency: 'BRL'}) : ''", width=130, cellStyle=cell_style_js)
            gb.configure_column("Total Período", headerName="Total Período (Vendas Brutas)", type=["numericColumn"], valueFormatter="x.toLocaleString('pt-BR', {style: 'currency', currency: 'BRL'})", width=180, cellStyle=cell_style_js, pinned='right')
            currency_formatter = "x != null && x != 0 ? x.toLocaleString('pt-BR', {style: 'currency', currency: 'BRL'}) : ''"
            gb.configure_column("VENDAS_BRUTAS", headerName="Vendas/C.Devolução", type=["numericColumn"], valueFormatter=currency_formatter, width=180)
            gb.configure_column("TOTAL_DEVOLVIDO", headerName="Total Devolvido", type=["numericColumn"], valueFormatter=currency_formatter, width=150)
            gb.configure_column("VENDAS_LIQUIDAS", headerName="Vendas/S.Devolução", type=["numericColumn"], valueFormatter=currency_formatter, width=180)
            gb.configure_column("TOTAL_PEDIDOS", headerName="Nº Pedidos", type=["numericColumn"], width=120)
            gb.configure_column("POSITIVACAO", headerName="Positivação (Clientes)", type=["numericColumn"], width=180)
            
            # Colunas que agora terão dados nos níveis mais baixos
            gb.configure_column("TIPO", headerName="Tipo", width=110)
            gb.configure_column("NUMPED", headerName="Nº Pedido", width=120)
            gb.configure_column("CLIENTE", headerName="Cliente", width=250)
            gb.configure_column("QT", headerName="Qtd.", width=80)
            gb.configure_column("PVENDA", headerName="Preço Venda", type=["numericColumn"], valueFormatter="x != null && x != 0 ? x.toLocaleString('pt-BR', {style: 'currency', currency: 'BRL'}) : ''", width=130)
            
            gb.configure_column('dataPath', hide=True); gb.configure_column('ENTIDADE', hide=True)
            grid_options = gb.build()
            grid_options['getRowStyle'] = JsCode(""" function(params) { switch (params.node.level) { case 1: return { 'background-color': 'rgba(255, 255, 255, 0.04)' }; case 2: return { 'background-color': 'rgba(255, 255, 255, 0.07)' }; case 3: return { 'background-color': 'rgba(255, 255, 255, 0.1)' }; case 4: return { 'background-color': 'rgba(255, 255, 255, 0.13)' }; case 5: return { 'background-color': 'rgba(255, 255, 255, 0.16)' }; default: return null; }} """)
            grid_options['treeData'] = True; grid_options['animateRows'] = True
            grid_options['getDataPath'] = JsCode("function(data) { return data.dataPath; }")
            grid_options['groupDefaultExpanded'] = 0
            grid_options['autoGroupColumnDef'] = { "headerName": "Hierarquia", "minWidth": 400, "pinned": "left", "cellRendererParams": { "suppressCount": True }, "valueGetter": "data.ENTIDADE" }
            AgGrid(pd.DataFrame(tree_data), gridOptions=grid_options, height=700, width='100%', theme='streamlit', allow_unsafe_jscode=True, enable_enterprise_modules=True, key='fornecedor_tree_grid')

    # --- NÍVEL 3: ANÁLISE GRÁFICA ---
    # MODIFICADO: A função de gráficos agora usa o DataFrame separado e mais amplo.
    display_charts(df_vendas_graficos)

if __name__ == '__main__':
    initialize_database()
    main()
