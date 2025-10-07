import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, date, timedelta
import locale
import io
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
from streamlit_autorefresh import st_autorefresh

# Função principal
def main():
    # Configurações iniciais
    try:
        locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
    except locale.Error:
        st.warning("Locale pt_BR.UTF-8 não disponível. Usando formatação alternativa.")
        locale.setlocale(locale.LC_ALL, '')

    # Inicializar last_refresh
    if 'last_refresh' not in st.session_state:
        st.session_state.last_refresh = datetime.now()

    # Forçar atualização se o intervalo passou
    if (datetime.now() - st.session_state.last_refresh).total_seconds() >= 300:
        st.session_state.cached_data = None
        st.session_state.cached_range = (None, None)
        st.session_state.last_refresh = datetime.now()

    # Auto-refresh
    st_autorefresh(interval=300000, key="data_refresh")
    next_refresh = st.session_state.last_refresh + timedelta(seconds=300)
    st.write(f"Próxima atualização em: {next_refresh.strftime('%H:%M:%S')}")

    # Botão de atualização manual
    if st.button("Atualizar Dados"):
        st.session_state["force_refresh"] = True
        st.session_state.cached_data = None
        st.session_state.cached_range = (None, None)
        st.session_state.last_refresh = datetime.now()

    # Título
    st.title("Relatório de Vendas e Positivação por Vendedor")

    # Mapeamento de fornecedores
    default_supplier_names = {
        99678: "JTI", 5832: "PMB", 5065: "SEDAS", 6521: "SEDAS", 99209: "GLOBALBEV",
        999573: "VCT", 91257: "CHIAMULERA", 999574: "MONIN", 99569: "BEAM SUNTORY",
        24: "GALLO", 999571: "BALY", 90671: "KRUG", 99528: "NATIQUE", 60: "PERNOD",
        99502: "BACARDI", 99534: "SALTON", 81: "SALTON", 34: "AURORA", 999579: "AURORA",
        18: "PECCIN", 999577: "FLORESTAL",
    }

    BRITVIC_TEMP_CODE = 999993
    default_supplier_names[BRITVIC_TEMP_CODE] = "BRITVIC"

    # Lista ordenada dos fornecedores
    ordered_suppliers = [
        "GALLO", "GLOBALBEV", "FLORESTAL", "PECCIN", "SEDAS", "JTI", "PMB", "VCT",
        "CHIAMULERA", "MONIN", "BEAM SUNTORY", "AURORA", "SALTON", "BACARDO",
        "PERNOD", "BALY", "KRUG", "NATIQUE", "BRITVIC"
    ]

    # Lista de códigos de produtos associados ao fornecedor BRITVIC
    britvic_product_codes = [
        2798, 1044, 989, 560, 163, 57, 5006, 4988, 4987, 4415, 4414, 4200, 4199,
        3871, 3870, 3385, 3123, 3058, 2797, 2796, 2795, 2794, 1047, 58, 5386,
        5385, 5303, 5302, 5301, 5299, 5298, 5297, 5296, 5294, 5293, 5292,
        5291, 5290, 5288, 5287, 5286, 5285, 5284, 5283, 5282, 5281, 5280, 5234, 5233,
        5232, 5231, 5230, 5229, 5228, 5227, 5226, 5225, 5224, 5223, 5222, 5221, 5220,
        5219, 5218, 5217, 5216, 5215, 5214, 5213, 5212, 5211, 5210, 5209, 5208, 5207,
        3872, 1038, 988, 278
    ]

    dias_semana_map = {
        "SEGUNDA": 0, "TERCA": 1, "TERÇA": 1, "QUARTA": 2, "QUINTA": 3,
        "SEXTA": 4, "SABADO": 5, "SÁBADO": 5, "DOMINGO": 6
    }

    # Função para verificar se o pedido está dentro da rota
    def is_pedido_dentro_rota(dia_pedido, rota):
        dia_pedido_num = pd.to_datetime(dia_pedido).weekday()
        rota = rota.upper() if isinstance(rota, str) else ""
        if rota in dias_semana_map:
            return dia_pedido_num == dias_semana_map[rota]
        return False

    # Função para buscar dados do banco
    def fetch_data(data_inicial, data_final):
        db_path = "database/pcvendedor.db"
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            query = """
                SELECT 
                    DATAPEDIDO, VALOR, QUANTIDADE, CODIGOVENDA, CODFORNECEDOR, CODPRODUTO, 
                    CUSTOPRODUTO, PEDIDO, CODUSUR, VENDEDOR, CODCLIENTE, ROTA,
                    FORNECEDOR, CLIENTE, FANTASIA, RAMO, SUPERVISOR, PRODUTO
                FROM pcvendedor
                WHERE DATE(DATAPEDIDO) BETWEEN ? AND ?
                ORDER BY DATAPEDIDO
            """
            df = pd.read_sql_query(query, conn, params=(data_inicial.strftime("%Y-%m-%d"), data_final.strftime("%Y-%m-%d")))
            if df.empty:
                st.warning("Nenhum dado encontrado no banco pcvendedor.db para o período selecionado.")
            df['DATAPEDIDO'] = pd.to_datetime(df['DATAPEDIDO']).dt.date
            return df
        except sqlite3.Error as e:
            st.error(f"Erro ao conectar ao banco pcvendedor.db: {e}")
            return pd.DataFrame()
        finally:
            if conn:
                conn.close()

    # Função para obter dados (com cache)
    def get_data(data_inicial, data_final):
        force_refresh = st.session_state.get("force_refresh", False)
        
        if not force_refresh and 'cached_data' in st.session_state and st.session_state.cached_data is not None:
            if (st.session_state.cached_range[0] <= data_inicial and
                st.session_state.cached_range[1] >= data_final):
                df = st.session_state.cached_data
                return df[(df['DATAPEDIDO'] >= pd.to_datetime(data_inicial)) & 
                         (df['DATAPEDIDO'] <= pd.to_datetime(data_final))]
        
        df = fetch_data(data_inicial, data_final)
        if not df.empty:
            df['DATAPEDIDO'] = pd.to_datetime(df['DATAPEDIDO'])
            st.session_state.cached_data = df
            st.session_state.cached_range = (data_inicial, data_final)
        st.session_state["force_refresh"] = False
        return df

    # Processar dados para o relatório de resumo
    def process_summary_data(df, data_inicial, data_final):
        if df.empty:
            st.warning("Nenhum dado retornado do endpoint para o período selecionado.")
            return pd.DataFrame(), {}, pd.DataFrame()
        
        required_columns = ['DATAPEDIDO', 'VALOR', 'QUANTIDADE', 'CODIGOVENDA', 'CODFORNECEDOR', 
                        'CODPRODUTO', 'CUSTOPRODUTO', 'PEDIDO', 'CODUSUR', 'VENDEDOR', 
                        'CODCLIENTE', 'ROTA']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            st.warning(f"Colunas ausentes: {missing_columns}. Verifique o banco de dados.")
            return pd.DataFrame(), {}, pd.DataFrame()
        
        df = df[df['DATAPEDIDO'].between(pd.to_datetime(data_inicial), pd.to_datetime(data_final))]
        
        # Convert relevant columns to numeric, handling errors
        df['VALOR'] = pd.to_numeric(df['VALOR'], errors='coerce').fillna(0)
        df['QUANTIDADE'] = pd.to_numeric(df['QUANTIDADE'], errors='coerce').fillna(0)
        df['CODIGOVENDA'] = pd.to_numeric(df['CODIGOVENDA'], errors='coerce').fillna(1)
        df['CODFORNECEDOR'] = pd.to_numeric(df['CODFORNECEDOR'], errors='coerce').fillna(0)
        df['CODPRODUTO'] = pd.to_numeric(df['CODPRODUTO'], errors='coerce').fillna(0)
        df['CUSTOPRODUTO'] = pd.to_numeric(df['CUSTOPRODUTO'], errors='coerce').fillna(0)
        
        # Remove duplicates based on PEDIDO and CODPRODUTO
        df = df.drop_duplicates(subset=['PEDIDO', 'CODPRODUTO'])
        
        # Apply BRITVIC supplier code for relevant products
        df.loc[df['CODPRODUTO'].isin(britvic_product_codes), 'CODFORNECEDOR'] = BRITVIC_TEMP_CODE
        
        # Map suppliers
        supplier_map = default_supplier_names
        df['FORNECEDOR'] = df['CODFORNECEDOR'].map(supplier_map).fillna(df['CODFORNECEDOR'].astype(str))
        
        # Calculate DENTRO_ROTA
        df['DENTRO_ROTA'] = df.apply(lambda row: is_pedido_dentro_rota(row['DATAPEDIDO'], row['ROTA']), axis=1)
        
        # Calculate unique orders inside and outside route
        pedidos_dentro_rota = df[df['DENTRO_ROTA']].groupby(['CODUSUR', 'VENDEDOR'])['PEDIDO'].nunique().reset_index(name='PEDIDOS_DENTRO_ROTA')
        pedidos_fora_rota = df[~df['DENTRO_ROTA']].groupby(['CODUSUR', 'VENDEDOR'])['PEDIDO'].nunique().reset_index(name='PEDIDOS_FORA_ROTA')
        
        # Earliest order date
        earliest_date = df.groupby(['CODUSUR', 'VENDEDOR'])['DATAPEDIDO'].min().reset_index()
        earliest_date['DATAPEDIDO'] = earliest_date['DATAPEDIDO'].dt.strftime('%d/%m/%Y')
        
        # Orders with bonification
        pedidos_bonific = df[df['CODIGOVENDA'] != 1].groupby(['CODUSUR', 'VENDEDOR'])['PEDIDO'].nunique().reset_index(name='PEDIDOS_COM_BONIFICACAO')
        
        # Filter out bonified orders for financial calculations
        bonified_pedidos = df[df['CODIGOVENDA'] != 1]['PEDIDO'].unique()
        df_non_bonific = df[~df['PEDIDO'].isin(bonified_pedidos)]
        
        # Calculate total sales and cost
        df_non_bonific['TOTAL_ROW_VENDA'] = df_non_bonific['VALOR'] * df_non_bonific['QUANTIDADE']
        df_non_bonific['TOTAL_ROW_CUSTO'] = df_non_bonific['CUSTOPRODUTO'] * df_non_bonific['QUANTIDADE']
        
        total_vendido = df_non_bonific.groupby(['CODUSUR', 'VENDEDOR'])['TOTAL_ROW_VENDA'].sum().reset_index(name='TOTAL_VENDIDO')
        total_custo = df_non_bonific.groupby(['CODUSUR', 'VENDEDOR'])['TOTAL_ROW_CUSTO'].sum().reset_index(name='TOTAL_CUSTO')
        
        # Calculate positivacao (unique clients per supplier, only for positive quantities)
        df_positivacao = df[
            (df['QUANTIDADE'] > 0) &  # Only count sales, not returns
            (df['CODFORNECEDOR'].isin(default_supplier_names.keys()) | df['CODPRODUTO'].isin(britvic_product_codes))
        ]
        positivacao = df_positivacao.groupby(['CODUSUR', 'VENDEDOR', 'FORNECEDOR'])['CODCLIENTE'].nunique().reset_index(name='POSITIVACAO')
        
        # Pivot positivacao data
        positivacao_pivot = positivacao.pivot_table(
            index=['CODUSUR', 'VENDEDOR'],
            columns='FORNECEDOR',
            values='POSITIVACAO',
            aggfunc='sum',
            fill_value=0
        ).reset_index()
        
        # Ensure all suppliers are present in the pivot table
        for supplier in ordered_suppliers:
            if supplier not in positivacao_pivot.columns:
                positivacao_pivot[supplier] = 0
        
        positivacao_pivot = positivacao_pivot[['CODUSUR', 'VENDEDOR'] + ordered_suppliers]
        
        # Merge all data
        result = pedidos_bonific.merge(total_vendido, on=['CODUSUR', 'VENDEDOR'], how='outer')
        result = result.merge(total_custo, on=['CODUSUR', 'VENDEDOR'], how='outer')
        result = result.merge(positivacao_pivot, on=['CODUSUR', 'VENDEDOR'], how='outer')
        result = result.merge(pedidos_dentro_rota, on=['CODUSUR', 'VENDEDOR'], how='outer')
        result = result.merge(pedidos_fora_rota, on=['CODUSUR', 'VENDEDOR'], how='outer')
        result = result.merge(earliest_date, on=['CODUSUR', 'VENDEDOR'], how='outer')
        
        # Calculate total orders and financial metrics
        result['TOTAL'] = result['PEDIDOS_DENTRO_ROTA'].fillna(0) + result['PEDIDOS_FORA_ROTA'].fillna(0)
        result['MARKUP_TOTAL'] = ((result['TOTAL_VENDIDO'] - result['TOTAL_CUSTO']) / result['TOTAL_CUSTO'] * 100).round(2)
        result['MARGEM_TOTAL'] = ((result['TOTAL_VENDIDO'] - result['TOTAL_CUSTO']) / result['TOTAL_VENDIDO'] * 100).round(2)
        
        # Handle infinite and NaN values
        result['MARKUP_TOTAL'] = result['MARKUP_TOTAL'].replace([float('inf'), -float('inf')], 0).fillna(0)
        result['MARGEM_TOTAL'] = result['MARGEM_TOTAL'].replace([float('inf'), -float('inf')], 0).fillna(0)
        result = result.fillna(0)
        
        # Define column order
        columns_order = ['DATAPEDIDO', 'CODUSUR', 'VENDEDOR', 'PEDIDOS_DENTRO_ROTA', 'PEDIDOS_FORA_ROTA', 'TOTAL', 
                        'PEDIDOS_COM_BONIFICACAO', 'TOTAL_VENDIDO', 'MARKUP_TOTAL', 'MARGEM_TOTAL'] + ordered_suppliers
        result = result[columns_order]
        
        # Format columns
        result['PEDIDOS_COM_BONIFICACAO'] = result['PEDIDOS_COM_BONIFICACAO'].astype(int)
        result['PEDIDOS_DENTRO_ROTA'] = result['PEDIDOS_DENTRO_ROTA'].astype(int)
        result['PEDIDOS_FORA_ROTA'] = result['PEDIDOS_FORA_ROTA'].astype(int)
        result['TOTAL'] = result['TOTAL'].astype(int)
        result['TOTAL_VENDIDO'] = result['TOTAL_VENDIDO'].round(2)
        
        result['TOTAL_VENDIDO'] = result['TOTAL_VENDIDO'].apply(lambda x: locale.format_string("%.2f", x, grouping=True))
        result['MARKUP_TOTAL'] = result['MARKUP_TOTAL'].apply(lambda x: f"{x:.2f}%")
        result['MARGEM_TOTAL'] = result['MARGEM_TOTAL'].apply(lambda x: f"{x:.2f}%")
        
        return result, supplier_map, df

    # Processar dados para detalhes dos pedidos
    def process_detailed_orders(df, data_inicial, data_final, supplier_map):
        if df.empty:
            return pd.DataFrame()
        
        required_columns = ['DATAPEDIDO', 'VALOR', 'QUANTIDADE', 'CUSTOPRODUTO', 'CODPRODUTO', 
                        'CODFORNECEDOR', 'CODIGOVENDA', 'CODCLIENTE', 'CODUSUR', 'VENDEDOR', 'PEDIDO']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            st.warning(f"Colunas ausentes: {missing_columns}. Verifique o banco de dados.")
            return pd.DataFrame()
        
        df = df[df['DATAPEDIDO'].between(pd.to_datetime(data_inicial), pd.to_datetime(data_final))]
        
        # Convert QUANTIDADE, removing non-numeric characters and preserving sign
        df['QUANTIDADE'] = df['QUANTIDADE'].astype(str).str.replace(r'[^\d.-]', '', regex=True)
        df['QUANTIDADE'] = pd.to_numeric(df['QUANTIDADE'], errors='coerce').fillna(0)
        
        df['VALOR'] = pd.to_numeric(df['VALOR'], errors='coerce').fillna(0)
        df['CUSTOPRODUTO'] = pd.to_numeric(df['CUSTOPRODUTO'], errors='coerce').fillna(0)
        df['CODPRODUTO'] = pd.to_numeric(df['CODPRODUTO'], errors='coerce').fillna(0)
        df['CODFORNECEDOR'] = pd.to_numeric(df['CODFORNECEDOR'], errors='coerce').fillna(0)
        df['CODIGOVENDA'] = pd.to_numeric(df['CODIGOVENDA'], errors='coerce').fillna(1)
        df['CODCLIENTE'] = pd.to_numeric(df['CODCLIENTE'], errors='coerce').fillna(0)
        
        df.loc[df['CODPRODUTO'].isin(britvic_product_codes), 'CODFORNECEDOR'] = BRITVIC_TEMP_CODE
        
        def get_fornecedor(row, supplier_map):
            if row['CODFORNECEDOR'] in supplier_map:
                return supplier_map[row['CODFORNECEDOR']]
            elif 'FORNECEDOR' in df.columns and pd.notnull(row['FORNECEDOR']):
                return row['FORNECEDOR']
            return str(row['CODFORNECEDOR'])
        
        df['FORNECEDOR'] = df.apply(lambda row: get_fornecedor(row, supplier_map), axis=1)
        
        
        
        df['BONIFICACAO'] = df['CODIGOVENDA'].apply(lambda x: 'Sim' if x != 1 else 'Não')
        
        # Recalculate totals with net quantity
        df['VENDA_TOTAL'] = df['VALOR'] * df['QUANTIDADE']
        df['CUSTO_TOTAL'] = df['CUSTOPRODUTO'] * df['QUANTIDADE']
        
        df['MARGEM'] = ((df['VENDA_TOTAL'] - df['CUSTO_TOTAL']) / df['VENDA_TOTAL'] * 100).round(2)
        df['MARKUP'] = ((df['VENDA_TOTAL'] - df['CUSTO_TOTAL']) / df['CUSTO_TOTAL'] * 100).round(2)
        df['MARGEM'] = df['MARGEM'].replace([float('inf'), -float('inf')], 0).fillna(0)
        df['MARKUP'] = df['MARKUP'].replace([float('inf'), -float('inf')], 0).fillna(0)
        
        df['DATAPEDIDO'] = df['DATAPEDIDO'].dt.strftime('%d/%m/%Y')
        
        if 'PRODUTO' not in df.columns:
            df['PRODUTO'] = df['CODPRODUTO'].apply(lambda x: f"Produto_{x}")
        
        columns = [
            'DATAPEDIDO', 'SUPERVISOR', 'CODUSUR', 'VENDEDOR', 'CODCLIENTE', 'CLIENTE', 'FANTASIA', 'RAMO', 'PEDIDO',
            'BONIFICACAO', 'QUANTIDADE', 'VALOR', 'CUSTOPRODUTO', 'VENDA_TOTAL', 'CUSTO_TOTAL',
            'MARGEM', 'MARKUP', 'CODPRODUTO', 'PRODUTO', 'FORNECEDOR'
        ]
        available_columns = [col for col in columns if col in df.columns]
        result_df = df[available_columns].copy()
        
        rename_map = {
            'CODCLIENTE': 'CODCLI',
            'VALOR': 'PREÇO',
            'CUSTOPRODUTO': 'CUSTO'
        }
        result_df.rename(columns=rename_map, inplace=True)
        
        for supplier in ordered_suppliers:
            result_df[supplier] = result_df['FORNECEDOR'].apply(lambda x: 'S' if x == supplier else 'N')
        
        result_df['PREÇO'] = result_df['PREÇO'].apply(lambda x: locale.format_string("%.2f", x, grouping=True))
        result_df['CUSTO'] = result_df['CUSTO'].apply(lambda x: locale.format_string("%.2f", x, grouping=True))
        result_df['VENDA_TOTAL'] = result_df['VENDA_TOTAL'].apply(lambda x: locale.format_string("%.2f", x, grouping=True))
        result_df['CUSTO_TOTAL'] = result_df['CUSTO_TOTAL'].apply(lambda x: locale.format_string("%.2f", x, grouping=True))
        result_df['MARGEM'] = result_df['MARGEM'].apply(lambda x: f"{x:.2f}%")
        result_df['MARKUP'] = result_df['MARKUP'].apply(lambda x: f"{x:.2f}%")
        
        result_df.sort_values(['DATAPEDIDO', 'VENDEDOR', 'PEDIDO'], inplace=True)
        
        return result_df
    # Processar dados para resumo por ano/mês
    def process_year_month_summary(df, selected_year, selected_month):
        if df.empty:
            st.warning("Nenhum dado retornado para o período selecionado (Ano/Mês).")
            return pd.DataFrame()
        
        required_columns = ['DATAPEDIDO', 'VALOR', 'QUANTIDADE', 'CUSTOPRODUTO', 'CODIGOVENDA', 
                           'CODCLIENTE', 'VENDEDOR', 'CODUSUR', 'CLIENTE', 'FANTASIA', 'RAMO', 'PEDIDO']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            return pd.DataFrame()
        
        df = df[(df['DATAPEDIDO'].dt.year == selected_year) & (df['DATAPEDIDO'].dt.month == selected_month)]
        
        df['VALOR'] = pd.to_numeric(df['VALOR'], errors='coerce').fillna(0)
        df['QUANTIDADE'] = pd.to_numeric(df['QUANTIDADE'], errors='coerce').fillna(0)
        df['CUSTOPRODUTO'] = pd.to_numeric(df['CUSTOPRODUTO'], errors='coerce').fillna(0)
        df['CODIGOVENDA'] = pd.to_numeric(df['CODIGOVENDA'], errors='coerce').fillna(1)
        df['CODCLIENTE'] = pd.to_numeric(df['CODCLIENTE'], errors='coerce').fillna(0)
        
        bonified_pedidos = df[df['CODIGOVENDA'] != 1]['PEDIDO'].unique()
        df_non_bonific = df[~df['PEDIDO'].isin(bonified_pedidos)]
        
        df_non_bonific['FATURAMENTO_CLIENTE'] = df_non_bonific['VALOR'] * df_non_bonific['QUANTIDADE']
        df_non_bonific['CUSTO_MERCADORIA'] = df_non_bonific['CUSTOPRODUTO'] * df_non_bonific['QUANTIDADE']
        
        summary = df_non_bonific.groupby(['CODUSUR', 'VENDEDOR', 'CODCLIENTE', 'CLIENTE', 'FANTASIA', 'RAMO']).agg({
            'FATURAMENTO_CLIENTE': 'sum',
            'CUSTO_MERCADORIA': 'sum'
        }).reset_index()
        
        summary['CONT_MARG'] = summary['FATURAMENTO_CLIENTE'] - summary['CUSTO_MERCADORIA']
        summary['MARGEM'] = (summary['CONT_MARG'] / summary['FATURAMENTO_CLIENTE'] * 100).round(2)
        
        summary['MARGEM'] = summary['MARGEM'].replace([float('inf'), -float('inf')], 0).fillna(0)
        
        summary['CODCLIENTE'] = summary['CODCLIENTE'].astype(int)
        summary['FATURAMENTO_CLIENTE'] = summary['FATURAMENTO_CLIENTE'].apply(lambda x: locale.format_string("%.2f", x, grouping=True))
        summary['CUSTO_MERCADORIA'] = summary['CUSTO_MERCADORIA'].apply(lambda x: locale.format_string("%.2f", x, grouping=True))
        summary['CONT_MARG'] = summary['CONT_MARG'].apply(lambda x: locale.format_string("%.2f", x, grouping=True))
        summary['MARGEM'] = summary['MARGEM'].apply(lambda x: f"{x:.2f}%")
        
        summary.sort_values(['VENDEDOR', 'CODCLIENTE'], inplace=True)
        
        return summary

    # Lógica principal do aplicativo
    summary_placeholder = st.empty()
    detailed_placeholder = st.empty()
    year_month_placeholder = st.empty()

    default_start_date = date.today()
    default_end_date = date.today()

    if 'summary_reports' not in st.session_state:
        st.session_state.summary_reports = []
    if 'detailed_reports' not in st.session_state:
        st.session_state.detailed_reports = []
    if 'year_month_summaries' not in st.session_state:
        st.session_state.year_month_summaries = []

    # Limpar relatórios anteriores
    st.session_state.summary_reports = []
    st.session_state.detailed_reports = []
    st.session_state.year_month_summaries = []

    # Seção de Resumo
    with summary_placeholder.container():
        st.subheader("Resultados - Resumo")
        col1, col2 = st.columns(2)
        with col1:
            data_inicial_1 = st.date_input("Data Inicial (Resumo)", value=default_start_date, key="data_inicial_1")
        with col2:
            data_final_1 = st.date_input("Data Final (Resumo)", value=default_end_date, key="data_final_1")
        
        st.markdown(f"**Data Inicial:** {data_inicial_1.strftime('%d/%m/%Y')} | **Data Final:** {data_final_1.strftime('%d/%m/%Y')}")
        
        with st.spinner("Carregando resumo..."):
            df = get_data(data_inicial_1, data_final_1)
            if not df.empty:
                result_df, supplier_map, raw_df = process_summary_data(df, data_inicial_1, data_final_1)
                if not result_df.empty:
                    st.session_state.summary_reports.append({
                        'data_inicial': data_inicial_1,
                        'data_final': data_final_1,
                        'result_df': result_df,
                        'supplier_map': supplier_map,
                        'raw_df': raw_df
                    })
                else:
                    st.warning("Nenhum dado processado para o período selecionado (Resumo).")
            else:
                st.warning("Nenhum dado retornado do endpoint (Resumo).")

        for idx, report in enumerate(st.session_state.summary_reports):
            with st.container():
                result_df = report['result_df']
                
                gb = GridOptionsBuilder.from_dataframe(result_df)
                gb.configure_default_column(editable=False, filter=True, sortable=True, resizable=True)
                gb.configure_column("DATAPEDIDO", header_name="Data Pedido", width=120, filter="agDateColumnFilter")
                gb.configure_column("CODUSUR", header_name="Código Usuário", width=120)
                gb.configure_column("VENDEDOR", header_name="Vendedor", width=150)
                gb.configure_column("PEDIDOS_DENTRO_ROTA", header_name="Pedidos Dentro Rota", width=150)
                gb.configure_column("PEDIDOS_FORA_ROTA", header_name="Pedidos Fora Rota", width=150)
                gb.configure_column("TOTAL", header_name="Total Pedidos", width=120)
                gb.configure_column("PEDIDOS_COM_BONIFICACAO", header_name="Pedidos com Bonificação", width=150)
                gb.configure_column("TOTAL_VENDIDO", header_name="Total Vendido (R$)", width=150)
                gb.configure_column("MARKUP_TOTAL", header_name="Markup Total (%)", width=120)
                gb.configure_column("MARGEM_TOTAL", header_name="Margem Total (%)", width=120)
                for supplier in ordered_suppliers:
                    gb.configure_column(supplier, header_name=supplier, width=120)
                
                grid_options = gb.build()
                
                AgGrid(
                    result_df,
                    gridOptions=grid_options,
                    height=400,
                    fit_columns_on_grid_load=False,
                    update_mode=GridUpdateMode.NO_UPDATE,
                    allow_unsafe_jscode=True
                )
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    result_df.to_excel(writer, index=False, sheet_name='Relatório')
                excel_data = output.getvalue()
                st.download_button(
                    label="Baixar Resumo como Excel",
                    data=excel_data,
                    file_name=f"relatorio_vendas_positivacao_{report['data_inicial'].strftime('%Y%m%d')}_{report['data_final'].strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"download_summary_{idx}"
                )

    # Seção de Detalhes dos Pedidos
    with detailed_placeholder.container():
        st.subheader("Detalhes dos Pedidos por Vendedor")
        col1, col2 = st.columns(2)
        with col1:
            data_inicial_2 = st.date_input("Data Inicial (Detalhes)", value=default_start_date, key="data_inicial_2")
        with col2:
            data_final_2 = st.date_input("Data Final (Detalhes)", value=default_end_date, key="data_final_2")
        
        st.markdown(f"**Data Inicial:** {data_inicial_2.strftime('%d/%m/%Y')} | **Data Final:** {data_final_2.strftime('%d/%m/%Y')}")
        
        with st.spinner("Carregando detalhes..."):
            df = get_data(data_inicial_2, data_final_2)
            if not df.empty:
                _, supplier_map, raw_df = process_summary_data(df, data_inicial_2, data_final_2)
                detailed_df = process_detailed_orders(raw_df, data_inicial_2, data_final_2, supplier_map)
                if not detailed_df.empty:
                    st.session_state.detailed_reports.append({
                        'data_inicial': data_inicial_2,
                        'data_final': data_final_2,
                        'detailed_df': detailed_df,
                        'supplier_map': supplier_map
                    })
                else:
                    st.warning("Nenhum dado detalhado processado para o período selecionado (Detalhes).")
            else:
                st.warning("Nenhum dado retornado do endpoint (Detalhes).")

        for idx, report in enumerate(st.session_state.detailed_reports):
            with st.container():
                detailed_df = report['detailed_df']
                
                gb = GridOptionsBuilder.from_dataframe(detailed_df)
                gb.configure_default_column(editable=False, filter=True, sortable=True, resizable=True)
                gb.configure_column("DATAPEDIDO", header_name="Data Pedido", width=120, filter="agDateColumnFilter")
                gb.configure_column("SUPERVISOR", header_name="Supervisor", width=150)
                gb.configure_column("VENDEDOR", header_name="Vendedor", width=150)
                gb.configure_column("CODCLI", header_name="Cód. Cliente", width=120)
                gb.configure_column("CLIENTE", header_name="Cliente", width=120)
                gb.configure_column("FANTASIA", header_name="Fantasia", width=150)
                gb.configure_column("PEDIDO", header_name="Pedido", width=120)
                gb.configure_column("BONIFICACAO", header_name="Bonificação", width=100)
                gb.configure_column("QUANTIDADE", header_name="Quantidade", width=100)
                gb.configure_column("PREÇO", header_name="Preço (R$)", width=120)
                gb.configure_column("CUSTO", header_name="Custo (R$)", width=120)
                gb.configure_column("VENDA_TOTAL", header_name="Venda Total (R$)", width=120)
                gb.configure_column("CUSTO_TOTAL", header_name="Custo Total (R$)", width=120)
                gb.configure_column("MARGEM", header_name="Margem (%)", width=100)
                gb.configure_column("MARKUP", header_name="Markup (%)", width=100)
                gb.configure_column("CODPRODUTO", header_name="Cód. Produto", width=120)
                gb.configure_column("PRODUTO", header_name="Produto", width=200)
                gb.configure_column("FORNECEDOR", header_name="Fornecedor", width=150)
                for supplier in ordered_suppliers:
                    gb.configure_column(supplier, header_name=supplier, width=120)
                
                grid_options = gb.build()
                
                AgGrid(
                    detailed_df,
                    gridOptions=grid_options,
                    height=500,
                    fit_columns_on_grid_load=False,
                    update_mode=GridUpdateMode.NO_UPDATE,
                    allow_unsafe_jscode=True
                )
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    detailed_df.to_excel(writer, index=False, sheet_name='Detalhes_Pedidos')
                excel_data = output.getvalue()
                st.download_button(
                    label="Baixar Detalhes como Excel",
                    data=excel_data,
                    file_name=f"detalhes_pedidos_{report['data_inicial'].strftime('%Y%m%d')}_{report['data_final'].strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"download_detailed_{idx}"
                )

    # Seção de Resumo por Ano/Mês
    with year_month_placeholder.container():
        st.subheader("Resumo por Ano e Mês")
        
        year_month_start = date(2024, 1, 1)
        year_month_end = date.today()
        
        with st.spinner("Carregando dados para resumo por ano/mês..."):
            df = get_data(year_month_start, year_month_end)
        
        if not df.empty:
            df['DATAPEDIDO'] = pd.to_datetime(df['DATAPEDIDO'])
            available_years = list(range(2024, date.today().year + 1))
            available_months = sorted(df['DATAPEDIDO'].dt.month.unique())
            
            current_year = date.today().year
            current_month = date.today().month
            
            col1, col2 = st.columns(2)
            with col1:
                selected_year = st.selectbox(
                    "Selecione o Ano", 
                    available_years, 
                    index=available_years.index(current_year) if current_year in available_years else len(available_years)-1, 
                    key="year_select"
                )
            with col2:
                month_names = {
                    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio", 6: "Junho",
                    7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
                }
                month_options = [(m, month_names[m]) for m in available_months]
                selected_month = st.selectbox(
                    "Selecione o Mês", 
                    options=[m[0] for m in month_options], 
                    format_func=lambda x: month_names[x], 
                    index=[m[0] for m in month_options].index(current_month) if current_month in available_months else len(month_options)-1, 
                    key="month_select"
                )
            
            with st.spinner("Processando resumo por ano/mês..."):
                year_month_summary = process_year_month_summary(df, selected_year, selected_month)
                if not year_month_summary.empty:
                    st.session_state.year_month_summaries.append({
                        'year': selected_year,
                        'month': selected_month,
                        'month_name': month_names[selected_month],
                        'data': year_month_summary
                    })
                else:
                    st.warning("Nenhum dado processado para o ano/mês selecionado.")
        
        for idx, summary_info in enumerate(st.session_state.year_month_summaries):
            with st.container():
                st.markdown(f"### Resumo: {summary_info['month_name']} {summary_info['year']}")
                year_month_summary = summary_info['data']
                
                gb = GridOptionsBuilder.from_dataframe(year_month_summary)
                gb.configure_default_column(editable=False, filter=True, sortable=True, resizable=True)
                gb.configure_column("CODUSUR", header_name="Código Usuário", width=120)
                gb.configure_column("VENDEDOR", header_name="Vendedor", width=150)
                gb.configure_column("CODCLIENTE", header_name="Cód. Cliente", width=120)
                gb.configure_column("CLIENTE", header_name="Cliente", width=200)
                gb.configure_column("FANTASIA", header_name="Fantasia", width=200)
                gb.configure_column("RAMO", header_name="Ramo", width=150)
                gb.configure_column("FATURAMENTO_CLIENTE", header_name="Faturamento Cliente (R$)", width=200)
                gb.configure_column("CUSTO_MERCADORIA", header_name="Custo Mercadoria (R$)", width=200)
                gb.configure_column("CONT_MARG", header_name="Contribuição Margem (R$)", width=200)
                gb.configure_column("MARGEM", header_name="Margem (%)", width=120)
                
                grid_options = gb.build()
                
                AgGrid(
                    year_month_summary,
                    gridOptions=grid_options,
                    height=400,
                    fit_columns_on_grid_load=False,
                    update_mode=GridUpdateMode.NO_UPDATE,
                    allow_unsafe_jscode=True
                )
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    year_month_summary.to_excel(writer, index=False, sheet_name='Resumo_Ano_Mes')
                excel_data = output.getvalue()
                st.download_button(
                    label="Baixar Resumo Ano/Mês como Excel",
                    data=excel_data,
                    file_name=f"resumo_vendas_{summary_info['year']}_{summary_info['month']}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"download_year_month_{idx}"
                )

if __name__ == "__main__":
    main()