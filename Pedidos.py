import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, date
import locale
import sqlite3
import logging
from dateutil.relativedelta import relativedelta
from streamlit_autorefresh import st_autorefresh
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Set locale for currency formatting
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except locale.Error:
    logger.warning("Locale 'pt_BR.UTF-8' não disponível, usando padrão.")
    locale.setlocale(locale.LC_ALL, '')

# Configuração do caminho para os arquivos .db
DB_PATH = "database\\"  # Ajuste para o caminho real da pasta 'database' com barra final

def get_sqlite_connection(db_file):
    """Estabelece conexão com o banco SQLite."""
    try:
        connection = sqlite3.connect(f"{DB_PATH}{db_file}")
        logger.info(f"Conexão com {db_file} estabelecida com sucesso")
        return connection
    except sqlite3.Error as e:
        logger.error(f"Erro ao conectar ao {db_file}: {e}")
        st.error(f"Erro ao conectar ao {db_file}: {e}")
        return None

def fetch_pcmovendpend_data(data_inicial, data_final):
    """Busca dados de pcmovendpend diretamente do banco SQLite (pcmovendpend.db)."""
    pcmovendpend_sql = """
        SELECT 
            DTINICIOOS,
            DTFIMOS,
            CONFERENTE,
            STATUS
        FROM pcmovendpend
        WHERE DATE(DATA) BETWEEN ? AND ?
        ORDER BY DTFIMOS
    """
    
    connection = get_sqlite_connection("pcmovendpend.db")
    if connection is None:
        return pd.DataFrame()
    
    try:
        cursor = connection.cursor()
        cursor.execute(pcmovendpend_sql, (data_inicial.strftime('%Y-%m-%d'), data_final.strftime('%Y-%m-%d')))
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        connection.close()
        
        df['DTINICIOOS'] = pd.to_datetime(df['DTINICIOOS'], errors='coerce', format='%Y-%m-%d')
        df['DTFIMOS'] = pd.to_datetime(df['DTFIMOS'], errors='coerce', format='%Y-%m-%d')
        logger.info(f"Columns in pcmovendpend_df: {df.columns.tolist()}")
        return df
    except sqlite3.Error as e:
        logger.error(f"Erro ao buscar dados de pcmovendpend: {e}")
        st.error(f"Erro ao buscar dados de pcmovendpend: {e}")
        if connection:
            connection.close()
        return pd.DataFrame()
    except ValueError as e:
        logger.error(f"Erro ao converter datas em pcmovendpend: {e}")
        st.error(f"Erro ao converter datas em pcmovendpend: {e}")
        if connection:
            connection.close()
        return pd.DataFrame()

def fetch_pending_orders_data():
    """Busca todos os dados de pedidos pendentes e em conferência com QTDITENS."""
    detailed_sql = """
        SELECT 
            NUMPED,
            NUMCAR,
            TOTVOL,
            NUMTRANSWMS,
            CODCLIENTE,
            CLIENTE,
            DTINICIOOS,
            DTFIMOS,
            CONFERENTE,
            STATUS,
            ROTA,
            QTDITENS
        FROM PCMOVENDPEND
        WHERE STATUS IN ('NÃO INICIADO', 'EM CONFERÊNCIA')
    """
    
    connection = get_sqlite_connection("pcmovendpend.db")
    if connection is None:
        return pd.DataFrame()
    
    try:
        df = pd.read_sql_query(detailed_sql, connection)
        connection.close()
        
        if df.empty:
            logger.warning("Nenhum pedido pendente ou em conferência encontrado.")
        
        df['DTINICIOOS'] = pd.to_datetime(df['DTINICIOOS'], errors='coerce')
        df['DTFIMOS'] = pd.to_datetime(df['DTFIMOS'], errors='coerce')
        df['QTDITENS'] = pd.to_numeric(df['QTDITENS'], errors='coerce').fillna(0).astype(int)
        logger.info(f"Columns in pending_orders_df: {df.columns.tolist()}")
        return df
    except sqlite3.Error as e:
        logger.error(f"Erro ao buscar dados pendentes: {e}")
        st.error(f"Erro ao buscar dados pendentes: {e}")
        if connection:
            connection.close()
        return pd.DataFrame()
    except ValueError as e:
        logger.error(f"Erro ao converter dados pendentes: {e}")
        st.error(f"Erro ao converter dados pendentes: {e}")
        if connection:
            connection.close()
        return pd.DataFrame()

def fetch_pcpedc_data(data_inicial, data_final):
    """Busca dados de pcpedc diretamente do banco SQLite (pcpedc_posicao.db)."""
    pcpedc_sql = """
        SELECT 
            ROTA,
            DATA,
            L_COUNT,
            M_COUNT,
            F_COUNT,
            DESCRICAO
        FROM pcpedc_posicao
        WHERE DATE(DATA) BETWEEN ? AND ?
        ORDER BY DATA
    """
    
    connection = get_sqlite_connection("pcpedc_posicao.db")
    if connection is None:
        return pd.DataFrame()
    
    try:
        cursor = connection.cursor()
        cursor.execute(pcpedc_sql, (data_inicial.strftime('%Y-%m-%d'), data_final.strftime('%Y-%m-%d')))
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        connection.close()
        
        df['DATA'] = pd.to_datetime(df['DATA'], errors='coerce', format='%Y-%m-%d')
        df['L_COUNT'] = pd.to_numeric(df['L_COUNT'], errors='coerce')
        df['M_COUNT'] = pd.to_numeric(df['M_COUNT'], errors='coerce')
        df['F_COUNT'] = pd.to_numeric(df['F_COUNT'], errors='coerce')
        df['ROTA'] = pd.to_numeric(df['ROTA'], errors='coerce')
        logger.info(f"Columns in pcpedc_df: {df.columns.tolist()}")
        return df
    except sqlite3.Error as e:
        logger.error(f"Erro ao buscar dados de pcpedc_posicao: {e}")
        st.error(f"Erro ao buscar dados de pcpedc_posicao: {e}")
        if connection:
            connection.close()
        return pd.DataFrame()
    except ValueError as e:
        logger.error(f"Erro ao converter datas ou números em pcpedc_posicao: {e}")
        st.error(f"Erro ao converter datas ou números em pcpedc_posicao: {e}")
        if connection:
            connection.close()
        return pd.DataFrame()

def formatar_valor(valor):
    """Formata valores monetários."""
    try:
        return locale.currency(valor, grouping=True)
    except:
        logger.warning(f"Erro ao formatar valor: {valor}")
        return valor

def process_data(data):
    """Processa dados e agrupa por dia e total."""
    try:
        if not data.empty:
            data['DTFIMOS'] = pd.to_datetime(data['DTFIMOS'], errors='coerce')
            data['DIA'] = data['DTFIMOS'].dt.date
            daily_data = data[data['STATUS'] == 'CONCLUÍDA'].groupby(['CONFERENTE', 'DIA']).size().reset_index(name='PEDIDOS CONFERIDOS')
            total_data = data[data['STATUS'] == 'CONCLUÍDA'].groupby('CONFERENTE').size().reset_index(name='PEDIDOS_TOTAL')
            return daily_data, total_data
        return pd.DataFrame(), pd.DataFrame()
    except Exception as e:
        logger.error(f"Erro ao processar dados: {e}")
        st.error(f"Erro ao processar dados: {e}")
        return pd.DataFrame(), pd.DataFrame()

def clear_weekly_data():
    """Limpa os dados de pedidos concluídos da semana anterior no domingo à noite."""
    current_time = datetime.now()
    if current_time.weekday() == 6 and current_time.hour >= 20:
        connection = get_sqlite_connection("pcmovendpend.db")
        if connection:
            try:
                cursor = connection.cursor()
                cursor.execute("DELETE FROM PCMOVENDPEND WHERE STATUS = 'CONCLUÍDA' AND DATE(DTFIMOS) < ?", (date.today() - timedelta(days=7),))
                connection.commit()
                logger.info("Dados da semana anterior limpos com sucesso.")
            except sqlite3.Error as e:
                logger.error(f"Erro ao limpar dados: {e}")
                st.error(f"Erro ao limpar dados: {e}")
            finally:
                connection.close()

def main():
    # Custom CSS for beautiful and organized layout
    st.markdown("""
    <style>
        /* General styling */
        .stApp {
            background-color: #0F172A;
            color: #E2E8F0;
        }
        h1, h2, h3, h4 {
            color: #38BDF8;
            text-align: center;
            font-family: 'Segoe UI', sans-serif;
            text-shadow: 1px 1px 2px rgba(0, 0, 0, 0.3);
        }

        /* Card styling */
        .card {
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            padding: 12px;
            background: linear-gradient(135deg, #1E293B, #334155);
            border-radius: 12px;
            box-shadow: 0 6px 15px rgba(0, 0, 0, 0.4);
            width: 100%;
            text-align: center;
            height: auto;
            min-height: 180px;
            color: #E2E8F0;
            margin: 8px;
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }
        .card:hover {
            transform: scale(1.05);
            box-shadow: 0 8px 20px rgba(0, 0, 0, 0.5);
        }
        .title {
            font-size: 18px;
            font-weight: bold;
            margin-top: 8px;
            color: #60A5FA;
            text-transform: uppercase;
        }
        .number {
            font-size: 24px;
            font-weight: 600;
            margin: 6px 0;
            color: #34D399;
            text-shadow: 1px 1px 3px rgba(0, 0, 0, 0.2);
        }

        /* Table styling */
        .scrollable-table {
            max-height: 500px;
            overflow-y: auto;
            display: block;
            border: 2px solid #334155;
            border-radius: 8px;
            background: linear-gradient(135deg, #1E293B, #2A3346);
            width: 100%;
            margin: 10px 0;
        }
        table {
            width: 100% !important;
            border-collapse: collapse;
            font-size: 14px;
        }
        th, td {
            padding: 12px;
            text-align: center;
            border-bottom: 1px solid #475569;
            color: #E2E8F0;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            min-width: 150px;
        }
        th {
            background-color: #334155;
            color: #60A5FA;
            font-weight: 600;
            text-transform: uppercase;
            position: sticky;
            top: 0;
            z-index: 10;
        }
        tr:nth-child(even) {
            background-color: #2A3346;
        }
        tr:hover {
            background-color: #3B4970;
        }

        /* AgGrid Custom Theme */
        .ag-theme-custom-dark {
            --ag-foreground-color: #E2E8F0;
            --ag-background-color: #1E293B;
            --ag-header-background-color: #334155;
            --ag-header-foreground-color: #60A5FA;
            --ag-row-hover-background-color: #3B4970;
            --ag-column-hover-background-color: #3B4970;
            --ag-border-color: #475569;
            --ag-grid-size: 6px;
            --ag-header-height: 40px;
            --ag-row-height: 36px;
            --ag-font-size: 14px;
            --ag-font-family: 'Segoe UI', sans-serif;
        }
        .ag-theme-custom-dark .ag-row-even {
            background-color: #2A3346;
        }
        .ag-theme-custom-dark .ag-header-cell {
            border-bottom: 1px solid #475569;
        }
        .ag-theme-custom-dark .ag-cell {
            border-bottom: 1px solid #475569;
        }
        .ag-theme-custom-dark .ag-paging-panel {
            background-color: #1E293B;
            color: #E2E8F0;
        }
        .ag-theme-custom-dark .ag-paging-button {
            color: #60A5FA;
        }
        .ag-theme-custom-dark .ag-checkbox-input-wrapper {
            color: #E2E8F0;
        }
        .ag-theme-custom-dark .ag-filter-toolpanel {
            background-color: #1E293B;
            color: #E2E8F0;
        }
        .ag-theme-custom-dark .ag-side-bar {
            background-color: #1E293B;
            color: #E2E8F0;
        }

        /* Total container for report */
        .total-container {
            display: flex;
            flex-direction: column;
            gap: 15px;
            text-align: center;
            align-items: center;
            width: 100%;
            margin: 15px 0;
            padding: 15px;
            background: linear-gradient(135deg, #1E293B, #334155);
            border-radius: 12px;
            box-shadow: 0 6px 15px rgba(0, 0, 0, 0.4);
        }
        .total-item {
            font-size: 20px;
            font-weight: 600;
            color: #E2E8F0;
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 8px;
            background: rgba(255, 255, 255, 0.05);
            border-radius: 6px;
        }
        .total-paragrafo {
            font-size: 16px;
            color: #94A3B8;
            font-weight: 500;
        }
        .total-numero-conf {
            color: #34D399;
            font-size: 22px;
            font-weight: 700;
            text-shadow: 1px 1px 3px rgba(0, 0, 0, 0.2);
        }
        .total-item-final {
            font-size: 16px;
            color: #94A3B8;
            font-weight: 500;
        }

        /* Multiselect and inputs */
        .stMultiSelect [data-testid="stMarkdownContainer"] {
            color: #E2E8F0;
        }
        .stDateInput input {
            background-color: #1E293B;
            color: #E2E8F0;
            border: 1px solid #475569;
            border-radius: 6px;
        }

        /* Responsive for large screens */
        @media (min-width: 1200px) {
            .card {
                min-height: 180px;
            }
            .number {
                font-size: 26px;
            }
            .title {
                font-size: 18px;
            }
            .total-item {
                font-size: 22px;
            }
            .total-paragrafo {
                font-size: 18px;
            }
            .total-numero-conf {
                font-size: 24px;
            }
            table {
                font-size: 16px;
            }
            th, td {
                padding: 14px;
            }
        }
        .full-width-table {
            width: 100%;
            max-width: none;
        }
    </style>
    """, unsafe_allow_html=True)

    # Definir as datas - últimos 3 meses
    data_final = date.today()
    data_inicial = data_final - relativedelta(months=3)

    # Adicionar auto-refresh com intervalo de 2 minutos (120 segundos)
    st_autorefresh(interval=120000, key="data_refresh")

    # Show time until next refresh
    next_refresh = datetime.now() + timedelta(seconds=120)
    st.write(f"**Próxima atualização em:** {next_refresh.strftime('%H:%M:%S')}")

    st.markdown("<h1 style='margin: 0;'>Pedidos</h1>", unsafe_allow_html=True)

    # Fetch data from SQLite
    data_1 = fetch_pcmovendpend_data(data_inicial, data_final)
    data_2 = fetch_pcpedc_data(data_inicial, data_final)
    pending_data = fetch_pending_orders_data()
    clear_weekly_data()

    if not data_1.empty and not data_2.empty:
        daily_data, total_data = process_data(data_1)
        data_2['DATA'] = pd.to_datetime(data_2['DATA'], errors='coerce')
        rotas_desejadas = ["BR 262", "REGIAO NORTE", "REGIÃO SUL", "EXTREMO CENTRO/ES", "EXTREMO NORTE", "EXTREMO SUL", "GRANDE VITORIA"]

        total_liberados = data_2['L_COUNT'].sum()
        total_montados = data_2['M_COUNT'].sum()

        total_dia = daily_data[daily_data['DIA'] == date.today()]['PEDIDOS CONFERIDOS'].sum()
        total_semana = daily_data[(daily_data['DIA'] >= (date.today() - timedelta(days=date.today().weekday()))) & (daily_data['DIA'] <= date.today())]['PEDIDOS CONFERIDOS'].sum()
        total_mes = daily_data[(daily_data['DIA'] >= date.today().replace(day=1)) & (daily_data['DIA'] <= date.today())]['PEDIDOS CONFERIDOS'].sum()

        # Main layout with two columns
        col_left, col_right = st.columns([1.2, 1])

        with col_left:
            # Relatório de Pedidos
            st.markdown("<h3>Relatório de Pedidos</h3>", unsafe_allow_html=True)
            st.markdown(f"""
                <div class="total-container">
                    <div class="total-item">
                        <img src="https://cdn-icons-png.flaticon.com/512/10995/10995680.png" width="35">
                        <span class="total-paragrafo">TOTAL LIBERADOS:</span>
                        <span class="total-numero-conf">{total_liberados}</span>
                    </div>
                    <div class="total-item">
                        <img src="https://cdn-icons-png.flaticon.com/512/976/976438.png" width="35">
                        <span class="total-paragrafo">TOTAL MONTADOS:</span>
                        <span class="total-numero-conf">{total_montados}</span>
                    </div>
                    <div class="total-item">
                        <img src="https://cdn-icons-png.flaticon.com/512/5220/5220625.png" width="35">
                        <span class="total-paragrafo">CONF DIÁRIA:</span>
                        <span class="total-numero-conf">{total_dia}</span>
                        <span class="total-item-final">PEDIDOS</span>
                    </div>
                    <div class="total-item">
                        <img src="https://cdn-icons-png.flaticon.com/512/391/391175.png" width="35">
                        <span class="total-paragrafo">CONF SEMANAL:</span>
                        <span class="total-numero-conf">{total_semana}</span>
                        <span class="total-item-final">PEDIDOS</span>
                    </div>
                    <div class="total-item">
                        <img src="https://cdn-icons-png.flaticon.com/512/353/353267.png" width="35">
                        <span class="total-paragrafo">CONF MENSAL:</span>
                        <span class="total-numero-conf">{total_mes}</span>
                        <span class="total-item-final">PEDIDOS</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)

            # Pedidos Conferidos por Funcionário
            st.markdown("<h3>Pedidos Conferidos por Funcionário</h3>", unsafe_allow_html=True)
            data_inicial_conf = st.date_input("Data Inicial", value=date.today())
            data_final_conf = st.date_input("Data Final", value=date.today())
            filtered_data = daily_data[(daily_data['DIA'] >= data_inicial_conf) & (daily_data['DIA'] <= data_final_conf)]
            if filtered_data.empty:
                filtered_data = pd.DataFrame(columns=['CONFERENTE', 'DIA', 'PEDIDOS CONFERIDOS'])
            filtered_data_sorted = filtered_data.sort_values(by='PEDIDOS CONFERIDOS', ascending=False).reset_index(drop=True)
            st.markdown('<div class="scrollable-table">' + filtered_data_sorted.to_html(index=False, escape=False) + '</div>', unsafe_allow_html=True)

            # Pedidos Pendentes por Rota
            st.markdown("<h3>Pedidos Pendentes Conferência</h3>", unsafe_allow_html=True)
            num_colunas = 3
            cols = st.columns(num_colunas)

            # Lista de rotas desejadas
            rotas_desejadas = ["BR 262", "REGIAO NORTE", "REGIÃO SUL", "EXTREMO CENTRO/ES", "EXTREMO NORTE", "EXTREMO SUL", "GRANDE VITORIA"]

            # Criar DataFrame com todas as rotas desejadas (com 0 para rotas sem pedidos)
            pending_filtrado = pending_data[pending_data['ROTA'].isin(rotas_desejadas)]
            pending_aggregated = pending_filtrado.groupby(['ROTA']).size().reset_index(name='PENDENTES')

            # Criar DataFrame com todas as rotas desejadas
            rotas_df = pd.DataFrame({'ROTA': rotas_desejadas})
            pending_aggregated = rotas_df.merge(pending_aggregated, on='ROTA', how='left').fillna({'PENDENTES': 0})
            pending_aggregated['PENDENTES'] = pending_aggregated['PENDENTES'].astype(int)

            colors = {
                "GRANDE VITORIA": "#FF6347",
                "REGIÃO SUL": "#32CD32",
                "REGIAO NORTE": "#000080",
                "BR 262": "#6A5ACD",
                "EXTREMO SUL": "#FF69B4",
                "EXTREMO NORTE": "#20B2AA",
                "EXTREMO CENTRO/ES": "#FF4500",
            }

            for index, row in pending_aggregated.iterrows():
                rota_nome = row['ROTA']
                pendentes = row['PENDENTES']
                col = cols[index % num_colunas]
                with col:
                    st.markdown(f"""
                        <div class="card" style="background-color: {colors.get(rota_nome, '#1e1e1e')}">
                            <span class="title">{rota_nome}</span><br>
                            <div class="card-content">
                                <div style="display: flex; align-items: center; justify-content: center; gap: 8px;">
                                    <img src="https://cdn-icons-png.flaticon.com/512/5220/5220625.png" width="25">
                                    <p style="margin: 0; font-weight: bold;">PENDENTES:</p>
                                    <div class="number">{pendentes}</div>
                                </div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)

        with col_right:
            # Regiões section
            st.markdown("<h3>Regiões</h3>", unsafe_allow_html=True)
            rotas_selecionadas = st.multiselect("Selecione as Rotas", rotas_desejadas, default=rotas_desejadas)
            data_filtrada = data_2[data_2['DESCRICAO'].isin(rotas_selecionadas)]

            data_aggregated = data_filtrada.groupby(['DESCRICAO']).agg(
                pedidos_liberados=('L_COUNT', 'sum'),
                pedidos_montados=('M_COUNT', 'sum')
            ).reset_index()

            # Pedidos por Rota
            st.markdown("<h3>Pedidos por Rota</h3>", unsafe_allow_html=True)
            num_colunas = 3
            cols = st.columns(num_colunas)

            for index, row in data_aggregated.iterrows():
                rota_nome = row['DESCRICAO']
                pedidos_liberados = row['pedidos_liberados']
                pedidos_montados = row['pedidos_montados']
                col = cols[index % num_colunas]
                with col:
                    st.markdown(f"""
                        <div class="card" style="background-color: {colors.get(rota_nome, '#1e1e1e')}">
                            <span class="title">{rota_nome}</span><br>
                            <div class="card-content">
                                <div style="display: flex; align-items: center; justify-content: center; gap: 8px;">
                                    <img src="https://cdn-icons-png.flaticon.com/512/5629/5629260.png" width="25">
                                    <p style="margin: 0; font-weight: bold;">LIBERADOS:</p>
                                    <div class="number">{pedidos_liberados}</div>
                                </div>
                                <div style="display: flex; align-items: center; justify-content: center; gap: 8px;">
                                    <img src="https://cdn-icons-png.flaticon.com/512/9964/9964349.png" width="25">
                                    <p style="margin: 0; font-weight: bold;">MONTADOS:</p>
                                    <div class="number">{pedidos_montados}</div>
                                </div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)

            # Progresso de Pedidos (Enhanced Table)
            st.markdown("<h3>Progresso de Pedidos Conferência</h3>", unsafe_allow_html=True)
            display_columns = ['STATUS', 'NUMPED', 'NUMCAR', 'CLIENTE', 'CONFERENTE', 'ROTA', 'QTDITENS']
            if not pending_data.empty:
                # Calculate order level based on QTDITENS
                def get_order_level(qtd_itens):
                    if qtd_itens <= 50:
                        return 'Baixo'
                    elif qtd_itens <= 200:
                        return 'Médio'
                    elif qtd_itens <= 500:
                        return 'Alto'
                    else:
                        return 'Muito Alto'

                pending_data['NÍVEL'] = pending_data['QTDITENS'].apply(get_order_level)
                pending_data_renamed = pending_data[display_columns + ['NÍVEL']].rename(columns={
                    'NUMPED': 'Pedido',
                    'NUMCAR': 'Carregamento',
                    'CLIENTE': 'Cliente',
                    'STATUS': 'Progresso',
                    'CONFERENTE': 'Conferente',
                    'ROTA': 'Rota',
                    'QTDITENS': 'Qtd. Itens',
                    'NÍVEL': 'Nível'
                })
                pending_data_renamed['Progresso'] = pending_data_renamed['Progresso'].map({
                    'NÃO INICIADO': 'Pendente',
                    'EM CONFERÊNCIA': 'Em Conferência'
                })
            else:
                pending_data_renamed = pd.DataFrame(columns=['Progresso', 'Pedido', 'Carregamento', 'Cliente', 'Conferente', 'Rota', 'Qtd. Itens', 'Nível'])

            # Configuração do AgGrid para Pedidos Pendentes
            gb = GridOptionsBuilder.from_dataframe(pending_data_renamed)
            gb.configure_pagination(paginationAutoPageSize=True)
            gb.configure_side_bar()
            gb.configure_default_column(groupable=True, sortable=True, filter=True, resizable=True)
            gb.configure_selection('multiple', use_checkbox=True)

            # Estilo condicional para Progresso e Nível
            cell_style_progresso = JsCode("""
                function(params) {
                    if (params.data) {
                        if (params.data.Progresso === 'Pendente') {
                            return {color: '#E2E8F0', backgroundColor: '#FF6347'};
                        } else if (params.data.Progresso === 'Em Conferência') {
                            return {color: '#E2E8F0', backgroundColor: '#FFD700'};
                        }
                        return null;
                    }
                }
            """)
            cell_style_nivel = JsCode("""
                function(params) {
                    if (params.data) {
                        if (params.data.Nível === 'Baixo') {
                            return {color: '#E2E8F0', backgroundColor: '#34D399'};
                        } else if (params.data.Nível === 'Médio') {
                            return {color: '#E2E8F0', backgroundColor: '#FFA500'};
                        } else if (params.data.Nível === 'Alto') {
                            return {color: '#E2E8F0', backgroundColor: '#FF4500'};
                        } else if (params.data.Nível === 'Muito Alto') {
                            return {color: '#E2E8F0', backgroundColor: '#8B008B'};
                        }
                        return null;
                    }
                }
            """)
            gb.configure_column('Progresso', cellStyle=cell_style_progresso)
            gb.configure_column('Nível', cellStyle=cell_style_nivel)
            grid_options = gb.build()
            grid_options['autoSizeStrategy'] = {
                'type': 'fitGridWidth',
                'defaultMinWidth': 160,
                'defaultMaxWidth': 200
            }


            st.markdown('<div class="scrollable-table">', unsafe_allow_html=True)
            AgGrid(pending_data_renamed, grid_options, height=500, width=1005, theme='custom-dark', allow_unsafe_jscode=True)
            st.markdown('</div>', unsafe_allow_html=True)

    else:
        st.error("Não foi possível carregar os dados. Verifique a conexão com o banco de dados ou tente atualizar manualmente.")

if __name__ == "__main__":
    main()