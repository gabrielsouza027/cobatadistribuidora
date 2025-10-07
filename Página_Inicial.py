import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import locale
import plotly.express as px
import os
import logging
from dateutil.relativedelta import relativedelta
from streamlit_autorefresh import st_autorefresh

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuração de locale para formatação de moeda
locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')

# Configuração do banco de dados SQLite
DB_PATH = "database/pcpedc.db"

def test_db_connection():
    """Testa a conexão com o banco SQLite e lista tabelas."""
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        cursor.execute("SELECT COUNT(*) FROM pcpedc")
        row_count = cursor.fetchone()[0]
        conn.close()
        logger.info(f"Tabelas encontradas: {tables}, Registros em pcpedc: {row_count}")
        return tables, row_count
    except sqlite3.Error as e:
        logger.error(f"Erro ao conectar ao SQLite: {e}")
        return None, 0

def init_db():
    """Verifica se o banco de dados SQLite existe."""
    if not os.path.exists(DB_PATH):
        logger.error(f"Banco de dados não encontrado em {DB_PATH}")
        raise FileNotFoundError(f"Banco de dados não encontrado em {DB_PATH}")
    logger.info(f"Banco de dados encontrado em {DB_PATH}")

def fetch_db_data(data_inicial, data_final):
    """Busca dados da tabela pcpedc no SQLite."""
    logger.info(f"Buscando dados do SQLite de {data_inicial} a {data_final}")
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        data_inicial_str = data_inicial.strftime('%Y-%m-%d')
        data_final_str = data_final.strftime('%Y-%m-%d')
        query = """
            SELECT CODPROD, QT_SAIDA, NUMPED, DATA, PVENDA, CONDVENDA, NOME, CODUSUR, CODFILIAL, CODPRACA, CODCLI, NOME_EMITENTE, DEVOLUCAO
            FROM pcpedc
            WHERE DATA BETWEEN ? AND ?
        """
        data = pd.read_sql_query(query, conn, params=(data_inicial_str, data_final_str))
        conn.close()
        
        if data.empty:
            logger.warning("Nenhum dado encontrado no SQLite para o período especificado")
            return pd.DataFrame()

        # Converte DATA
        data['DATA'] = pd.to_datetime(data['DATA'], errors='coerce', format='%Y-%m-%d')
        if data['DATA'].isna().all():
            logger.warning("Formato de DATA inválido ou ausente em todos os registros")
            return pd.DataFrame()

        # Valida e converte colunas
        required_cols = ['PVENDA', 'QT_SAIDA', 'CODFILIAL', 'DATA', 'NUMPED']
        missing_cols = [col for col in required_cols if col not in data.columns]
        if missing_cols:
            logger.error(f"Colunas obrigatórias ausentes: {', '.join(missing_cols)}")
            return pd.DataFrame()

        data['PVENDA'] = pd.to_numeric(data['PVENDA'], errors='coerce').fillna(0)
        data['QT'] = pd.to_numeric(data['QT_SAIDA'], errors='coerce').fillna(0)
        data = data.dropna(subset=['DATA', 'PVENDA', 'QT_SAIDA', 'CODFILIAL'])

        data['VLTOTAL'] = data['PVENDA'] * data['QT_SAIDA']
        logger.info(f"Dados brutos retornados: {len(data)} linhas, CODFILIAL únicos: {data['CODFILIAL'].unique()}, últimas datas: {data['DATA'].max()}")
        return data
    except sqlite3.Error as e:
        logger.error(f"Erro ao consultar o SQLite: {e}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Erro inesperado: {e}")
        return pd.DataFrame()

def carregar_dados(data_inicial, data_final):
    """Carrega dados do SQLite, focando no período solicitado."""
    data_inicial = max(pd.to_datetime("2024-01-01"), pd.to_datetime(data_inicial))
    data_final = pd.to_datetime(data_final).normalize()
    
    if data_final < data_inicial:
        st.error("Data final deve ser posterior à data inicial.")
        return pd.DataFrame()
    
    return fetch_db_data(data_inicial, data_final)

def calcular_faturamento(data, hoje, ontem, semana_inicial, semana_passada_inicial):
    """Calcula métricas de faturamento considerando horário atual."""
    agora = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    faturamento_hoje = data.query("DATA >= @agora and DATA <= @hoje")['VLTOTAL'].sum()
    faturamento_ontem = data.query("DATA >= @ontem and DATA < @agora")['VLTOTAL'].sum()
    faturamento_semanal_atual = data.query("@semana_inicial <= DATA <= @hoje")['VLTOTAL'].sum()
    faturamento_semanal_passada = data.query("@semana_passada_inicial <= DATA < @semana_inicial")['VLTOTAL'].sum()
    logger.info(f"Faturamento - Hoje: {faturamento_hoje}, Ontem: {faturamento_ontem}, Semana Atual: {faturamento_semanal_atual}, Semana Passada: {faturamento_semanal_passada}")
    return faturamento_hoje, faturamento_ontem, faturamento_semanal_atual, faturamento_semanal_passada

def calcular_quantidade_pedidos(data, hoje, ontem, semana_inicial, semana_passada_inicial):
    """Calcula métricas de quantidade de pedidos."""
    agora = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    pedidos_hoje = data.query("DATA >= @agora and DATA <= @hoje")['NUMPED'].nunique()
    pedidos_ontem = data.query("DATA >= @ontem and DATA < @agora")['NUMPED'].nunique()
    pedidos_semanal_atual = data.query("@semana_inicial <= DATA <= @hoje")['NUMPED'].nunique()
    pedidos_semanal_passada = data.query("@semana_passada_inicial <= DATA < @semana_inicial")['NUMPED'].nunique()
    logger.info(f"Pedidos - Hoje: {pedidos_hoje}, Ontem: {pedidos_ontem}, Semana Atual: {pedidos_semanal_atual}, Semana Passada: {pedidos_semanal_passada}")
    return pedidos_hoje, pedidos_ontem, pedidos_semanal_atual, pedidos_semanal_passada

def calcular_comparativos(data, hoje, mes_atual, ano_atual):
    """Calcula métricas de comparação mensal."""
    mes_anterior = mes_atual - 1 if mes_atual > 1 else 12
    ano_anterior = ano_atual if mes_atual > 1 else ano_atual - 1
    faturamento_mes_atual = data.query("DATA.dt.month == @mes_atual and DATA.dt.year == @ano_atual")['VLTOTAL'].sum()
    pedidos_mes_atual = data.query("DATA.dt.month == @mes_atual and DATA.dt.year == @ano_atual")['NUMPED'].nunique()
    faturamento_mes_anterior = data.query("DATA.dt.month == @mes_anterior and DATA.dt.year == @ano_anterior")['VLTOTAL'].sum()
    pedidos_mes_anterior = data.query("DATA.dt.month == @mes_anterior and DATA.dt.year == @ano_anterior")['NUMPED'].nunique()
    logger.info(f"Mês Atual - Faturamento: {faturamento_mes_atual}, Pedidos: {pedidos_mes_atual}, Mês Anterior - Faturamento: {faturamento_mes_anterior}, Pedidos: {pedidos_mes_anterior}")
    return faturamento_mes_atual, faturamento_mes_anterior, pedidos_mes_atual, pedidos_mes_anterior

def formatar_valor(valor):
    """Formata valor como moeda."""
    return locale.currency(valor, grouping=True, symbol=True)

def main():
    try:
        init_db()
    except FileNotFoundError as e:
        st.error(str(e))
        return

    # Testa conexão e verifica tabela
    tables, row_count = test_db_connection()
    if not tables or ('pcpedc',) not in tables:
        st.error("Tabela 'pcpedc' não encontrada no banco de dados.")
        return
    if row_count == 0:
        st.warning("A tabela 'pcpedc' está vazia. Aguarde a atualização do Flask ou verifique a conexão com o Oracle.")
        return

    # Configura auto-refresh a cada 5 minutos (300000 ms)
    st_autorefresh(interval=300000, key="data_refresh")
    st.write(f"Próxima atualização em: {(datetime.now() + timedelta(seconds=300)).strftime('%H:%M:%S')}")

    # Adicionar botão de refresh manual
    if st.button("Atualizar Dados"):
        st.session_state["force_refresh"] = True

    

    st.markdown("""
    <style>
        .st-emotion-cache-1ibsh2c {
            width: 100%;
            padding: 0rem 1rem 0rem;
            max-width: initial;
            min-width: auto;
        }
        .card-container {
            display: flex;
            align-items: center;
            background-color: #302d2d;
            padding: 10px;
            border-radius: 8px;
            margin-bottom: 10px;
            color: white;
            flex-direction: column;
            text-align: center;
        }
        .card-container img {
            width: 51px;
            height: 54px;
            margin-bottom: 5px;
        }
        .number {
            font-size: 20px;
            font-weight: bold;
            margin-top: 5px;
        }
    </style>
    """, unsafe_allow_html=True)

    st.title('Dashboard de Faturamento')
    st.markdown("### Resumo de Vendas")

    data_inicial_padrao = pd.to_datetime("2024-01-01")
    data_final_padrao = pd.to_datetime("today").normalize()
    data = carregar_dados(data_inicial_padrao, data_final_padrao)

    if not data.empty:
        data['DATA'] = pd.to_datetime(data['DATA'], errors='coerce')
        data = data.dropna(subset=['DATA'])
        


        col1, col2 = st.columns(2)
        with col1:
            filial_1 = st.checkbox("Filial 1", value=True)
        with col2:
            filial_2 = st.checkbox("Filial 2", value=True)

        filiais_selecionadas = []
        if filial_1:
            filiais_selecionadas.append(1)
        if filial_2:
            filiais_selecionadas.append(2)

        if not filiais_selecionadas:
            st.warning("Por favor, selecione pelo menos uma filial para exibir os dados.")
            return

        # Filtra dados por filial e verifica
        data_filtrada = data.query("CODFILIAL in @filiais_selecionadas")
        if data_filtrada.empty:
            st.warning(f"Nenhum dado encontrado para as filiais selecionadas: {filiais_selecionadas}. Valores de CODFILIAL disponíveis: {data['CODFILIAL'].dropna().unique()}")
            return
        logger.info(f"Dados filtrados por filiais {filiais_selecionadas}: {len(data_filtrada)} linhas")

        hoje = pd.to_datetime('today').normalize()
        agora = datetime.now()  # Inclui hora atual para dados parciais do dia
        ontem = hoje - timedelta(days=1)
        semana_inicial = hoje - timedelta(days=hoje.weekday())
        semana_passada_inicial = semana_inicial - timedelta(days=7)

        faturamento_hoje, faturamento_ontem, faturamento_semanal_atual, faturamento_semanal_passada = calcular_faturamento(data_filtrada, hoje, ontem, semana_inicial, semana_passada_inicial)
        pedidos_hoje, pedidos_ontem, pedidos_semanal_atual, pedidos_semanal_passada = calcular_quantidade_pedidos(data_filtrada, hoje, ontem, semana_inicial, semana_passada_inicial)
    
        mes_atual = hoje.month
        ano_atual = hoje.year
        faturamento_mes_atual, faturamento_mes_anterior, pedidos_mes_atual, pedidos_mes_anterior = calcular_comparativos(data_filtrada, hoje, mes_atual, ano_atual)

        # Verifica se os dados estão desatualizados
        ultima_data = data['DATA'].max()
        if ultima_data < (hoje - timedelta(days=1)):
            st.warning(f"Dados podem estar desatualizados. Última data encontrada: {ultima_data.strftime('%Y-%m-%d')}. Aguarde a próxima atualização.")

        col1, col2, col3, col4, col5 = st.columns(5)

        def calcular_variacao(atual, anterior):
            if anterior == 0:
                return 100 if atual > 0 else 0
            return ((atual - anterior) / abs(anterior)) * 100
        
        def icone_variacao(valor):
            if valor > 0:
                return f"<span style='color: green;'>▲ {valor:.2f}%</span>"
            elif valor < 0:
                return f"<span style='color: red;'>▼ {valor:.2f}%</span>"
            else:
                return f"{valor:.2f}%"

        var_faturamento_mes = calcular_variacao(faturamento_mes_atual, faturamento_mes_anterior)
        var_pedidos_mes = calcular_variacao(pedidos_mes_atual, pedidos_mes_anterior)
        var_faturamento_hoje = calcular_variacao(faturamento_hoje, faturamento_ontem)
        var_pedidos_hoje = calcular_variacao(pedidos_hoje, pedidos_ontem)
        var_faturamento_semananterior = calcular_variacao(faturamento_semanal_atual, faturamento_semanal_passada)

        def grafico_pizza_variacao(labels, valores, titulo):
            fig = px.pie(
                names=labels,
                values=[abs(v) for v in valores],
                title=titulo,
                hole=0.4,
                color_discrete_sequence=["#33B950", '#EF553B']
            )
            fig.update_layout(margin=dict(t=30, b=10, l=10, r=10), showlegend=False)
            return fig

        with col1:
            st.markdown(f"""
                <div class="card-container">
                    <img src="https://cdn-icons-png.flaticon.com/512/2460/2460494.png" alt="Ícone Hoje">
                    <span>Hoje:</span> 
                    <div class="number">{formatar_valor(faturamento_hoje)}</div>
                    <small>Variação: {icone_variacao(var_faturamento_hoje)}</small>
                </div>
                <div class="card-container">
                    <img src="https://cdn-icons-png.flaticon.com/512/3703/3703896.png" alt="Ícone Ontem">
                    <span>Ontem:</span> 
                    <div class="number">{formatar_valor(faturamento_ontem)}</div>
                </div>
            """, unsafe_allow_html=True)

        with col2:
            st.markdown(f"""
                <div class="card-container">
                    <img src="https://cdn-icons-png.flaticon.com/512/4435/4435153.png" alt="Ícone Semana Atual">
                    <span>Semana Atual:</span> 
                    <div class="number">{formatar_valor(faturamento_semanal_atual)}</div>
                    <small>Variação: {icone_variacao(var_faturamento_semananterior)}</small>
                </div>
                <div class="card-container">
                    <img src="https://cdn-icons-png.flaticon.com/512/4435/4435153.png" alt="Ícone Semana Passada">
                    <span>Semana Passada:</span> 
                    <div class="number">{formatar_valor(faturamento_semanal_passada)}</div>
                </div>
            """, unsafe_allow_html=True)

        with col3:
            st.markdown(f"""
                <div class="card-container">
                    <img src="https://cdn-icons-png.flaticon.com/512/10535/10535844.png" alt="Ícone Mês Atual">
                    <span>Mês Atual:</span> 
                    <div class="number">{formatar_valor(faturamento_mes_atual)}</div>
                    <small>Variação: {icone_variacao(var_faturamento_mes)}</small>
                </div>
                <div class="card-container">
                    <img src="https://cdn-icons-png.flaticon.com/512/584/584052.png" alt="Ícone Mês Anterior">
                    <span>Mês Anterior:</span> 
                    <div class="number">{formatar_valor(faturamento_mes_anterior)}</div>
                </div>
            """, unsafe_allow_html=True)

        with col4:
            st.markdown(f"""
                <div class="card-container">
                    <img src="https://cdn-icons-png.flaticon.com/512/6632/6632848.png" alt="Ícone Pedidos Mês Atual">
                    <span>Pedidos Mês Atual:</span> 
                    <div class="number">{pedidos_mes_atual}</div>
                    <small>Variação: {icone_variacao(var_pedidos_mes)}</small>
                </div>
                <div class="card-container">
                    <img src="https://cdn-icons-png.flaticon.com/512/925/925049.png" alt="Ícone Pedidos Mês Anterior">
                    <span>Pedidos Mês Anterior:</span> 
                    <div class="number">{pedidos_mes_anterior}</div>
                </div>
            """, unsafe_allow_html=True)

        with col5:
            st.markdown(f"""
                <div class="card-container">
                    <img src="https://cdn-icons-png.flaticon.com/512/14018/14018701.png" alt="Ícone Pedidos Hoje">
                    <span>Pedidos Hoje:</span> 
                    <div class="number">{pedidos_hoje}</div>
                    <small>Variação: {icone_variacao(var_pedidos_hoje)}</small>
                </div>
                <div class="card-container">
                    <img src="https://cdn-icons-png.flaticon.com/512/5220/5220625.png" alt="Ícone Pedidos Ontem">
                    <span>Pedidos Ontem:</span> 
                    <div class="number">{pedidos_ontem}</div>
                </div>
            """, unsafe_allow_html=True)

        st.markdown("---")

        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.plotly_chart(grafico_pizza_variacao(["Hoje", "Ontem"], [faturamento_hoje, faturamento_ontem], "Variação de Faturamento (Hoje x Ontem)"), use_container_width=True)
        with col2:
            st.plotly_chart(grafico_pizza_variacao(["Semana Atual", "Semana Passada"], [faturamento_semanal_atual, faturamento_semanal_passada], "Variação de Faturamento (Semana)"), use_container_width=True)
        with col3:
            st.plotly_chart(grafico_pizza_variacao(["Mês Atual", "Mês Anterior"], [faturamento_mes_atual, faturamento_mes_anterior], "Variação de Faturamento (Mês)"), use_container_width=True)
        with col4:
            st.plotly_chart(grafico_pizza_variacao(["Pedidos Mês Atual", "Pedidos Mês Passado"], [pedidos_mes_atual, pedidos_mes_anterior], "Variação de Pedidos (Mês)"), use_container_width=True)
        with col5:
            st.plotly_chart(grafico_pizza_variacao(["Pedidos Hoje", "Pedidos Ontem"], [pedidos_hoje, pedidos_ontem], "Variação de Pedidos (Hoje x Ontem)"), use_container_width=True)

        st.subheader("Comparação de Vendas por Mês e Ano")

        col_data1, col_data2 = st.columns(2)
        with col_data1:
            data_inicial = st.date_input(
                label="Selecione a Data Inicial",
                value=data_inicial_padrao,
                min_value=data_inicial_padrao,
                max_value=data_final_padrao,
                key="data_inicial"
            )
        with col_data2:
            data_final = st.date_input(
                label="Selecione a Data Final",
                value=data_final_padrao,
                min_value=data_inicial_padrao,
                max_value=data_final_padrao,
                key="data_final"
            )

        df_periodo = carregar_dados(pd.to_datetime(data_inicial), pd.to_datetime(data_final))

        if not df_periodo.empty:
            df_periodo['Ano'] = df_periodo['DATA'].dt.year
            df_periodo['Mês'] = df_periodo['DATA'].dt.month
            vendas_por_mes_ano = df_periodo.groupby(['Ano', 'Mês']).agg(
                Valor_Total_Vendido=('VLTOTAL', 'sum')
            ).reset_index()

            fig = px.line(vendas_por_mes_ano, x='Mês', y='Valor_Total_Vendido', color='Ano',
                          title=f'Vendas por Mês ({data_inicial} a {data_final})',
                          labels={'Mês': 'Mês', 'Valor_Total_Vendido': 'Valor Total Vendido (R$)', 'Ano': 'Ano'},
                          markers=True)

            fig.update_layout(
                title_font_size=20,
                xaxis_title_font_size=16,
                yaxis_title_font_size=16,
                xaxis_tickfont_size=14,
                yaxis_tickfont_size=14,
                xaxis_tickangle=-45,
                xaxis=dict(tickmode='array', tickvals=list(range(1, 13)), ticktext=['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']),
                margin=dict(t=30, b=10, l=10, r=10)
            )

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Nenhum dado disponível para o período selecionado no gráfico de vendas por mês.")
    else:
        st.warning("Nenhum dado disponível para exibição. Verifique o formato da coluna 'DATA' no SQLite (esperado: YYYY-MM-DD) e os filtros aplicados.")

if __name__ == "__main__":
    main()