import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, date, timedelta
import locale
import plotly.express as px
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
from streamlit_autorefresh import st_autorefresh

locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')

# Configuração do caminho para os arquivos .db
DB_PATH = "database\\"

def get_db_connection(db_file):
    """Estabelece conexão com o banco SQLite."""
    try:
        conn = sqlite3.connect(f"{DB_PATH}{db_file}")
        return conn
    except sqlite3.Error as e:
        st.error(f"Erro ao conectar ao {db_file}: {e}")
        return None

def fetch_pcpedc_data():
    """Busca dados diretamente do banco SQLite pcpedc.db com a nova query."""
    conn = get_db_connection("pcpedc.db")
    if conn is None:
        return pd.DataFrame()
    
    try:
        query = """
            SELECT 
                CODPROD ,
                QT_SAIDA ,
                VALOR_DEVOLVIDO ,
                NUMPED ,
                DATA ,
                PVENDA ,
                CONDVENDA ,
                NOME ,
                CODUSUR ,
                CODFILIAL ,
                CODPRACA ,
                CODCLI ,
                NOME_EMITENTE ,
                DEVOLUCAO
            FROM pcpedc 
            WHERE DEVOLUCAO = 'S'
        """
        df = pd.read_sql(query, conn)
        return df
    except sqlite3.Error as e:
        st.error(f"Erro ao buscar dados de pcpedc.db: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def fetch_pcvendedor_data():
    """Busca dados diretamente do banco SQLite pcvendedor.db."""
    conn = get_db_connection("pcvendedor.db")
    if conn is None:
        return pd.DataFrame()
    
    try:
        query = """
            SELECT CODIGOVENDA, SUPERVISOR, CUSTOPRODUTO, CODCIDADE, CODPRODUTO, CODUSUR, VENDEDOR, ROTA, PERIODO, CODCLIENTE, CLIENTE, FANTASIA, DATAPEDIDO, PRODUTO, PEDIDO, FORNECEDOR, QUANTIDADE, BLOQUEADO, VALOR, CODFORNECEDOR, RAMO, ENDERECO, BAIRRO, MUNICIPIO, CIDADE, VLBONIFIC, BONIFIC
            FROM pcvendedor
            ORDER BY DATAPEDIDO
        """
        df = pd.read_sql(query, conn)
        return df
    except sqlite3.Error as e:
        st.error(f"Erro ao buscar dados de pcvendedor.db: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def calcular_detalhes_vendedores(data, data_inicial, data_final):
    # Remover espaços em branco dos nomes das colunas
    data.columns = data.columns.str.strip()

    # Verificar se as colunas necessárias estão presentes
    required_columns = ['DATA', 'PVENDA', 'QT_SAIDA', 'VALOR_DEVOLVIDO', 'CODCLI', 'NUMPED', 'NOME', 'CODUSUR', 'DEVOLUCAO', 'CODPROD']
    for col in required_columns:
        if col not in data.columns:
            st.error(f"A coluna '{col}' não está presente no DataFrame.")
            return pd.DataFrame(), pd.DataFrame()

    # Certificar-se de que a coluna 'DATA' está no formato datetime
    data['DATA'] = pd.to_datetime(data['DATA'], errors='coerce')

    # Filtrar os dados com base no período selecionado
    data_filtrada = data[(data['DATA'] >= data_inicial) & (data['DATA'] <= data_final)]

    # Verificar se há dados após o filtro
    if data_filtrada.empty:
        st.warning("Não há dados para o período selecionado.")
        return pd.DataFrame(), data_filtrada

    # Filtrar devoluções: excluir pares de NUMPED e CODPROD com DEVOLUCAO 'S/ED'
    devolucoes = data_filtrada.groupby(['NUMPED', 'CODPROD'])['DEVOLUCAO'].nunique().reset_index()
    pedidos_com_devolucao = devolucoes[devolucoes['DEVOLUCAO'] > 1][['NUMPED', 'CODPROD']]
    
    # Excluir esses pedidos do DataFrame
    data_filtrada = data_filtrada.merge(
        pedidos_com_devolucao,
        on=['NUMPED', 'CODPROD'],
        how='left',
        indicator=True
    )
    data_filtrada = data_filtrada[data_filtrada['_merge'] == 'left_only'].drop(columns=['_merge'])

    # Calcular o total de vendas líquidas: (PVENDA * QT_VENDIDA) - VALOR_DEVOLVIDO
    data_filtrada['TOTAL_VENDAS'] = (data_filtrada['PVENDA'] * data_filtrada['QT_SAIDA']) - data_filtrada['VALOR_DEVOLVIDO']

    # Agrupar os dados por vendedor e calcular as métricas
    vendedores = data_filtrada.groupby('CODUSUR').agg(
        vendedor=('NOME', 'first'),
        total_vendas=('TOTAL_VENDAS', 'sum'),
        total_clientes=('CODCLI', 'nunique'),
        total_pedidos=('NUMPED', 'nunique'),
    ).reset_index()

    vendedores.rename(columns={
        'CODUSUR': 'RCA',
        'vendedor': 'NOME',
        'total_vendas': 'TOTAL VENDAS',
        'total_clientes': 'TOTAL CLIENTES',
        'total_pedidos': 'TOTAL PEDIDOS'
    }, inplace=True)

    return vendedores, data_filtrada

def exibir_detalhes_vendedores(vendedores):
    st.markdown(
        """
        <div style="display: flex; align-items: center;">
            <img src="https://cdn-icons-png.flaticon.com/512/6633/6633057.png" 
                 width="40" style="margin-right: 10px;">
            <p style="margin: 0;">Vendedores</p>
        </div>
        """,
        unsafe_allow_html=True)

    st.dataframe(vendedores.style.format({
        'TOTAL VENDAS': formatar_valor,
    }), use_container_width=True)

def formatar_valor(valor):
    """Função para formatar valores monetários com separador de milhar e vírgula como decimal"""
    return locale.currency(valor, grouping=True, symbol=True)

def exibir_grafico_vendas_por_vendedor(data, vendedor_selecionado, ano_selecionado):
    # Filtrar dados pelo vendedor e ano selecionado
    dados_vendedor = data[
        (data['NOME'] == vendedor_selecionado) & 
        (data['DATA'].dt.year == ano_selecionado)
    ].copy()

    if dados_vendedor.empty:
        st.warning(f"Nenhum dado encontrado para o vendedor {vendedor_selecionado} no ano {ano_selecionado}.")
        return

    # Criar um DataFrame com todos os meses do ano selecionado
    meses = [f"{ano_selecionado}-{str(m).zfill(2)}" for m in range(1, 13)]
    vendas_mensais = pd.DataFrame({'MÊS': meses})

    # Calcular o total de vendas líquidas: (PVENDA * QT_VENDIDA) - VALOR_DEVOLVIDO
    dados_vendedor['TOTAL_VENDAS'] = (dados_vendedor['PVENDA'] * dados_vendedor['QT_SAIDA']) - dados_vendedor['VALOR_DEVOLVIDO']

    # Agrupar por mês
    vendas_por_mes = dados_vendedor.groupby(dados_vendedor['DATA'].dt.strftime('%Y-%m')).agg(
        total_vendas=('TOTAL_VENDAS', 'sum'),
        total_clientes=('CODCLI', 'nunique'),
        total_pedidos=('NUMPED', 'nunique'),
    ).reset_index().rename(columns={'DATA': 'MÊS'})

    # Mesclar com o DataFrame de meses para garantir todos os meses
    vendas_mensais = vendas_mensais.merge(vendas_por_mes, on='MÊS', how='left').fillna({
        'total_vendas': 0,
        'total_clientes': 0,
        'total_pedidos': 0
    })

    vendas_mensais.rename(columns={
        'total_vendas': 'TOTAL VENDIDO',
        'total_clientes': 'TOTAL CLIENTES',
        'total_pedidos': 'TOTAL PEDIDOS',
    }, inplace=True)

    # Criar o gráfico de barras
    fig = px.bar(
        vendas_mensais, 
        x='TOTAL VENDIDO', 
        y='MÊS', 
        orientation='h', 
        title=f'Vendas Mensais de {vendedor_selecionado} ({ano_selecionado})',
        color='MÊS', 
        color_discrete_sequence=px.colors.qualitative.Plotly,
        hover_data={'TOTAL CLIENTES': True, 'TOTAL PEDIDOS': True, 'TOTAL VENDIDO': ':,.2f'}
    )

    # Atualizar layout do gráfico
    fig.update_layout(
        xaxis_title="Total Vendido (R$)",
        yaxis_title="Mês",
        title_font_size=20,
        xaxis_title_font_size=16,
        yaxis_title_font_size=16,
        xaxis_tickfont_size=14,
        yaxis_tickfont_size=14,
        yaxis={'autorange': 'reversed'},  # Inverter a ordem dos meses (mais recente no topo)
        showlegend=True
    )

    st.plotly_chart(fig, use_container_width=True)
    
    # Exibir métricas adicionais
    col1, col2 = st.columns(2)
    with col1:
        st.write("TOTAL DE CLIENTES 🧍‍♂️:", int(vendas_mensais['TOTAL CLIENTES'].sum()))
    with col2:
        st.write("TOTAL DE PEDIDOS 🚚:", int(vendas_mensais['TOTAL PEDIDOS'].sum()))

def criar_tabela_vendas_mensais(data, tipo_filtro, valores_filtro, vendedor=None):
    try:
        # Verifica e remove colunas duplicadas
        if data.columns.duplicated().any():
            data = data.loc[:, ~data.columns.duplicated()]

        # Verifica colunas obrigatórias
        obrigatorias = ['DATAPEDIDO', 'CODCLIENTE', 'CLIENTE', 'QUANTIDADE']
        faltantes = [col for col in obrigatorias if col not in data.columns]
        
        if faltantes:
            st.error(f"Colunas obrigatórias faltando: {', '.join(faltantes)}")
            return pd.DataFrame()
        
        # Converte DATAPEDIDO para datetime e cria MES_ANO
        data['DATAPEDIDO'] = pd.to_datetime(data['DATAPEDIDO'], errors='coerce')
        data['MES_ANO'] = data['DATAPEDIDO'].dt.to_period('M').astype(str)

        # Filtra por vendedor, se especificado
        if vendedor and 'VENDEDOR' in data.columns:
            data = data[data['VENDEDOR'] == vendedor]
            if data.empty:
                return pd.DataFrame()

        # Aplica o filtro de fornecedor ou produto (múltiplos valores)
        if tipo_filtro == "Fornecedor":
            if 'FORNECEDOR' not in data.columns:
                st.error("A coluna 'FORNECEDOR' não está presente nos dados filtrados.")
                return pd.DataFrame()
            data = data[data['FORNECEDOR'].isin(valores_filtro)]
        elif tipo_filtro == "Produto":
            if 'PRODUTO' not in data.columns:
                st.error("A coluna 'PRODUTO' não está presente nos dados filtrados.")
                return pd.DataFrame()
            data = data[data['PRODUTO'].isin(valores_filtro)]

        if data.empty:
            st.warning(f"Nenhum dado encontrado para {tipo_filtro}: {', '.join(valores_filtro)}")
            return pd.DataFrame()

        # Define colunas de agrupamento base
        group_cols = ['CODUSUR', 'VENDEDOR', 'ROTA', 'PERIODO', 'CODCLIENTE', 'CLIENTE', 'FANTASIA']

        # Agrupa os dados por cliente e mês, somando as quantidades
        tabela = data.groupby(group_cols + ['MES_ANO'])['QUANTIDADE'].sum().unstack(fill_value=0).reset_index()

        # Converte CODCLIENTE para string sem vírgulas
        tabela['CODCLIENTE'] = tabela['CODCLIENTE'].astype(int).astype(str)

        # Define as colunas de meses e reordena
        meses = sorted([col for col in tabela.columns if col not in group_cols])
        
        # Adiciona uma coluna com o total geral por cliente
        tabela['TOTAL'] = tabela[meses].sum(axis=1)

        return tabela[group_cols + meses + ['TOTAL']]
    
    except Exception as e:
        st.error(f"Erro ao processar dados: {str(e)}")
        return pd.DataFrame()

def criar_tabela_vendas_mensais_por_produto(data, fornecedor, ano):
    data_filtrada = data[(data['FORNECEDOR'] == fornecedor) & (data['DATAPEDIDO'].dt.year == ano)].copy()

    if data_filtrada.empty:
        return pd.DataFrame()
    
    data_filtrada['MES'] = data_filtrada['DATAPEDIDO'].dt.strftime('%b')

    tabela = pd.pivot_table(
        data_filtrada,
        values='QUANTIDADE',
        index='PRODUTO',
        columns='MES',
        aggfunc='sum',
        fill_value=0
    )

    mes_ordenado = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']
    tabela = tabela.reindex(columns=[m for m in mes_ordenado if m in tabela.columns])

    tabela['TOTAL'] = tabela.sum(axis=1)

    tabela = tabela.reset_index()

    return tabela

def main():
    # Adicionar auto-refresh a cada 3 minutos (180 segundos)
    refresh_interval = 300
    st_autorefresh(interval=refresh_interval * 1000, key="data_refresh")

    # Exibir a próxima atualização
    next_refresh = datetime.now() + timedelta(seconds=refresh_interval)
    st.write(f"Próxima atualização em: {next_refresh.strftime('%H:%M:%S')}")

    # Botão de atualização manual
    if st.button("Atualizar Dados"):
        st.session_state["force_refresh"] = True

    st.markdown(
        """
        <div style="display: flex; align-items: center;">
            <img src="https://cdn-icons-png.flaticon.com/512/1028/1028011.png" 
                 width="40" style="margin-right: 10px;">
            <h2 style="margin: 0;"> Detalhes Vendedores</h2>
        </div>
        """,
        unsafe_allow_html=True)

    st.markdown("### Resumo de Vendas")
    data = fetch_pcpedc_data()
    
    if data.empty:
        st.error("Não foi possível carregar os dados de pcpedc.db.")
        return

    st.markdown(
        """
        <div style="display: flex; align-items: center;">
            <img src="https://cdn-icons-png.flaticon.com/512/6428/6428747.png" 
                 width="40" style="margin-right: 10px;">
            <p style="margin: 0;">Filtro</p>
        </div>
        """,
        unsafe_allow_html=True)
    data_inicial = st.date_input("Data Inicial", value=date.today())
    data_final = st.date_input("Data Final", value=date.today())

    if data_inicial > data_final:
        st.error("A Data Inicial não pode ser maior que a Data Final.")
        return

    data_inicial = pd.to_datetime(data_inicial)
    data_final = pd.to_datetime(data_final)
    vendedores, data_filtrada = calcular_detalhes_vendedores(data, data_inicial, data_final)

    if not vendedores.empty:
        exibir_detalhes_vendedores(vendedores)
        vendedores_sorted = vendedores['NOME'].str.strip().str.upper().sort_values().reset_index(drop=True)

        if 'ALTOMERCADO' in vendedores_sorted.values:
            vendedor_default = vendedores_sorted[vendedores_sorted == 'ALTOMERCADO'].index[0]
        else:
            vendedor_default = 0

        vendedor_default = int(vendedor_default)
        vendedores_display = vendedores['NOME'].str.strip().sort_values().reset_index(drop=True)
        vendedor_selecionado = st.selectbox("Selecione um Vendedor", vendedores_display, index=vendedor_default)
        ano_selecionado = st.selectbox("Selecione um Ano para o Gráfico", [2024, 2025])
        exibir_grafico_vendas_por_vendedor(data, vendedor_selecionado, ano_selecionado)
    else:
        st.warning("Não há dados para o período selecionado.")

    # Seção de vendas por cliente
    st.markdown("---")
    st.markdown("## Detalhamento Venda Produto ##")

    # Seletor de data para a seção de vendas por cliente
    st.markdown("### Filtro de Período")
    vendas_data_inicial = st.date_input("Data Inicial para Vendas", value=date.today(), key="vendas_inicial")
    vendas_data_final = st.date_input("Data Final para Vendas", value=date.today())

    if vendas_data_inicial > vendas_data_final:
        st.error("A Data Inicial não pode ser maior que a Data Final na seção de vendas por cliente.")
        return

    vendas_data_inicial = pd.to_datetime(vendas_data_inicial)
    vendas_data_final = pd.to_datetime(vendas_data_final)

    # Carrega dados
    data_vendas = fetch_pcvendedor_data()

    if data_vendas.empty:
        st.error("Dados de vendas não puderam ser carregados de pcvendedor.db.")
        return

    data_vendas['DATAPEDIDO'] = pd.to_datetime(data_vendas['DATAPEDIDO'], errors='coerce')
    data_vendas = data_vendas[(data_vendas['DATAPEDIDO'] >= vendas_data_inicial) & 
                              (data_vendas['DATAPEDIDO'] <= vendas_data_final)]

    if data_vendas.empty:
        st.warning("Nenhum dado encontrado para o período selecionado na seção de vendas por cliente.")
        return

    # Verifica colunas disponíveis
    opcoes_filtro = []
    if 'FORNECEDOR' in data_vendas.columns:
        opcoes_filtro.append("Fornecedor")
    if 'PRODUTO' in data_vendas.columns:
        opcoes_filtro.append("Produto")

    if not opcoes_filtro:
        st.error("❌ Nenhum filtro disponível.")
        st.stop()

    # Interface de filtros principal
    tipo_filtro = st.radio(
        "Filtrar por:", 
        opcoes_filtro, 
        horizontal=True,
        key="filtro_principal_radio"
    )

    # Dividindo em colunas
    col_filtros, col_bloqueado = st.columns(2)

    with col_filtros:
        if tipo_filtro == "Fornecedor":
            fornecedores = sorted(data_vendas['FORNECEDOR'].dropna().unique())
            selecionar_todos = st.checkbox(
                "Selecionar Todos os Fornecedores", 
                key="todos_fornecedores_check"
            )
            if selecionar_todos:
                itens_selecionados = fornecedores
                placeholder = "Todos os fornecedores selecionados"
            else:
                itens_selecionados = st.multiselect(
                    "Selecione os fornecedores:",
                    fornecedores,
                    key="fornecedores_multiselect"
                )
                placeholder = None
            
            # Mostra apenas um placeholder quando "Selecionar Todos" está ativo
            if selecionar_todos:
                st.text(placeholder)
        
        elif tipo_filtro == "Produto":
            produtos = sorted(data_vendas['PRODUTO'].dropna().unique())
            selecionar_todos = st.checkbox(
                "Selecionar Todos os Produtos", 
                key="todos_produtos_check"
            )
            if selecionar_todos:
                itens_selecionados = produtos
                placeholder = "Todos os produtos selecionados"
            else:
                itens_selecionados = st.multiselect(
                    "Selecione os produtos:",
                    produtos,
                    key="produtos_multiselect"
                )
                placeholder = None
            
            if selecionar_todos:
                st.text(placeholder)
    
    with col_bloqueado:
        if 'BLOQUEADO' in data_vendas.columns:
            filtro_bloqueado = st.radio(
                "Clientes:", 
                ["Todos", "Bloqueado", "Não bloqueado"],
                horizontal=True,
                key="filtro_bloqueado_radio"
            )
        else:
            st.warning("Coluna 'BLOQUEADO' não encontrada nos dados")
            filtro_bloqueado = "Todos"

    # Filtro de vendedores
    vendedores = sorted(data_vendas['VENDEDOR'].dropna().unique())
    selecionar_todos_vendedores = st.checkbox(
        "Selecionar Todos os Vendedores", 
        key="todos_vendedores_check"
    )
    if selecionar_todos_vendedores:
        vendedores_selecionados = vendedores
        st.text("Todos os vendedores selecionados")
    else:
        vendedores_selecionados = st.multiselect(
            "Filtrar por Vendedor (opcional):",
            vendedores,
            key="vendedores_multiselect"
        )

    if st.button("Gerar Relatório", key="gerar_relatorio_btn"):
        if not itens_selecionados:
            st.warning("Por favor, selecione pelo menos um item para gerar o relatório.")
            return

        with st.spinner("Processando dados..."):
            if 'BLOQUEADO' in data_vendas.columns:
                if filtro_bloqueado == "Bloqueado":
                    data_vendas = data_vendas[data_vendas['BLOQUEADO'] == 'S']
                elif filtro_bloqueado == "Não bloqueado":
                    data_vendas = data_vendas[data_vendas['BLOQUEADO'] == 'N']
            
            if not vendedores_selecionados or len(vendedores_selecionados) == len(vendedores):
                tabela = criar_tabela_vendas_mensais(data_vendas, tipo_filtro, itens_selecionados)
                if not tabela.empty:
                    # Configuração do AgGrid com filtros nas colunas
                    gb = GridOptionsBuilder.from_dataframe(tabela)
                    gb.configure_default_column(filter=True, sortable=True, resizable=True)
                    gb.configure_column("TOTAL", filter=False)  # Desativa filtro na coluna TOTAL
                    grid_options = gb.build()

                    # Exibe a tabela com filtros embutidos
                    AgGrid(
                        tabela,
                        gridOptions=grid_options,
                        update_mode=GridUpdateMode.NO_UPDATE,
                        fit_columns_on_grid_load=False,
                        height=400,
                        allow_unsafe_jscode=True,
                    )

                    # Botão de download da tabela original
                    csv = tabela.to_csv(index=False, sep=';', decimal=',').encode('utf-8')
                    st.download_button(
                        f"📥 Baixar CSV - {tipo_filtro}", 
                        data=csv,
                        file_name=f"vendas_{tipo_filtro.lower()}_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime='text/csv'
                    )
                else:
                    st.warning(f"Nenhum dado encontrado para {tipo_filtro}: {', '.join(itens_selecionados)}")
            
            else:
                for vendedor in vendedores_selecionados:
                    st.markdown(f"#### Vendedor: {vendedor}")
                    tabela = criar_tabela_vendas_mensais(data_vendas, tipo_filtro, itens_selecionados, vendedor)
                    if not tabela.empty:
                        # Configuração do AgGrid com filtros nas colunas
                        gb = GridOptionsBuilder.from_dataframe(tabela)
                        gb.configure_default_column(filter=True, sortable=True, resizable=True)
                        gb.configure_column("TOTAL", filter=False)  # Desativa filtro na coluna TOTAL
                        grid_options = gb.build()

                        # Exibe a tabela com filtros embutidos
                        AgGrid(
                            tabela,
                            gridOptions=grid_options,
                            update_mode=GridUpdateMode.NO_UPDATE,
                            fit_columns_on_grid_load=False,
                            height=400,
                            allow_unsafe_jscode=True,
                            wrapText=True,
                            autoHeight=True,
                            autoWeight=True
                        )

                        # Botão de download da tabela original
                        csv = tabela.to_csv(index=False, sep=';', decimal=',').encode('utf-8')
                        st.download_button(
                            f"📥 Baixar CSV - {tipo_filtro} - {vendedor}", 
                            data=csv,
                            file_name=f"vendas_{tipo_filtro.lower()}_{vendedor}_{datetime.now().strftime('%Y%m%d')}.csv",
                            mime='text/csv'
                        )
                    else:
                        st.warning(f"Nenhum dado encontrado para {tipo_filtro}: {', '.join(itens_selecionados)} e vendedor {vendedor}")

if __name__ == "__main__":
    main()