import json
from flask import Flask, jsonify, request
import cx_Oracle
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
import hashlib
from concurrent.futures import ThreadPoolExecutor
from tenacity import retry, stop_after_attempt, wait_exponential
import logging
import os
import sqlite3
import sys

app = Flask(__name__)

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÃO DE SEGURANÇA: LEITURA DE VARIÁVEIS DE AMBIENTE ---
ORACLE_USERNAME = 'COBATA'
ORACLE_PASSWORD = 'C0BAT4D1T'
ORACLE_HOST = '192.168.0.254'
ORACLE_PORT = 1523
ORACLE_SID = 'WINT'

if not all([ORACLE_USERNAME, ORACLE_PASSWORD, ORACLE_HOST]):
    logger.critical("ERRO CRÍTICO: As variáveis de ambiente ORACLE_USERNAME, ORACLE_PASSWORD e ORACLE_HOST devem ser definidas.")
    sys.exit(1)

# Diretório para bancos de dados SQLite
db_dir = 'database'
if not os.path.exists(db_dir):
    os.makedirs(db_dir)

# --- MAPA DE TABELAS E SUAS COLUNAS DE DATA (para a lógica de DELETE) ---
TABLE_DATE_COLUMNS = {
    'vwsomelier': 'DATA',
    'pcpedc': 'DATA',
    'pceest': 'DTULTSAIDA',
    'pcpedi_fornecedor': 'DATA_PEDIDO',
    'pcmovendpend': 'DATA',
    'pcpedi': 'DATA',
    'pcvendedor': 'DATAPEDIDO',
    'pcvendedor2': 'DATA',
    'pcpedc_posicao': 'DATA',
}

def connect_to_sqlite(db_name):
    return sqlite3.connect(f'{db_dir}/{db_name}.db', timeout=10)

def create_sqlite_tables():
    # Tabela para vwsomelier
    with connect_to_sqlite('vwsomelier') as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS vwsomelier (
                DESCRICAO_1 TEXT, DESCRICAO_2 TEXT, CODPROD INTEGER, DATA TEXT, QT REAL, PVENDA REAL,
                VLCUSTOFIN REAL, CONDVENDA INTEGER, NUMPED INTEGER, CODOPER TEXT, DTCANCEL TEXT,
                PRIMARY KEY (NUMPED, CODPROD)
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_vwsomelier_data ON vwsomelier (DATA)')
        conn.commit()

    # Tabela para pcpedc
    with connect_to_sqlite('pcpedc') as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pcpedc (
                CODPROD INTEGER, QT_SAIDA REAL, QT_VENDIDA_LIQUIDA REAL, PVENDA REAL, VALOR_VENDIDO_BRUTO REAL,
                VALOR_VENDIDO_LIQUIDO REAL, VALOR_DEVOLVIDO REAL, NUMPED INTEGER, DATA TEXT, DATA_DEVOLUCAO TEXT,
                CONDVENDA INTEGER, NOME TEXT, CODUSUR INTEGER, CODFILIAL INTEGER, CODPRACA INTEGER, CODCLI INTEGER,
                NOME_EMITENTE TEXT, DEVOLUCAO TEXT, PRIMARY KEY (NUMPED, CODPROD)
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_pcpedc_data ON pcpedc (DATA)')
        conn.commit()

    # Tabela para pceest
    with connect_to_sqlite('pceest') as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pceest (
                NOMES_PRODUTO TEXT, QTULTENT REAL, DTULTENT TEXT, DTULTSAIDA TEXT, CODFILIAL INTEGER, QTVENDSEMANA REAL,
                QTVENDSEMANA1 REAL, QTVENDSEMANA2 REAL, QTVENDSEMANA3 REAL, QTVENDMES REAL, QTVENDMES1 REAL,
                QTVENDMES2 REAL, QTVENDMES3 REAL, QTGIRODIA REAL, QTDEVOLMES REAL, QTDEVOLMES1 REAL, QTDEVOLMES2 REAL,
                QTDEVOLMES3 REAL, CODPROD INTEGER, QT_ESTOQUE REAL, QTRESERV REAL, QTINDENIZ REAL, DTULTPEDCOMPRA TEXT,
                BLOQUEADA REAL, CODFORNECEDOR REAL, FORNECEDOR TEXT, CATEGORIA TEXT,
                PRIMARY KEY (CODPROD, NOMES_PRODUTO, CODFILIAL)
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_pceest_dtultsaida ON pceest (DTULTSAIDA)')
        conn.commit()

    # Tabela para pcpedi_fornecedor
    with connect_to_sqlite('pcpedi_fornecedor') as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pcpedi_fornecedor (
                CODPROD INTEGER, NOME_PRODUTO TEXT, NUMPED INTEGER, DATA_PEDIDO TEXT, FORNECEDOR TEXT,
                PRIMARY KEY (NUMPED, CODPROD)
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_pcpedi_fornecedor_data_pedido ON pcpedi_fornecedor (DATA_PEDIDO)')
        conn.commit()

    # Tabela para pcmovendpend
    with connect_to_sqlite('pcmovendpend') as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pcmovendpend (
                NUMOS INTEGER, QTDITENS INTEGER, TIPOOS INTEGER, NUMCAR TEXT, CODCLIENTE INTEGER, CLIENTE TEXT,
                CODOPER TEXT, NUMPED INTEGER, DESCRICAO TEXT, NUMTRANSWMS TEXT, NUMPALETE INTEGER, PESO REAL,
                VOLUME REAL, TEMPOSEP TEXT, TEMPOCONF TEXT, TOTVOL INTEGER, TOTPECAS INTEGER, STATUS TEXT,
                DEPOSITOORIG INTEGER, DEPOSITODEST INTEGER, MOVIMENT TEXT, DATA TEXT, CONFERENTE TEXT, ROTA TEXT,
                DTINICIOOS TEXT, DTFIMOS TEXT, PRIMARY KEY (NUMOS, NUMPED)
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_pcmovendpend_data ON pcmovendpend (DATA)')
        conn.commit()

    # Tabela para pcpedi
    with connect_to_sqlite('pcpedi') as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pcpedi (
                NUMPED INTEGER, NUMCAR INTEGER, DATA TEXT, CODCLI INTEGER, QT REAL, CODPROD INTEGER, PVENDA REAL,
                POSICAO TEXT, CLIENTE TEXT, DESCRICAO_PRODUTO TEXT, CODIGO_VENDEDOR INTEGER, NOME_VENDEDOR TEXT,
                NUMNOTA TEXT, OBS TEXT, OBS1 TEXT, OBS2 TEXT, CODFILIAL INTEGER, MUNICIPIO INTEGER, CODPRACA INTEGER,
                PRACA TEXT, CODROTA INTEGER, DESCRICAO_ROTA TEXT, PRIMARY KEY (NUMPED, CODPROD)
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_pcpedi_data ON pcpedi (DATA)')
        conn.commit()

    # Tabela para pcvendedor
    with connect_to_sqlite('pcvendedor') as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pcvendedor (
                CODIGOVENDA INTEGER, SUPERVISOR TEXT, CUSTOPRODUTO REAL, CODCIDADE INTEGER, CODPRODUTO INTEGER,
                CODUSUR INTEGER, VENDEDOR TEXT, ROTA TEXT, PERIODO TEXT, CODCLIENTE INTEGER, CLIENTE TEXT, FANTASIA TEXT,
                DATAPEDIDO TEXT, PRODUTO TEXT, PEDIDO INTEGER, FORNECEDOR TEXT, QUANTIDADE REAL, BLOQUEADO TEXT, VALOR REAL,
                CODFORNECEDOR INTEGER, RAMO TEXT, ENDERECO TEXT, BAIRRO TEXT, MUNICIPIO TEXT, CIDADE TEXT, VLBONIFIC REAL,
                BONIFIC TEXT, PRIMARY KEY (PEDIDO, CODPRODUTO)
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_pcvendedor_datapedido ON pcvendedor (DATAPEDIDO)')
        conn.commit()

    # Tabela para pcvendedor2
    with connect_to_sqlite('pcvendedor2') as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pcvendedor2 (
                CODIGOVENDEDOR INTEGER, CODPROD TEXT, PVENDA REAL, QT REAL, NUMPED TEXT, CODCLI TEXT, DATA TEXT,
                CODFORNECEDOR TEXT, FORNECEDOR TEXT, VLBONIFIC TEXT, CONDVENDA TEXT, PRODUTO TEXT, VENDEDOR TEXT,
                CLIENTE TEXT, CODOPER TEXT,
                PRIMARY KEY (NUMPED, CODPROD, CODIGOVENDEDOR, CODCLI, DATA)
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_pcvendedor2_data ON pcvendedor2 (DATA)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_pcvendedor2_fornecedor ON pcvendedor2 (FORNECEDOR)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_pcvendedor2_numped ON pcvendedor2 (NUMPED)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_pcvendedor2_codprod ON pcvendedor2 (CODPROD)')
        conn.commit()

    # Tabela para pcpedc_posicao
    with connect_to_sqlite('pcpedc_posicao') as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pcpedc_posicao (
                ROTA INTEGER, M_COUNT INTEGER, L_COUNT INTEGER, F_COUNT INTEGER, DESCRICAO TEXT, DATA TEXT,
                PRIMARY KEY (ROTA, DATA)
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_pcpedc_posicao_data ON pcpedc_posicao (DATA)')
        conn.commit()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def connect_to_oracle():
    try:
        dsn = cx_Oracle.makedsn(ORACLE_HOST, ORACLE_PORT, sid=ORACLE_SID)
        return cx_Oracle.connect(ORACLE_USERNAME, ORACLE_PASSWORD, dsn)
    except cx_Oracle.DatabaseError as e:
        logger.error(f"Erro ao conectar com o banco de dados Oracle: {e}")
        raise

def review_and_update_data(db_name, fetch_function, fields, start_date, end_date, is_initial_load):
    try:
        log_prefix = f"[{'CARGA INICIAL' if is_initial_load else 'ATUALIZAÇÃO'}]"
        logger.info(f"{log_prefix} Buscando dados para '{db_name}' de {start_date} a {end_date}")
        new_data, _ = fetch_function(start_date, end_date, 1, 999999999)

        if not new_data and not is_initial_load:
            logger.info(f"{log_prefix} Nenhum dado novo retornado do Oracle para '{db_name}' no período. Nenhuma ação necessária.")
            return

        with connect_to_sqlite(db_name) as conn:
            cursor = conn.cursor()

            if is_initial_load:
                logger.info(f"{log_prefix} Limpando a tabela '{db_name}' para carga inicial completa...")
                cursor.execute(f"DELETE FROM {db_name}")
            else:
                date_column = TABLE_DATE_COLUMNS.get(db_name)
                if date_column:
                    logger.info(f"{log_prefix} Limpando dados da janela de 13 meses da tabela '{db_name}'...")
                    cursor.execute(
                        f"DELETE FROM {db_name} WHERE {date_column} BETWEEN ? AND ?",
                        (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
                    )
                else:
                    logger.warning(f"{log_prefix} Coluna de data não mapeada para '{db_name}'. Pulando delete otimizado.")

            if new_data:
                placeholders = ', '.join('?' for _ in fields)
                insert_query = f"INSERT OR REPLACE INTO {db_name} ({', '.join(fields)}) VALUES ({placeholders})"
                insert_values = [[row.get(field) for field in fields] for row in new_data]
                
                cursor.executemany(insert_query, insert_values)
                logger.info(f"{log_prefix} Inseridos/Atualizados {len(insert_values)} registros na tabela '{db_name}'")
            else:
                logger.info(f"{log_prefix} Nenhum dado novo para inserir em '{db_name}'.")

            conn.commit()
            logger.info(f"{log_prefix} Sincronização da tabela '{db_name}' concluída com sucesso.")

    except sqlite3.Error as e:
        logger.error(f"Erro de SQLite ao recarregar dados de '{db_name}': {e}")
    except Exception as e:
        logger.error(f"Erro inesperado em review_and_update_data para '{db_name}': {e}")

# --- FUNÇÕES DE BUSCA DE DADOS DO ORACLE (ORIGINAIS) ---

def get_oracle_data_paginated_vwsomelier(data_inicial, data_final, pagina, limite, last_update=None):
    try:
        connection = connect_to_oracle()
        if connection is None: return [], None
        cursor = connection.cursor()
        offset = (pagina - 1) * limite
        query = """
            WITH base_filtrada AS (
            SELECT 
                VS.DESCRICAO, VS.CODPROD, VS.DATA, VS.QT, VS.PVENDA, VS.VLCUSTOFIN, VS.CONDVENDA, VS.NUMPED,
                PM.CODOPER, PC.DTCANCEL,
                ROW_NUMBER() OVER (ORDER BY VS.DATA) AS row_num
            FROM VW_SOMELIER VS
            LEFT JOIN PCMOV PM ON VS.NUMPED = PM.NUMPED AND VS.CODPROD = PM.CODPROD
            LEFT JOIN PCPEDC PC ON PM.NUMPED = PC.NUMPED
            WHERE TRUNC(VS.DATA) BETWEEN :data_inicial AND :data_final 
                AND VS.CONDVENDA = 1
                AND VS.CODUSUR NOT IN (219, 3, 63, 100, 12, 104, 186, 217, 172, 173, 73, 144, 107, 207, 174, 149, 167, 199, 191, 218, 196, 214, 96) 
                AND (PM.CODOPER IN ('S', 'ED') OR PM.CODOPER IS NULL)
                AND VS.CODFILIAL IN (1, 2)
            ),
            validos AS (
                SELECT * FROM base_filtrada bf
                WHERE (bf.CODOPER IS NULL OR bf.CODOPER = 'S')
                AND NOT EXISTS (
                    SELECT 1 FROM PCMOV pm2
                    WHERE pm2.NUMPED = bf.NUMPED AND pm2.CODPROD = bf.CODPROD AND pm2.CODOPER = 'ED'
                )
            )
            SELECT DISTINCT 
                DESCRICAO AS DESCRICAO_1, DESCRICAO AS DESCRICAO_2, CODPROD, DATA, QT, PVENDA, VLCUSTOFIN,
                CONDVENDA, NUMPED, CODOPER, DTCANCEL 
            FROM validos
            WHERE row_num > :offset AND row_num <= :offset_plus_limit AND DTCANCEL IS NULL
            ORDER BY DATA
        """
        params = {'data_inicial': data_inicial, 'data_final': data_final, 'offset': offset, 'offset_plus_limit': offset + limite}
        cursor.execute(query, params)
        rows = cursor.fetchall()
        data_hash = hashlib.md5(str(rows).encode()).hexdigest()
        results = [{'DESCRICAO_1': row[0], 'DESCRICAO_2': row[1], 'CODPROD': row[2], 'DATA': row[3].strftime('%Y-%m-%d') if isinstance(row[3], (date, datetime)) else None, 'QT': row[4], 'PVENDA': row[5], 'VLCUSTOFIN': row[6], 'CONDVENDA': row[7], 'NUMPED': row[8], 'CODOPER': row[9], 'DTCANCEL': row[10].strftime('%Y-%m-%d') if isinstance(row[10], (date, datetime)) else None} for row in rows]
        return results, data_hash
    except cx_Oracle.DatabaseError as e:
        logger.error(f"Ocorreu um erro ao executar a consulta vwsomelier: {e}")
        return [], None
    finally:
        if 'cursor' in locals() and cursor: cursor.close()
        if 'connection' in locals() and connection: connection.close()

def get_oracle_data_paginated_vwsomelier(data_inicial, data_final, pagina, limite, last_update=None):
    try:
        connection = connect_to_oracle()
        if connection is None:
            return [], None
        cursor = connection.cursor()
        offset = (pagina - 1) * limite
        query = """
            WITH base_filtrada AS (
            SELECT 
                VS.DESCRICAO, 
                VS.CODPROD,
                VS.DATA, 
                VS.QT, 
                VS.PVENDA, 
                VS.VLCUSTOFIN,
                VS.CONDVENDA,
                VS.NUMPED,
                PM.CODOPER,
                PC.DTCANCEL,
                ROW_NUMBER() OVER (ORDER BY VS.DATA) AS row_num
            FROM VW_SOMELIER VS
            LEFT JOIN PCMOV PM ON VS.NUMPED = PM.NUMPED AND VS.CODPROD = PM.CODPROD
            LEFT JOIN PCPEDC PC ON PM.NUMPED = PC.NUMPED
            WHERE TRUNC(VS.DATA) BETWEEN :data_inicial AND :data_final 
                AND VS.CONDVENDA = 1
                AND VS.CODUSUR NOT IN (219, 3, 63, 100, 12, 104, 186, 217, 172, 173, 73, 144, 107, 207, 174, 149, 167, 199, 191, 218, 196, 214, 96) 
                AND (PM.CODOPER IN ('S', 'ED') OR PM.CODOPER IS NULL)
                AND VS.CODFILIAL IN (1, 2)
            ),
            validos AS (
                SELECT *
                FROM base_filtrada bf
                WHERE (bf.CODOPER IS NULL OR bf.CODOPER = 'S')
                AND NOT EXISTS (
                    SELECT 1
                    FROM PCMOV pm2
                    WHERE pm2.NUMPED = bf.NUMPED
                        AND pm2.CODPROD = bf.CODPROD
                        AND pm2.CODOPER = 'ED'
                )
            )
            SELECT DISTINCT 
                DESCRICAO AS DESCRICAO_1,  
                DESCRICAO AS DESCRICAO_2,  
                CODPROD, 
                DATA, 
                QT, 
                PVENDA, 
                VLCUSTOFIN,
                CONDVENDA,
                NUMPED,
                CODOPER, 
                DTCANCEL 
            FROM validos
            WHERE row_num > :offset 
            AND row_num <= :offset_plus_limit
            AND DTCANCEL IS NULL
            ORDER BY DATA
        """
        params = {
            'data_inicial': data_inicial, 
            'data_final': data_final,
            'offset': offset,
            'offset_plus_limit': offset + limite
        }
        cursor.execute(query, params)
        rows = cursor.fetchall()
        data_hash = hashlib.md5(str(rows).encode()).hexdigest()
        results = [
            {
                'DESCRICAO_1': row[0], 'DESCRICAO_2': row[1], 'CODPROD': row[2],
                'DATA': row[3].strftime('%Y-%m-%d') if isinstance(row[3], (date, datetime)) else None,
                'QT': row[4], 'PVENDA': row[5], 'VLCUSTOFIN': row[6], 'CONDVENDA': row[7],
                'NUMPED': row[8], 'CODOPER': row[9],
                'DTCANCEL': row[10].strftime('%Y-%m-%d') if isinstance(row[10], (date, datetime)) else None
            } for row in rows
        ]
        return results, data_hash
    except cx_Oracle.DatabaseError as e:
        logger.error(f"Ocorreu um erro ao executar a consulta vwsomelier: {e}")
        return [], None
    finally:
        if 'cursor' in locals() and cursor: cursor.close()
        if 'connection' in locals() and connection: connection.close()
def get_oracle_data_paginated_pcpedc(data_inicial, data_final, pagina, limite, last_update=None):
    try:
        connection = connect_to_oracle()
        if connection is None: return [], None
        cursor = connection.cursor()
        offset = (pagina - 1) * limite
        query = """
            SELECT DISTINCT
                PC.CODPROD, PC.QT_SAIDA, (PC.QT_SAIDA - COALESCE(PM.QT_DEVOLUCAO, 0)) AS QT_VENDIDA_LIQUIDA,
                PC.PVENDA, (PC.QT_SAIDA * PC.PVENDA) AS VALOR_VENDIDO_BRUTO,
                ((PC.QT_SAIDA - COALESCE(PM.QT_DEVOLUCAO, 0)) * PC.PVENDA) AS VALOR_VENDIDO_LIQUIDO,
                -(COALESCE(PM.QT_DEVOLUCAO, 0) * PC.PVENDA) AS VALOR_DEVOLVIDO, PC.NUMPED, PC.DATA,
                DATA_DEVOLUCAO, PCC.CONDVENDA, PU.NOME AS NOME, PC.CODUSUR, PCC.CODFILIAL,
                PR.PRACA AS CODPRACA, PCC.CODCLI, EM.NOME AS NOME_EMITENTE,
                CASE WHEN PM.QT_DEVOLUCAO > 0 THEN 'S/ED' ELSE 'S' END AS DEVOLUCAO
            FROM (
                SELECT DISTINCT CODPROD, QT AS QT_SAIDA, NUMPED, DATA, PVENDA, CODUSUR, CODCLI,
                    ROW_NUMBER() OVER (ORDER BY DATA) AS row_num
                FROM PCPEDI
                WHERE CODCLI NOT IN (91530, 111564, 112598, 1, 3)
                    AND CODUSUR NOT IN (219, 3, 63, 100, 12, 104, 217, 172, 173, 73, 144, 107, 207, 174, 149, 167, 199, 191, 196, 214, 96)
            ) PC
            LEFT JOIN PCPEDC PCC ON PC.NUMPED = PCC.NUMPED
            LEFT JOIN PCPRACA PR ON PCC.CODPRACA = PR.CODPRACA
            LEFT JOIN PCUSUARI PU ON PC.CODUSUR = PU.CODUSUR
            LEFT JOIN PCEMPR EM ON PCC.CODEMITENTE = EM.MATRICULA
            LEFT JOIN (
                SELECT NUMPED, CODPROD, SUM(QT) AS QT_DEVOLUCAO, MAX(DTMOV) AS DATA_DEVOLUCAO
                FROM PCMOV WHERE CODOPER = 'ED' GROUP BY NUMPED, CODPROD
            ) PM ON PC.NUMPED = PM.NUMPED AND PC.CODPROD = PM.CODPROD
            WHERE PC.DATA BETWEEN :data_inicial AND :data_final
                AND PCC.CONDVENDA = 1
                AND PCC.CODFILIAL IN (1, 2)
                AND PCC.DTCANCEL IS NULL
                AND (PC.QT_SAIDA - COALESCE(PM.QT_DEVOLUCAO, 0)) >= 0
                AND PC.row_num > :offset
                AND PC.row_num <= :offset_plus_limit
            ORDER BY PC.DATA
        """
        params = {
            'data_inicial': data_inicial, 'data_final': data_final,
            'offset': offset, 'offset_plus_limit': offset + limite
        }
        cursor.execute(query, params)
        rows = cursor.fetchall()
        data_hash = hashlib.md5(str(rows).encode()).hexdigest()
        results = [
            {
                'CODPROD': row[0], 'QT_SAIDA': row[1], 'QT_VENDIDA_LIQUIDA': row[2], 'PVENDA': row[3],
                'VALOR_VENDIDO_BRUTO': row[4], 'VALOR_VENDIDO_LIQUIDO': row[5], 'VALOR_DEVOLVIDO': row[6],
                'NUMPED': row[7], 'DATA': row[8].strftime('%Y-%m-%d') if row[8] else None,
                'DATA_DEVOLUCAO': row[9].strftime('%Y-%m-%d') if row[9] else None,
                'CONDVENDA': row[10], 'NOME': row[11], 'CODUSUR': row[12], 'CODFILIAL': row[13],
                'CODPRACA': row[14], 'CODCLI': row[15], 'NOME_EMITENTE': row[16], 'DEVOLUCAO': row[17]
            } for row in rows
        ]
        return results, data_hash
    except cx_Oracle.DatabaseError as e:
        logger.error(f"Ocorreu um erro ao executar a consulta pcpedc: {e}")
        return [], None
    finally:
        if 'cursor' in locals() and cursor: cursor.close()
        if 'connection' in locals() and connection: connection.close()
def get_oracle_data_paginated_pcest(data_inicial, data_final, pagina, limite, last_update=None):
    try:
        connection = connect_to_oracle()
        if connection is None: return [], None
        cursor = connection.cursor()
        offset = (pagina - 1) * limite
        query = """
            SELECT NOMES_PRODUTO, QTULTENT, DTULTENT, DTULTSAIDA, CODFILIAL, QTVENDSEMANA, QTVENDSEMANA1, QTVENDSEMANA2,
                       QTVENDSEMANA3, QTVENDMES, QTVENDMES1, QTVENDMES2, QTVENDMES3, QTGIRODIA, QTDEVOLMES, QTDEVOLMES1,
                       QTDEVOLMES2, QTDEVOLMES3, CODPROD, QT_ESTOQUE, QTRESERV, QTINDENIZ, DTULTPEDCOMPRA, BLOQUEADA,
                       CODFORNECEDOR, FORNECEDOR, CATEGORIA
            FROM (
                SELECT P.DESCRICAO AS NOMES_PRODUTO, PE.QTULTENT, PE.DTULTENT, PE.DTULTSAIDA, PE.CODFILIAL,
                       PE.QTVENDSEMANA, PE.QTVENDSEMANA1, PE.QTVENDSEMANA2, PE.QTVENDSEMANA3, PE.QTVENDMES,
                       PE.QTVENDMES1, PE.QTVENDMES2, PE.QTVENDMES3, PE.QTGIRODIA, PE.QTDEVOLMES, PE.QTDEVOLMES1,
                       PE.QTDEVOLMES2, PE.QTDEVOLMES3, PE.CODPROD, (PE.QTESTGER - PE.QTBLOQUEADA - PE.QTRESERV) AS QT_ESTOQUE,
                       PE.QTRESERV, PE.QTINDENIZ, PE.DTULTPEDCOMPRA, (PE.QTBLOQUEADA - PE.QTINDENIZ) AS BLOQUEADA,
                       PF.CODFORNEC AS CODFORNECEDOR, PF.FORNECEDOR, PC.CATEGORIA,
                       ROW_NUMBER() OVER (ORDER BY PE.CODPROD, PE.CODFILIAL) AS row_num
                FROM PCEST PE
                LEFT JOIN PCPRODUT P ON PE.CODPROD = P.CODPROD
                LEFT JOIN PCFORNEC PF ON P.CODFORNEC = PF.CODFORNEC
                LEFT JOIN PCCATEGORIA PC ON P.CODSEC = PC.CODSEC
                WHERE TRUNC(PE.DTULTSAIDA) BETWEEN :data_inicial AND :data_final
                    AND PE.QTESTGER <> 0 AND PE.CODFILIAL IN (1, 2, 3)
            )
            WHERE row_num > :offset AND row_num <= :offset_plus_limit
        """
        params = {
            'data_inicial': data_inicial, 'data_final': data_final,
            'offset': offset, 'offset_plus_limit': offset + limite
        }
        cursor.execute(query, params)
        colnames = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        data_hash = hashlib.md5(str(rows).encode()).hexdigest()
        results = []
        for row in rows:
            row_dict = dict(zip(colnames, row))
            for key in ['DTULTENT', 'DTULTSAIDA', 'DTULTPEDCOMPRA']:
                if row_dict.get(key) and isinstance(row_dict[key], (date, datetime)):
                    row_dict[key] = row_dict[key].strftime('%Y-%m-%d')
            results.append(row_dict)
        return results, data_hash
    except cx_Oracle.DatabaseError as e:
        logger.error(f"Ocorreu um erro ao executar a consulta pceest: {e}")
        return [], None
    finally:
        if 'cursor' in locals() and cursor: cursor.close()
        if 'connection' in locals() and connection: connection.close()
def get_oracle_data_with_supplier(data_inicial, data_final, pagina, limite, last_update=None):
    try:
        connection = connect_to_oracle()
        if connection is None: return [], None
        cursor = connection.cursor()
        offset = (pagina - 1) * limite
        query = """
            SELECT PE.CODPROD, PR.DESCRICAO AS NOME_PRODUTO, PE.NUMPED, PE.DATA AS DATA_PEDIDO, F.FORNECEDOR
            FROM (
                SELECT CODPROD, NUMPED, DATA, ROW_NUMBER() OVER (ORDER BY DATA) AS row_num
                FROM PCPEDI WHERE TRUNC(DATA) BETWEEN :data_inicial AND :data_final
            ) PE
            LEFT JOIN PCPRODUT PR ON PE.CODPROD = PR.CODPROD
            LEFT JOIN PCFORNEC F ON PR.CODFORNEC = F.CODFORNEC
            WHERE PE.row_num > :offset AND PE.row_num <= :offset_plus_limit
        """
        params = {
            'data_inicial': data_inicial, 'data_final': data_final,
            'offset': offset, 'offset_plus_limit': offset + limite
        }
        cursor.execute(query, params)
        rows = cursor.fetchall()
        data_hash = hashlib.md5(str(rows).encode()).hexdigest()
        results = [
            {
                'CODPROD': row[0], 'NOME_PRODUTO': row[1], 'NUMPED': row[2],
                'DATA_PEDIDO': row[3].strftime('%Y-%m-%d') if row[3] else None,
                'FORNECEDOR': row[4]
            } for row in rows
        ]
        return results, data_hash
    except cx_Oracle.DatabaseError as e:
        logger.error(f"Ocorreu um erro ao executar a consulta pcpedi_fornecedor: {e}")
        return [], None
    finally:
        if 'cursor' in locals() and cursor: cursor.close()
        if 'connection' in locals() and connection: connection.close()
def get_oracle_data_pcmovendpend(data_inicial, data_final, pagina, limite, last_update=None):
    try:
        connection = connect_to_oracle()
        if connection is None: return [], None
        cursor = connection.cursor()
        offset = (pagina - 1) * limite
        query = """
        SELECT * FROM (
            SELECT ROWNUM AS rn, a.* FROM (
                SELECT 
                    M.NUMOS,
                    (SELECT COUNT(1) FROM PCMOVENDPEND X WHERE X.CODFILIAL = 1 AND X.DATA BETWEEN :data_inicial AND :data_final AND NVL(X.NUMPED, 0) = NVL(M.NUMPED, 0) AND X.NUMOS = M.NUMOS) QTDITENS,
                    M.TIPOOS, M.NUMCAR, C.CODCLI AS CODCLIENTE, C.CLIENTE, M.CODOPER, M.NUMPED, S.DESCRICAO, M.NUMTRANSWMS,
                    MAX(M.NUMPALETE) AS NUMPALETE, SUM(M.QT * P.PESOBRUTO) AS PESO, SUM(M.QT * P.VOLUME) AS VOLUME,
                    (CASE WHEN (MIN(M.DTINICIOOS) IS NULL AND MAX(M.DTFIMOS) IS NULL AND MAX(M.DTFIMSEPARACAO) IS NULL) THEN '00:00:00' WHEN (MAX(M.DTFIMSEPARACAO) IS NULL AND MAX(M.DTFIMOS) IS NULL) THEN CALCULATEMPOENTREDUASDATAS(MIN(M.DTINICIOOS), SYSDATE) WHEN (MAX(M.DTFIMSEPARACAO) IS NULL) THEN CALCULATEMPOENTREDUASDATAS(MIN(M.DTINICIOOS), MAX(M.DTFIMOS)) ELSE CALCULATEMPOENTREDUASDATAS(MIN(M.DTINICIOOS), MAX(M.DTFIMSEPARACAO)) END) AS TEMPOSEP,
                    (CASE WHEN (MAX(M.DTINICIOCONFERENCIA) IS NULL AND MAX(M.DTFIMCONFERENCIA) IS NULL) THEN '00:00:00' WHEN (MAX(M.DTFIMCONFERENCIA) IS NULL) THEN CALCULATEMPOENTREDUASDATAS(MAX(M.DTINICIOCONFERENCIA), SYSDATE) ELSE CALCULATEMPOENTREDUASDATAS(MAX(M.DTINICIOCONFERENCIA), MAX(M.DTFIMCONFERENCIA)) END) AS TEMPOCONF,
                    (CASE WHEN M.TIPOOS = 17 THEN SUM(NVL(M.NUMVOL, 0)) WHEN M.TIPOOS = 13 THEN MAX(NVL(M.NUMVOL, 0)) WHEN M.TIPOOS = 20 THEN (SELECT SUM(NUMVOL) FROM (SELECT NUMOS, CODPROD, CODENDERECO, MAX(NVL(NUMVOL, 0)) NUMVOL FROM PCMOVENDPEND WHERE TIPOOS = 20 AND DTESTORNO IS NULL GROUP BY NUMOS, CODPROD, CODENDERECO) WHERE NUMOS = M.NUMOS GROUP BY NUMOS) WHEN M.TIPOOS = 22 THEN (SELECT COUNT(1) AS QTVOLUME FROM PCVOLUMEOS WHERE NUMOS = M.NUMOS AND DTESTORNO IS NULL) ELSE (ROUND(SUM(M.QT) / NULLIF(MAX(P.QTUNITCX), 0))) END) AS TOTVOL,
                    SUM((SELECT CASE WHEN P1.PESOVARIAVEL = 'S' AND P1.TIPOESTOQUE = 'FR' THEN (NVL(M.QTPECAS, CEIL(M.QT / DECODE(P1.PESOPECA, 0, 1, NULL, 1, P1.PESOPECA)))) ELSE 0 END FROM PCPRODUT P1 WHERE P1.CODPROD = M.CODPROD)) AS TOTPECAS,
                    (CASE WHEN TO_CHAR(M.DTFIMOS, 'DD/MM/YYYY HH24:MI') IS NOT NULL AND TO_CHAR(M.DTESTORNO, 'DD/MM/YYYY HH24:MI') IS NULL AND NVL(M.POSICAO, 'P') = 'C' THEN 'CONCLUÍDA' WHEN NVL(M.POSICAO, 'P') = 'A' THEN 'AGUARDANDO' WHEN MIN(M.DTINICIOOS) IS NOT NULL AND NVL(M.POSICAO, 'P') <> 'C' THEN 'EM ANDAMENTO' WHEN TO_CHAR(M.DTESTORNO, 'DD/MM/YYYY HH24:MI') IS NOT NULL THEN 'ESTORNADA' WHEN MIN(M.DTINICIOOS) IS NULL THEN 'NÃO INICIADO' WHEN MIN(M.DTINICIOOS) IS NOT NULL AND MAX(M.DTFIMSEPARACAO) IS NULL AND M.POSICAO = 'P' THEN 'EM ANDAMENTO' WHEN MIN(M.DTINICIOOS) IS NOT NULL AND TO_CHAR(M.DTESTORNO, 'DD/MM/YYYY HH24:MI') IS NOT NULL THEN 'ESTORNADA' WHEN MAX(M.DTINICIOCONFERENCIA) IS NOT NULL AND TO_CHAR(M.DTFIMOS, 'DD/MM/YYYY HH24:MI') IS NULL AND M.POSICAO = 'P' THEN 'EM ANDAMENTO' WHEN MAX(M.DTINICIOCONFERENCIA) IS NOT NULL AND TO_CHAR(M.DTESTORNO, 'DD/MM/YYYY HH24:MI') IS NOT NULL THEN 'ESTORNADA' WHEN TO_CHAR(M.DTFIMOS, 'DD/MM/YYYY HH24:MI') IS NULL AND MAX(M.DTFIMSEPARACAO) IS NOT NULL AND M.POSICAO = 'P' THEN 'EM ANDAMENTO' END) AS STATUS,
                    NVL((SELECT MIN(DEPOSITO) FROM PCENDERECO WHERE EXISTS (SELECT 1 FROM PCMOVENDPEND WHERE CODENDERECOORIG = PCENDERECO.CODENDERECO AND DATA BETWEEN :data_inicial AND :data_final AND NUMOS = M.NUMOS)), 1) AS DEPOSITOORIG,
                    (SELECT MIN(DEPOSITO) FROM PCENDERECO WHERE EXISTS (SELECT 1 FROM PCMOVENDPEND WHERE CODENDERECO = PCENDERECO.CODENDERECO AND DATA BETWEEN :data_inicial AND :data_final AND NUMOS = M.NUMOS)) AS DEPOSITODEST,
                    (CASE WHEN M.NUMBONUS > 0 THEN 'B - ' || M.NUMBONUS WHEN M.NUMCAR > 0 THEN 'C - ' || M.NUMCAR WHEN MAX(M.NUMPED) > 0 THEN 'P - ' || MAX(M.NUMPED) WHEN M.NUMTRANS > 0 THEN 'T - ' || M.NUMTRANS ELSE 'T - ' || M.CODROTINA END) AS MOVIMENT,
                    M.DATA, E.NOME AS CONFERENTE, RE.DESCRICAO AS ROTA, M.DTINICIOOS, M.DTFIMOS
                FROM PCMOVENDPEND M
                LEFT JOIN PCTIPOOS S ON M.TIPOOS = S.CODIGO
                LEFT JOIN PCPRODUT P ON M.CODPROD = P.CODPROD
                LEFT JOIN PCPEDC PDC ON M.NUMPED = PDC.NUMPED
                LEFT JOIN PCCLIENT C ON PDC.CODCLI = C.CODCLI
                LEFT JOIN PCEMPR E ON M.CODFUNCCONF = E.MATRICULA
                LEFT JOIN PCPRACA PR ON PDC.CODPRACA = PR.CODPRACA
                LEFT JOIN PCROTAEXP RE ON PR.ROTA = RE.CODROTA
                WHERE M.CODPROD = P.CODPROD AND M.TIPOOS = S.CODIGO AND M.NUMOS > 0 AND M.CODFILIAL = 1
                    AND M.TIPOOS IN (10, 13) AND M.DTESTORNO IS NULL AND M.DATA BETWEEN :data_inicial AND :data_final
                    AND M.POSICAO IN ('C', 'P') AND RE.CODROTA IN (1,8,2,3,25,6,4)
                GROUP BY M.NUMOS, M.TIPOOS, M.NUMCAR, M.CODOPER, M.NUMPED, S.DESCRICAO, M.NUMTRANSWMS, M.NUMBONUS,
                         M.NUMTRANS, M.CODROTINA, M.POSICAO, C.CODCLI, C.CLIENTE, TO_CHAR(M.DTFIMOS, 'DD/MM/YYYY HH24:MI'),
                         TO_CHAR(M.DTESTORNO, 'DD/MM/YYYY HH24:MI'), M.DATA, E.NOME, RE.DESCRICAO, M.DTINICIOOS, M.DTFIMOS
                ORDER BY M.TIPOOS, M.NUMOS
            ) a
        ) WHERE rn > :offset AND rn <= :offset_plus_limit
        """
        params = {
            'data_inicial': data_inicial, 'data_final': data_final,
            'offset': offset, 'offset_plus_limit': offset + limite
        }
        cursor.execute(query, params)
        colnames = [desc[0] for desc in cursor.description][1:]
        rows = cursor.fetchall()
        data_hash = hashlib.md5(str(rows).encode()).hexdigest()
        results = []
        for row in rows:
            row_dict = dict(zip(colnames, row[1:]))
            for key in ['DATA', 'DTINICIOOS', 'DTFIMOS']:
                if row_dict.get(key) and isinstance(row_dict[key], (date, datetime)):
                    row_dict[key] = row_dict[key].strftime('%Y-%m-%d')
            results.append(row_dict)
        return results, data_hash
    except cx_Oracle.DatabaseError as e:
        logger.error(f"Ocorreu um erro ao executar a consulta pcmovendpend: {e}")
        return [], None
    finally:
        if 'cursor' in locals() and cursor: cursor.close()
        if 'connection' in locals() and connection: connection.close()
def get_oracle_data_paginated_pcpedi(data_inicial, data_final, pagina, limite, last_update=None):
    try:
        connection = connect_to_oracle()
        if connection is None: return [], None
        cursor = connection.cursor()
        offset = (pagina - 1) * limite
        query = """
            SELECT 
                PC.NUMPED, PC.NUMCAR, PC.DATA, PC.CODCLI, PC.QT, PC.CODPROD, PC.PVENDA, PC.POSICAO, CL.CLIENTE,
                PR.DESCRICAO AS DESCRICAO_PRODUTO, PC.CODUSUR AS CODIGO_VENDEDOR, PU.NOME AS NOME_VENDEDOR,
                PDC.NUMNOTA, PDC.OBS, PDC.OBS1, PDC.OBS2, PDC.CODFILIAL, NVL(PCC.MUNICCOB, 0) AS MUNICIPIO,
                PRP.CODPRACA, PRP.PRACA, PRE.CODROTA, PRE.DESCRICAO AS DESCRICAO_ROTA
            FROM (
                SELECT NUMPED, NUMCAR, DATA, CODCLI, QT, CODPROD, PVENDA, POSICAO, CODUSUR,
                       ROW_NUMBER() OVER (ORDER BY DATA) AS row_num
                FROM PCPEDI WHERE TRUNC(DATA) BETWEEN :data_inicial AND :data_final
            ) PC
            LEFT JOIN PCCLIENT CL ON PC.CODCLI = CL.CODCLI
            LEFT JOIN PCPRODUT PR ON PC.CODPROD = PR.CODPROD
            LEFT JOIN PCUSUARI PU ON PC.CODUSUR = PU.CODUSUR
            LEFT JOIN PCCLIENT PCC ON PC.CODCLI = PCC.CODCLI
            LEFT JOIN PCPEDC PDC ON PC.NUMPED = PDC.NUMPED AND PDC.DTCANCEL IS NULL
            LEFT JOIN PCPRACA PRP ON PDC.CODPRACA = PRP.CODPRACA
            LEFT JOIN PCROTAEXP PRE ON PRP.ROTA = PRE.CODROTA
            WHERE PC.row_num > :offset AND PC.row_num <= :offset_plus_limit
        """
        params = {
            'data_inicial': data_inicial, 'data_final': data_final,
            'offset': offset, 'offset_plus_limit': offset + limite
        }
        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        data_hash = hashlib.md5(str(rows).encode()).hexdigest()
        results = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            if row_dict.get('DATA') and isinstance(row_dict['DATA'], (date, datetime)):
                row_dict['DATA'] = row_dict['DATA'].strftime('%Y-%m-%d')
            results.append(row_dict)
        return results, data_hash
    except cx_Oracle.DatabaseError as e:
        logger.error(f"Ocorreu um erro ao executar a consulta pcpedi: {e}")
        return [], None
    finally:
        if 'cursor' in locals() and cursor: cursor.close()
        if 'connection' in locals() and connection: connection.close()
def get_oracle_data_pcvendedorpositivacao(data_inicial, data_final, pagina, limite, last_update=None):
    try:
        connection = connect_to_oracle()
        if connection is None: return [], None
        cursor = connection.cursor()
        offset = (pagina - 1) * limite
        query = """
            WITH pedidos_filtrados AS (
                SELECT DISTINCT PCP.CONDVENDA AS CODIGOVENDA, PCS.NOME AS SUPERVISOR, PCPEDI.VLCUSTOFIN AS CUSTOPRODUTO,
                    PDA.CODCIDADE AS CODCIDADE, PCPEDI.CODPROD AS CODPRODUTO, PCPEDI.CODUSUR AS CODUSUR, PU.NOME AS VENDEDOR,
                    PR.DIASEMANA AS ROTA, PR.PERIODICIDADE AS PERIODO, PCPEDI.CODCLI AS CODCLIENTE, PCC.CLIENTE AS CLIENTE,
                    PCC.FANTASIA AS FANTASIA, TRUNC(PCPEDI.DATA) AS DATAPEDIDO, PCPROD.DESCRICAO AS PRODUTO,
                    PCPEDI.NUMPED AS PEDIDO, PCFORN.FORNECEDOR AS FORNECEDOR, PCPEDI.QT AS QUANTIDADE_ORIGINAL,
                    PCC.BLOQUEIO AS BLOQUEADO, PCPEDI.PVENDA AS VALOR, PCFORN.CODFORNEC AS CODFORNECEDOR, PCA.RAMO AS RAMO,
                    PCC.ENDERENT AS ENDERECO, PCC.BAIRROENT AS BAIRRO, PCC.MUNICENT AS MUNICIPIO, PDA.NOMECIDADE AS CIDADE,
                    PCPEDI.VLBONIFIC AS VLBONIFIC, PCPEDI.BONIFIC AS BONIFIC, PM.CODOPER,
                    ROW_NUMBER() OVER (ORDER BY PCPEDI.DATA) AS rn
                FROM PCPEDI
                LEFT JOIN PCUSUARI PU ON PCPEDI.CODUSUR = PU.CODUSUR
                LEFT JOIN (SELECT CODCLI, DIASEMANA, PERIODICIDADE, ROW_NUMBER() OVER (PARTITION BY CODCLI ORDER BY DIASEMANA) AS rn_rota FROM PCROTACLI) PR ON PCPEDI.CODCLI = PR.CODCLI AND PR.rn_rota = 1
                LEFT JOIN PCCLIENT PCC ON PCPEDI.CODCLI = PCC.CODCLI
                LEFT JOIN PCPRODUT PCPROD ON PCPEDI.CODPROD = PCPROD.CODPROD
                LEFT JOIN PCFORNEC PCFORN ON PCPROD.CODFORNEC = PCFORN.CODFORNEC
                LEFT JOIN PCATIVI PCA ON PCC.CODATV1 = PCA.CODATIV
                LEFT JOIN PCCIDADE PDA ON PCC.CODCIDADE = PDA.CODCIDADE
                LEFT JOIN PCPEDC PCP ON PCPEDI.NUMPED = PCP.NUMPED
                LEFT JOIN PCSUPERV PCS ON PU.CODSUPERVISOR = PCS.CODSUPERVISOR
                LEFT JOIN PCMOV PM ON PCPEDI.NUMPED = PM.NUMPED AND PCPEDI.CODPROD = PM.CODPROD
                WHERE (TRUNC(PCPEDI.DATA) BETWEEN :data_inicial AND :data_final OR TRUNC(PM.DTMOV) BETWEEN :data_inicial AND :data_final)
                    AND PCPEDI.CODUSUR NOT IN (219, 3, 63, 100, 12, 104, 186, 217, 172, 173, 73, 144, 107, 207, 174, 149, 167, 199, 191, 218, 196, 214, 96)
                    AND PCP.DTCANCEL IS NULL
            ),
            produtos_a_excluir AS (
                SELECT PEDIDO, CODPRODUTO FROM pedidos_filtrados WHERE CODOPER IN ('S', 'ED')
                GROUP BY PEDIDO, CODPRODUTO HAVING COUNT(DISTINCT CODOPER) = 2
            ),
            pedidos_validos AS (
                SELECT pf.CODIGOVENDA, pf.SUPERVISOR, pf.CUSTOPRODUTO, pf.CODCIDADE, pf.CODPRODUTO, pf.CODUSUR,
                       pf.VENDEDOR, pf.ROTA, pf.PERIODO, pf.CODCLIENTE, pf.CLIENTE, pf.FANTASIA, pf.DATAPEDIDO,
                       pf.PRODUTO, pf.PEDIDO, pf.FORNECEDOR,
                       CASE WHEN pf.CODOPER = 'ED' THEN -1 * pf.QUANTIDADE_ORIGINAL ELSE pf.QUANTIDADE_ORIGINAL END AS QUANTIDADE,
                       pf.BLOQUEADO, pf.VALOR, pf.CODFORNECEDOR, pf.RAMO, pf.ENDERECO, pf.BAIRRO, pf.MUNICIPIO,
                       pf.CIDADE, pf.VLBONIFIC, pf.BONIFIC, pf.CODOPER, pf.rn
                FROM pedidos_filtrados pf
                LEFT JOIN produtos_a_excluir pae ON pf.PEDIDO = pae.PEDIDO AND pf.CODPRODUTO = pae.CODPRODUTO
                WHERE (pf.CODOPER IS NULL OR pf.CODOPER IN ('S', 'ED', 'SB')) AND pae.PEDIDO IS NULL
            )
            SELECT DISTINCT CODIGOVENDA, SUPERVISOR, CUSTOPRODUTO, CODCIDADE, CODPRODUTO, NVL(CODUSUR, 0) AS CODUSUR,
                   NVL(VENDEDOR, 0) AS VENDEDOR, NVL(ROTA, 0) AS ROTA, NVL(PERIODO, 0) AS PERIODO, CODCLIENTE, CLIENTE,
                   FANTASIA, DATAPEDIDO, PRODUTO, NVL(PEDIDO, 0) AS PEDIDO, FORNECEDOR, QUANTIDADE, BLOQUEADO,
                   NVL(VALOR, 0) AS VALOR, CODFORNECEDOR, RAMO, ENDERECO, BAIRRO, MUNICIPIO, CIDADE, VLBONIFIC, BONIFIC
            FROM pedidos_validos
            WHERE rn > :offset AND rn <= :offset_plus_limit
            ORDER BY DATAPEDIDO, PEDIDO
        """
        params = {
            'data_inicial': data_inicial, 'data_final': data_final,
            'offset': offset, 'offset_plus_limit': offset + limite
        }
        cursor.execute(query, params)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        data_hash = hashlib.md5(str(rows).encode()).hexdigest()
        results = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            if row_dict.get('DATAPEDIDO') and isinstance(row_dict['DATAPEDIDO'], (date, datetime)):
                row_dict['DATAPEDIDO'] = row_dict['DATAPEDIDO'].strftime('%Y-%m-%d')
            results.append(row_dict)
        return results, data_hash
    except Exception as e:
        logger.error(f"Erro ao buscar dados pcvendedor: {str(e)}")
        return [], None
    finally:
        if 'cursor' in locals() and cursor: cursor.close()
        if 'connection' in locals() and connection: connection.close()

# *** FUNÇÃO CORRIGIDA ***
def get_oracle_data_pcvendedorpositivacao2(data_inicial, data_final, pagina, limite, last_update=None):
    try:
        connection = connect_to_oracle()
        if connection is None: return [], None

        cursor = connection.cursor()
        
        # Lógica de paginação foi REMOVIDA da consulta para garantir que todos os dados sejam retornados.
        query = """
            WITH transacoes_no_periodo AS (
                SELECT
                    PED.CODUSUR AS CODIGOVENDEDOR,
                    PED.CODPROD,
                    PED.PVENDA,
                    PM.QT,
                    PED.NUMPED,
                    PED.CODCLI,
                    TRUNC(PM.DTMOV) AS DATA_TRANSACAO,
                    PROD.CODFORNEC AS CODFORNECEDOR,
                    FORN.FORNECEDOR,
                    PED.VLBONIFIC,
                    PDC.CONDVENDA,
                    PROD.DESCRICAO AS PRODUTO,
                    PCUSUARI.NOME AS VENDEDOR,
                    PCCLIENT.CLIENTE,
                    PM.CODOPER
                FROM PCPEDI PED
                LEFT JOIN PCUSUARI ON PED.CODUSUR = PCUSUARI.CODUSUR
                LEFT JOIN PCPRODUT PROD ON PED.CODPROD = PROD.CODPROD
                LEFT JOIN PCFORNEC FORN ON PROD.CODFORNEC = FORN.CODFORNEC
                LEFT JOIN PCPEDC PDC ON PED.NUMPED = PDC.NUMPED
                LEFT JOIN PCCLIENT ON PED.CODCLI = PCCLIENT.CODCLI
                INNER JOIN PCMOV PM ON PED.NUMPED = PM.NUMPED AND PED.CODPROD = PM.CODPROD
                WHERE TRUNC(PM.DTMOV) BETWEEN :data_inicial AND :data_final
                    AND PM.CODOPER IN ('S', 'ED')
                    AND PED.CODCLI NOT IN (3, 91503, 111564, 1)
                    AND PDC.CONDVENDA = 1
                    AND PDC.DTCANCEL IS NULL
                    AND PM.CODUSUR NOT IN (219, 3, 63, 100, 12, 104, 186, 217, 172, 173, 73, 144, 107, 207, 174, 149, 167, 199, 191, 218, 196, 214, 96)
                    AND PM.CODFILIAL = 1
            ),
            pedidos_agregados AS (
                SELECT
                    CODIGOVENDEDOR, CODPROD, PVENDA, NUMPED, CODCLI,
                    DATA_TRANSACAO,
                    CODFORNECEDOR, FORNECEDOR, VLBONIFIC, CONDVENDA, PRODUTO, VENDEDOR, CLIENTE,
                    SUM(CASE WHEN CODOPER = 'S' THEN QT WHEN CODOPER = 'ED' THEN -QT ELSE 0 END) AS QT_LIQUIDA,
                    LISTAGG(CODOPER, ', ') WITHIN GROUP (ORDER BY CODOPER) AS OPERACOES_ENVOLVIDAS
                FROM transacoes_no_periodo
                GROUP BY
                    CODIGOVENDEDOR, CODPROD, PVENDA, NUMPED, CODCLI, DATA_TRANSACAO,
                    CODFORNECEDOR, FORNECEDOR, VLBONIFIC, CONDVENDA, PRODUTO, VENDEDOR, CLIENTE
            ),
            final_results AS (
                SELECT
                    CODIGOVENDEDOR, NVL(TO_CHAR(CODPROD), '') AS CODPROD, PVENDA, QT_LIQUIDA AS QT,
                    NVL(TO_CHAR(NUMPED), '') AS NUMPED, NVL(TO_CHAR(CODCLI), '') AS CODCLI,
                    DATA_TRANSACAO AS DATA,
                    NVL(TO_CHAR(CODFORNECEDOR), '') AS CODFORNECEDOR, NVL(FORNECEDOR, '') AS FORNECEDOR,
                    NVL(TO_CHAR(VLBONIFIC), '') AS VLBONIFIC, NVL(TO_CHAR(CONDVENDA), '') AS CONDVENDA,
                    NVL(PRODUTO, '') AS PRODUTO, NVL(VENDEDOR, '') AS VENDEDOR, NVL(CLIENTE, '') AS CLIENTE,
                    OPERACOES_ENVOLVIDAS AS CODOPER
                FROM pedidos_agregados
                WHERE QT_LIQUIDA <> 0
            )
            SELECT * FROM final_results
        """
        # Parâmetros de paginação foram removidos
        params = {
            'data_inicial': data_inicial, 'data_final': data_final
        }
        cursor.execute(query, params)
        
        # Adicionando um log para verificar quantas linhas o Python está recebendo do Oracle
        rows = cursor.fetchall()
        logger.info(f"Consulta para pcvendedor2 retornou {len(rows)} linhas do Oracle.")

        data_hash = hashlib.md5(str(rows).encode()).hexdigest()
        
        columns = [col[0] for col in cursor.description]
        results = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            if row_dict.get('DATA') and isinstance(row_dict['DATA'], (date, datetime)):
                row_dict['DATA'] = row_dict['DATA'].strftime('%Y-%m-%d')
            results.append(row_dict)

        return results, data_hash
    except Exception as e:
        logger.error(f"Erro ao buscar dados pcvendedor2: {str(e)}")
        return [], None
    finally:
        if 'cursor' in locals() and cursor: cursor.close()
        if 'connection' in locals() and connection: connection.close()


def get_data_pcpedc_por_posicao(data_inicial, data_final, pagina, limite, last_update=None):
    try:
        connection = connect_to_oracle()
        if connection is None: return [], None
        cursor = connection.cursor()
        offset = (pagina - 1) * limite
        query = """
            SELECT ROTA, M_COUNT, L_COUNT, F_COUNT, DESCRICAO, DATA
            FROM (
                SELECT PC.DATA, PR.ROTA,
                       COUNT(CASE WHEN PC.POSICAO = 'M' THEN 1 END) AS M_COUNT,
                       COUNT(CASE WHEN PC.POSICAO = 'L' THEN 1 END) AS L_COUNT,
                       COUNT(CASE WHEN PC.POSICAO = 'F' THEN 1 END) AS F_COUNT,
                       RE.DESCRICAO,
                       ROW_NUMBER() OVER (ORDER BY PR.ROTA) AS row_num
                FROM PCPEDC PC
                JOIN PCPRACA PR ON PC.CODPRACA = PR.CODPRACA
                JOIN PCROTAEXP RE ON PR.ROTA = RE.CODROTA
                WHERE TRUNC(PC.DATA) BETWEEN :data_inicial AND :data_final
                    AND PC.CODFILIAL IN (1, 3)
                GROUP BY PC.DATA, PR.ROTA, RE.DESCRICAO
            )
            WHERE row_num > :offset AND row_num <= :offset_plus_limit
        """
        params = {
            'data_inicial': data_inicial, 'data_final': data_final,
            'offset': offset, 'offset_plus_limit': offset + limite
        }
        cursor.execute(query, params)
        rows = cursor.fetchall()
        data_hash = hashlib.md5(str(rows).encode()).hexdigest()
        
        results = [
            {
                'ROTA': row[0], 'M_COUNT': row[1], 'L_COUNT': row[2], 'F_COUNT': row[3],
                'DESCRICAO': row[4], 'DATA': row[5].strftime('%Y-%m-%d') if row[5] else None
            } for row in rows
        ]
        return results, data_hash
    except cx_Oracle.DatabaseError as e:
        logger.error(f"Ocorreu um erro ao executar a consulta pcpedc_por_posicao: {e}")
        return [], None
    finally:
        if 'cursor' in locals() and cursor: cursor.close()
        if 'connection' in locals() and connection: connection.close()


# Função de orquestração para ser usada com o ThreadPool
def orchestrate_update(config, start_date, end_date, is_initial_load):
    db_name, fetch_function, fields = config
    logger.info(f"Iniciando orquestração para a tabela: {db_name}")
    try:
        review_and_update_data(db_name, fetch_function, fields, start_date, end_date, is_initial_load)
    except Exception as e:
        logger.error(f"Falha na orquestração para '{db_name}': {e}", exc_info=True)

def atualizar_dados(is_initial_load=False):
    today = date.today()
    if is_initial_load:
        start_date = date(2024, 1, 1)
        end_date = today
        logger.info(f"MODO CARGA INICIAL: Buscando todos os dados de {start_date} até {end_date}.")
    else:
        start_date = today - relativedelta(months=13)
        end_date = today
        logger.info(f"MODO ATUALIZAÇÃO: Buscando dados na janela de {start_date} a {end_date}.")
    
    create_sqlite_tables()
    
    # Esta lista deve conter TODAS as suas tabelas e funções de busca
    tasks_config = [
        ('vwsomelier', get_oracle_data_paginated_vwsomelier, ['DESCRICAO_1', 'DESCRICAO_2', 'CODPROD', 'DATA', 'QT', 'PVENDA', 'VLCUSTOFIN', 'CONDVENDA', 'NUMPED', 'CODOPER', 'DTCANCEL']),
        ('pcpedc', get_oracle_data_paginated_pcpedc, ['CODPROD', 'QT_SAIDA', 'QT_VENDIDA_LIQUIDA', 'PVENDA', 'VALOR_VENDIDO_BRUTO', 'VALOR_VENDIDO_LIQUIDO', 'VALOR_DEVOLVIDO', 'NUMPED', 'DATA', 'DATA_DEVOLUCAO', 'CONDVENDA', 'NOME', 'CODUSUR', 'CODFILIAL', 'CODPRACA', 'CODCLI', 'NOME_EMITENTE', 'DEVOLUCAO']),
        ('pceest', get_oracle_data_paginated_pcest, ['NOMES_PRODUTO','QTULTENT','DTULTENT','DTULTSAIDA','CODFILIAL','QTVENDSEMANA','QTVENDSEMANA1','QTVENDSEMANA2','QTVENDSEMANA3','QTVENDMES','QTVENDMES1','QTVENDMES2','QTVENDMES3','QTGIRODIA','QTDEVOLMES','QTDEVOLMES1','QTDEVOLMES2','QTDEVOLMES3','CODPROD','QT_ESTOQUE','QTRESERV','QTINDENIZ','DTULTPEDCOMPRA','BLOQUEADA','CODFORNECEDOR','FORNECEDOR','CATEGORIA']),
        ('pcpedi_fornecedor', get_oracle_data_with_supplier, ['CODPROD', 'NOME_PRODUTO', 'NUMPED', 'DATA_PEDIDO', 'FORNECEDOR']),
        ('pcmovendpend', get_oracle_data_pcmovendpend, ['NUMOS', 'QTDITENS', 'TIPOOS', 'NUMCAR', 'CODCLIENTE', 'CLIENTE', 'CODOPER', 'NUMPED', 'DESCRICAO', 'NUMTRANSWMS', 'NUMPALETE', 'PESO', 'VOLUME', 'TEMPOSEP', 'TEMPOCONF', 'TOTVOL', 'TOTPECAS', 'STATUS', 'DEPOSITOORIG', 'DEPOSITODEST', 'MOVIMENT', 'DATA', 'CONFERENTE', 'ROTA', 'DTINICIOOS', 'DTFIMOS']),
        ('pcpedi', get_oracle_data_paginated_pcpedi, ['NUMPED', 'NUMCAR', 'DATA', 'CODCLI', 'QT', 'CODPROD', 'PVENDA', 'POSICAO', 'CLIENTE', 'DESCRICAO_PRODUTO', 'CODIGO_VENDEDOR', 'NOME_VENDEDOR', 'NUMNOTA', 'OBS', 'OBS1', 'OBS2', 'CODFILIAL', 'MUNICIPIO', 'CODPRACA', 'PRACA', 'CODROTA', 'DESCRICAO_ROTA']),
        ('pcvendedor', get_oracle_data_pcvendedorpositivacao, ['CODIGOVENDA', 'SUPERVISOR', 'CUSTOPRODUTO', 'CODCIDADE', 'CODPRODUTO', 'CODUSUR', 'VENDEDOR', 'ROTA', 'PERIODO', 'CODCLIENTE', 'CLIENTE', 'FANTASIA', 'DATAPEDIDO', 'PRODUTO', 'PEDIDO', 'FORNECEDOR', 'QUANTIDADE', 'BLOQUEADO', 'VALOR', 'CODFORNECEDOR', 'RAMO', 'ENDERECO', 'BAIRRO', 'MUNICIPIO', 'CIDADE', 'VLBONIFIC', 'BONIFIC']),
        ('pcvendedor2', get_oracle_data_pcvendedorpositivacao2, ['CODIGOVENDEDOR', 'CODPROD', 'PVENDA', 'QT', 'NUMPED', 'CODCLI', 'DATA', 'CODFORNECEDOR', 'FORNECEDOR', 'VLBONIFIC', 'CONDVENDA', 'PRODUTO', 'VENDEDOR', 'CLIENTE', 'CODOPER']),
        ('pcpedc_posicao', get_data_pcpedc_por_posicao, ['ROTA', 'M_COUNT', 'L_COUNT', 'F_COUNT', 'DESCRICAO', 'DATA'])
    ]

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(orchestrate_update, config, start_date, end_date, is_initial_load) for config in tasks_config]
        for future in futures:
            future.result()
    
    logger.info("Ciclo de atualização de todos os bancos de dados concluído.")

def setup_scheduler():
    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(atualizar_dados, 'interval', minutes=5, kwargs={'is_initial_load': False})
    
    def job_listener(event):
        if event.exception:
            logger.error(f"Erro ao executar o job agendado {event.job_id}: {event.exception}")
        else:
            logger.info(f"Job {event.job_id} executado com sucesso.")
            
    scheduler.add_listener(job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    scheduler.start()
    logger.info("Agendador iniciado para atualizar os dados a cada 5 minutos.")

# --- ENDPOINTS OTIMIZADOS ---

def create_endpoint(endpoint_name, table_name, date_column, columns):
    def endpoint():
        data_inicial_str = request.args.get('data_inicial')
        data_final_str = request.args.get('data_final')
        if not data_inicial_str or not data_final_str:
            return jsonify({"error": "Parâmetros 'data_inicial' e 'data_final' são obrigatórios."}), 400

        try:
            data_inicial = datetime.strptime(data_inicial_str, "%Y-%m-%d").date()
            data_final = datetime.strptime(data_final_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({"error": "Formato de data inválido. Use YYYY-MM-DD."}), 400

        pagina = int(request.args.get('pagina', 1))
        limite = int(request.args.get('limite', 999999999))
        offset = (pagina - 1) * limite

        try:
            with connect_to_sqlite(table_name) as conn:
                cursor = conn.cursor()
                # OTIMIZAÇÃO: Consulta direta na coluna para usar o índice
                query = f"""
                    SELECT {', '.join(columns)}
                    FROM {table_name}
                    WHERE {date_column} BETWEEN ? AND ?
                    ORDER BY {date_column}
                    LIMIT ? OFFSET ?
                """
                params = (data_inicial.strftime('%Y-%m-%d'), data_final.strftime('%Y-%m-%d'), limite, offset)
                cursor.execute(query, params)
                rows = cursor.fetchall()
                
                if not rows:
                    return jsonify({"message": "Nenhum dado encontrado para o intervalo de datas.", "data": []}), 200

                results = [dict(zip(columns, row)) for row in rows]
                return jsonify(results)
        except sqlite3.Error as e:
            logger.error(f"Erro ao consultar SQLite para {endpoint_name}: {e}")
            return jsonify({"error": "Erro interno ao consultar dados."}), 500

    endpoint.__name__ = endpoint_name
    app.route(f'/{endpoint_name}', methods=['GET'])(endpoint)

# --- CRIAÇÃO DOS ENDPOINTS ---
# Certifique-se de que a lista de colunas aqui corresponde exatamente à da tabela SQLite
create_endpoint('dados_vwsomelier', 'vwsomelier', 'DATA', ['DESCRICAO_1', 'DESCRICAO_2', 'CODPROD', 'DATA', 'QT', 'PVENDA', 'VLCUSTOFIN', 'CONDVENDA', 'NUMPED', 'CODOPER', 'DTCANCEL'])
create_endpoint('dados_pcpedc', 'pcpedc', 'DATA', ['CODPROD', 'QT_SAIDA', 'QT_VENDIDA_LIQUIDA', 'PVENDA', 'VALOR_VENDIDO_BRUTO', 'VALOR_VENDIDO_LIQUIDO', 'VALOR_DEVOLVIDO', 'NUMPED', 'DATA', 'DATA_DEVOLUCAO', 'CONDVENDA', 'NOME', 'CODUSUR', 'CODFILIAL', 'CODPRACA', 'CODCLI', 'NOME_EMITENTE', 'DEVOLUCAO'])
create_endpoint('dados_pceest', 'pceest', 'DTULTSAIDA', ['NOMES_PRODUTO','QTULTENT','DTULTENT','DTULTSAIDA','CODFILIAL','QTVENDSEMANA','QTVENDSEMANA1','QTVENDSEMANA2','QTVENDSEMANA3','QTVENDMES','QTVENDMES1','QTVENDMES2','QTVENDMES3','QTGIRODIA','QTDEVOLMES','QTDEVOLMES1','QTDEVOLMES2','QTDEVOLMES3','CODPROD','QT_ESTOQUE','QTRESERV','QTINDENIZ','DTULTPEDCOMPRA','BLOQUEADA','CODFORNECEDOR','FORNECEDOR','CATEGORIA'])
create_endpoint('dados_pcpedi_fornecedor', 'pcpedi_fornecedor', 'DATA_PEDIDO', ['CODPROD', 'NOME_PRODUTO', 'NUMPED', 'DATA_PEDIDO', 'FORNECEDOR'])
create_endpoint('dados_pcmovendpend', 'pcmovendpend', 'DATA', ['NUMOS', 'QTDITENS', 'TIPOOS', 'NUMCAR', 'CODCLIENTE', 'CLIENTE', 'CODOPER', 'NUMPED', 'DESCRICAO', 'NUMTRANSWMS', 'NUMPALETE', 'PESO', 'VOLUME', 'TEMPOSEP', 'TEMPOCONF', 'TOTVOL', 'TOTPECAS', 'STATUS', 'DEPOSITOORIG', 'DEPOSITODEST', 'MOVIMENT', 'DATA', 'CONFERENTE', 'ROTA', 'DTINICIOOS', 'DTFIMOS'])
create_endpoint('dados_pcpedi', 'pcpedi', 'DATA', ['NUMPED', 'NUMCAR', 'DATA', 'CODCLI', 'QT', 'CODPROD', 'PVENDA', 'POSICAO', 'CLIENTE', 'DESCRICAO_PRODUTO', 'CODIGO_VENDEDOR', 'NOME_VENDEDOR', 'NUMNOTA', 'OBS', 'OBS1', 'OBS2', 'CODFILIAL', 'MUNICIPIO', 'CODPRACA', 'PRACA', 'CODROTA', 'DESCRICAO_ROTA'])
create_endpoint('dados_pcvendedor', 'pcvendedor', 'DATAPEDIDO', ['CODIGOVENDA', 'SUPERVISOR', 'CUSTOPRODUTO', 'CODCIDADE', 'CODPRODUTO', 'CODUSUR', 'VENDEDOR', 'ROTA', 'PERIODO', 'CODCLIENTE', 'CLIENTE', 'FANTASIA', 'DATAPEDIDO', 'PRODUTO', 'PEDIDO', 'FORNECEDOR', 'QUANTIDADE', 'BLOQUEADO', 'VALOR', 'CODFORNECEDOR', 'RAMO', 'ENDERECO', 'BAIRRO', 'MUNICIPIO', 'CIDADE', 'VLBONIFIC', 'BONIFIC'])
create_endpoint('dados_pcvendedor2', 'pcvendedor2', 'DATA', ['CODIGOVENDEDOR', 'CODPROD', 'PVENDA', 'QT', 'NUMPED', 'CODCLI', 'DATA', 'CODFORNECEDOR', 'FORNECEDOR', 'VLBONIFIC', 'CONDVENDA', 'PRODUTO', 'VENDEDOR', 'CLIENTE', 'CODOPER'])
create_endpoint('dados_pcpedc_por_posicao', 'pcpedc_posicao', 'DATA', ['ROTA', 'M_COUNT', 'L_COUNT', 'F_COUNT', 'DESCRICAO', 'DATA'])

if __name__ == '__main__':
    db_check_file = os.path.join(db_dir, 'pcpedc.db')
    is_first_run = not os.path.exists(db_check_file)

    if is_first_run:
        logger.info("PRIMEIRA EXECUÇÃO DETECTADA. Iniciando carga inicial completa de 2024...")
    else:
        logger.info("Execução subsequente. O cache já existe. Iniciando atualização padrão.")
    
    atualizar_dados(is_initial_load=is_first_run)
    
    setup_scheduler()
    
    app.run(host='0.0.0.0', port=5000, debug=False)
