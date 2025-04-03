import pyodbc
import logging

# Configuração do logger
logger = logging.getLogger(__name__)

DATABASE_PATH = r"\\servidor\geral\SISTEMA ST\BANCO DE DADOS\SSDB_teste.accdb"

def get_connection():
    """Cria e retorna uma conexão com o banco de dados Access."""
    try:
        conn = pyodbc.connect(
            r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
            f"DBQ={DATABASE_PATH};"
        )
        return conn
    except pyodbc.Error as e:
        logger.error(f"Erro ao conectar ao banco de dados: {e}")
        raise

def get_index_load(maquina):
    """Recupera o INDEX_LOAD para a máquina especificada."""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        query = "SELECT INDEX_LOAD FROM MAQ_STS WHERE MAQUINA = ?"
        cursor.execute(query, (maquina,))
        row = cursor.fetchone()
        
        if row:
            index_load = row[0]
            logger.info(f"INDEX_LOAD recuperado para máquina {maquina}: {index_load}")
            return index_load
        else:
            logger.warning(f"INDEX_LOAD não encontrado para máquina {maquina}")
            return None
    except pyodbc.Error as e:
        logger.error(f"Erro ao obter INDEX_LOAD para máquina {maquina}: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_amostragem(index_load):
    """Recupera informações de amostragem, quantidade e lote para o INDEX_LOAD especificado."""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT B.AMOSTRAGEM, B.TAM_LOTE, A.QTD
            FROM CAD_OP_TL A
            LEFT JOIN CAD_TL B ON A.COD_ITEM = B.INDEX 
            WHERE A.INDEX = ?
        """
        cursor.execute(query, (index_load,))
        row = cursor.fetchone()
        
        if row:
            amostragem = row[0]
            lote = row[1]
            qtd = row[2]
            logger.info(f"Dados recuperados para INDEX_LOAD {index_load}: amostragem={amostragem}, qtd={qtd}, lote={lote}")
            return amostragem, qtd, lote
        else:
            logger.warning(f"Dados não encontrados para INDEX_LOAD {index_load}")
            return None, None, None
    except pyodbc.Error as e:
        logger.error(f"Erro ao obter amostragem para INDEX_LOAD {index_load}: {e}")
        return None, None, None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def update_table_with_cotas(index_load, amostragem, lra_values, operador, rem_a, rem_b, qtd, lote, atrib):
    """Atualiza a tabela com os valores de cotas e informações adicionais."""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        total = int(qtd / lote) if qtd and lote else 0
        
        logger.info(f"Iniciando atualização para INDEX_LOAD {index_load}, amostragem {amostragem}, total {total}")
        
        for i in range(1, total + 1):
            table_name = f"PIP_TL_PCT_{i}"
            
            select_query = f"SELECT * FROM {table_name} WHERE INDEX = ? AND medicao < ?"
            cursor.execute(select_query, (index_load, amostragem))
            row = cursor.fetchone()
            
            if row:
                column_names = [desc[0].lower() for desc in cursor.description]
                
                current_medicao = row[column_names.index("medicao")]
                new_medicao = current_medicao + 1
                
                update_values = {"medicao": new_medicao}
                
                # Atualiza as colunas cota_a_min, cota_a_max, cota_b_min, cota_b_max com os valores do LRA
                for idx, medida in enumerate(lra_values):
                    cota_base = f"cota_{chr(97 + idx)}"
                    min_col = f"{cota_base}_min"
                    max_col = f"{cota_base}_max"
                    
                    # Verifica se as colunas min e max existem na tabela
                    if min_col in column_names and max_col in column_names:
                        existing_min = row[column_names.index(min_col)] or 0
                        existing_max = row[column_names.index(max_col)] or 0

                        # Atualiza os valores de min e max com base na nova medida
                        new_min = medida if existing_min == 0 else min(existing_min, medida)
                        new_max = medida if existing_max == 0 else max(existing_max, medida)

                        update_values[min_col] = new_min
                        update_values[max_col] = new_max
                
                # Adiciona outros dados mantidos no JSON
                if "rem_a" in column_names and rem_a is not None:
                    update_values["rem_a"] = rem_a
                if "rem_b" in column_names and rem_b is not None:
                    update_values["rem_b"] = rem_b
                if "atrib" in column_names and atrib is not None:
                    update_values["atrib"] = atrib
                
                if "insp" in column_names:
                    existing_insp = row[column_names.index("insp")] or ""
                    if operador not in existing_insp.split(", "):
                        update_values["insp"] = f"{existing_insp}, {operador}".strip(", ")

                update_values["3DM"] = "OK"
                
                update_query = f"""
                    UPDATE {table_name}
                    SET {', '.join([f'{col} = ?' for col in update_values])}
                    WHERE INDEX = ?
                """
                
                cursor.execute(update_query, list(update_values.values()) + [index_load])
                conn.commit()
                logger.info(f"Tabela {table_name} atualizada com sucesso. Novo valor de MEDICAO: {new_medicao}")
                break
    except pyodbc.Error as e:
        logger.error(f"Erro ao atualizar tabela para INDEX_LOAD {index_load}: {e}")
    except Exception as e:
        logger.exception(f"Erro inesperado ao atualizar tabela: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()