import pyodbc
import logging
import contextlib
import threading
import functools
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Tuple, Optional, Dict, Any, List, Union

# Configuração do logger
logger = logging.getLogger(__name__)

# Constantes
DATABASE_PATH = r"\\servidor\geral\SISTEMA ST\BANCO DE DADOS\SSDB_123.accdb"
CONNECTION_TIMEOUT = 10  # segundos
MAX_WORKERS = 5  # Número máximo de threads para operações de banco de dados

# Pool de conexões
connection_pool = []
connection_pool_lock = threading.Lock()
connection_pool_max_size = 5

# Cache para resultados frequentes
index_load_cache = {}
index_load_cache_lock = threading.Lock()
index_load_cache_ttl = 300  # 5 minutos

amostragem_cache = {}
amostragem_cache_lock = threading.Lock()
amostragem_cache_ttl = 300  # 5 minutos

# Executor para operações assíncronas
db_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

@contextlib.contextmanager
def get_connection():
    """
    Gerenciador de contexto para conexões com o banco de dados.
    Implementa pool de conexões para reutilização e melhor desempenho.
    """
    conn = None
    try:
        # Tenta obter uma conexão do pool
        with connection_pool_lock:
            if connection_pool:
                conn = connection_pool.pop()
                try:
                    # Verifica se a conexão ainda está ativa
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.close()
                except Exception:
                    # Conexão inativa, cria uma nova
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = None

        # Se não conseguiu do pool, cria uma nova
        if conn is None:
            conn = pyodbc.connect(
                r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
                f"DBQ={DATABASE_PATH};",
                timeout=CONNECTION_TIMEOUT
            )
            # Otimização de desempenho: desabilita autocommit para operações em lote
            conn.autocommit = False

        yield conn
        
        # Confirma todas as transações pendentes
        conn.commit()
        
        # Devolve a conexão para o pool
        with connection_pool_lock:
            if len(connection_pool) < connection_pool_max_size:
                connection_pool.append(conn)
                conn = None  # Evita fechar a conexão abaixo
                
    except pyodbc.Error as e:
        logger.error(f"Erro de banco de dados: {e}")
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
        raise
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def cached_result(cache_dict, cache_lock, ttl):
    """
    Decorador para cache de resultados de funções.
    
    Args:
        cache_dict: Dicionário para armazenar o cache
        cache_lock: Lock para acesso seguro ao cache
        ttl: Tempo de vida do cache em segundos
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Cria uma chave de cache baseada nos argumentos
            cache_key = str(args) + str(sorted(kwargs.items()))
            
            with cache_lock:
                if cache_key in cache_dict:
                    timestamp, result = cache_dict[cache_key]
                    if time.time() - timestamp < ttl:
                        return result
            
            # Cache miss ou expirado, executa a função
            result = func(*args, **kwargs)
            
            # Atualiza o cache
            with cache_lock:
                cache_dict[cache_key] = (time.time(), result)
            
            return result
        return wrapper
    return decorator

@cached_result(index_load_cache, index_load_cache_lock, index_load_cache_ttl)
def get_index_load(maquina: str) -> Optional[str]:
    """
    Recupera o INDEX_LOAD para a máquina especificada com cache.
    
    Args:
        maquina: ID da máquina
        
    Returns:
        INDEX_LOAD ou None se não encontrado
    """
    try:
        with get_connection() as conn:
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
    except Exception as e:
        logger.error(f"Erro ao obter INDEX_LOAD para máquina {maquina}: {e}")
        return None

@cached_result(amostragem_cache, amostragem_cache_lock, amostragem_cache_ttl)
def get_amostragem(index_load: str) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    """
    Recupera informações de amostragem, quantidade e lote para o INDEX_LOAD com cache.
    
    Args:
        index_load: INDEX_LOAD para consulta
        
    Returns:
        Tupla (amostragem, qtd, lote) ou (None, None, None) se não encontrado
    """
    try:
        with get_connection() as conn:
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
    except Exception as e:
        logger.error(f"Erro ao obter amostragem para INDEX_LOAD {index_load}: {e}")
        return None, None, None

def update_table_with_cotas(
    index_load: str, 
    amostragem: int, 
    lra_values: List[float], 
    operador: str, 
    rem_a: str, 
    rem_b: str, 
    qtd: int, 
    lote: int, 
    atrib: str
) -> bool:
    """
    Atualiza a tabela com os valores de cotas e informações adicionais.
    Versão otimizada com transações e batch updates.
    
    Args:
        index_load: INDEX_LOAD para atualização
        amostragem: Valor de amostragem para o filtro
        lra_values: Lista de valores dimensionais
        operador: Nome do operador
        rem_a: Valor para REM_A
        rem_b: Valor para REM_B
        qtd: Quantidade
        lote: Tamanho do lote
        atrib: Valor para atributo
        
    Returns:
        True se atualização for bem-sucedida, False caso contrário
    """
    try:
        if not qtd or not lote:
            logger.warning(f"QTD ou LOTE inválidos para INDEX_LOAD {index_load}")
            return False
            
        total = int(qtd / lote)
        if total <= 0:
            logger.warning(f"Total de lotes calculado é zero para INDEX_LOAD {index_load}")
            return False
            
        logger.info(f"Iniciando atualização para INDEX_LOAD {index_load}, amostragem {amostragem}, total {total}")
        
        # Otimização: procura em múltiplas tabelas simultaneamente
        table_names = [f"PIP_TL_PCT_{i}" for i in range(1, total + 1)]
        update_data = None
        updated_table = None
        
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Primeiro passo: encontra a tabela correta para atualização
            for table_name in table_names:
                select_query = f"SELECT * FROM {table_name} WHERE INDEX = ? AND medicao < ?"
                cursor.execute(select_query, (index_load, amostragem))
                row = cursor.fetchone()
                
                if row:
                    column_names = [desc[0].lower() for desc in cursor.description]
                    current_medicao = row[column_names.index("medicao")]
                    new_medicao = current_medicao + 1
                    
                    # Prepara os dados para atualização
                    update_values = {"medicao": new_medicao}
                    
                    # Atualiza cotas com melhor manipulação de nulos
                    for idx, medida in enumerate(lra_values):
                        if idx >= 26:  # Limitado a a-z (26 letras)
                            break
                            
                        cota_base = f"cota_{chr(97 + idx)}"
                        min_col = f"{cota_base}_min"
                        max_col = f"{cota_base}_max"
                        
                        if min_col in column_names and max_col in column_names:
                            existing_min = row[column_names.index(min_col)]
                            existing_max = row[column_names.index(max_col)]
                            
                            existing_min = 0 if existing_min is None else existing_min
                            existing_max = 0 if existing_max is None else existing_max
                            
                            update_values[min_col] = medida if existing_min == 0 else min(existing_min, medida)
                            update_values[max_col] = medida if existing_max == 0 else max(existing_max, medida)
                    
                    # Adiciona outros campos de dados
                    field_mapping = {
                        "rem_a": rem_a,
                        "rem_b": rem_b,
                        "atrib": atrib
                    }
                    
                    for field, value in field_mapping.items():
                        if field in column_names and value is not None:
                            update_values[field] = value
                    
                    # Atualiza o campo INSP para incluir o operador
                    if "insp" in column_names:
                        existing_insp = row[column_names.index("insp")] or ""
                        inspectors = set(filter(None, [x.strip() for x in existing_insp.split(",")]))
                        
                        if operador and operador not in inspectors:
                            inspectors.add(operador)
                            update_values["insp"] = ", ".join(inspectors)
                    
                    update_values["3DM"] = "OK"
                    
                    # Constrói a query de atualização
                    update_query = f"""
                        UPDATE {table_name}
                        SET {', '.join([f'{col} = ?' for col in update_values])}
                        WHERE INDEX = ?
                    """
                    
                    # Executa a atualização
                    cursor.execute(update_query, list(update_values.values()) + [index_load])
                    conn.commit()
                    
                    logger.info(f"Tabela {table_name} atualizada com sucesso. Novo valor de MEDICAO: {new_medicao}")
                    return True
            
            logger.warning(f"Não foram encontrados registros para atualizar em nenhuma tabela para INDEX {index_load}")
            return False
            
    except Exception as e:
        logger.exception(f"Erro ao atualizar tabela para INDEX_LOAD {index_load}: {e}")
        return False

def clear_cache():
    """Limpa os caches para forçar nova leitura dos dados."""
    with index_load_cache_lock:
        index_load_cache.clear()
    
    with amostragem_cache_lock:
        amostragem_cache.clear()
    
    logger.info("Caches limpos com sucesso")

def update_table_async(
    index_load: str, 
    amostragem: int, 
    lra_values: List[float], 
    operador: str, 
    rem_a: str, 
    rem_b: str, 
    qtd: int, 
    lote: int, 
    atrib: str
) -> None:
    """
    Versão assíncrona da função update_table_with_cotas.
    Agenda a atualização para ser executada em thread separada.
    
    Args:
        Mesmos que update_table_with_cotas
    """
    db_executor.submit(
        update_table_with_cotas,
        index_load, 
        amostragem, 
        lra_values, 
        operador, 
        rem_a, 
        rem_b, 
        qtd, 
        lote, 
        atrib
    )
    logger.debug(f"Atualização agendada para INDEX_LOAD {index_load}")

def shutdown():
    """Finaliza recursos do módulo de banco de dados."""
    db_executor.shutdown(wait=True)
    
    # Fecha todas as conexões no pool
    with connection_pool_lock:
        for conn in connection_pool:
            try:
                conn.close()
            except Exception:
                pass
        connection_pool.clear()
    
    logger.info("Recursos do módulo de banco de dados finalizados")