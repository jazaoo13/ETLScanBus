import threading
import time
import json
import logging
import os
import queue
import signal
import atexit
import multiprocessing
from functools import lru_cache
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any, Set, Union
from concurrent.futures import ThreadPoolExecutor
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Importações dos módulos otimizados
from database_module import (
    get_index_load, get_amostragem, update_table_with_cotas, 
    update_table_async, clear_cache, shutdown as db_shutdown
)
from socket_server import notify_specific_client, start_socket_server, notify_clients

# Configuração do multiprocessing para melhor desempenho
multiprocessing.set_start_method('spawn', force=True)

# Constantes
ROOT_DIRECTORY = r'C:\Users\Engenharia\Documents\Export'
#ROOT_DIRECTORY = "D:\\Documentos\\Export\\"
LOG_DIRECTORY = r"C:\Users\Engenharia\Documents\Export"
FILE_PROCESSING_DELAY = 0.5  # segundos para aguardar estabilização do arquivo
MAX_PROCESSING_THREADS = 1
MAX_QUEUE_SIZE = 1000

# Estruturas de dados para controle de processamento
processing_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
processing_files = set()
processing_lock = threading.Lock()
processing_timers = {}
timer_lock = threading.Lock()

# Contador de arquivos processados para métricas
stats = {
    "files_processed": 0,
    "processing_errors": 0,
    "start_time": time.time()
}
stats_lock = threading.Lock()

# Evento para sinalizar encerramento
stop_event = threading.Event()

# Configuração do logging
def setup_logging() -> logging.Logger:
    """
    Configura o sistema de logs com rotação de arquivos.
    
    Returns:
        Logger configurado
    """
    log_dir = Path(LOG_DIRECTORY)
    log_dir.mkdir(exist_ok=True)
    
    log_file = log_dir / f"app_{datetime.now().strftime('%Y%m%d')}.log"
    
    # Configuração do logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Limpa handlers existentes
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Handler para arquivo
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    
    # Handler para console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(file_format)
    
    # Adiciona os handlers ao logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# Inicializa o logger
logger = setup_logging()

# Thread pool para processamento
processing_executor = ThreadPoolExecutor(max_workers=MAX_PROCESSING_THREADS)

@lru_cache(maxsize=100)
def get_file_encoding(file_path: str) -> str:
    """
    Determina a codificação do arquivo de forma eficiente.
    
    Args:
        file_path: Caminho do arquivo
        
    Returns:
        Codificação detectada ou 'latin1' como fallback
    """
    encodings = ['utf-8', 'latin1', 'cp1252']
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                f.read(1024)  # Tenta ler um trecho
                return encoding
        except UnicodeDecodeError:
            continue
    
    return 'latin1'  # Fallback padrão

def extract_data_from_json(file_path: str) -> Tuple[
    Optional[str], 
    Optional[List[float]], 
    Optional[str], 
    Optional[str], 
    Optional[str], 
    Optional[str]
]:
    """
    Extrai dados estruturados do arquivo JSON de forma eficiente.
    
    Args:
        file_path: Caminho do arquivo JSON
        
    Returns:
        Tupla (machine_id, dimensional_values, operador, rem_a, rem_b, atrib)
    """
    try:
        # Detecta ou usa cache para encoding do arquivo
        encoding = get_file_encoding(file_path)
        
        with open(file_path, "r", encoding=encoding) as file:
            # Limitando o tamanho máximo para evitar problemas de memória
            data = json.loads(file.read(10 * 1024 * 1024))  # 10MB máximo
        
        tube_inspection = data.get("Tube_Inspection", {})
        machine_id = tube_inspection.get("Machine_id")
        operador = tube_inspection.get("Operador", "Desconhecido")
        
        # Coleta dados adicionais
        rem_a = tube_inspection.get("REM_A")
        rem_b = tube_inspection.get("REM_B")
        atrib = tube_inspection.get("ATRIB")
        
        # Extrai dados dimensionais de forma otimizada
        dimensional_data = tube_inspection.get("DIMENSIONAL", [])
        
        if not machine_id or not dimensional_data:
            logger.error(f"Dados inválidos no JSON {file_path}")
            return None, None, None, None, None, None
        
        # Usa list comprehension para melhor desempenho
        dimensional_values = [
            item.get("Medida") for item in dimensional_data
            if isinstance(item.get("Medida"), (int, float))
        ]
        
        logger.debug(f"Dados extraídos: machine_id={machine_id}, {len(dimensional_values)} valores dimensionais")
        return machine_id, dimensional_values, operador, rem_a, rem_b, atrib
    except json.JSONDecodeError as e:
        logger.error(f"Erro de formato JSON no arquivo {file_path}: {e}")
    except MemoryError:
        logger.error(f"Erro de memória ao processar arquivo {file_path} - arquivo muito grande")
    except Exception as e:
        logger.exception(f"Erro ao extrair dados do JSON {file_path}: {e}")
    
    return None, None, None, None, None, None

def extract_lra_fail_data(file_path: str) -> Dict[str, float]:
    """
    Extrai dados de falha LRA do arquivo JSON de forma otimizada.
    
    Args:
        file_path: Caminho do arquivo JSON
        
    Returns:
        Dicionário com dados de falha LRA
    """
    try:
        # Usa o mesmo encoding já detectado
        encoding = get_file_encoding(file_path)
        
        with open(file_path, "r", encoding=encoding) as file:
            data = json.loads(file.read(10 * 1024 * 1024))  # 10MB máximo
        
        tube_inspection = data.get("Tube_Inspection", {})
        lra_correction = tube_inspection.get("LRA_CORRECTION", [])
        resultado = {}
        
        # Mapeamento dos prefixos para o sufixo desejado
        mapping = {
            "DOBRA": "dobra",
            "Length": "tamanho",
            "GIRO": "giro"
        }
        
        # Algoritmo otimizado
        for correction in lra_correction:
            for item in correction.get("LRA", []):
                if item.get("Teste") == "Fail":
                    nome_original = item.get("Nome", "")
                    
                    # Otimiza a busca do prefixo
                    for prefixo, sufixo in mapping.items():
                        if nome_original.startswith(prefixo):
                            # Parse mais eficiente
                            partes = nome_original.split("_", 1)
                            if len(partes) > 1:
                                bend_number = partes[1]
                                nova_chave = f"{sufixo}_{bend_number}"
                                resultado[nova_chave] = item.get("Desvio", 0) * -1
                            break
        
        return resultado
    except Exception as e:
        logger.exception(f"Erro ao extrair dados LRA do arquivo {file_path}: {e}")
        return {}

def process_file_update(
    machine_id: str, 
    dimensional_values: List[float], 
    operador: str, 
    rem_a: str, 
    rem_b: str, 
    atrib: str,
    use_async: bool = True
) -> bool:
    """
    Processa a atualização dos dados extraídos do arquivo.
    
    Args:
        machine_id: ID da máquina
        dimensional_values: Lista de valores dimensionais
        operador: Nome do operador
        rem_a: Valor para REM_A
        rem_b: Valor para REM_B
        atrib: Valor para atributo
        use_async: Se True, usa atualização assíncrona
        
    Returns:
        True se processado com sucesso, False caso contrário
    """
    try:
        # Obtém o INDEX_LOAD para a máquina
        index_load = get_index_load(machine_id)
        
        if index_load is None:
            logger.warning(f"INDEX_LOAD não encontrado para máquina {machine_id}")
            return False
        
        # Obtém informações de amostragem
        amostragem, qtd, lote = get_amostragem(index_load)
        
        if amostragem is None:
            logger.warning(f"Amostragem não encontrada para máquina {machine_id}, INDEX_LOAD {index_load}")
            return False
        
        # Atualiza a tabela com os dados
        if use_async:
            update_table_async(index_load, amostragem, dimensional_values, operador, rem_a, rem_b, qtd, lote, atrib)
            logger.info(f"Solicitação de atualização assíncrona enviada para máquina {machine_id}, INDEX_LOAD {index_load}")
            return True
        else:
            result = update_table_with_cotas(index_load, amostragem, dimensional_values, operador, rem_a, rem_b, qtd, lote, atrib)
            if result:
                logger.info(f"Dados atualizados para máquina {machine_id}, INDEX_LOAD {index_load}")
            return result
    except Exception as e:
        logger.exception(f"Erro ao processar atualização para máquina {machine_id}: {e}")
        return False

def process_file(file_path: str) -> None:
    """
    Processa um arquivo JSON completo.
    
    Args:
        file_path: Caminho do arquivo a ser processado
    """
    try:
        with stats_lock:
            stats["files_processed"] += 1
            
        logger.info(f"Processando arquivo: {file_path}")
        
        # Extrai os dados do arquivo
        machine_id, dimensional_values, operador, rem_a, rem_b, atrib = extract_data_from_json(file_path)
        
        if machine_id and dimensional_values:
            # Processa a atualização dos dados
            process_file_update(machine_id, dimensional_values, operador, rem_a, rem_b, atrib, use_async=True)
            
            # Extrai dados LRA para notificação
            lra_data = extract_lra_fail_data(file_path)
            
            # Notifica os clientes
            if machine_id:
                logger.info(f"Notificando cliente específico: {machine_id}")
                notify_specific_client(lra_data, machine_id)
            else:
                logger.info("Notificando todos os clientes")
                notify_clients(lra_data)
        else:
            logger.warning(f"Dados inválidos ou incompletos no arquivo {file_path}.")
            with stats_lock:
                stats["processing_errors"] += 1
                
    except Exception as e:
        logger.exception(f"Erro ao processar arquivo {file_path}: {e}")
        with stats_lock:
            stats["processing_errors"] += 1
    finally:
        # Remove o arquivo da lista de processamento
        with processing_lock:
            if file_path in processing_files:
                processing_files.remove(file_path)

def process_queue_worker() -> None:
    """Thread worker para processar arquivos da fila."""
    logger.info("Iniciando worker de processamento de arquivos")
    
    while not stop_event.is_set():
        try:
            # Obtém um arquivo da fila com timeout para permitir verificação do evento de parada
            file_path = processing_queue.get(timeout=0.5)
            
            try:
                # Processa o arquivo
                process_file(file_path)
            finally:
                # Marca a tarefa como concluída
                processing_queue.task_done()
                
        except queue.Empty:
            # Fila vazia, apenas continua
            continue
        except Exception as e:
            logger.exception(f"Erro no worker de processamento: {e}")
    
    logger.info("Worker de processamento de arquivos encerrado")

class FileChangeHandler(FileSystemEventHandler):
    """
    Manipulador de eventos do sistema de arquivos otimizado.
    Detecta criação e modificação de arquivos JSON.
    """
    def on_created(self, event):
        """Processa arquivo recém-criado."""
        self._handle_file_event(event)

    def on_modified(self, event):
        """Processa arquivo modificado."""
        self._handle_file_event(event)
        
    def _handle_file_event(self, event):
        """
        Lógica comum para manipulação de eventos de arquivo.
        Implementa debounce para evitar processamento duplicado.
        """
        if event.is_directory or not event.src_path.endswith(".json"):
            return

        with timer_lock:
            # Cancela timer anterior se houver
            if event.src_path in processing_timers:
                processing_timers[event.src_path].cancel()
            
            # Cria novo timer para aguardar estabilização do arquivo
            processing_timers[event.src_path] = threading.Timer(
                FILE_PROCESSING_DELAY, 
                self._queue_file_for_processing, 
                [event.src_path]
            )
            processing_timers[event.src_path].start()

    def _queue_file_for_processing(self, file_path):
        """
        Adiciona arquivo à fila de processamento se não estiver
        já sendo processado.
        """
        try:
            # Limpa o timer
            with timer_lock:
                if file_path in processing_timers:
                    del processing_timers[file_path]
            
            # Verifica se o arquivo já está sendo processado
            with processing_lock:
                if file_path in processing_files:
                    logger.debug(f"Arquivo {file_path} já está sendo processado, ignorando")
                    return
                
                # Marca o arquivo como em processamento
                processing_files.add(file_path)
            
            # Adiciona à fila para processamento
            try:
                processing_queue.put_nowait(file_path)
                logger.debug(f"Arquivo {file_path} adicionado à fila de processamento")
            except queue.Full:
                logger.warning("Fila de processamento cheia, evento ignorado")
                # Remove da lista de processamento se não foi adicionado à fila
                with processing_lock:
                    processing_files.remove(file_path)
                    
        except Exception as e:
            logger.exception(f"Erro ao enfileirar arquivo {file_path}: {e}")
            # Garante remoção do arquivo da lista em caso de erro
            with processing_lock:
                if file_path in processing_files:
                    processing_files.remove(file_path)

def print_stats():
    """Imprime estatísticas de processamento."""
    with stats_lock:
        elapsed = time.time() - stats["start_time"]
        files_processed = stats["files_processed"]
        errors = stats["processing_errors"]
    
    logger.info(f"Estatísticas de processamento:")
    logger.info(f"  - Tempo em execução: {elapsed:.2f} segundos")
    logger.info(f"  - Arquivos processados: {files_processed}")
    logger.info(f"  - Erros de processamento: {errors}")
    logger.info(f"  - Taxa de processamento: {files_processed / elapsed if elapsed > 0 else 0:.2f} arquivos/segundo")
    logger.info(f"  - Tamanho da fila atual: {processing_queue.qsize()}")

def cleanup():
    """Função de limpeza para encerramento adequado do aplicativo."""
    logger.info("Iniciando limpeza para encerramento...")
    
    # Sinaliza para todas as threads pararem
    stop_event.set()
    
    # Limpa cache do módulo de banco de dados
    clear_cache()
    
    # Encerra conexões com banco de dados
    db_shutdown()
    
    # Aguarda a fila de processamento terminar (com timeout)
    try:
        processing_queue.join(timeout=5.0)
        logger.info("Fila de processamento encerrada com sucesso")
    except Exception:
        logger.warning("Timeout ao aguardar término da fila de processamento")
    
    # Encerra ThreadPoolExecutor
    processing_executor.shutdown(wait=False)
    
    # Imprime estatísticas finais
    print_stats()
    
    logger.info("Limpeza finalizada. Aplicação encerrada.")

def signal_handler(sig, frame):
    """Manipulador de sinais para encerramento limpo."""
    logger.info(f"Sinal recebido: {sig}. Iniciando encerramento limpo...")
    stop_event.set()
    cleanup()

def start_file_monitor():
    """Inicia o monitoramento de diretório."""
    try:
        observer = Observer()
        event_handler = FileChangeHandler()
        observer.schedule(event_handler, path=ROOT_DIRECTORY, recursive=False)
        observer.start()
        logger.info(f"Monitorando diretório: {ROOT_DIRECTORY}")
        
        while not stop_event.is_set():
            time.sleep(1)
        
        observer.stop()
        observer.join(timeout=3.0)
        logger.info("Monitoramento de arquivos encerrado")
    except Exception as e:
        logger.exception(f"Erro no monitoramento de arquivos: {e}")

def start_worker_threads(num_workers=MAX_PROCESSING_THREADS):
    """Inicia as threads trabalhadoras para processar a fila."""
    workers = []
    for i in range(num_workers):
        worker = threading.Thread(
            target=process_queue_worker,
            name=f"ProcessWorker-{i}",
            daemon=True
        )
        worker.start()
        workers.append(worker)
    
    logger.info(f"{num_workers} threads trabalhadoras iniciadas")
    return workers

def main():
    """Função principal do aplicativo."""
    logger.info("Iniciando aplicação otimizada de monitoramento de arquivos")
    
    # Configura manipuladores de sinais para encerramento limpo
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Registra função de limpeza para ser chamada no encerramento
    atexit.register(cleanup)
    
    # Verifica e cria diretório de monitoramento se necessário
    root_dir = Path(ROOT_DIRECTORY)
    if not root_dir.exists():
        logger.warning(f"Diretório de monitoramento não encontrado: {ROOT_DIRECTORY}")
        try:
            root_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Diretório de monitoramento criado: {ROOT_DIRECTORY}")
        except Exception as e:
            logger.error(f"Falha ao criar diretório de monitoramento: {e}")
            return 1
    
    # Inicia as threads trabalhadoras
    worker_threads = start_worker_threads()
    
    # Inicia servidor de socket em uma thread
    socket_thread = threading.Thread(
        target=start_socket_server,
        args=("0.0.0.0", 6789),
        daemon=True,
        name="SocketServer"
    )
    socket_thread.start()
    logger.info("Servidor de socket iniciado em 0.0.0.0:6789")
    
    # Inicia o monitoramento de arquivos
    monitor_thread = threading.Thread(
        target=start_file_monitor,
        daemon=True,
        name="FileMonitor"
    )
    monitor_thread.start()
    
    # Thread para imprimir estatísticas periodicamente
    def stats_reporter():
        while not stop_event.is_set():
            time.sleep(60)  # A cada minuto
            print_stats()
    
    stats_thread = threading.Thread(
        target=stats_reporter,
        daemon=True,
        name="StatsReporter"
    )
    stats_thread.start()
    
    try:
        # Aguarda as threads principais
        monitor_thread.join()
        socket_thread.join()
    except KeyboardInterrupt:
        logger.info("Interrupção de teclado detectada")
        stop_event.set()
    except Exception as e:
        logger.exception(f"Erro inesperado na thread principal: {e}")
    finally:
        # Certifica-se de que a limpeza seja executada
        if not stop_event.is_set():
            cleanup()
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)