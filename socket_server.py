import socket
import threading
import json
import logging
import queue
import time

logger = logging.getLogger(__name__)
# Estruturas de dados simplificadas
client_queues = {}  # Filas de mensagens (chave: addr, valor: queue)
client_names = {}   # Nome dos clientes (chave: addr, valor: nome)
client_connections = {}  # Conexões ativas (chave: addr, valor: conexão)
name_to_addr = {}   # Mapeamento de nome para endereço (chave: nome, valor: addr)
lock = threading.Lock()  # Lock para acesso às estruturas compartilhadas

def load_json_from_file(file_path):
    """Carrega o conteúdo do arquivo JSON."""
    try:
        with open(file_path, "r") as file:
            content = json.load(file)
        return json.dumps(content)
    except Exception as e:
        logger.error(f"Erro ao carregar JSON do arquivo {file_path}: {e}")
        return "{}"

def notify_clients(lra_data=None):
    """Envia o JSON para todos os clientes."""
    if not lra_data:
        logger.warning("Nenhum dado LRA para enviar.")
        return
    
    try:
        json_content = json.dumps(lra_data)
        logger.debug(f"JSON preparado para envio: {json_content}")
    except (TypeError, ValueError) as e:
        logger.error(f"Erro ao converter LRA data em JSON: {e}")
        return
    
    with lock:
        client_count = len(client_names)
        logger.info(f"Notificando {client_count} clientes sobre atualização de dados")
        
        # Adiciona o JSON à fila de cada cliente
        for addr in list(client_queues.keys()):
            try:
                client_queues[addr].put_nowait(json_content)
                logger.debug(f"Cliente {client_names.get(addr, addr)} marcado para notificação")
            except queue.Full:
                logger.warning(f"Fila do cliente {client_names.get(addr, addr)} está cheia, ignorando notificação")

def notify_specific_client(lra_data, target_client):
    """Envia dados apenas para um cliente específico identificado pelo nome."""
    logger.debug(f"Clientes conectados no momento: {list(client_names.values())}")
    
    try:
        json_content = json.dumps(lra_data)
    except (TypeError, ValueError) as e:
        logger.error(f"Erro ao converter LRA data em JSON para cliente {target_client}: {e}")
        return
    
    with lock:
        # Usa o mapeamento inverso para encontrar o cliente rapidamente
        if target_client in name_to_addr:
            addr = name_to_addr[target_client]
            if addr in client_queues:
                try:
                    client_queues[addr].put_nowait(json_content)
                    logger.info(f"Cliente {target_client} marcado para receber dados")
                except queue.Full:
                    logger.warning(f"Fila do cliente {target_client} está cheia, ignorando notificação")
            else:
                logger.warning(f"Fila do cliente {target_client} não encontrada")
        else:
            logger.warning(f"Cliente alvo {target_client} não encontrado entre os clientes conectados")

def sender_thread(conn, addr, message_queue):
    """Thread dedicada para enviar mensagens ao cliente."""
    client_name = client_names.get(addr, f"Cliente_{addr[1]}")
    
    try:
        while True:
            try:
                # Tenta obter uma mensagem da fila com um timeout curto
                message = message_queue.get(timeout=0.5)
                
                # Envia a mensagem imediatamente
                start_time = time.time()
                logger.info(f"Enviando JSON atualizado para '{client_name}'")
                
                conn.sendall((message + "\n").encode('utf-8'))
                message_queue.task_done()
                
                # Log de tempo para monitoramento de performance
                elapsed = (time.time() - start_time) * 1000
                logger.debug(f"Envio para '{client_name}' completado em {elapsed:.2f}ms")
                
            except queue.Empty:
                # Sem mensagens para enviar, apenas continua o loop
                continue
                
            except (BrokenPipeError, ConnectionResetError, OSError) as e:
                logger.error(f"Erro ao enviar dados para '{client_name}': {e}")
                break
                
    except Exception as e:
        logger.exception(f"Erro inesperado na thread de envio para '{client_name}': {e}")
    
    logger.info(f"Thread de envio para '{client_name}' encerrada")
    
    # Limpa recursos quando a thread termina
    try:
        with lock:
            if addr in client_names:
                disconnected_name = client_names[addr]
                logger.info(f"Cliente '{disconnected_name}' ({addr}) removido.")
                
                # Limpa todas as referências ao cliente
                del client_names[addr]
                if addr in client_queues:
                    del client_queues[addr]
                if addr in client_connections:
                    del client_connections[addr]
                if disconnected_name in name_to_addr and name_to_addr[disconnected_name] == addr:
                    del name_to_addr[disconnected_name]
    except Exception as e:
        logger.exception(f"Erro ao limpar recursos do cliente '{client_name}': {e}")

def handle_client(conn, addr):
    client_name = ""
    sender = None
    
    try:
        # Recebe o nome do cliente com timeout
        conn.settimeout(5)
        client_name = conn.recv(1024).decode().strip() or f"Cliente_{addr[1]}"
        conn.settimeout(None)  # Remove o timeout
        
        with lock:
            # Verifica se já existe um cliente com o mesmo nome
            if client_name in name_to_addr:
                old_addr = name_to_addr[client_name]
                if old_addr in client_connections and old_addr != addr:
                    logger.info(f"Cliente duplicado '{client_name}' detectado. Fechando conexão anterior.")
                    
                    # Tenta fechar a conexão anterior
                    try:
                        client_connections[old_addr].close()
                    except Exception as e:
                        logger.error(f"Erro ao fechar conexão duplicada: {e}")
                    
                    # A limpeza dos recursos será feita pela thread do sender quando terminar
            
            # Configura o novo cliente
            client_queues[addr] = queue.Queue(maxsize=100)  # Limita a 100 mensagens em fila
            client_names[addr] = client_name
            client_connections[addr] = conn
            name_to_addr[client_name] = addr
        
        logger.info(f"Cliente '{client_name}' conectado de {addr}")
        
        # Inicia a thread de envio
        sender = threading.Thread(
            target=sender_thread, 
            args=(conn, addr, client_queues[addr]),
            daemon=True
        )
        sender.start()
        
        # Loop simples para manter a conexão aberta e detectar desconexões
        while True:
            try:
                # Define um timeout longo para detectar desconexão sem muitas verificações
                conn.settimeout(60)
                data = conn.recv(1024)
                if not data:
                    logger.info(f"Cliente '{client_name}' desconectou normalmente.")
                    break
                # Os dados do cliente são ignorados, só verificamos se a conexão ainda está viva
            except socket.timeout:
                # Timeout é normal, apenas continua
                continue
            except (ConnectionResetError, BrokenPipeError, OSError) as e:
                logger.error(f"Conexão perdida com cliente '{client_name}': {e}")
                break
                
    except socket.timeout:
        logger.warning(f"Timeout ao receber nome do cliente {addr}")
    except (ConnectionResetError, BrokenPipeError, socket.error) as e:
        logger.error(f"Erro de conexão com '{client_name}': {e}")
    except Exception as e:
        logger.exception(f"Erro inesperado no tratamento do cliente '{client_name}': {e}")
    finally:
        # Tenta fechar a conexão
        try:
            conn.close()
        except Exception:
            pass
        
        # A limpeza das estruturas será feita pela thread de envio quando terminar

def start_socket_server(host, port):
    """Inicia o servidor de socket para comunicação com clientes."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((host, port))
            s.listen(100)
            logger.info(f"Servidor de socket iniciado em {host}:{port}")
            
            while True:
                try:
                    conn, addr = s.accept()
                    logger.info(f"Nova conexão recebida de {addr}")
                    
                    # Inicia thread para lidar com o cliente
                    client_thread = threading.Thread(
                        target=handle_client, 
                        args=(conn, addr), 
                        daemon=True
                    )
                    client_thread.start()
                except (ConnectionResetError, OSError) as e:
                    logger.error(f"Erro ao aceitar conexão: {e}")
                except Exception as e:
                    logger.exception(f"Erro inesperado ao aceitar conexão: {e}")
                
    except OSError as e:
        logger.critical(f"Não foi possível iniciar o servidor de socket: {e}")
    except Exception as e:
        logger.exception(f"Erro fatal no servidor de socket: {e}")