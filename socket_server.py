import socket
import threading
import json
import logging

logger = logging.getLogger(__name__)

file_updated = False  # Variável para indicar atualização de arquivo
client_names = {}  # Dicionário para armazenar o nome dos clientes (chave: addr, valor: nome)
client_update_status = {}  # Dicionário para o status de atualização individual de cada cliente (chave: addr, valor: JSON a enviar)
client_connections = {}  # Dicionário para armazenar as conexões ativas de cada cliente (chave: addr, valor: conexão)
lock = threading.Lock()  # Controle de acesso às variáveis compartilhadas

def load_json_from_file(file_path):
    """Carrega o conteúdo do arquivo JSON."""
    try:
        with open(file_path, "r") as file:
            content = json.load(file)  # Carrega o conteúdo como um dicionário
        return json.dumps(content)  # Retorna o conteúdo como uma string JSON
    except Exception as e:
        logger.error(f"Erro ao carregar JSON do arquivo {file_path}: {e}")
        return "{}"

def notify_clients(lra_data=None):
    """Envia o JSON diretamente para todos os clientes."""
    global file_updated

    # Verifica se lra_data foi fornecido
    json_content = ""
    if lra_data:
        try:
            # Converte lra_data em string JSON
            json_content = json.dumps(lra_data)
            logger.debug(f"JSON preparado para envio: {json_content}")
        except (TypeError, ValueError) as e:
            logger.error(f"Erro ao converter LRA data em JSON: {e}")
            return
    else:
        logger.warning("Nenhum dado LRA para enviar.")
        return

    with lock:
        file_updated = True
        # Marca que todos os clientes precisam ser notificados e envia o JSON
        client_count = len(client_names)
        logger.info(f"Notificando {client_count} clientes sobre atualização de dados")
        for addr in client_names:
            client_update_status[addr] = json_content  # Armazena o JSON para envio
            logger.debug(f"Cliente {client_names[addr]} ({addr}) marcado para notificação")

def notify_specific_client(lra_data, target_client):
    """Envia dados apenas para um cliente específico identificado pelo nome."""
    try:
        json_content = json.dumps(lra_data)
        logger.debug(f"JSON preparado para envio ao cliente {target_client}: {json_content}")
    except (TypeError, ValueError) as e:
        logger.error(f"Erro ao converter LRA data em JSON para cliente {target_client}: {e}")
        return

    with lock:
        global file_updated
        file_updated = True
        
        client_found = False        
        # Percorre os clientes conectados e envia apenas para o cliente desejado
        for addr, client_name in client_names.items():
            if client_name == target_client:
                client_found = True
                client_update_status[addr] = json_content
                logger.info(f"Cliente {target_client} marcado para receber dados")
        
        if not client_found:
            logger.warning(f"Cliente alvo {target_client} não encontrado entre os clientes conectados")

def handle_client(conn, addr):
    global file_updated
    client_name = ""
    try:
        with conn:
            # Solicita o nome do cliente ao conectar
            client_name = conn.recv(1024).decode().strip() or f"Cliente_{addr[1]}"
            with lock:
                # Verifica se já existe um cliente com o mesmo nome e remove os anteriores
                for other_addr in list(client_names.keys()):
                    if client_names[other_addr] == client_name and other_addr != addr:
                        logger.info(f"Cliente duplicado '{client_name}' detectado. Fechando conexão de {other_addr}.")
                        # Tenta fechar a conexão duplicada, se existir
                        if other_addr in client_connections:
                            try:
                                client_connections[other_addr].close()
                            except Exception as e:
                                logger.error(f"Erro ao fechar conexão duplicada para '{client_name}': {e}")
                        # Remove a conexão duplicada dos dicionários
                        del client_names[other_addr]
                        if other_addr in client_update_status:
                            del client_update_status[other_addr]
                        if other_addr in client_connections:
                            del client_connections[other_addr]

                # Registra o novo cliente
                client_names[addr] = client_name
                client_update_status[addr] = ""  # Inicializa o status de notificação do cliente
                client_connections[addr] = conn
            logger.info(f"Cliente '{client_name}' conectado de {addr}")

            while True:
                with lock:
                    # Envia o JSON se houver atualização para o cliente
                    if file_updated and client_update_status.get(addr):
                        try:
                            logger.info(f"Enviando JSON atualizado para '{client_name}'")
                            conn.sendall((client_update_status[addr] + "\n").encode('utf-8'))
                            client_update_status[addr] = ""  # Marca como enviado para o cliente
                            file_updated = False  # Reinicia o flag de atualização após o envio
                        except (BrokenPipeError, ConnectionResetError) as e:
                            logger.error(f"Erro ao enviar dados para '{client_name}': {e}")
                            break  # Sai do loop se não puder enviar dados para o cliente

                # Verifica se o cliente ainda está conectado (timeout para recepção de dados)
                conn.settimeout(10)
                try:
                    data = conn.recv(1024).decode()
                    if not data:
                        logger.info(f"Cliente '{client_name}' desconectado.")
                        break
                except socket.timeout:
                    # Timeout atingido, continua verificando notificações
                    continue
                except (ConnectionResetError, BrokenPipeError) as e:
                    logger.error(f"Conexão perdida com cliente '{client_name}': {e}")
                    break

    except (ConnectionResetError, BrokenPipeError, socket.error) as e:
        logger.error(f"Erro de conexão com '{client_name}': {e}")
    finally:
        # Remove o cliente desconectado da lista de clientes
        with lock:
            if addr in client_names:
                logger.info(f"Cliente '{client_name}' ({addr}) removido da lista de clientes.")
                del client_names[addr]
            if addr in client_update_status:
                del client_update_status[addr]
            if addr in client_connections:
                del client_connections[addr]

def start_socket_server(host, port):
    """Inicia o servidor de socket para comunicação com clientes."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # Configura o socket para reutilizar endereço (evita "Address already in use")
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((host, port))
            s.listen()
            logger.info(f"Servidor de socket iniciado em {host}:{port}")
            
            while True:
                try:
                    conn, addr = s.accept()
                    logger.info(f"Nova conexão recebida de {addr}")
                    client_thread = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
                    client_thread.start()
                    logger.debug(f"Thread iniciada para cliente {addr}")
                except ConnectionResetError as e:
                    logger.error(f"Erro ao aceitar conexão: {e}")
                    continue
                except Exception as e:
                    logger.exception(f"Erro inesperado no servidor de socket: {e}")
                    continue
    except OSError as e:
        logger.critical(f"Não foi possível iniciar o servidor de socket: {e}")
    except Exception as e:
        logger.exception(f"Erro fatal no servidor de socket: {e}")
