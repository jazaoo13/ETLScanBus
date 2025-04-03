import threading
import time
import json
import logging
import os
import requests
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from database_module import get_index_load, get_amostragem, update_table_with_cotas
from socket_server import notify_specific_client, start_socket_server, notify_clients

# Configuração da API endpoint
API_ENDPOINT = "http://your-api-endpoint.com/data"  # Substitua pela URL da sua API
#https://fluxo.oniun.com.br/webhook/trento
# Configuração do logging
def setup_logging():
    log_directory = "logs"
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    
    log_file = os.path.join(log_directory, f"app_{datetime.now().strftime('%Y%m%d')}.log")
    
    # Configuração do logger principal
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
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

logger = setup_logging()

ROOT_DIRECTORY = "C:\\Users\\Engenharia\\Documents\\Export"
last_update = time.time()
processing_timers = {}

def extract_data_from_json(file_path):
    """
    Extrai os seguintes dados do JSON:
      - machine_id: obtido em Tube_Inspection["Machine_id"]
      - operador: obtido em Tube_Inspection["Operador"]
      - dimensional_values: lista com REM_A, REM_B, ATRIB nas primeiras posições,
        seguidos pelas medidas do campo "DIMENSIONAL", em que os itens
        Cylinder_1, Cylinder_2 e Cylinder_3 são consolidados em uma média.
    Retorna uma tupla com 4 valores.
    """
    try:
        with open(file_path, "r", encoding="latin1") as file:
            data = json.load(file)
        
        tube_inspection = data.get("Tube_Inspection", {})
        machine_id = tube_inspection.get("Machine_id")
        operador = tube_inspection.get("Operador", "Desconhecido")
        
        # Coleta dos REM_A, REM_B e ATRIB
        rem_a = tube_inspection.get("REM_A", "OK")
        rem_b = tube_inspection.get("REM_B", "OK")
        atrib = tube_inspection.get("ATRIB", "OK")
        
        # Extrai os dados dimensionais
        dimensional_data = tube_inspection.get("DIMENSIONAL", [])
        if not machine_id or not dimensional_data:
            logger.error(f"Dados inválidos no JSON {file_path}")
            return None, None, None, None
        
        # Inicializa dimensional_values com REM_A, REM_B e ATRIB como primeiros valores
        dimensional_values = []
        quality_values = [rem_a,rem_b,atrib]
        cylinder_values = []
        
        # Percorre cada item em DIMENSIONAL
        for item in dimensional_data:
            nome = item.get("Nome")
            medida = item.get("Medida")
            if isinstance(medida, (int, float)):
                # Substitui valor 0 por 0.001
                if medida == 0:
                    medida = 0.001

                # Se for algum dos cilindros, armazena para posterior média
                if nome in ["Cylinder_1", "Cylinder_2", "Cylinder_3"]:
                    cylinder_values.append(medida)
                else:
                    dimensional_values.append(medida)
        
        # Se houver medidas dos cilindros, calcula a média e a adiciona
        if cylinder_values:
            average_cylinder = sum(cylinder_values) / len(cylinder_values)
            dimensional_values.append(average_cylinder)
        
        logger.debug(f"Dados extraídos: machine_id={machine_id}, {len(dimensional_values)} valores dimensionais")
        return machine_id, dimensional_values, operador, quality_values
    except Exception as e:
        logger.exception(f"Erro ao extrair dados do JSON {file_path}: {e}")
        return None, None, None, None

def send_data_to_api(machine_id, dimensional_values, operador, quality_values):
    """
    Envia os dados extraídos para a API via POST request.
    Os valores rem_a, rem_b e atrib já estão incluídos em dimensional_values.
    """
    payload = {
        "machine_id": machine_id,
        "dimensional_values": dimensional_values,
        "quality_values": quality_values,
        "operador": operador
    }
    print(payload)
    try:
        logger.info(f"Enviando dados para API: {machine_id}")
        response = requests.post(API_ENDPOINT, json=payload)
        
        if response.status_code == 200:
            logger.info(f"Dados enviados com sucesso para API: {machine_id}")
        else:
            logger.error(f"Erro ao enviar dados para API. Status code: {response.status_code}")
            logger.error(f"Resposta: {response.text}")
    except Exception as e:
        logger.exception(f"Erro na requisição HTTP: {e}")

def process_file_update(machine_id, dimensional_values, operador, quality_values):
    # Envia os dados para a API
    send_data_to_api(machine_id, dimensional_values, operador, quality_values)
    
    # Depois continua com o processamento normal
    index_load = get_index_load(machine_id)
    
    if index_load is not None:
        amostragem, qtd, lote = get_amostragem(index_load)
        if amostragem is not None:
            # Extrai os valores de rem_a, rem_b e atrib das primeiras posições da lista dimensional_values
            # se eles não forem fornecidos como parâmetros
            if len(quality_values) > 0:
                rem_a = quality_values[0]
            if len(quality_values) > 1:
                rem_b = quality_values[1]
            if len(quality_values) > 2:
                atrib = quality_values[2]
                
            update_table_with_cotas(index_load, amostragem, dimensional_values, operador, rem_a, rem_b, qtd, lote, atrib)
            logger.info(f"Dados atualizados para máquina {machine_id}, INDEX_LOAD {index_load}")
        else:
            logger.warning(f"Amostragem não encontrada para máquina {machine_id}")
    else:
        logger.warning(f"INDEX_LOAD não encontrado para máquina {machine_id}")


def extract_lra_fail_data(file_path):
    """
    Extrai os valores de "Desvio" dos itens em "LRA_CORRECTION" que possuem "Teste" igual a "Fail",
    utilizando o campo "Nome" diretamente para formar as chaves:
      - "DOBRA_<bend_number>" -> "<bend_number>_angulo"
      - "Length_<bend_number>" -> "<bend_number>_tamanho"
      - "GIRO_<bend_number>" -> "<bend_number>_giro"
      
    Retorna um dicionário onde cada chave é formada conforme o padrão acima e o valor é o desvio correspondente.
    """
    try:
        with open(file_path, "r", encoding="latin1") as file:
            data = json.load(file)
    except Exception as e:
        logger.exception(f"Erro ao ler o arquivo JSON {file_path}: {e}")
        return {}
    
    tube_inspection = data.get("Tube_Inspection", {})
    lra_correction = tube_inspection.get("LRA_CORRECTION", [])
    resultado = {}
    
    # Mapeamento dos prefixos para o sufixo desejado
    mapping = {
        "DOBRA": "dobra",
        "Length": "tamanho",
        "GIRO": "giro"
    }
    
    for correction in lra_correction:
        for item in correction.get("LRA", []):
            if item.get("Teste") == "Fail": 
                nome_original = item.get("Nome", "")
                for prefixo, sufixo in mapping.items():
                    if nome_original.startswith(prefixo):
                        # Extrai o número da dobra do nome (por exemplo, "DOBRA_1" -> "1")
                        partes = nome_original.split("_")
                        if len(partes) > 1:
                            bend_number = partes[1]
                            nova_chave = f"{sufixo}_{bend_number}"
                            resultado[nova_chave] = item.get("Desvio") * -1
                        break  # Se encontrou o mapeamento, não precisa testar outros
    
    logger.debug(f"Dados LRA extraídos: {resultado}")
    return resultado

class FileChangeHandler(FileSystemEventHandler):
    def on_modified(self, event):
        global processing_timers
        
        if event.is_directory or not event.src_path.endswith(".json"):
            return

        # Cancela timer anterior se houver alteração recente no mesmo arquivo
        if event.src_path in processing_timers:
            processing_timers[event.src_path].cancel()
        
        processing_timers[event.src_path] = threading.Timer(1.0, self.process_file, [event.src_path])
        processing_timers[event.src_path].start()

    def process_file(self, file_path):
        global last_update
        last_update = time.time()
        logger.info(f"Processando arquivo: {file_path}")
        
        try:
            machine_id, dimensional_values, operador, quality_values = extract_data_from_json(file_path)
            
            if machine_id and dimensional_values:
                process_file_update(machine_id, dimensional_values, operador, quality_values)
            else:
                logger.warning(f"Erro ao processar dados do arquivo {file_path}.")
            
            lra_data = extract_lra_fail_data(file_path)
            
            # Se o machine_id estiver presente, assume que o cliente se identifica por esse valor
            if machine_id:
                logger.info(f"Notificando cliente específico: {machine_id}")
                notify_specific_client(lra_data, machine_id)
            else:
                # Se não houver machine_id, você pode optar por notificar todos os clientes
                logger.info("Notificando todos os clientes")
                notify_clients(lra_data)
        except Exception as e:
            logger.exception(f"Erro ao processar {file_path}: {e}")

def start_file_monitor():
    observer = Observer()
    event_handler = FileChangeHandler()
    observer.schedule(event_handler, path=ROOT_DIRECTORY, recursive=False)
    logger.info(f"Monitorando a pasta: {ROOT_DIRECTORY}")
    
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        observer.join()
        logger.info("Monitoramento interrompido.")

if __name__ == "__main__":
    logger.info("Iniciando aplicação")
    
    # Verifica se o módulo requests está instalado
    try:
        import requests
    except ImportError:
        logger.error("Módulo 'requests' não encontrado. Tentando instalar...")
        try:
            import pip
            pip.main(['install', 'requests'])
            import requests
            logger.info("Módulo 'requests' instalado com sucesso.")
        except Exception as e:
            logger.exception(f"Falha ao instalar o módulo 'requests': {e}")
            logger.critical("Não foi possível iniciar a aplicação. O módulo 'requests' é necessário.")
            exit(1)
    
    # Verificando diretório de monitoramento
    if not os.path.exists(ROOT_DIRECTORY):
        logger.warning(f"Diretório de monitoramento não encontrado: {ROOT_DIRECTORY}")
        try:
            os.makedirs(ROOT_DIRECTORY)
            logger.info(f"Diretório de monitoramento criado: {ROOT_DIRECTORY}")
        except Exception as e:
            logger.error(f"Falha ao criar diretório de monitoramento: {e}")
    
    # Inicia o monitor de arquivos em uma thread
    monitor_thread = threading.Thread(target=start_file_monitor, daemon=True)
    monitor_thread.start()
    logger.info("Thread de monitoramento de arquivos iniciada")
    
    # Inicia o socket server em outra thread
    socket_thread = threading.Thread(target=start_socket_server, args=("0.0.0.0", 6789), daemon=True)
    socket_thread.start()
    logger.info("Servidor de socket iniciado em 0.0.0.0:6789")
    
    try:
        # Aguarda ambas as threads terminarem
        monitor_thread.join()
        socket_thread.join()
    except KeyboardInterrupt:
        logger.info("Aplicação encerrada pelo usuário")
    except Exception as e:
        logger.exception(f"Erro inesperado na aplicação: {e}")