import json
import time
import threading
import psutil
import os
import argparse
from databricks import sql
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate
from datetime import datetime  # Per i timestamp



# --- CONFIGURAZIONE AI ---
LLM_URL = "http://localhost:1234/v1"
RESET_ON_START = True 

WORKER_PROMPT_FILE = "worker_prompt.txt"
BOSS_PROMPT_FILE = "boss_prompt.txt"

llm = ChatOpenAI(
    base_url=LLM_URL,
    api_key="lm-studio",
    model="local-model",
    temperature=0.1
)

# --- UTILITY: CARICAMENTO PROMPT ---
def load_prompt_from_file(filename):
    try:
        with open(filename, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        print(f"ERRORE CRITICO: Il file '{filename}' non esiste!")
        raise

# --- SISTEMA DI MONITORAGGIO ---
class SystemMonitor:
    def __init__(self, interval=0.5):
        self.interval = interval
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.process = psutil.Process(os.getpid())

    def start(self):
        print("📊 [MONITOR] Avvio monitoraggio...", flush=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        print("📊 [MONITOR] Stop monitoraggio.", flush=True)

    def _monitor_loop(self):
        psutil.cpu_percent(interval=None)
        while not self.stop_event.is_set():
            py_mem = self.process.memory_info().rss / 1024 / 1024 
            sys_cpu = psutil.cpu_percent(interval=None)
            sys_ram = psutil.virtual_memory().percent
            if sys_cpu > 5.0:
                print(f"   [SYS] CPU: {sys_cpu:5.1f}% | RAM: {sys_ram:4.1f}% | Script: {py_mem:.1f} MB", flush=True)
            time.sleep(self.interval)

# --- DATABASE DATABRICKS ---
def get_connection():
    return sql.connect(
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
        # Rimosso use_inline_params per maggiore compatibilità
    )

def init_db(conn):
    cursor = conn.cursor()
    if RESET_ON_START:
        print(f"--- Reset tabelle in {DEST_SCHEMA} ---")
        cursor.execute(f"DROP TABLE IF EXISTS {CALLS_TABLE}")
        cursor.execute(f"DROP TABLE IF EXISTS {CATS_TABLE}")
    
    # Tabelle minimaliste senza vincoli SQL complessi
    cursor.execute(f'''CREATE TABLE IF NOT EXISTS {CALLS_TABLE} 
                     (conversationid STRING, 
                      transcription STRING, 
                      status STRING,
                      worker_output STRING, 
                      final_category STRING)''')
    
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {CATS_TABLE} (name STRING)")

def load_initial_data(conn):
    cursor = conn.cursor()
    print(f"--- Caricamento prime 10 chiamate da {SOURCE_TABLE} ---")
    
    # Pulizia preliminare se non abbiamo resettato
    cursor.execute(f"DELETE FROM {CALLS_TABLE} WHERE status = 'NEW'")
    
    insert_query = f"""
        INSERT INTO {CALLS_TABLE} (conversationid, transcription, status)
        SELECT conversationid, testo_concatenato, 'NEW'
        FROM {SOURCE_TABLE}
        ORDER BY conversationid ASC
        LIMIT 100
    """
    cursor.execute(insert_query)
    print("--- Dati caricati con successo ---")
def log_msg(msg, level="INFO"):
    """Utility per stampare log con timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {msg}", flush=True)

# --- WORKER AGENT ---
def run_worker_agent(conn):
    cursor = conn.cursor()
    cursor.execute(f"SELECT conversationid, transcription FROM {CALLS_TABLE} WHERE status='NEW'")
    rows = cursor.fetchall()
    
    if not rows: 
        log_msg("Nessuna nuova chiamata da elaborare.", "Worker")
        return

    log_msg(f"Inizio elaborazione di {len(rows)} chiamate...", "Worker")
    
    prompt_text = load_prompt_from_file(WORKER_PROMPT_FILE)
    prompt = PromptTemplate.from_template(prompt_text)
    chain = prompt | llm

    for row in rows:
        conv_id, text = row
        log_msg(f"Elaborazione ConvID {conv_id}...", "Worker")
        try:
            res = chain.invoke({"text": text})
            intent = res.content.strip()
            
            query = f"UPDATE {CALLS_TABLE} SET status='WORKER_DONE', worker_output=? WHERE conversationid=?"
            cursor.execute(query, [intent, conv_id])
            
        except Exception as e:
            log_msg(f"Errore ID {conv_id}: {e}", "ERROR")

# --- BOSS AGENT ---
def run_boss_agent(conn):
    cursor = conn.cursor()
    cursor.execute(f"SELECT conversationid, worker_output FROM {CALLS_TABLE} WHERE status='WORKER_DONE'")
    rows = cursor.fetchall()
    
    if not rows: 
        log_msg("Nessun ticket pronto per la clusterizzazione.", "Boss")
        return

    log_msg(f"Clusterizzazione di {len(rows)} ticket...", "Boss")
    
    prompt_text = load_prompt_from_file(BOSS_PROMPT_FILE)
    prompt = PromptTemplate.from_template(prompt_text)
    chain = prompt | llm

    for row in rows:
        conv_id, worker_output = row
        intent = worker_output.strip() if worker_output else "N/A"
        
        cursor.execute(f"SELECT name FROM {CATS_TABLE}")
        cats = [r[0] for r in cursor.fetchall()]
        cats_str = ", ".join(cats) if cats else "NESSUNA"

        log_msg(f"Valuto '{intent}'...", "Boss")
        try:
            res = chain.invoke({"intent": intent, "existing_categories": cats_str})
            
            content = res.content.replace("```json", "").replace("```", "").strip()
            if "{" in content and "}" in content:
                content = content[content.find("{"):content.rfind("}")+1]

            data = json.loads(content)
            cat = data['category'].upper().strip()
            
            if data.get('is_new') and cat not in cats:
                log_msg(f"*** NUOVA CATEGORIA: {cat} ***", "Boss")
                cursor.execute(f"INSERT INTO {CATS_TABLE} (name) VALUES (?)", [cat])
            else:
                log_msg(f"Assegnato a: {cat}", "Boss")
            
            query = f"UPDATE {CALLS_TABLE} SET status='BOSS_DONE', final_category=? WHERE conversationid=?"
            cursor.execute(query, [cat, conv_id])
            
        except Exception as e:
            log_msg(f"Errore Boss su {conv_id}: {e}", "ERROR")

# --- MAIN ---
if __name__ == "__main__":
    log_msg("Processo avviato")
    parser = argparse.ArgumentParser()
    parser.add_argument("--monitor", action="store_true", help="Attiva monitoraggio CPU/RAM")
    args = parser.parse_args()

    try:
        load_prompt_from_file(WORKER_PROMPT_FILE)
        load_prompt_from_file(BOSS_PROMPT_FILE)
    except Exception:
        exit(1)

    # Connessione unica a Databricks
    connection = get_connection()
    
    try:
        init_db(connection)
        load_initial_data(connection)
        
        monitor = None
        if args.monitor:
            monitor = SystemMonitor(interval=0.5)
            monitor.start()
        
        run_worker_agent(connection)
        run_boss_agent(connection)
        
    except Exception as e:
        log_msg(f"ERRORE CRITICO: {e}")
    finally:
        if 'monitor' in locals() and monitor: monitor.stop()
        connection.close()
        log_msg("\n--- Processo completato su Databricks ---")