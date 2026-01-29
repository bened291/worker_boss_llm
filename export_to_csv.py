import sqlite3
import csv

DB_NAME = "call_center_lmstudio.db"
OUTPUT_FILE = "report_chiamate.csv"

def export_data():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # Selezioniamo tutto dalla tabella calls
    c.execute("SELECT * FROM calls")
    rows = c.fetchall()
    
    # Recuperiamo i nomi delle colonne (intestazioni)
    # cursor.description contiene le info sulle colonne
    headers = [description[0] for description in c.description]
    
    print(f"Sto esportando {len(rows)} righe in '{OUTPUT_FILE}'...")
    
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f, delimiter=';') # Usa ; per Excel italiano
        
        # Scrivi intestazioni
        writer.writerow(headers)
        
        # Scrivi i dati
        writer.writerows(rows)
        
    print("Fatto! Puoi aprire il file con Excel.")
    conn.close()

if __name__ == "__main__":
    export_data()