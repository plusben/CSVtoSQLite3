import csv
import sqlite3
import os

def infer_data_type(value):
    """Infer the most appropriate data type for a given value."""
    if value.isdigit():
        return 'INTEGER'  # Wenn der Wert eine Ganzzahl ist, ist der Datentyp INTEGER.
    try:
        float(value)
        return 'REAL'  # Wenn der Wert eine Fließkommazahl ist, ist der Datentyp REAL.
    except ValueError:
        return 'TEXT'  # Andernfalls wird der Datentyp TEXT angenommen.

def create_sqlite_from_csv(csv_file_path, sqlite_db_path):
    # Überprüfen, ob die Datenbankdatei bereits existiert
    db_exists = os.path.exists(sqlite_db_path)

    # Verbindung zur SQLite-Datenbank herstellen
    conn = sqlite3.connect(sqlite_db_path)
    cur = conn.cursor()

    # Wenn die Datenbank neu ist, erstelle die Tabelle
    if not db_exists:
        with open(csv_file_path, 'r') as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader)  # Die erste Zeile enthält die Spaltennamen

            # Datentypen für jede Spalte bestimmen
            sample_size = 5  # Anzahl der Zeilen zur Bestimmung des Datentyps
            data_types = ['TEXT'] * len(headers)  # Standardmäßig wird jeder Datentyp auf TEXT gesetzt
            for i, row in enumerate(reader):
                if i >= sample_size:
                    break
                for j, value in enumerate(row):
                    if value:  # Nur für nicht-leere Werte
                        inferred_type = infer_data_type(value)
                        if data_types[j] == 'TEXT' and inferred_type != 'TEXT':
                            data_types[j] = inferred_type

            # SQL-Befehl zur Tabellenerstellung generieren
            columns_with_types = ', '.join(f'{header.replace(" ", "_")} {data_type}'
                                           for header, data_type in zip(headers, data_types))
            create_table_statement = f"CREATE TABLE IF NOT EXISTS tablename ({columns_with_types})"
            cur.execute(create_table_statement)

            # Zurück zum Anfang der Datei und Daten einfügen
            csvfile.seek(0)
            next(reader)  # Kopfzeile überspringen
            for row in reader:
                placeholders = ', '.join('?' * len(row))
                insert_statement = f"INSERT INTO tablename VALUES ({placeholders})"
                cur.execute(insert_statement, row)

    # Änderungen speichern und Verbindung schließen
    conn.commit()
    conn.close()

# Beispielaufruf
csv_file_path = 'titanic.csv'  # Pfad zur CSV-Datei, ersetze durch deinen Dateipfad
sqlite_db_path = 'titanic.db'  # Pfad zur SQLite-Datenbank, ersetze durch deinen Dateipfad
create_sqlite_from_csv(csv_file_path, sqlite_db_path)
