import sqlite3
import pandas as pd
import os

def init_db(db_path):
    """Inicializar la base de datos y crear tablas."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Crear tabla poblacio si no existe
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS poblacio (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Data_Referencia TEXT,
            Codi_Districte INTEGER,
            Nom_Districte TEXT,
            Codi_Barri INTEGER,
            Nom_Barri TEXT,
            AEB INTEGER,
            Seccio_Censal INTEGER,
            Valor INTEGER,
            SEXE INTEGER
        );
        ''')
        
        conn.commit()
    except Exception as e:
        print(f"Error al inicializar la base de datos: {e}")
    finally:
        conn.close()

def clean_data(df):
    """Limpiar los datos del DataFrame."""
    df = df.dropna()
    df['Codi_Districte'] = pd.to_numeric(df['Codi_Districte'], errors='coerce')
    df['Codi_Barri'] = pd.to_numeric(df['Codi_Barri'], errors='coerce')
    df['AEB'] = pd.to_numeric(df['AEB'], errors='coerce')
    df['Seccio_Censal'] = pd.to_numeric(df['Seccio_Censal'], errors='coerce')
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')
    df['SEXE'] = pd.to_numeric(df['SEXE'], errors='coerce')
    df = df.dropna()
    return df

def import_data():
    """Importar datos de los archivos CSV a la base de datos."""
    csv_files = [
        'data/2020_pad_mdbas_sexe.csv',
        'data/2021_pad_mdbas_sexe.csv',
        'data/2022_pad_mdbas_sexe.csv',
        'data/2023_pad_mdbas_sexe.csv',
        'data/2024_pad_mdbas_sexe.csv'
    ]

    conn = sqlite3.connect('app.db')

    for csv_file in csv_files:
        if os.path.exists(csv_file):
            try:
                df = pd.read_csv(csv_file)
                df = clean_data(df)
                df.to_sql('poblacio', conn, if_exists='append', index=False)
                print(f"Dades importades correctament des de {csv_file}.")
            except Exception as e:
                print(f"Error al processar el fitxer {csv_file}: {e}")
        else:
            print(f"El fitxer {csv_file} no existeix.")

    conn.close()

def get_population(districte=None, barri=None, sexe=None, aeb=None):
    """Obtener los datos filtrados de la población desde la base de datos."""
    conn = sqlite3.connect('app.db')
    cursor = conn.cursor()

    query = "SELECT * FROM poblacio WHERE 1=1"
    params = []

    # Filtrar por districte
    if districte:
        query += " AND Codi_Districte = ?"
        params.append(districte)

    # Filtrar por barri
    if barri:
        query += " AND Codi_Barri = ?"
        params.append(barri)

    # Filtrar por sexe
    if sexe:
        query += " AND SEXE = ?"
        params.append(sexe)

    # Filtrar por aeb
    if aeb:
        query += " AND AEB = ?"
        params.append(aeb)

    cursor.execute(query, params)
    rows = cursor.fetchall()

    # Convertir a una lista de diccionarios
    columns = ['Data_Referencia', 'Codi_Districte', 'Nom_Districte', 'Codi_Barri', 'Nom_Barri', 'AEB', 'Seccio_Censal', 'Valor', 'SEXE']
    result = [dict(zip(columns, row)) for row in rows]

    conn.close()
    
    return result

def add_population(data):
    """Añadir un nuevo registro de población a la base de datos."""
    conn = sqlite3.connect('app.db')
    cursor = conn.cursor()

    cursor.execute('''
    INSERT INTO poblacio (Data_Referencia, Codi_Districte, Nom_Districte, Codi_Barri, Nom_Barri, AEB, Seccio_Censal, Valor, SEXE)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (data['Data_Referencia'], data['Codi_Districte'], data['Nom_Districte'], data['Codi_Barri'], data['Nom_Barri'], data['AEB'], data['Seccio_Censal'], data['Valor'], data['SEXE']))

    conn.commit()
    conn.close()

def update_population(id, data):
    """Actualizar un registro de población en la base de datos."""
    conn = sqlite3.connect('app.db')
    cursor = conn.cursor()

    cursor.execute('''
    UPDATE poblacio
    SET Data_Referencia = ?, Codi_Districte = ?, Nom_Districte = ?, Codi_Barri = ?, Nom_Barri = ?, AEB = ?, Seccio_Censal = ?, Valor = ?, SEXE = ?
    WHERE id = ?
    ''', (data['Data_Referencia'], data['Codi_Districte'], data['Nom_Districte'], data['Codi_Barri'], data['Nom_Barri'], data['AEB'], data['Seccio_Censal'], data['Valor'], data['SEXE'], id))

    conn.commit()
    conn.close()

def delete_population(id):
    """Eliminar un registro de población de la base de datos."""
    conn = sqlite3.connect('app.db')
    cursor = conn.cursor()

    cursor.execute('DELETE FROM poblacio WHERE id = ?', (id,))

    conn.commit()
    conn.close()
