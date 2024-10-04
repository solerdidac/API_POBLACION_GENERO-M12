from flask import Flask, jsonify, request
import pandas as pd
import sqlite3
import os

app = Flask(__name__)

# Configuracion de la base de datos
app.config['DATABASE'] = 'app.db'

def init_db():
    """Crear la tabla poblacio si no existe."""
    conn = sqlite3.connect(app.config['DATABASE'])
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS poblacio (
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

    conn = sqlite3.connect(app.config['DATABASE'])

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

# Inicializar la base de datos y cargar los datos cuando se inicia la aplicacion
with app.app_context():
    init_db()
    import_data()

@app.route('/')
def home():
    """Ruta inicial."""
    return "API de Població i Gènere en funcionament!"

@app.route('/poblacio', methods=['GET'])
def get_population():
    """Obtener datos de la población con filtros."""
    conn = sqlite3.connect(app.config['DATABASE'])
    cursor = conn.cursor()
    
    query = "SELECT * FROM poblacio WHERE 1=1"
    params = []

    # Filtrar por distrito
    districte = request.args.get('districte')
    if districte:
        query += " AND Codi_Districte = ?"
        params.append(districte)

    # Filtrar por barrio
    barri = request.args.get('barri')
    if barri:
        query += " AND Codi_Barri = ?"
        params.append(barri)

    # Filtrar por sexo
    sexe = request.args.get('sexe')
    if sexe:
        query += " AND SEXE = ?"
        params.append(sexe)

    # Filtrar por AEB
    aeb = request.args.get('aeb')
    if aeb:
        query += " AND AEB = ?"
        params.append(aeb)

    cursor.execute(query, params)
    rows = cursor.fetchall()

    # Convertir a una lista de diccionarios
    columns = ['Data_Referencia', 'Codi_Districte', 'Nom_Districte', 'Codi_Barri', 'Nom_Barri', 'AEB', 'Seccio_Censal', 'Valor', 'SEXE']
    result = [dict(zip(columns, row)) for row in rows]

    conn.close()
    
    return jsonify(result)

if __name__ == '__main__':
    app.run(debug=True)
