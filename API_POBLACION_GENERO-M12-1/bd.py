import mysql.connector
import pandas as pd
import os

def connect_db():
    """Conectar con la base de datos MySQL"""
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="poblacio_genero"
    )

def insert_districte(cursor, codi_districte, nom_districte):
    """Insertar un distrito si no existe."""
    cursor.execute("""
        INSERT IGNORE INTO districte (id_districte, nom_districte)
        VALUES (%s, %s)
    """, (codi_districte, nom_districte))

def insert_barri(cursor, codi_barri, nom_barri, codi_districte, aeb):
    """Insertar un barrio si no existe."""
    cursor.execute("""
        INSERT IGNORE INTO barri (id_barri, nom_barri, id_districte, aeb)
        VALUES (%s, %s, %s, %s)
    """, (codi_barri, nom_barri, codi_districte, aeb))

def insert_seccio_censal(cursor, seccio_censal, codi_barri):
    """Insertar una sección censal si no existe."""
    cursor.execute("""
        INSERT IGNORE INTO seccio_censal (id_seccio_censal, id_barri, codi_seccio_censal)
        VALUES (%s, %s, %s)
    """, (seccio_censal, codi_barri, seccio_censal))

def insert_poblacio(cursor, data_referencia, seccio_censal, sexe, valor):
    """Insertar un registro de población."""
    cursor.execute("""
        INSERT INTO poblacio (data_referencia, id_seccio_censal, sexe, valor)
        VALUES (%s, %s, %s, %s)
    """, (data_referencia, seccio_censal, sexe, valor))

def clean_data(df):
    """Limpiar y formatear los datos del DataFrame."""
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

    conn = connect_db()
    cursor = conn.cursor()

    for csv_file in csv_files:
        if os.path.exists(csv_file):
            try:
                df = pd.read_csv(csv_file)
                df = clean_data(df)

                for _, row in df.iterrows():
                    # Insertar districte
                    insert_districte(cursor, row['Codi_Districte'], row['Nom_Districte'])

                    # Insertar barri
                    insert_barri(cursor, row['Codi_Barri'], row['Nom_Barri'], row['Codi_Districte'], row['AEB'])

                    # Insertar seccio censal
                    insert_seccio_censal(cursor, row['Seccio_Censal'], row['Codi_Barri'])

                    # Insertar població
                    insert_poblacio(cursor, row['Data_Referencia'], row['Seccio_Censal'], row['SEXE'], row['Valor'])

                conn.commit()
                print(f"Dades importades correctament des de {csv_file}.")
            except Exception as e:
                print(f"Error al processar el fitxer {csv_file}: {e}")
        else:
            print(f"El fitxer {csv_file} no existeix.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    import_data()
