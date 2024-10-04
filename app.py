from flask import Flask, jsonify, request, abort
import mysql.connector

app = Flask(__name__)

# Configuracion de la base de datos
app.config['DATABASE'] = {
    'host': 'localhost',
    'user': 'root',  # CAMBIAR
    'password': 'sergi',  # CAMBIAR
    'database': 'poblacio_genero'  # EL NOMBRE DE LA BASE DE DATOS
}

def get_db_connection():
    """Establece una conexion a la base de datos MySQL."""
    conn = mysql.connector.connect(
        host=app.config['DATABASE']['host'],
        user=app.config['DATABASE']['user'],
        password=app.config['DATABASE']['password'],
        database=app.config['DATABASE']['database']
    )
    return conn

@app.route('/')
def home():
    """Ruta inicial."""
    return "API de Població en funcionamiento,\nendpoints: /poblacio"

@app.route('/poblacio', methods=['GET'])
def get_population():
    """Obtener datos de la población con filtros."""
    conn = get_db_connection()
    cursor = conn.cursor()

    query = "SELECT * FROM poblacio WHERE 1=1"
    params = []

    # Filtrar por distrito
    districte = request.args.get('districte')
    if districte:
        if not districte.isdigit():
            return jsonify({"error": "Codi_Districte debe ser un número"}), 400
        query += " AND Codi_Districte = %s"
        params.append(int(districte))

    # Filtrar por barrio
    barri = request.args.get('barri')
    if barri:
        if not barri.isdigit():
            return jsonify({"error": "Codi_Barri debe ser un número"}), 400
        query += " AND Codi_Barri = %s"
        params.append(int(barri))

    # Filtrar por sexo
    sexe = request.args.get('sexe')
    if sexe:
        if not sexe.isdigit():
            return jsonify({"error": "SEXE debe ser un número"}), 400
        query += " AND SEXE = %s"
        params.append(int(sexe))

    # Filtrar por AEB
    aeb = request.args.get('aeb')
    if aeb:
        if not aeb.isdigit():
            return jsonify({"error": "AEB debe ser un número"}), 400
        query += " AND AEB = %s"
        params.append(int(aeb))

    # Agregar paginación
    limit = request.args.get('limit', 10)
    offset = request.args.get('offset', 0)
    query += " LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    try:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    except mysql.connector.Error as e:
        return jsonify({"error": str(e)}), 500

    # Convertir a una lista de diccionarios
    columns = ['id', 'Data_Referencia', 'Codi_Districte', 'Nom_Districte', 'Codi_Barri', 'Nom_Barri', 'AEB', 'Seccio_Censal', 'Valor', 'SEXE']
    result = [dict(zip(columns, row)) for row in rows]

    cursor.close()
    conn.close()

    return jsonify(result)

@app.route('/poblacio/<int:id>', methods=['PUT'])
def update_population(id):
    """Actualizar un registro de la población."""
    data = request.get_json()
    required_fields = ['Data_Referencia', 'Codi_Districte', 'Nom_Districte', 'Codi_Barri', 'Nom_Barri', 'AEB', 'Seccio_Censal', 'Valor', 'SEXE']

    # Verifica que los campos esten presentes
    if not all(field in data for field in required_fields):
        abort(400, description="Faltan campos en la solicitud")

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Ejecutar la consulta para actualizar
        cursor.execute('''
        UPDATE poblacio
        SET Data_Referencia = %s, Codi_Districte = %s, Nom_Districte = %s, Codi_Barri = %s, Nom_Barri = %s, AEB = %s, Seccio_Censal = %s, Valor = %s, SEXE = %s
        WHERE id = %s
        ''', (
            data['Data_Referencia'], data['Codi_Districte'], data['Nom_Districte'],
            data['Codi_Barri'], data['Nom_Barri'], data['AEB'], data['Seccio_Censal'], data['Valor'], data['SEXE'], id
        ))

        conn.commit()
    except mysql.connector.Error as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

    return jsonify({"message": "Registro actualizado correctamente"}), 200

@app.route('/poblacio/<int:id>', methods=['DELETE'])
def delete_population(id):
    """Eliminar un registro de población de la base de datos."""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute('DELETE FROM poblacio WHERE id = %s', (id,))
        conn.commit()
    except mysql.connector.Error as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

    return jsonify({"message": "Registro eliminado correctamente"}), 200

if __name__ == '__main__':
    app.run(debug=True)
