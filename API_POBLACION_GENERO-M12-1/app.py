from flask import Flask, jsonify, request, abort
import mysql.connector

app = Flask(__name__)

# Configuración de la base de datos
app.config['DATABASE'] = {
    'host': 'localhost',
    'user': 'root',  # Cambia por tu usuario de MySQL
    'password': '',  # Cambia por tu password de MySQL
    'database': 'poblacio_genero',  # El nombre de la base de datos
    'charset': 'utf8'  # Asegúrate de utilizar el charset adecuado
}

def get_db_connection():
    """Establece una conexión a la base de datos MySQL."""
    conn = mysql.connector.connect(
        host=app.config['DATABASE']['host'],
        user=app.config['DATABASE']['user'],
        password=app.config['DATABASE']['password'],
        database=app.config['DATABASE']['database'],
        charset=app.config['DATABASE']['charset']  # Usar el charset definido
    )
    return conn

@app.route('/')
def home():
    """Ruta inicial."""
    return "API de Població en funcionamiento,\nendpoints: /poblacio, /barrio, /distrito, /barrio_info"

@app.route('/poblacio', methods=['GET'])
def get_population():
    """Obtener datos de la población con filtros."""
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
    SELECT p.id_poblacio, p.data_referencia, s.codi_seccio_censal, b.nom_barri, 
           d.nom_districte, p.sexe, p.valor
    FROM poblacio p
    JOIN seccio_censal s ON p.id_seccio_censal = s.id_seccio_censal
    JOIN barri b ON s.id_barri = b.id_barri
    JOIN districte d ON b.id_districte = d.id_districte
    WHERE 1=1
    """
    params = []

    # Filtrar por distrito
    districte = request.args.get('districte')
    if districte:
        if not districte.isdigit():
            return jsonify({"error": "El id_districte debe ser un número"}), 400
        query += " AND d.id_districte = %s"
        params.append(int(districte))

    # Filtrar por barrio
    barri = request.args.get('barri')
    if barri:
        if not barri.isdigit():
            return jsonify({"error": "El id_barri debe ser un número"}), 400
        query += " AND b.id_barri = %s"
        params.append(int(barri))

    # Filtrar por sexo
    sexe = request.args.get('sexe')
    if sexe:
        if not sexe.isdigit() or int(sexe) not in [1, 2]:
            return jsonify({"error": "El sexe debe ser 1 (hombres) o 2 (mujeres)"}), 400
        query += " AND p.sexe = %s"
        params.append(int(sexe))

    # Agregar paginación
    limit = request.args.get('limit', 10)
    offset = request.args.get('offset', 0)
    query += " LIMIT %s OFFSET %s"
    params.extend([int(limit), int(offset)])

    try:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    except mysql.connector.Error as e:
        return jsonify({"error": str(e)}), 500

    # Convertir a una lista de diccionarios
    columns = ['id_poblacio', 'data_referencia', 'codi_seccio_censal', 'nom_barri', 'nom_districte', 'sexe', 'valor']
    result = [dict(zip(columns, row)) for row in rows]

    cursor.close()
    conn.close()

    return jsonify(result)

@app.route('/barrio', methods=['GET'])
def get_population_by_barrio():
    """Obtener el número de hombres y mujeres por barrio."""
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
    SELECT b.nom_barri, 
           SUM(CASE WHEN p.sexe = 1 THEN p.valor ELSE 0 END) AS hombres,
           SUM(CASE WHEN p.sexe = 2 THEN p.valor ELSE 0 END) AS mujeres
    FROM barri b
    LEFT JOIN seccio_censal s ON b.id_barri = s.id_barri
    LEFT JOIN poblacio p ON s.id_seccio_censal = p.id_seccio_censal
    GROUP BY b.nom_barri
    """

    try:
        cursor.execute(query)
        rows = cursor.fetchall()
    except mysql.connector.Error as e:
        return jsonify({"error": str(e)}), 500

    result = [{'nom_barri': row[0], 'hombres': row[1], 'mujeres': row[2]} for row in rows]

    cursor.close()
    conn.close()

    return jsonify(result)

@app.route('/distrito', methods=['GET'])
def get_population_by_distrito():
    """Obtener el número de hombres y mujeres por distrito."""
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
    SELECT d.nom_districte, 
           SUM(CASE WHEN p.sexe = 1 THEN p.valor ELSE 0 END) AS hombres,
           SUM(CASE WHEN p.sexe = 2 THEN p.valor ELSE 0 END) AS mujeres
    FROM districte d
    LEFT JOIN barri b ON d.id_districte = b.id_districte
    LEFT JOIN seccio_censal s ON b.id_barri = s.id_barri
    LEFT JOIN poblacio p ON s.id_seccio_censal = p.id_seccio_censal
    GROUP BY d.nom_districte
    """

    try:
        cursor.execute(query)
        rows = cursor.fetchall()
    except mysql.connector.Error as e:
        return jsonify({"error": str(e)}), 500

    result = [{'nom_districte': row[0], 'hombres': row[1], 'mujeres': row[2]} for row in rows]

    cursor.close()
    conn.close()

    return jsonify(result)

@app.route('/barrio_info', methods=['GET'])
def get_barrio_info():
    """Obtener información detallada de los barrios filtrada por año."""
    year = request.args.get('year', type=int)

    if year is None:
        return jsonify({"error": "Se requiere un año."}), 400

    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
    SELECT b.nom_barri, 
           d.nom_districte,
           s.id_seccio_censal,
           SUM(CASE WHEN p.sexe = 1 THEN p.valor ELSE 0 END) AS total_hombres,
           SUM(CASE WHEN p.sexe = 2 THEN p.valor ELSE 0 END) AS total_mujeres
    FROM barri b
    LEFT JOIN seccio_censal s ON b.id_barri = s.id_barri
    LEFT JOIN poblacio p ON s.id_seccio_censal = p.id_seccio_censal
    WHERE YEAR(p.data_referencia) = %s
    GROUP BY b.nom_barri, d.nom_districte, s.id_seccio_censal
    """
    
    try:
        cursor.execute(query, (year,))
        rows = cursor.fetchall()
    except mysql.connector.Error as e:
        return jsonify({"error": str(e)}), 500

    result = [{'nom_barri': row[0], 'nom_districte': row[1], 'id_seccio_censal': row[2],
               'total_hombres': row[3], 'total_mujeres': row[4]} for row in rows]

    cursor.close()
    conn.close()

    return jsonify(result)

@app.route('/poblacio/<int:id>', methods=['PUT'])
def update_population(id):
    """Actualizar un registro de la población."""
    data = request.get_json()
    required_fields = ['data_referencia', 'id_seccio_censal', 'sexe', 'valor']

    # Verifica que los campos estén presentes
    if not all(field in data for field in required_fields):
        abort(400, description="Faltan campos en la solicitud")

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Ejecutar la consulta para actualizar
        cursor.execute('''UPDATE poblacio
        SET data_referencia = %s, id_seccio_censal = %s, sexe = %s, valor = %s
        WHERE id_poblacio = %s''', (
            data['data_referencia'], data['id_seccio_censal'], data['sexe'], data['valor'], id
        ))

        conn.commit()
        if cursor.rowcount == 0:
            return jsonify({"error": "No se encontró el registro para actualizar"}), 404
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
        cursor.execute('DELETE FROM poblacio WHERE id_poblacio = %s', (id,))
        conn.commit()
        if cursor.rowcount == 0:
            return jsonify({"error": "No se encontró el registro para eliminar"}), 404
    except mysql.connector.Error as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

    return jsonify({"message": "Registro eliminado correctamente"}), 200

if __name__ == '__main__':
    app.run(debug=True)
