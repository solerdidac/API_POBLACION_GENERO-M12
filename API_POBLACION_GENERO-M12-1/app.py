from flask import Flask, jsonify, request, abort
import mysql.connector

app = Flask(__name__)

# Configuración de la base de datos
app.config['DATABASE'] = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'poblacio_genero',
    'charset': 'utf8'
}

def get_db_connection():
    """Establece una conexión a la base de datos MySQL."""
    conn = mysql.connector.connect(
        host=app.config['DATABASE']['host'],
        user=app.config['DATABASE']['user'],
        password=app.config['DATABASE']['password'],
        database=app.config['DATABASE']['database'],
        charset=app.config['DATABASE']['charset']
    )
    return conn

@app.route('/')
def home():
    """Ruta inicial."""
    return "API de Població en funcionamiento,\nendpoints: /poblacio, /barrio, /distrito"

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

    # Filtrar por sexo(1-2)
    sexe = request.args.get('sexe')
    if sexe:
        if not sexe.isdigit() or int(sexe) not in [1, 2]:
            return jsonify({"error": "El sexe debe ser 1 (hombres) o 2 (mujeres)"}), 400
        query += " AND p.sexe = %s"
        params.append(int(sexe))

    limit = request.args.get('limit', 10)
    offset = request.args.get('offset', 0)
    query += " LIMIT %s OFFSET %s"
    params.extend([int(limit), int(offset)])

    try:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    except mysql.connector.Error as e:
        return jsonify({"error": str(e)}), 500

    columns = ['id_poblacio', 'data_referencia', 'codi_seccio_censal', 'nom_barri', 'nom_districte', 'sexe', 'valor']
    result = [dict(zip(columns, row)) for row in rows]

    cursor.close()
    conn.close()

    return jsonify(result)

@app.route('/barrio', methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE'])
def manage_barrio():
    """Manejar el barrio con todos los métodos HTTP: GET, POST, PUT, PATCH, DELETE."""
    conn = get_db_connection()
    cursor = conn.cursor()

    if request.method == 'GET':
        hombres = request.args.get('hombres')
        mujeres = request.args.get('mujeres')
        total = request.args.get('total')

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

        if hombres == "1":
            result = [{'nom_barri': row[0], 'hombres': row[1]} for row in rows]
        elif mujeres == "2":
            result = [{'nom_barri': row[0], 'mujeres': row[2]} for row in rows]
        elif total == "1":
            result = [{'nom_barri': row[0], 'total': row[1] + row[2]} for row in rows]
        else:
            result = [{'nom_barri': row[0], 'hombres': row[1], 'mujeres': row[2]} for row in rows]

        cursor.close()
        conn.close()

        return jsonify(result)

    if request.method == 'POST':
        data = request.get_json()

        if 'nom_barri' not in data or 'id_districte' not in data:
            abort(400, description="Faltan campos en la solicitud")

        try:
            cursor.execute('''INSERT INTO barri (nom_barri, id_districte) VALUES (%s, %s)''',
                           (data['nom_barri'], data['id_districte']))
            conn.commit()
        except mysql.connector.Error as e:
            conn.rollback()
            return jsonify({"error": str(e)}), 500
        finally:
            cursor.close()
            conn.close()

        return jsonify({"message": "Barrio creado correctamente"}), 201

    if request.method == 'PUT':
        data = request.get_json()

        if 'id_barri' not in data or 'nom_barri' not in data or 'id_districte' not in data:
            abort(400, description="Faltan campos en la solicitud")

        try:
            cursor.execute('''UPDATE barri SET nom_barri = %s, id_districte = %s WHERE id_barri = %s''',
                           (data['nom_barri'], data['id_districte'], data['id_barri']))
            conn.commit()
            if cursor.rowcount == 0:
                return jsonify({"error": "No se encontró el barrio para actualizar"}), 404
        except mysql.connector.Error as e:
            conn.rollback()
            return jsonify({"error": str(e)}), 500
        finally:
            cursor.close()
            conn.close()

        return jsonify({"message": "Barrio actualizado correctamente"}), 200

    if request.method == 'PATCH':
        data = request.get_json()

        if 'id_barri' not in data:
            abort(400, description="Faltan campos en la solicitud")

        updates = []
        params = []
        if 'nom_barri' in data:
            updates.append("nom_barri = %s")
            params.append(data['nom_barri'])
        if 'id_districte' in data:
            updates.append("id_districte = %s")
            params.append(data['id_districte'])

        if not updates:
            abort(400, description="No se proporcionaron campos para actualizar")

        params.append(data['id_barri'])
        query = f"UPDATE barri SET {', '.join(updates)} WHERE id_barri = %s"

        try:
            cursor.execute(query, params)
            conn.commit()
            if cursor.rowcount == 0:
                return jsonify({"error": "No se encontró el barrio para actualizar"}), 404
        except mysql.connector.Error as e:
            conn.rollback()
            return jsonify({"error": str(e)}), 500
        finally:
            cursor.close()
            conn.close()

        return jsonify({"message": "Barrio actualizado parcialmente"}), 200

    if request.method == 'DELETE':
        data = request.get_json()

        if 'id_barri' not in data:
            abort(400, description="Falta el campo id_barri en la solicitud")

        try:
            cursor.execute('''DELETE FROM barri WHERE id_barri = %s''', (data['id_barri'],))
            conn.commit()
            if cursor.rowcount == 0:
                return jsonify({"error": "No se encontró el barrio para eliminar"}), 404
        except mysql.connector.Error as e:
            conn.rollback()
            return jsonify({"error": str(e)}), 500
        finally:
            cursor.close()
            conn.close()

        return jsonify({"message": "Barrio eliminado correctamente"}), 200

@app.route('/distrito', methods=['GET'])
def get_population_by_distrito():
    """Obtener el número de hombres, mujeres o el total por distrito según los parámetros de consulta."""
    conn = get_db_connection()
    cursor = conn.cursor()

    hombres = request.args.get('hombres')
    mujeres = request.args.get('mujeres')
    total = request.args.get('total')

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

    result = []
    if hombres == "1":
        result = [{'nom_districte': row[0], 'hombres': row[1]} for row in rows]
    elif mujeres == "2":
        result = [{'nom_districte': row[0], 'mujeres': row[2]} for row in rows]
    elif total == "1":
        result = [{'nom_districte': row[0], 'total': row[1] + row[2]} for row in rows]
    else:
        result = [{'nom_districte': row[0], 'hombres': row[1], 'mujeres': row[2]} for row in rows]

    cursor.close()
    conn.close()

    return jsonify(result)

if __name__ == '__main__':
    app.run(debug=True)
