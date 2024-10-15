from flask import request, jsonify
from functools import wraps

# Clave API Administrador
ADMIN_API_KEY = "123456789"

def verify_api_key(api_key):
    # Verificar si la API Key proporcionada es la clave maestra.
    return api_key == ADMIN_API_KEY

def api_key_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        api_key = request.headers.get('x-api-key')
        if not api_key or not verify_api_key(api_key):
            return jsonify({"message": "API Key inválida o faltante"}), 401
        return f(*args, **kwargs)
    return decorated_function
