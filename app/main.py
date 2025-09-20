import os
from flask import Flask
from app.apps.pdf_processor import bp as pdf_bp
from app.apps.encurtador import bp as encurtador_bp


def create_app():
    app = Flask(__name__)
    # Registra blueprints sem prefixo: mantém paths originais
    app.register_blueprint(pdf_bp)          # /compilar, /pdf2texto, /token-status
    app.register_blueprint(encurtador_bp)   # /encurtador/* e redireciono "/<codigo>"
    return app

app = create_app()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
