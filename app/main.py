import os
from flask import Flask
from app.apps.pdf_processor import bp as pdf_bp
from app.apps.encurtador import bp as encurtador_bp
from app.apps.email_financeiro import bp as email_financeiro_bp  # novo

def create_app():
    app = Flask(__name__)

    # módulos já existentes
    app.register_blueprint(pdf_bp)           # /compilar, /pdf2texto, /token-status
    app.register_blueprint(encurtador_bp)    # /encurtador/*

    # coletor financeiro
    app.register_blueprint(email_financeiro_bp, url_prefix="/api/email_financeiro")

    @app.route("/")
    def index():
        return {
            "status": "ok",
            "message": "Serviço ativo: PDF, Encurtador e Email Financeiro integrados.",
            "modules": ["pdf_processor", "encurtador", "email_financeiro"]
        }

    return app

app = create_app()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
