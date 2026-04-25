import os
from flask import Flask
from app.apps.pdf_processor import bp as pdf_bp
from app.apps.encurtador import bp as encurtador_bp
from app.apps.email_financeiro import bp as email_financeiro_bp
from app.apps.sheets_sync import bp as sheets_sync_bp
from app.apps.atualizaspbotao import bp as atualizaspbotao_bp

def create_app():
    app = Flask(__name__)

    app.register_blueprint(pdf_bp)
    app.register_blueprint(encurtador_bp)
    app.register_blueprint(email_financeiro_bp,   url_prefix="/api/email_financeiro")
    app.register_blueprint(sheets_sync_bp,         url_prefix="/api/sheets_sync")
    app.register_blueprint(atualizaspbotao_bp,     url_prefix="/api/atualizaspbotao")

    @app.route("/")
    def index():
        return {
            "status": "ok",
            "modules": ["pdf_processor", "encurtador", "email_financeiro", "sheets_sync", "atualizaspbotao"]
        }

    return app

app = create_app()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
