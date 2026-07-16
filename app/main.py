import os
from flask import Flask
from app.apps.pdf_processor import bp as pdf_bp
from app.apps.encurtador import bp as encurtador_bp
from app.apps.email_financeiro import bp as email_financeiro_bp
from app.apps.sheets_sync import bp as sheets_sync_bp
from app.apps.atualizaspbotao import bp as atualizaspbotao_bp
from app.apps.validasp import bp as validasp_bp
from app.apps.chatbot import bp as chatbot_bp
from app.apps.baixabradesco import bp as baixabradesco_bp
from app.apps.sync_logs        import bp as sync_logs_bp
from app.apps.processarnovasp  import bp as processarnovasp_bp
from app.apps.emissaonf        import bp as emissao_bp            # ← emissão NFS-e
from app.apps.whatsapp_gateway import bp as whatsapp_gateway_bp   # ← gateway WhatsApp / Evolution
from app.apps.telegram         import bp as telegram_bp           # ← NOVO (bot Telegram / autocadastro)


def create_app():
    app = Flask(__name__)

    app.register_blueprint(pdf_bp)
    app.register_blueprint(encurtador_bp)
    app.register_blueprint(email_financeiro_bp,  url_prefix="/api/email_financeiro")
    app.register_blueprint(sheets_sync_bp,        url_prefix="/api/sheets_sync")
    app.register_blueprint(atualizaspbotao_bp,    url_prefix="/api/atualizaspbotao")
    app.register_blueprint(validasp_bp,           url_prefix="/api/validasp")
    app.register_blueprint(chatbot_bp,            url_prefix="/api/chatbot")
    app.register_blueprint(baixabradesco_bp,      url_prefix="/api/baixabradesco")
    app.register_blueprint(sync_logs_bp,          url_prefix="/api/sync_logs")
    app.register_blueprint(processarnovasp_bp,    url_prefix="/api/processarnovasp")
    app.register_blueprint(emissao_bp,            url_prefix="/emissao")
    # SEM url_prefix: as rotas /instances/<id>/token/<tk>/send-* espelham o Z-API.
    # As rotas internas (/api/whatsapp_gateway/webhook e /health) já trazem o
    # prefixo embutido no próprio módulo.
    app.register_blueprint(whatsapp_gateway_bp)
    # SEM url_prefix: as rotas já trazem o prefixo /telegram embutido no módulo
    # (/telegram/webhook e /telegram/health).
    app.register_blueprint(telegram_bp)                                             # ← NOVO

    @app.route("/")
    def index():
        return {
            "status": "ok",
            "modules": [
                "pdf_processor", "encurtador", "email_financeiro",
                "sheets_sync", "atualizaspbotao", "validasp",
                "chatbot", "baixabradesco", "sync_logs", "processarnovasp",
                "emissao", "whatsapp_gateway", "telegram"
            ]
        }

    return app


app = create_app()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
