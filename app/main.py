import logging
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
from app.apps.erp              import bp as erp_bp               # ← ERP financeiro (Postgres)

# O painel entra por importação PROTEGIDA, diferente dos demais. Ele é o módulo
# mais novo e o único que depende de biblioteca de dados (pandas/pyarrow) na hora
# de atualizar. Se algo faltar no ambiente, o certo é o painel ficar fora do ar
# sozinho — não levar junto os outros 14 módulos que estão em produção.
try:
    from app.apps.painel import bp as painel_bp      # ← Painel financeiro OMIE
except Exception as _erro_painel:                     # noqa: BLE001
    painel_bp = None
    logging.getLogger(__name__).exception(
        "Painel OMIE não carregou (%s). Os demais módulos seguem normalmente.",
        _erro_painel)


def create_app():
    app = Flask(__name__)
    # Chave de sessão — usada pelo login do ERP. Defina ERP_SECRET_KEY no Render.
    app.secret_key = os.getenv("ERP_SECRET_KEY") or os.getenv("SECRET_KEY") or "bws-erp-dev"

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
    # SEM url_prefix: as rotas do ERP já trazem /erp embutido no módulo
    # (/erp, /erp/entrar, /erp/api/... e /erp/health).
    app.register_blueprint(erp_bp)
    # SEM url_prefix: as rotas do painel já trazem /painel embutido no módulo.
    if painel_bp is not None:
        app.register_blueprint(painel_bp)

    @app.route("/")
    def index():
        return {
            "status": "ok",
            "modules": [
                "pdf_processor", "encurtador", "email_financeiro",
                "sheets_sync", "atualizaspbotao", "validasp",
                "chatbot", "baixabradesco", "sync_logs", "processarnovasp",
                "emissao", "whatsapp_gateway", "telegram", "erp",
            ] + (["painel"] if painel_bp is not None else [])
        }

    return app


app = create_app()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
