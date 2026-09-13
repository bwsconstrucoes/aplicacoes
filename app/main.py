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

# Mesma proteção, mesma razão: a Análise de SPs é o módulo mais novo e depende
# de banco e de Google. Se algo faltar, ela fica fora do ar sozinha em vez de
# levar junto os 15 módulos que estão em produção.
try:
    from app.apps.analisesps import bp as analisesps_bp   # ← Análise de SPs
except Exception as _erro_analisesps:                     # noqa: BLE001
    analisesps_bp = None
    logging.getLogger(__name__).exception(
        "Análise de SPs não carregou (%s). Os demais módulos seguem normalmente.",
        _erro_analisesps)


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
    # SEM url_prefix: as rotas já trazem /analisesps embutido no módulo.
    if analisesps_bp is not None:
        app.register_blueprint(analisesps_bp)

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
              + (["analisesps"] if analisesps_bp is not None else [])
        }

    # TEMPORÁRIO — remover após o diagnóstico
    # Testa se este serviço (Render) alcança o portal do DETRAN-CE. Sem
    # autenticação, sem banco, sem variável de ambiente: só sai para a internet
    # e mostra o resultado em texto. Nunca lança erro — captura tudo e exibe.
    @app.route("/teste-detran")
    def teste_detran():
        import time
        import requests as _requests

        cabecalhos = {
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:149.0) "
                           "Gecko/20100101 Firefox/149.0"),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        }
        alvos = [
            "https://sistemas.detran.ce.gov.br/central",
            "https://www.detran.ce.gov.br",
            "https://example.com",  # controle
        ]
        linhas = ["Diagnóstico de rede — DETRAN-CE", ""]

        try:
            inicio = time.monotonic()
            ip = _requests.get("https://api.ipify.org", timeout=25).text.strip()
            linhas.append(f"IP de saída: {ip} ({time.monotonic() - inicio:.1f}s)")
        except Exception as e:  # noqa: BLE001
            linhas.append(f"IP de saída: FALHOU — {type(e).__name__}: {e}")
        linhas.append("")

        for url in alvos:
            inicio = time.monotonic()
            try:
                r = _requests.get(url, headers=cabecalhos, timeout=25)
                gasto = time.monotonic() - inicio
                linhas.append(f"{url}\n  HTTP {r.status_code} | {len(r.text)} caracteres | "
                              f"{gasto:.1f}s | URL final: {r.url}")
            except Exception as e:  # noqa: BLE001
                gasto = time.monotonic() - inicio
                linhas.append(f"{url}\n  FALHOU após {gasto:.1f}s | "
                              f"{type(e).__name__}: {e}")
            linhas.append("")

        return "\n".join(linhas), 200, {"Content-Type": "text/plain; charset=utf-8"}

    return app


app = create_app()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
