import logging
import re

from flask import Blueprint, redirect
from datetime import datetime
from .sheets import buscar_url_por_codigo
# Import original blueprints
from .routes.encurtador import encurtador_routes
from .routes.api import api_routes

logger = logging.getLogger(__name__)

# Composite blueprint (no prefix) to preserve original paths
bp = Blueprint("encurtador_root", __name__)

# Register original blueprints into composite
bp.register_blueprint(encurtador_routes)  # keeps url_prefix='/encurtador'
# O /painel deste módulo era um marcador vazio ("Painel administrativo - Em
# construção"). Foi removido porque o endereço /painel passou a ser o Painel
# Financeiro OMIE, e o marcador sombreava a tela de verdade.
bp.register_blueprint(api_routes)


# ---------------------------------------------------------------------------
# O QUE NÃO É CÓDIGO CURTO — e por que isso importa (22/09/2026)
#
# Esta rota é um curinga: ela pega QUALQUER endereço de um pedaço só que
# nenhum dos 18 módulos reconheceu. Sem a peneira abaixo, `/favicon.ico` — que
# o navegador pede sozinho ao abrir a tela de entrada — virava uma consulta à
# planilha do Google pela internet, e respondia **erro 500**.
#
# O custo é real e some no meio do log: a produção roda com 1 processo e 4
# linhas de atendimento; cada endereço errado prendia uma delas numa chamada
# externa. Endereço digitado torto, link velho ou robô de busca bastavam.
#
# A peneira é por FORMA, não por lista: código curto é uma palavra sem ponto e
# sem barra. Arquivo tem extensão; caminho de serviço tem ponto ou começa com
# ponto. Nada disso chega à planilha.
# ---------------------------------------------------------------------------
_FORMATO_DE_CODIGO = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_NUNCA_SAO_CODIGO = {"favicon", "robots", "sitemap", "healthz", "health",
                     "static", "assets", "apple-touch-icon"}


@bp.route("/<codigo>")
def redirecionador_global(codigo):
    if not _FORMATO_DE_CODIGO.match(codigo or "") or codigo.lower() in _NUNCA_SAO_CODIGO:
        return "Link não encontrado", 404

    try:
        link = buscar_url_por_codigo(codigo)
    except Exception as e:                       # noqa: BLE001
        # PLANILHA FORA DO AR NÃO É ERRO DO SISTEMA para quem clicou: ele
        # pediu um link, e o link não pôde ser achado. Antes isto subia como
        # 500 e aparecia como "falha do sistema" — assustando por um link
        # velho que talvez nem exista.
        logger.warning("Encurtador: não consegui consultar a planilha (%s)", e)
        return "Link não encontrado", 404

    if not link:
        return "Link não encontrado", 404

    # Handles 'nunca' or ISO date
    if link["expira_em"].lower() != "nunca":
        try:
            expira = datetime.fromisoformat(link["expira_em"])
            if expira < datetime.now():
                return "Link expirado", 410
        except Exception as e:
            logger.warning("Encurtador: validade ilegível em %r (%s)", codigo, e)
            return "Erro ao processar validade do link", 500

    return redirect(link["url"])
