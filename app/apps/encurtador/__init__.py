from flask import Blueprint, redirect
from datetime import datetime
from .sheets import buscar_url_por_codigo
# Import original blueprints
from .routes.encurtador import encurtador_routes
from .routes.painel import painel_routes
from .routes.api import api_routes

# Composite blueprint (no prefix) to preserve original paths
bp = Blueprint("encurtador_root", __name__)

# Register original blueprints into composite
bp.register_blueprint(encurtador_routes)  # keeps url_prefix='/encurtador'
bp.register_blueprint(painel_routes)
bp.register_blueprint(api_routes)

# Global redirect route from original app-bws/app.py
@bp.route("/<codigo>")
def redirecionador_global(codigo):
    link = buscar_url_por_codigo(codigo)
    if not link:
        return "Link não encontrado", 404

    # Handles 'nunca' or ISO date
    if link["expira_em"].lower() != "nunca":
        try:
            expira = datetime.fromisoformat(link["expira_em"])
            if expira < datetime.now():
                return "Link expirado", 410
        except Exception as e:
            print(f"[ERRO] Falha ao interpretar 'expira_em': {e}")
            return "Erro ao processar validade do link", 500

    return redirect(link["url"])
