from flask import Blueprint

painel_routes = Blueprint('painel', __name__)

@painel_routes.route("/painel")
def painel():
    return "<h2>Painel administrativo - Em construção</h2>"
