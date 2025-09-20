from .encurtador import encurtador_routes
from .painel import painel_routes
from .api import api_routes

def register_routes(app):
    app.register_blueprint(encurtador_routes)
    app.register_blueprint(painel_routes)
    app.register_blueprint(api_routes)
