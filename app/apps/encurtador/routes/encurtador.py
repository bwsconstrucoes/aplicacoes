from flask import Blueprint, request, jsonify, redirect
from urllib.parse import quote
from ..sheets import buscar_url_por_codigo, adicionar_link
import requests, csv, time

encurtador_routes = Blueprint("encurtador", __name__, url_prefix="/encurtador")

@encurtador_routes.route("/<codigo>")
def obter_link(codigo):
    """
    Retorna JSON do link (codigo, url, expira_em) ou 404 se não encontrado/expirado.
    """
    link = buscar_url_por_codigo(codigo)
    if not link or (isinstance(link, dict) and link.get("erro")):
        return jsonify({"erro": "Link não encontrado"}), 404
    return jsonify(link)

@encurtador_routes.route("/go/<codigo>")
def redirecionar_codigo(codigo):
    """
    Redireciona para a URL do código. Útil para testes via /encurtador/go/<codigo>.
    (O redirecionamento em /<codigo> fica no app.main)
    """
    link = buscar_url_por_codigo(codigo)
    if not link or (isinstance(link, dict) and link.get("erro")):
        return "Link não encontrado", 404
    return redirect(link.get("url"), code=302)

@encurtador_routes.route("/novo", methods=["POST"])
def novo_link():
    """
    Cria um link curto. Aceita JSON ou x-www-form-urlencoded.
    Campos obrigatórios: codigo, url, expira_em (use 'nunca' para sem expiração).
    """
    data = request.get_json(silent=True) or request.form
    codigo = (data.get("codigo") or "").strip()
    url = (data.get("url") or "").strip()
    expira_em = (data.get("expira_em") or "").strip()

    if not codigo or not url or not expira_em:
        return jsonify({"erro": "Campos obrigatórios: codigo, url, expira_em"}), 400

    ok = adicionar_link(codigo, url, expira_em)
    if not ok:
        return jsonify({"erro": "Falha ao gravar na planilha"}), 500

    base = "https://aplicacoes.bwsconstrucoes.com.br"
    return jsonify({
        "status": "ok",
        "short_url": f"{base}/{codigo}",
        "original_url": url,
        "expira_em": expira_em
    }), 200

@encurtador_routes.route("/debug")
def debug_linhas():
    """
    Mostra as linhas cruas do CSV público da planilha (com cache-bust e detecção de delimitador).
    Serve apenas para diagnóstico.
    """
    SHEET_ID = "1k-ydMq9JEhWGSt7P3D0ucYj2bWNMkhA9uk1kBJiOMb8"
    SHEET_NAME = "Links"
    cb = int(time.time())
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={quote(SHEET_NAME)}&cachebust={cb}"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        lines = [ln for ln in r.content.decode("utf-8", errors="replace").splitlines() if ln.strip()]
        if not lines:
            return jsonify([])

        first = lines[0]
        delim = "\t" if "\t" in first else (";" if first.count(";") > first.count(",") else ",")

        reader = csv.DictReader(lines, delimiter=delim)
        rows = [row for row in reader]
        return jsonify(rows)
    except Exception as e:
        return jsonify({"erro": f"Falha ao acessar planilha: {str(e)}"}), 500
