from flask import Blueprint, request, jsonify, redirect
from urllib.parse import quote, urlparse, parse_qsl, urlencode, urlunparse
from ..sheets import buscar_url_por_codigo, adicionar_link
import logging
import requests, csv, time

logger = logging.getLogger(__name__)

encurtador_routes = Blueprint("encurtador", __name__, url_prefix="/encurtador")

# Limiar pra offload do parâmetro json= pra coluna D (rota2).
# Acima disso o Make consome o base64 lendo da planilha em vez do browser.
LIMITE_JSON_ROTA2 = 10000


def _aplicar_rota2(url: str):
    """
    Se a URL tiver acao=validar e json= com mais de LIMITE_JSON_ROTA2 caracteres,
    devolve (url_curta_com_json_rota2, conteudo_base64_extraido).
    Caso contrário, devolve (url, "").
    """
    try:
        parsed = urlparse(url)
    except Exception:
        return url, ""

    # parse_qsl preserva ordem; keep_blank_values=True por segurança
    pares = parse_qsl(parsed.query, keep_blank_values=True)
    if not pares:
        return url, ""

    # Só aplica quando acao=validar (cenário do Make adaptado pra essa rota)
    acao = next((v for k, v in pares if k == "acao"), None)
    if acao != "validar":
        return url, ""

    json_val = next((v for k, v in pares if k == "json"), None)
    if not json_val or len(json_val) <= LIMITE_JSON_ROTA2:
        return url, ""

    # Reescreve json=<base64> -> json=rota2 preservando ordem dos demais params
    novos_pares = [(k, "rota2" if k == "json" else v) for k, v in pares]
    nova_query = urlencode(novos_pares, doseq=True)
    nova_url = urlunparse(parsed._replace(query=nova_query))
    return nova_url, json_val


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

    Se a URL tiver acao=validar e json= maior que LIMITE_JSON_ROTA2, o base64 é
    salvo na coluna D (conteudo_base64) e a URL gravada na coluna B fica com
    json=rota2. O Make pesca o base64 da planilha em vez de receber via browser.
    """
    data = request.get_json(silent=True) or request.form
    codigo = (data.get("codigo") or "").strip()
    url = (data.get("url") or "").strip()
    expira_em = (data.get("expira_em") or "").strip()

    if not codigo or not url or not expira_em:
        return jsonify({"erro": "Campos obrigatórios: codigo, url, expira_em"}), 400

    url_para_gravar, conteudo_base64 = _aplicar_rota2(url)
    if conteudo_base64:
        logger.info(
            "Offload rota2 aplicado: codigo=%s, tamanho_base64=%d",
            codigo, len(conteudo_base64),
        )

    ok = adicionar_link(codigo, url_para_gravar, expira_em, conteudo_base64)
    if not ok:
        return jsonify({"erro": "Falha ao gravar na planilha"}), 500

    base = "https://aplicacoes.bwsconstrucoes.com.br"
    return jsonify({
        "status": "ok",
        "short_url": f"{base}/{codigo}",
        "original_url": url,
        "expira_em": expira_em,
        "rota2": bool(conteudo_base64),
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