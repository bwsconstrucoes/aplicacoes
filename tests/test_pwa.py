"""O ERP instalável no celular — e a regra que não pode ser afrouxada.

O ganho é simples: o navegador do celular passa a poder colocar o ERP como
ícone na tela inicial, abrindo em tela cheia. Não há aplicativo nativo, não há
loja, não há segunda base de código.

O RISCO É QUE ESTE ARQUIVO GUARDA. O jeito comum de escrever um service worker
é guardar as respostas e devolvê-las quando a rede demora. Num site de notícia
isso é ótimo. Aqui seria perigoso: a pessoa abriria o ERP no celular, veria o
"a pagar" de ontem, e decidiria em cima disso — sem nada na tela dizendo que o
número é velho. Um número errado com cara de certo é o que este sistema inteiro
tenta impedir.

Por isso a varredura abaixo recusa qualquer cache que não seja de arquivo
estático. É o tipo de regra que alguém afrouxa de boa-fé, para "ficar mais
rápido", sem perceber o que está trocando.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

SW = Path("app/apps/erp/static/sw.js").read_text(encoding="utf-8")
BASE = Path("app/apps/erp/templates/erp_base.html").read_text(encoding="utf-8")
ESTATICOS = Path("app/apps/erp/static")


# ---------------------------------------------------------------------------
# A regra que não se afrouxa
# ---------------------------------------------------------------------------
def test_o_service_worker_so_guarda_arquivo_estatico():
    assert 'url.pathname.startsWith("/erp/static/")' in SW, (
        "O service worker precisa dizer explicitamente que só guarda "
        "/erp/static/. Sem esse recorte, ele guarda resposta de API.")


def test_o_service_worker_nao_guarda_nada_de_api():
    """Nenhum endereço de API pode aparecer na lista do que vai para o cache."""
    lista = re.search(r"const CASCA = \[(.*?)\]", SW, re.S)
    assert lista, "A lista do que é guardado sumiu — alguém reescreveu o arquivo"
    for linha in lista.group(1).splitlines():
        endereco = linha.strip().strip('",')
        if not endereco:
            continue
        assert endereco.startswith("/erp/static/"), (
            f"{endereco} entrou no cache do celular e não é arquivo estático. "
            f"Se for dado, a pessoa vai ver o número de ontem achando que é "
            f"o de hoje.")


def test_o_service_worker_ignora_o_que_nao_e_get():
    """POST que grava não pode nem passar perto do cache."""
    assert 'req.method !== "GET"' in SW


def test_sem_internet_a_navegacao_mostra_aviso_e_nao_numero():
    assert "Sem internet" in SW
    assert "503" in SW


# ---------------------------------------------------------------------------
# Instalável de verdade
# ---------------------------------------------------------------------------
@pytest.fixture
def app_pwa():
    """Um ERP de mentira, SEM banco nenhum.

    As duas rotas do celular não consultam nada — e é isso que permite que
    elas respondam antes do login. Montar o app sem banco no teste é a prova
    disso: se alguém puser uma consulta ali dentro, este arquivo quebra.
    """
    from flask import Flask
    from app.apps.erp import routes

    a = Flask(__name__)
    a.secret_key = "teste"
    a.register_blueprint(routes.bp)
    return a


@pytest.fixture
def manifesto(app_pwa):
    resposta = app_pwa.test_client().get("/erp/manifest.webmanifest")
    assert resposta.status_code == 200, (
        "O manifesto precisa responder SEM login — o navegador o busca antes "
        "de qualquer sessão, e um 302 faz a instalação nem ser oferecida.")
    return resposta.get_json()


def test_o_manifesto_tem_o_que_o_navegador_exige(manifesto):
    for campo in ("name", "short_name", "start_url", "display", "icons"):
        assert manifesto.get(campo), f"o manifesto está sem {campo}"
    assert manifesto["display"] == "standalone"


def test_o_manifesto_abre_dentro_do_erp(manifesto):
    """start_url fora do /erp/ abriria o ícone numa tela que não é a nossa."""
    assert manifesto["start_url"].startswith("/erp/")
    assert manifesto["scope"] == "/erp/"


def test_os_icones_do_manifesto_existem_no_disco(manifesto):
    for icone in manifesto["icons"]:
        nome = icone["src"].rsplit("/", 1)[-1]
        assert (ESTATICOS / nome).exists(), (
            f"o manifesto promete {nome} e o arquivo não está no repositório — "
            f"o celular mostraria um ícone quebrado")


def test_tem_icone_grande_e_um_recortavel(manifesto):
    """O Android recorta o ícone em círculo. Sem um 'maskable', ele corta o
    nome BWS pela metade."""
    tamanhos = {i["sizes"] for i in manifesto["icons"]}
    assert "192x192" in tamanhos and "512x512" in tamanhos
    assert any(i.get("purpose") == "maskable" for i in manifesto["icons"])


def test_o_service_worker_responde_com_o_alcance_certo(app_pwa):
    resposta = app_pwa.test_client().get("/erp/sw.js")
    assert resposta.status_code == 200
    assert resposta.headers.get("Service-Worker-Allowed") == "/erp/", (
        "Sem este cabeçalho o navegador recusa o alcance /erp/ — e não avisa.")


def test_a_tela_aponta_para_o_manifesto_e_registra_o_service_worker():
    assert 'rel="manifest"' in BASE
    assert "serviceWorker" in BASE and "register" in BASE


def test_o_registro_nao_derruba_a_tela_se_falhar():
    """Navegador antigo, acesso por http, política do aparelho: o ERP tem de
    continuar funcionando igual."""
    assert ".catch(" in BASE.split("serviceWorker")[-1]
