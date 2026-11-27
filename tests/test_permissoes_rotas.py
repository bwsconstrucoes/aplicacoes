"""Bloco 0 — o padrão de autorização é NEGAR.

Estes testes são a rede que torna a inversão durável. O teste de inventário
falha quando alguém acrescenta rota sem declarar permissão: o esquecimento
passa a quebrar a suíte antes de virar brecha em produção.

Rodam sem banco. O guard nega antes de o handler abrir sessão, e o
`_usuario_logado` só faz `s.get(Usuario, id)` — que a sessão dublada atende.
"""
from __future__ import annotations

import contextlib

import pytest
from flask import Flask

from app.apps.erp import routes
from app.apps.erp.core.auth.permissoes import PERMISSOES
from app.apps.erp.db.models.cadastros import PerfilUsuario as P

from conftest import SessaoFalsa, novo_usuario


@pytest.fixture
def app():
    """App mínimo só com o blueprint do ERP.

    Não usa `create_app()` de propósito: ele importa os outros 13 blueprints,
    que puxam gspread, dropbox e openai — dependências que o teste não precisa.
    """
    a = Flask(__name__)
    a.secret_key = "teste"
    a.register_blueprint(routes.bp)
    return a


def endpoints_do_erp(app):
    return sorted({r.endpoint for r in app.url_map.iter_rules()
                   if r.endpoint.startswith("erp.")})


# ---------------------------------------------------------------------------
# Inventário — nenhuma rota fica sem declarar
# ---------------------------------------------------------------------------
def test_toda_rota_declara_permissao_ou_e_publica_explicita(app):
    declarados = set(routes._REGISTRO_PERMISSOES) | set(routes._ENDPOINTS_PUBLICOS)
    sem_declaracao = [e for e in endpoints_do_erp(app)
                      if e not in declarados and e not in routes._ISENTOS]

    assert sem_declaracao == [], (
        "Rota sem @permissao(...) nem @permissao_publica(...). O guard vai negar "
        "em produção; declare a ação: " + ", ".join(sem_declaracao))


def test_rota_que_recebe_id_e_aberta_a_perfil_restrito_precisa_checar_escopo():
    """Fecha a brecha de classe que o teste de inventário não pegava.

    O inventário garante que toda rota declara uma AÇÃO. Não garante que uma
    rota de ação ampla, ao receber o número de um registro, verifique se aquele
    registro é do usuário. Foi assim que empreitas e locações passaram batido
    pelos blocos: a auditoria enumerou casos à mão, e mão esquece.

    Aqui a checagem é derivada do código, não de uma lista: se a ação é de
    perfil restrito e a rota recebe um id, tem de haver escopo.
    """
    import ast
    from pathlib import Path

    fonte = Path(routes.__file__).read_text(encoding="utf-8")
    # ações que perfis presos a obra/autoria possuem
    # `cancelar_titulo` entrou em 12/09/2026: quem tem "lancar" a recebe por
    # implicação, então um perfil preso a obra ou a autoria alcança a rota — e
    # ela recebe o número do título.
    AMPLAS = {"ver_erp", "lancar", "ver_pessoal", "lancar_dc", "cancelar_titulo",
              # `encaminhar` (12/09/2026) manda informação do sistema para um
              # celular. Sem o recorte, bastaria mandar o número de um título
              # de outra obra para receber o conteúdo dele no próprio aparelho.
              "encaminhar"}

    def chama_escopo(fn):
        """Procura a CHAMADA, não o nome solto.

        Conferir apenas o texto acusaria a linha de import como se fosse
        verificação — e o teste passaria com a trava removida.
        """
        for sub in ast.walk(fn):
            if isinstance(sub, ast.Call):
                alvo = sub.func
                nome = getattr(alvo, "id", None) or getattr(alvo, "attr", "")
                if nome.startswith("exigir_") and nome.endswith("_no_escopo"):
                    return True
        return False

    # ⚑ A ÚNICA DISPENSA, e ela é escrita aqui para ser discutida na revisão:
    # `manual_portal_tela` recebe o NOME DE UM ARQUIVO do manual, não o número
    # de um registro de ninguém. Não há "registro dele" a conferir — as telas
    # são as mesmas para todo mundo, de um banco de teste, com nomes
    # inventados. O que ela faz no lugar do escopo é conferir o nome contra a
    # lista do que existe na pasta, e responder 404 fora dela.
    SEM_REGISTRO_A_RECORTAR = {"manual_portal_tela"}

    sem_escopo = []
    for no in ast.walk(ast.parse(fonte)):
        if not isinstance(no, ast.FunctionDef):
            continue
        if no.name in SEM_REGISTRO_A_RECORTAR:
            continue
        mapa = routes._REGISTRO_PERMISSOES.get(f"erp.{no.name}")
        if not mapa or not (set(mapa.values()) & AMPLAS):
            continue
        if not no.args.args:            # rota sem id na URL
            continue
        if chama_escopo(no):
            continue
        sem_escopo.append(no.name)

    assert sem_escopo == [], (
        "Rota recebe o número de um registro e é aberta a perfil restrito, mas "
        "não confere se o registro é dele. Chame um exigir_*_no_escopo: "
        + ", ".join(sem_escopo))


def test_nenhuma_acao_declarada_e_desconhecida():
    """Erro de digitação em @permissao viraria rota impossível de acessar."""
    usadas = {a for mapa in routes._REGISTRO_PERMISSOES.values() for a in mapa.values()}

    assert usadas <= set(PERMISSOES), f"ações inexistentes: {usadas - set(PERMISSOES)}"


def test_rota_publica_tem_motivo_escrito():
    for endpoint, motivo in routes._ENDPOINTS_PUBLICOS.items():
        assert motivo and len(motivo) > 10, f"{endpoint} sem motivo real"


def test_o_conjunto_de_rotas_publicas_e_pequeno_e_conhecido():
    """Se esta lista crescer, alguém abriu uma rota — que apareça na revisão.

    `api_agente_rodar` entrou em 07/09/2026 e é a única que aceita ORDEM sem
    sessão: é a rotina diária, de fora, mandando o agente varrer as pendências.
    Ela não é aberta de verdade — recusa sem `ERP_AGENTE_SECRET` no corpo, e
    RECUSA TAMBÉM se o segredo não estiver configurado no ambiente, em vez de
    liberar. O teste logo abaixo prova as duas recusas.

    `manifesto_pwa` e `service_worker` entraram em 11/09/2026, quando o ERP
    passou a poder ser instalado como ícone no celular. São públicas por
    OBRIGAÇÃO DO NAVEGADOR: ele busca as duas antes de qualquer login, e um
    302 para a tela de entrar faria a instalação simplesmente não ser
    oferecida — sem erro nenhum aparecendo. **Nem uma nem outra consulta o
    banco**: uma é a ficha do ícone, a outra é código de navegador. O teste
    `tests/test_pwa.py` monta o ERP SEM banco configurado só para provar isso
    — se alguém puser uma consulta ali dentro, aquele arquivo quebra.

    **AS QUATRO DO PORTAL SAÍRAM DAQUI EM 07/11/2026**, e a saída é a notícia.
    Elas eram públicas porque liam a sessão do APLICATIVO DO PONTO, para quem
    não tinha login do ERP. O dono desfez esse desenho:

    > *"o ponto fica só para ponto, e o ERP a gente utiliza um outro acesso
    > mais completo (…) o cara vai ter o login, acesso do ERP e vai fazer a
    > requisição dele. Aí, só que num ambiente, digamos, mais enxuto para
    > utilizar no celular."*

    A tela enxuta virou `/erp/portal`, rota comum do ERP, com login e com ação
    declarada. **A lista de públicas voltou a ter só o que não pode ter dono**:
    login, saída, saúde, o agente, a porta dos comprovantes (que se defende por
    segredo) e as duas do PWA. Rota pública é superfície de ataque, e esta lista
    só deve encolher.

    **A DÉCIMA E A DÉCIMA PRIMEIRA ENTRARAM EM 10/11/2026: as duas do
    PORTAL** — procurar o colaborador e ler o documento anexado.

    ⚑ ELAS NÃO SÃO ABERTAS, e a palavra "pública" aqui engana: o portal tem
    autenticação PRÓPRIA, a do ponto, e as duas chamam `quem_esta()` na
    primeira linha — quem não entrou leva 401. Elas estão nesta lista porque
    o guard do ERP olha a sessão do ERP, que o portal não usa; a mesma razão
    pela qual a tela do portal já estava aqui.

    O que elas expõem a quem ENTROU: a busca devolve no máximo 20 pessoas,
    só das obras que a pessoa alcança, e o CPF volta com os três últimos
    dígitos — nunca inteiro. Foi de propósito: a caixinha antiga descia com
    até 500 nomes, e pôr o CPF de todos no navegador para poder filtrar ali
    seria entregar o cadastro a quem abrisse o código da página.

    **A DÉCIMA SEGUNDA ENTROU EM 15/11/2026: conferir o CPF no portal.**

    Ele perguntou, olhando o formulário de cadastro: *"o CPF vai ser criticado
    se a pessoa já existir?"* No portal, não era — a obra preenchia o cadastro
    inteiro, fotografava os três documentos, e só ao enviar descobria que a
    pessoa já existe.

    ⚑ MESMA RAZÃO DAS OUTRAS TRÊS DO PORTAL, e a mesma defesa: ela chama
    `quem_esta()` na primeira linha, então quem não entrou leva 401. Está nesta
    lista porque o guard do ERP olha a sessão do ERP, que o portal não usa.

    O que ela devolve a quem ENTROU é o mínimo: se o CPF existe, o nome, e se
    está ativo ou desligado. Nada de salário, endereço, telefone ou documento —
    quem abre pedido não precisa disso, e varrer CPFs não mapearia o cadastro.

    **A NONA ENTROU EM 10/11/2026: "esqueci minha senha".** Pelo mesmo motivo
    da oitava — quem esqueceu a senha não tem como entrar para pedir. Ela não
    diz se a conta existe (a resposta é idêntica nos dois casos), não lista
    nada, e só aceita um pedido a cada dez minutos por pessoa, para não virar
    máquina de encher caixa de e-mail nem de invalidar o link de alguém.

    **A OITAVA ENTROU EM 09/11/2026: o primeiro acesso.** Ela é pública pelo
    único motivo que justifica: *quem abre o link ainda não tem senha*, então
    não existe login a exigir. O que a protege não é a sessão, é o TOKEN — 32
    bytes sorteados, guardado no banco só como resumo (SHA-256), de uso único
    e com prazo de sete dias. Ela não lista nada, não aceita e-mail como
    entrada e não diz se um token existe: o erro é o mesmo para inexistente,
    usado e vencido.

    **AS TRÊS ÚLTIMAS ENTRARAM EM 18/11/2026: criar o próprio acesso.** Ele
    encontrou o buraco tentando orientar a equipe — quem recebia o link do
    portal sem ter conta batia na tela de entrada sem saída, e a alternativa
    era pedir pelo aplicativo do PONTO: *"qual o sentido que tem de uma pessoa
    que vai lançar um diário ter que ir lá no aplicativo do ponto?"*

    Elas são públicas pelo mesmo motivo das outras: quem não tem conta não
    pode entrar para pedir uma. O que as protege, em quatro camadas:

      · o código vai para o telefone **do cadastro**, nunca para o digitado —
        o que a pessoa digita é prova, não endereço;
      · a resposta é **idêntica** exista o CPF ou não, bata o telefone ou não,
        então a tela não serve para descobrir quem está na base;
      · teto de três códigos por hora **por pessoa** e cinco tentativas por
        código, para seis dígitos não caírem na força bruta;
      · e, no fim, **elas não criam acesso nenhum**: só põem o pedido na fila,
        onde gente aprova. Decisão dele no mesmo dia: *"ainda assim precisa
        ser validado por alguém no sistema."*
    """
    assert set(routes._ENDPOINTS_PUBLICOS) == {
        "erp.pagina_login", "erp.sair", "erp.health", "erp.api_agente_rodar",
        "erp.api_comprovantes_lote", "erp.manifesto_pwa", "erp.service_worker",
        "erp.pagina_primeiro_acesso", "erp.pagina_esqueci_a_senha",
        "erp.api_portal_pessoas", "erp.api_portal_ler_documento",
        # 19/11/2026: a leitura de VÁRIOS papéis numa chamada só (admissão).
        "erp.api_portal_ler_documentos",
        "erp.api_portal_conferir_cpf",
        "erp.pagina_primeiro_acesso_portal",
        "erp.api_primeiro_acesso_codigo", "erp.api_primeiro_acesso_concluir",
        # 27/11/2026: a ASSINATURA DO CONTRATO DE EMPREITA por quem é de fora.
        # O empreiteiro não tem login nem deve ter. O que protege: o TOKEN do
        # link (24 bytes sorteados, guardado só como resumo, trocado a cada
        # reenvio), o código de 6 dígitos no telefone/e-mail do cadastro (15
        # minutos, cinco tentativas, cinco códigos por hora), o CPF/CNPJ
        # conferido e o teto por IP. Link inválido e vencido respondem igual.
        "erp.pagina_assinar_contrato", "erp.api_assinar_dados",
        "erp.api_assinar_codigo", "erp.api_assinar", "erp.api_assinar_pdf"}


def test_o_manual_do_portal_exige_login(app):
    """O MANUAL NÃO É PÚBLICO — decisão dele em 25/11/2026.

    Ele nasceu público, no mesmo desenho do manual do ponto. No mesmo dia, ao
    procurar o link, o dono mudou a regra: *"eu acho que o ideal é na tela
    depois do login (…) porque não fica exposto esse manual. Só para quem tem
    acesso."*

    O manual do PONTO continua público, e não é incoerência: lá o manual
    ensina o PRIMEIRO acesso de quem ainda não tem senha, então não há login a
    exigir. Aqui o manual descreve como a empresa trabalha por dentro — quem
    pede o quê, quem assina, quem libera —, e o acesso já existe.

    ⚠️ **A trava de verdade não é este teste: é a PASTA.** O manual mora em
    `app/apps/erp/manual_portal/`, e não em `static/`, porque o `static/` do
    blueprint é servido pelo Flask sem passar por login. Enquanto esteve lá,
    `/erp/static/manual_portal/index.html` respondia a qualquer um — e o
    login destas rotas seria encenação. Há teste disso logo abaixo.
    """
    from app.apps.erp import routes

    for endpoint in ("erp.pagina_manual_portal",
                     "erp.pagina_manual_portal_sem_barra",
                     "erp.manual_portal_tela", "erp.manual_portal_pdf"):
        assert endpoint not in routes._ENDPOINTS_PUBLICOS, (
            f"{endpoint} voltou a ser pública — o manual ficaria aberto na "
            "internet. Foi decisão do dono que ele exige login.")


def test_o_manual_do_portal_nao_mora_no_static(app):
    """Guardá-lo em `static/` o abre de novo, por outro endereço e em
    silêncio: nenhum teste de permissão olha para o `static/` do blueprint,
    porque é lá que moram o CSS e os ícones, que são públicos de propósito."""
    from pathlib import Path

    estatico = (Path(__file__).resolve().parents[1]
                / "app" / "apps" / "erp" / "static" / "manual_portal")
    assert not estatico.exists(), (
        "o manual do portal voltou para `static/`, que o Flask serve em "
        "/erp/static/... sem login. Ele mora em `app/apps/erp/manual_portal/`.")


def test_o_portal_mudou_de_dono_e_nao_de_endereco(app):
    """O endereço `/erp/portal` CONTINUA — o que mudou é quem o guarda.

    Ele era público e se defendia sozinho com a sessão do aplicativo do ponto.
    Em 07/11/2026 passou a exigir o login do ERP. O endereço ficou o mesmo por
    escolha do dono — e isso é melhor: endereço que a obra já pode ter salvo no
    celular não se muda sem motivo, e o motivo era a PORTA, não o nome.

    **É justamente por continuar no mesmo lugar que este teste precisa
    existir.** Rota que troca de guarda sem trocar de endereço é a que mais
    facilmente volta a ficar aberta sem ninguém notar: o endereço parece o
    mesmo, as telas funcionam, e a diferença é invisível até alguém olhar.
    """
    from app.apps.erp import routes

    for endpoint in ("erp.pagina_portal", "erp.api_portal_inicio",
                     "erp.api_portal_abrir", "erp.api_portal_devolver"):
        assert endpoint not in routes._ENDPOINTS_PUBLICOS, (
            f"{endpoint} voltou para a lista de rotas públicas")
    with app.test_client() as c:
        for metodo, url in (("get", "/erp/portal"),
                            ("get", "/erp/portal/api/inicio"),
                            ("post", "/erp/portal/api/pedidos"),
                            ("post", "/erp/portal/api/devolucoes/1")):
            r = getattr(c, metodo)(url, json={})
            assert r.status_code in (302, 401, 403), (
                f"{url} respondeu {r.status_code} a quem não entrou — "
                f"ela voltou a ser pública?")


def test_a_porta_dos_comprovantes_recusa_sem_segredo_e_com_segredo_errado(app, monkeypatch):
    """Esta rota RECEBE ARQUIVO e DÁ BAIXA — é a que mais precisa se defender.

    Sem sessão, quem a alcança é qualquer um. Por isso ela recusa duas vezes: se
    o segredo não estiver configurado no ambiente (não pode virar porta aberta
    por esquecimento) e se o segredo do pedido não bater.
    """
    with app.test_client() as c:
        monkeypatch.delenv("ERP_COMPROVANTE_SECRET", raising=False)
        r = c.post("/erp/api/pagamentos/comprovantes/lote", json={})
        assert r.status_code == 503
        assert "não configurado" in r.get_json()["erro"]

        monkeypatch.setenv("ERP_COMPROVANTE_SECRET", "o-segredo-de-verdade")
        assert c.post("/erp/api/pagamentos/comprovantes/lote",
                      json={"secret": "chute"}).status_code == 403
        assert c.post("/erp/api/pagamentos/comprovantes/lote",
                      json={}).status_code == 403


def test_a_rota_do_agente_recusa_sem_segredo_e_com_segredo_errado(app, monkeypatch):
    """Rota sem login é rota que qualquer um alcança — então ela tem de se
    defender sozinha. Duas recusas, e a primeira é a que mais importa:
    ambiente SEM segredo configurado não pode virar porta aberta."""
    with app.test_client() as c:
        monkeypatch.delenv("ERP_AGENTE_SECRET", raising=False)
        r = c.post("/erp/api/agente/rodar", json={})
        assert r.status_code == 503, "sem segredo no ambiente tem de recusar"
        assert "não configurado" in r.get_json()["erro"]

        monkeypatch.setenv("ERP_AGENTE_SECRET", "o-segredo-de-verdade")
        r = c.post("/erp/api/agente/rodar", json={"secret": "chute"})
        assert r.status_code == 403
        r = c.post("/erp/api/agente/rodar", json={})
        assert r.status_code == 403, "sem segredo no corpo também recusa"


# ---------------------------------------------------------------------------
# O default é negar
# ---------------------------------------------------------------------------
def test_rota_nao_declarada_e_negada(app, monkeypatch):
    """O coração da inversão: rota que ninguém declarou não passa.

    Simula o esquecimento tirando a declaração de uma rota real — é o estado
    exato em que uma rota nova nasce. Não adianta registrar rota solta no app
    de teste: `bp.before_request` só governa endpoints DO blueprint, e é essa
    a fronteira que o guard cobre.
    """
    monkeypatch.delitem(routes._REGISTRO_PERMISSOES, "erp.api_titulos")
    monkeypatch.setattr(routes, "get_session",
                        _sessao_com(novo_usuario(1, P.ADMIN)))

    with app.test_client() as c:
        with c.session_transaction() as sessao:
            sessao["erp_usuario_id"] = 1
        r = c.get("/erp/api/titulos")

    assert r.status_code == 403, "rota sem declaração deveria ser negada, mesmo ao ADMIN"


def _sessao_com(usuario):
    @contextlib.contextmanager
    def _fake():
        yield SessaoFalsa(usuario)
    return _fake


def _pedir(app, monkeypatch, caminho, perfil, metodo="get"):
    monkeypatch.setattr(routes, "get_session", _sessao_com(novo_usuario(1, perfil)))
    with app.test_client() as c:
        with c.session_transaction() as sessao:
            sessao["erp_usuario_id"] = 1
        return getattr(c, metodo)(caminho)


@pytest.mark.parametrize("perfil", [P.CONSULTA, P.ADMINISTRATIVO_OBRA, P.LANCADOR])
def test_perfil_sem_alcada_nao_registra_pagamento(app, monkeypatch, perfil):
    r = _pedir(app, monkeypatch, "/erp/api/pagamentos/baixar", perfil, "post")

    assert r.status_code == 403


@pytest.mark.parametrize("perfil", [P.CONSULTA, P.ADMINISTRATIVO_OBRA, P.SUPERVISOR_OBRA])
def test_perfil_sem_alcada_nao_altera_configuracao(app, monkeypatch, perfil):
    r = _pedir(app, monkeypatch, "/erp/api/config/conta", perfil, "post")

    assert r.status_code == 403


@pytest.mark.parametrize("perfil", [P.ADMINISTRATIVO_OBRA, P.LANCADOR, P.CONSULTA])
def test_dados_de_pagamento_negados_a_quem_nao_pode_ve_los(app, monkeypatch, perfil):
    """A regra que já existia em /api/titulos agora vale na rota que a burlava."""
    r = _pedir(app, monkeypatch, "/erp/api/pagamentos/detalhe/1", perfil)

    assert r.status_code == 403


@pytest.mark.parametrize("perfil", [P.ADMINISTRATIVO_OBRA, P.LANCADOR, P.CONSULTA])
def test_relatorio_gerencial_negado_a_perfil_operacional(app, monkeypatch, perfil):
    r = _pedir(app, monkeypatch, "/erp/api/relatorios", perfil, "post")

    assert r.status_code == 403


def test_consulta_nao_cancela_titulo(app, monkeypatch):
    """Cancelar não tinha checagem nenhuma: qualquer logado cancelava em lote."""
    r = _pedir(app, monkeypatch, "/erp/api/titulos/acao", P.CONSULTA, "post")

    assert r.status_code == 403


def test_quem_tem_alcada_passa_pelo_guard(app, monkeypatch):
    """O guard libera FINANCEIRO em pagamento.

    Depois dele o handler roda com a sessão dublada e falha por outro motivo —
    então aqui só se afirma o que este teste pode afirmar: não foi barrado por
    permissão. O caminho feliz completo depende de Postgres e é verificado em
    homologação.
    """
    r = _pedir(app, monkeypatch, "/erp/api/pagamentos/baixar", P.FINANCEIRO, "post")

    assert r.status_code != 403


@pytest.mark.parametrize("caminho, metodo", [
    ("/erp/api/importar/pipefy", "post"),
    ("/erp/api/importar/csv", "post"),
    ("/erp/api/importar/ofx", "post"),
    ("/erp/api/receber/baixar", "post"),
    ("/erp/api/conciliacao/executar", "post"),
    ("/erp/api/conciliacao/manual", "post"),
    ("/erp/api/obras/1/fase", "post"),
    ("/erp/api/config/categoria", "post"),
    ("/erp/api/obras/nova", "post"),
    ("/erp/api/obras/documento", "post"),
    ("/erp/api/obras/documento/ler", "post"),
    ("/erp/api/config/depara/definir", "post"),
])
def test_administrativo_de_obra_nao_executa_acao_de_alcada(app, monkeypatch,
                                                           caminho, metodo):
    """Bloco 2: todas estas rodavam para qualquer pessoa com login."""
    r = _pedir(app, monkeypatch, caminho, P.ADMINISTRATIVO_OBRA, metodo)

    assert r.status_code == 403


@pytest.mark.parametrize("caminho", [
    "/erp/api/pagamentos/agenda",
    "/erp/api/lotes",
    "/erp/api/conciliacao/painel",
    "/erp/api/conciliacao/extrato",
    "/erp/api/movimentacoes/neutras",
    "/erp/api/mapa",
])
def test_painel_financeiro_negado_a_perfil_operacional(app, monkeypatch, caminho):
    """Bloco 4: agregados que expunham a posição financeira inteira."""
    r = _pedir(app, monkeypatch, caminho, P.ADMINISTRATIVO_OBRA)

    assert r.status_code == 403


def test_consulta_nao_enumera_operadores(app, monkeypatch):
    """Lista de quem existe é o passo 1 para escolher alvo."""
    r = _pedir(app, monkeypatch, "/erp/api/operadores/contato", P.CONSULTA)

    assert r.status_code == 403


def test_rota_publica_responde_sem_sessao(app):
    with app.test_client() as c:
        r = c.get("/erp/health")

    assert r.status_code != 403


def test_metodo_nao_declarado_no_mapa_e_recusado(app, monkeypatch):
    """`api_interessados` declara GET/POST/DELETE. PUT não existe no mapa."""
    mapa = routes._REGISTRO_PERMISSOES["erp.api_interessados"]

    assert "PUT" not in mapa
    assert set(mapa) == {"GET", "POST", "DELETE"}


# ---------------------------------------------------------------------------
# Escopo de objeto — a outra metade (usada a partir do bloco 1)
# ---------------------------------------------------------------------------
def test_fora_do_escopo_responde_404_e_nao_403(app):
    """Nunca 403 aqui: dizer 'sem permissão' confirmaria que o id existe.

    Exercita o handler diretamente. A prova ponta a ponta vem no bloco 1, com
    as rotas que passam a chamar `exigir_titulo_no_escopo`.
    """
    from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado

    with app.app_context():
        resposta, codigo = routes._fora_do_escopo(
            ErroNaoEncontrado("Título não encontrado."))

    assert codigo == 404
    assert resposta.get_json()["erro"] == "Título não encontrado."


def test_handler_de_fora_do_escopo_esta_registrado_no_blueprint():
    """Sem o registro, o vazamento volta calado como 500."""
    from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado

    registrados = set()
    for por_codigo in routes.bp.error_handler_spec.values():
        for mapa in por_codigo.values():
            registrados.update(mapa)

    assert ErroNaoEncontrado in registrados


# ---------------------------------------------------------------------------
# O MENU SOME PARA QUEM NÃO TEM A ÁREA (13/09/2026)
#
# Decisão do dono: *"se a pessoa está liberada apenas pra visualizar lançamento
# financeiro, ela não tem que ver nada do suprimento. Não vai ver cadastro de
# suprimento, de insumo, pedidos de compra — não vai ver nada disso se eu não
# disponibilizar pra aquele perfil"*.
#
# Para uma área SUMIR, cada tela precisa de ação própria: `ver_erp` é a porta
# de entrada, e esconder por ela esconderia o ERP inteiro. Este teste é a
# trava estrutural disso — aba nova declarada com `ver_erp` aparece para todo
# mundo, e isso tem de ser uma escolha escrita, não um descuido.
# ---------------------------------------------------------------------------
ABAS_ABERTAS_A_TODOS = {
    # Cada pessoa vê a PRÓPRIA semana aqui; quem vê a equipe precisa de
    # `ver_uso_da_equipe`, que é conferido dentro da tela. Decisão do dono em
    # 10/09/2026: "deixa de ser vigilância para virar retorno".
    "uso",
}


def test_toda_aba_do_menu_tem_acao_propria():
    from app.apps.erp.routes import MODULOS, _REGISTRO_PERMISSOES

    sem_acao_propria = []
    for m in MODULOS:
        for chave, rotulo, rota in m["abas"]:
            acao = (_REGISTRO_PERMISSOES.get(rota) or {}).get("GET")
            if acao is None:
                sem_acao_propria.append((chave, "sem declaração"))
            elif acao == "ver_erp" and chave not in ABAS_ABERTAS_A_TODOS:
                sem_acao_propria.append((chave, "só ver_erp"))
    assert sem_acao_propria == [], (
        "estas abas apareceriam no menu de todo mundo — dê a cada uma a ação "
        f"da própria tela, ou registre em ABAS_ABERTAS_A_TODOS: {sem_acao_propria}")


def test_o_botao_do_modulo_leva_a_uma_aba_que_a_pessoa_abre():
    """O módulo aponta para a PRIMEIRA aba visível, não para a primeira da
    lista — senão quem tem acesso parcial cairia numa tela que responde 403."""
    from app.apps.erp.routes import MODULOS, _menu_da_pessoa, _acao_da_aba

    # Uma pessoa que só abre "Cotações" dentro de Suprimentos.
    pode = {a: False for m in MODULOS for _, _, r in m["abas"]
            for a in [_acao_da_aba(r)]}
    pode["comprar"] = True
    modulos, _ = _menu_da_pessoa(pode)
    assert [m["chave"] for m in modulos] == ["suprimentos"]
    primeira = modulos[0]["abas"][0]
    assert pode[_acao_da_aba(primeira[2])] is True
