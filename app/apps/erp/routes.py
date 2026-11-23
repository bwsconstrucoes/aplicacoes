# ============================================================================
# ERP — app/apps/erp/routes.py
# Blueprint do ERP financeiro dentro do monorepo. Serve a interface HTML e a
# API JSON consumida por ela. Segue os padrões da casa: blueprint próprio,
# respostas {ok: true/false}, logging PT-BR, credenciais por envvar.
#
# Envvars: DATABASE_URL (Internal Database URL do erp-db) e ERP_SECRET_KEY
# (assinatura da sessão; usa fallback derivado se ausente).
# ============================================================================
from __future__ import annotations

import json
import logging
import os
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from functools import wraps

from flask import (
    Blueprint, g, jsonify, make_response, redirect, render_template, request,
    session, url_for,
)

from sqlalchemy.exc import IntegrityError, ProgrammingError

from app.apps.erp.core.auth.permissoes import (
    ACAO_ROTULOS, PERMISSOES, PROTEGIDAS_DO_ADMIN, ROTULOS,
    acoes_do_perfil_no_banco, decidir, pode,
)
from app.apps.erp.core.auth.service import ErroAutenticacao, autenticar
from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroPermissao, ErroValidacao,
)
from app.apps.erp.core.comum.formato import recado_de_falha
from app.apps.erp.core.comum.ia_custo import SemSaldoDeIa
from app.apps.erp.core.titulos import service as svc_titulos
from app.apps.erp.db.database import get_session
from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario

logger = logging.getLogger(__name__)

bp = Blueprint("erp", __name__, template_folder="templates", static_folder="static",
               static_url_path="/erp/static")

_LIMITE_GRADE = 500


# ---------------------------------------------------------------------------
# O TERMÔMETRO: quanto tempo cada tela leva
#
# Mede toda requisição do ERP e acumula na memória do processo; a gravação
# desce ao banco de tempos em tempos, agregada por dia e por rota. Medir não
# pode custar mais que o que se mede.
#
# NADA AQUI PODE DERRUBAR UMA TELA. Os dois ganchos estão embrulhados: se a
# medição falhar, o número se perde e a vida segue. Sistema que cai por causa
# do próprio termômetro é pior que sistema sem termômetro.
# ---------------------------------------------------------------------------
@bp.before_request
def _saude_comecou():
    try:
        from time import perf_counter
        g._saude_inicio = perf_counter()
    except Exception:
        pass
    # A LINHA DE TRABALHO EM SEGUNDO PLANO acorda na primeira requisição depois
    # de o serviço subir. Tem de ser aqui, e não no import: se um trabalho
    # ficou pendente antes do reinício, ninguém mais o notaria — e no import o
    # banco pode nem existir ainda, o que derrubaria os catorze módulos juntos.
    # Depois da primeira vez isto é a checagem de uma variável.
    global _FILA_ACORDADA
    if not _FILA_ACORDADA:
        _FILA_ACORDADA = True
        try:
            from app.apps.erp.core.comum import tarefas, trabalhos
            trabalhos.registrar_todos()
            tarefas.acordar()
        except Exception:
            logger.warning("ERP/tarefas: não deu para iniciar a fila", exc_info=True)
    # ⚑ A VIRADA DO DIA DOS DESLIGAMENTOS (17/11/2026). Ele: *"o ideal é cortar
    # tudo, porque aí corre o risco de a gente esquecer."*
    #
    # Quem tem data de saída vencida e ainda está ativo é desligado aqui, e o
    # login dela cai junto. Ver `core/cadastros/desligamentos_do_dia.py`.
    #
    # ⚠️ ESTÁ NESTE GANCHO, E NÃO EM OUTRO, DE PROPÓSITO: ele roda ANTES da
    # guarda de permissão (que vem mais abaixo neste arquivo). Então, mesmo que
    # a primeira pessoa a tocar o ERP no dia seja justamente quem saiu, a
    # própria requisição dela desliga o cadastro e a guarda a recusa em
    # seguida. Pendurado depois da guarda, ela entraria uma última vez.
    #
    # Depois da primeira vez do dia é a comparação de uma variável.
    try:
        from app.apps.erp.core.cadastros import desligamentos_do_dia
        if desligamentos_do_dia.precisa_rodar():
            with get_session() as s:
                desligamentos_do_dia.aplicar_se_preciso(s)
    except Exception:                               # noqa: BLE001
        logger.warning("ERP/desligamentos: a virada do dia não rodou", exc_info=True)
    # ⚑ A RODADA DIÁRIA DO AGENTE DE DP (20/11/2026) — vai para a fila de
    # trabalhos, uma vez por dia. Ver `agente_dp.rodada_do_dia_se_preciso`.
    try:
        from app.apps.erp.core import agente_dp
        if agente_dp.precisa_da_rodada():
            with get_session() as s:
                agente_dp.rodada_do_dia_se_preciso(s)
    except Exception:                               # noqa: BLE001
        logger.warning("ERP/agente: a rodada do dia não foi enfileirada", exc_info=True)


_FILA_ACORDADA = False


@bp.after_request
def _saude_terminou(resposta):
    try:
        from time import perf_counter
        from app.apps.erp.core.comum import saude
        inicio = getattr(g, "_saude_inicio", None)
        if inicio is not None and request.endpoint:
            ms = int((perf_counter() - inicio) * 1000)
            saude.registrar(request.endpoint, ms,
                            erro=(resposta.status_code >= 500))
        # A gravação acontece DEPOIS de a resposta estar pronta, e só de
        # minuto em minuto — a pessoa não espera por ela.
        if saude.hora_de_gravar():
            saude.gravar()
    except Exception:
        pass
    return resposta

# ---------------------------------------------------------------------------
# Navegação em MÓDULOS
# O sistema não é um financeiro com apêndices: cada área é um módulo, com suas
# próprias telas. A barra de abas mostra só o módulo em que a pessoa está.
# ---------------------------------------------------------------------------
MODULOS = [
    {
        "chave": "financeiro", "nome": "Financeiro", "sigla": "FIN",
        "descricao": "Solicitações de pagamento, recebimentos, conciliação e relatórios",
        "cor": "var(--azul-claro)",
        "abas": [
            ("lancar", "Lançar", "erp.pagina_lancar"),
            ("prestacao", "Fundo fixo e cartão", "erp.pagina_prestacao"),
            ("titulos", "Solicitações", "erp.pagina_titulos"),
            ("confirmar", "Confirmar", "erp.pagina_confirmar"),
            ("pagamentos", "Pagamentos", "erp.pagina_pagamentos"),
            ("empreitas", "Empreitas", "erp.pagina_empreitas"),
            ("conciliacao", "Conciliação", "erp.pagina_conciliacao"),
            ("notas", "Notas fiscais", "erp.pagina_notas"),
            ("notas_emitidas", "Notas emitidas", "erp.pagina_notas_emitidas"),
            ("receber", "Receber", "erp.pagina_receber"),
            ("relatorios", "Relatórios", "erp.pagina_relatorios"),
            ("importar", "Importar", "erp.pagina_importar"),
            # PERGUNTAR SAIU DAQUI EM 11/09/2026, a pedido do dono: *"o
            # perguntar que está na barra lá em cima é ser acessado de forma
            # geral, e não por exemplo dentro do financeiro"*. Ele tem razão —
            # as perguntas já alcançam obras, contratos e suprimentos, e ficar
            # numa aba do Financeiro dava a entender que era coisa de lá.
            #
            # Agora o assistente mora no CANTO DE TODA TELA (o botão redondo,
            # em `erp_base.html`), e a tela cheia continua existindo em
            # /erp/perguntar — alcançada pelo ⤢ do painel, para quando a
            # resposta tem tabela grande demais para o cantinho.
        ],
    },
    {
        "chave": "obras", "nome": "Obras", "sigla": "OBR",
        "descricao": "Contratos, aditivos, vigências, tributação e documentação das obras",
        "cor": "var(--amarelo)",
        "abas": [
            ("obras", "Painel de obras", "erp.pagina_obras"),
            ("contratos", "Contratos e medições", "erp.pagina_contratos"),
            ("acompanhamento", "Acompanhamento", "erp.pagina_acompanhamento"),
            ("diario", "Diário de obra", "erp.pagina_diario"),
            ("agenda", "Agenda", "erp.pagina_agenda"),
        ],
    },
    {
        "chave": "pessoal", "nome": "Pessoal", "sigla": "PES",
        "descricao": "Colaboradores e despesas com colaboradores (diárias, produção, verbas)",
        "cor": "var(--verde)",
        "abas": [
            ("dc", "Despesas com colaborador", "erp.pagina_dc"),
            ("colaboradores", "Colaboradores", "erp.pagina_colaboradores"),
            # CARGOS E CONVENCOES NAO E ABA (05/11/2026). Eu havia dado a ela
            # uma aba no topo, e o dono cortou: *"Cargos e convencoes, eu acho
            # que foi dado muito destaque a uma tela dessa. Isso aqui e para
            # estar dentro de uma especie de configuracoes."*
            #
            # Ele esta certo, e ha precedente: EMPRESAS saiu do topo pelo mesmo
            # motivo, em 10/09/2026. E tabela que se mexe quando sai dissidio,
            # nao trabalho de todo dia — e aba no topo e para o que se abre
            # todo dia. A tela continua a mesma, alcancada por
            # Configuracoes > Cargos e convencoes.
            # A fila de pedidos administrativos (05/11/2026): o segundo pipe do
            # Pipefy. Os pedidos de PONTO já vivem no módulo do ponto.
            ("dp_solicitacoes", "Solicitações", "erp.pagina_dp_solicitacoes"),
            # A programação de férias (20/11/2026): o que a contabilidade
            # confirmou, o que está chegando e o que está para mudar.
            ("dp_ferias", "Férias", "erp.pagina_dp_ferias"),
            ("dp_contabilidade", "Contabilidade e agente",
             "erp.pagina_dp_contabilidade"),
            # ⚑ AS CONDICIONAIS QUE O PIPEFY NÃO EXPORTA (17/11/2026). Ele:
            # *"o pipe não exporta condicionais, por isso minha pergunta sobre
            # mais ajuda na construção."* Esta tela MEDE a regra nos cadastros
            # em vez de perguntá-la — ver core/cadastros/regras_pelos_dados.py.
            ("dp_regras", "Regras do cadastro", "erp.pagina_dp_regras"),
        ],
    },
    {
        "chave": "suprimentos", "nome": "Suprimentos", "sigla": "SUP",
        "descricao": "Insumos, fornecedores, cotação e pedidos de compra",
        "cor": "var(--ambar)",
        "abas": [
            # ⚑ O PAINEL VEM PRIMEIRO (20/11/2026). Ele: *"tem que ter um
            # painel do coordenador, para ele poder bater o olho e compreender
            # a coisa (…) o que é prioritário, o que está atrasando, o que
            # deveria ter chegado e não chegou."*
            ("sup_painel", "Painel", "erp.pagina_suprimentos_painel"),
            ("sup_solicitacoes", "Solicitações", "erp.pagina_suprimentos"),
            ("sup_planejar", "Planejar cotações", "erp.pagina_suprimentos_planejar"),
            ("sup_cotacoes", "Cotações", "erp.pagina_suprimentos_cotacoes"),
            ("sup_pedidos", "Pedidos", "erp.pagina_suprimentos_pedidos"),
            # ⚑ AS DUAS VISÕES QUE FALTAVAM (20/11/2026). "Acompanhar" é a do
            # coordenador de suprimentos: cada compra do pedido ao pagamento,
            # e o que falta para fechar o ciclo. "Projeção" é a do financeiro:
            # o que as compras ainda vão custar, e quando — antes de a nota
            # chegar. Ele: *"o financeiro não pode ficar no escuro em relação
            # ao planejamento do compras."*
            # A porta da OBRA: conferir e registrar o que desceu do caminhão.
            ("sup_receber", "Receber material", "erp.pagina_suprimentos_receber"),
            ("sup_acompanhar", "Acompanhar compras", "erp.pagina_suprimentos_acompanhar"),
            ("sup_projecao", "Projeção de pagamentos", "erp.pagina_suprimentos_projecao"),
            ("locacoes", "Locações", "erp.pagina_locacoes"),
            ("sup_compras", "Compras", "erp.pagina_compras"),
            ("sup_precos", "Banco de preços", "erp.pagina_suprimentos_precos"),
            # Os relatórios de GESTÃO (21/11/2026): economia da cotação, preço
            # contra a compra anterior, desempenho do fornecedor, tempo do ciclo.
            ("sup_relatorios", "Relatórios", "erp.pagina_suprimentos_relatorios"),
            ("sup_cadastros", "Cadastros", "erp.pagina_suprimentos_insumos"),
        ],
    },
    {
        "chave": "admin", "nome": "Administração", "sigla": "ADM",
        "descricao": "Plano financeiro, operadores, banco de dados e auditoria",
        "cor": "var(--roxo)",
        # EMPRESAS NÃO É ABA, é configuração. São três ou quatro empresas,
        # cadastradas uma vez — o dono viu isso em 10/09/2026: *"não tem
        # sentido ficar uma aba lá na parte superior do aplicativo só pra
        # isso"*. A tela continua a mesma; ela só passou a morar dentro de
        # Configurações, e é de lá que se chega nela.
        "abas": [
            ("config", "Configurações", "erp.pagina_config"),
            ("arquivo", "Arquivo", "erp.pagina_arquivo"),
            # Aberta a TODO operador de propósito: cada um vê a própria semana.
            # Foi decisão do dono em 10/09/2026 — mesmo dado, e deixa de ser
            # vigilância para virar retorno. Quem vê a EQUIPE precisa da ação
            # `ver_uso_da_equipe`, que mora numa rota separada.
            ("uso", "Trabalho no sistema", "erp.pagina_uso"),
        ],
    },
]

# Ações que as telas consultam para esconder botão que a pessoa não pode usar.
# Repetir a lista aqui é de propósito: `_contexto` roda em TODA página, e
# perguntar por 20 ações a cada carregamento seria desperdício.
ACOES_NA_TELA = (
                 # Pedir à contabilidade: decide se a tela mostra o botão de
                 # enviar, cobrar e conferir o retorno.
                 "pedir_contabilidade","administrar_insumos", "administrar_fornecedores", "comprar",
                 "autorizar_pedido", "solicitar_suprimento", "receber_material",
                 "liberar_solicitacao",
                 # Decide se "Receber material" oferece o atalho para lançar a
                 # nota e o boleto logo depois do recebimento.
                 "lancar",
                 "configurar",
                 "cruzar_notas", "arquivar", "receber", "emitir_nota",
                 "tratar_agenda", "aprovar",
                 # O DIÁRIO DE OBRA (08/11/2026): decide se a tela mostra o
                 # botão de gravar e fechar. Sem isto a tela abre em modo de
                 # leitura para TODO MUNDO, inclusive para o ADMIN — foi o que
                 # a primeira foto mostrou, com o formulário inteiro na tela e
                 # nenhum botão de gravar embaixo dele.
                 "lancar_diario",
                 # ⚑ VER REMUNERAÇÃO (17/11/2026): decide se a tela de Pessoal
                 # mostra as colunas de salário e diária. O corte de verdade é
                 # no servidor (`pessoal._sem_remuneracao`) — isto aqui é para
                 # a tela não desenhar coluna que vai vir vazia.
                 "ver_remuneracao",
                 # ⚑ GERIR USUÁRIOS (17/11/2026): decide se a ficha da pessoa
                 # mostra os botões de liberar e desligar o acesso dela ao ERP.
                 # Quem não tem a ação continua VENDO se a pessoa entra no
                 # sistema e com qual e-mail — o que ele não ganha é um botão
                 # que responderia 403.
                 "gerir_usuarios",
                 # ⚑ LIBERAR ACESSO AO PORTAL (18/11/2026, "libera o DP
                 # também"): decide se a tela de Pessoal mostra a fila de quem
                 # pediu acesso pelo ponto. É ação PRÓPRIA, mais estreita que
                 # `gerir_usuarios` — ver o comentário dela em `permissoes.py`.
                 "liberar_acesso_portal",
                 # ⚑ CONFIGURAR MENSAGENS (17/11/2026): o bloco "Mensagens" da
                 # ficha da pessoa mora no `erp_base.html` (é o mesmo na ficha
                 # do Pessoal e na do ponto) e oferece "Mandar um teste agora".
                 # Sem a ação aqui, o botão não apareceria em tela nenhuma
                 # além da própria tela Mensagens — inclusive para o ADMIN.
                 "configurar_mensagens",
                 # Cancelar título é ação própria desde 12/09/2026: quem lança
                 # cancela o PRÓPRIO lançamento, e a tela precisa saber disso
                 # para mostrar o botão a quem não aprova.
                 "cancelar_titulo",
                 # Encaminhar informação por WhatsApp (12/09/2026): decide se o
                 # botão "↗ Encaminhar" aparece no lançamento e no documento.
                 "encaminhar",
                 # Encadeamento: a tela só transforma obra/conta/credor/pedido
                 # em link para quem consegue abrir o destino.
                 "ver_suprimentos", "ver_pedidos_compra",
                 # Decide se a tela de Trabalho no sistema mostra a equipe ou
                 # só a própria semana de quem abriu.
                 "ver_uso_da_equipe",
                 # Acompanhamento (18/09/2026): decide se a tela mostra o botão
                 # de abrir processo, a caixa de lançar andamento e o "assumir".
                 # Sem esta linha `pode.tocar_processo` chega indefinido ao
                 # template — e indefinido é falso, então a tela abriria só de
                 # leitura para todo mundo, sem erro nenhum para denunciar.
                 "tocar_processo",
                 # Cadastro de pessoas (05/11/2026): decide quem vê o cadastro
                 # COMPLETO (endereço, RG, PIS, filiação, dependentes) e quem
                 # desliga, readmite e registra alteração salarial. Mesmo motivo
                 # da linha acima: sem ela a tela abre de leitura e ninguém
                 # descobre por quê.
                 "editar_colaboradores",
                 # A fila do DP (05/11/2026): a tela esconde Atender e Recusar
                 # de quem só pode abrir pedido.
                 "atender_dp")

# aba → módulo a que pertence
_MODULO_DA_ABA = {aba[0]: m["chave"] for m in MODULOS for aba in m["abas"]}


def _migracoes_pendentes() -> list[str]:
    """Consulta barata, usada para avisar em toda tela quando o banco está
    atrás do código — evita a tela quebrar com 'column does not exist'."""
    try:
        from app.apps.erp.core.comum.migracoes import listar_estado
        return listar_estado().get("pendentes", [])
    except Exception:
        return []


def _acao_da_aba(rota: str) -> str:
    """A ação que a TELA daquela aba exige, lida da própria declaração da rota.

    Um lugar só: se a rota muda de ação, o menu acompanha sem ninguém lembrar.
    Repetir a lista aqui criaria a segunda resposta possível para a mesma
    pergunta — e um dia as duas discordariam.
    """
    return (_REGISTRO_PERMISSOES.get(rota) or {}).get("GET", "ver_erp")


def _menu_da_pessoa(pode: dict[str, bool]) -> tuple[list[dict], dict[str, list]]:
    """Os módulos e abas que esta pessoa PODE abrir.

    Decisão do dono em 13/09/2026: *"se a pessoa está liberada apenas pra
    visualizar lançamento financeiro, ela não tem que ver nada do suprimento
    (…) ela não vai ver nada disso se eu não disponibilizar pra aquele
    perfil"*.

    Esconder não é trava — a trava continua sendo o `@permissao` de cada rota.
    É para o menu não oferecer o que vai responder 403, e para uma área que a
    pessoa não usa simplesmente não existir na tela dela.
    """
    abas_por_modulo = {}
    modulos = []
    for m in MODULOS:
        visiveis = [a for a in m["abas"] if pode.get(_acao_da_aba(a[2]), False)]
        abas_por_modulo[m["chave"]] = visiveis
        if visiveis:
            # O botão do módulo aponta para a primeira aba QUE ESTA PESSOA
            # ABRE. Apontar para a primeira da lista mandaria justamente quem
            # tem acesso parcial para uma tela que responde 403.
            modulos.append({**m, "abas": visiveis})
    return modulos, abas_por_modulo


def _acoes_das_abas() -> tuple[str, ...]:
    """As ações que decidem quais abas aparecem.

    Calculada na hora, e não uma vez na importação: o registro de permissões é
    preenchido pelos decoradores à medida que as rotas são declaradas, e neste
    ponto do arquivo ele ainda está vazio. São ~27 consultas a um dicionário —
    barato, e sem a chance de congelar uma lista errada.
    """
    return tuple(sorted({_acao_da_aba(rota)
                         for m in MODULOS for _, _, rota in m["abas"]}))


def _contexto(aba: str) -> dict:
    chave = _MODULO_DA_ABA.get(aba, "financeiro")
    modulo = next(m for m in MODULOS if m["chave"] == chave)
    # Uma leitura só para as duas perguntas: o que a tela esconde e o que o
    # menu mostra. Duas chamadas seriam duas idas ao banco em TODA página.
    pode = _pode_agora(*ACOES_NA_TELA, *_acoes_das_abas())
    modulos_visiveis, abas_por_modulo = _menu_da_pessoa(pode)
    return {"modulos": modulos_visiveis, "modulo": modulo,
            "abas": abas_por_modulo.get(chave, modulo["abas"]),
            "aba_ativa": aba,
            "usuario_nome": session.get("erp_usuario_nome", ""),
            "usuario_perfil": session.get("erp_usuario_perfil", ""),
            # O que esta pessoa pode, para a tela não oferecer botão que vai
            # responder 403. NÃO é a trava — a trava é o @permissao da rota.
            "pode": pode,
            "migracoes_pendentes": _migracoes_pendentes()}
_ABERTOS = ("EM_ANALISE", "AGUARDANDO_APROVACAO", "APROVADO", "BLOQUEADO", "PAGO_PARCIAL")


# ---------------------------------------------------------------------------
# Sessão
# ---------------------------------------------------------------------------
def _usuario_logado(s) -> Usuario | None:
    uid = session.get("erp_usuario_id")
    if not uid:
        return None
    u = s.get(Usuario, uid)
    if u is not None:
        u.permissoes_extras = _excecoes_brutas(s, uid)
        u.acoes_do_perfil = acoes_do_perfil_no_banco(s, uid)
    return u


def _exigir_saldo_de_ia():
    """O teto de IA do MÊS desta pessoa, conferido antes de gastar.

    Existe como função de uma linha porque são oito rotas que gastam IA, e a
    conferência repetida em oito lugares é a que alguém esquece na nona. Há
    varredura estrutural na suíte cobrando esta chamada em toda rota que gasta
    (`tests/test_teto_ia_por_pessoa.py`).

    Levanta `SemSaldoDeIa`, que cada rota devolve como 402 com o recado pronto.
    """
    from app.apps.erp.core.comum.ia_custo import exigir_saldo_de_ia
    with get_session() as s:
        atual = _usuario_logado(s)
        exigir_saldo_de_ia(s, atual.id if atual else None)


def _excecoes_brutas(s, usuario_id: int) -> dict[str, bool]:
    """As marcações de permissão da pessoa, por SQL direto (ação → concedida).

    SQL direto e não ORM pelo mesmo motivo do perfil: esta leitura acontece em
    toda requisição, inclusive na tela que aplica as migrações. Enquanto a
    migração 032 não tiver rodado, a tabela não existe — e aí a resposta certa
    é "nenhuma exceção", que faz valer o cargo, e não derrubar o ERP.
    """
    from sqlalchemy import text as _text
    try:
        linhas = s.execute(
            _text("SELECT acao, concedida FROM usuario_permissoes WHERE usuario_id = :i"),
            {"i": usuario_id}).all()
    except Exception:
        logger.warning("ERP/permissao: usuario_permissoes indisponível "
                       "(migração 032 pendente?) — valendo só o cargo")
        return {}
    return {acao: bool(concedida) for acao, concedida in linhas}


def _perfil_bruto(s) -> str:
    """Perfil por SQL direto, sem passar pelo ORM.

    Necessário no caminho de manutenção: se o banco ainda não tem as colunas
    que o modelo espera, carregar o objeto Usuario quebra — e justamente o
    botão que conserta o banco ficaria inutilizável (impasse circular)."""
    from sqlalchemy import text as _text
    uid = session.get("erp_usuario_id")
    if not uid:
        return ""
    linha = s.execute(_text("SELECT perfil::text FROM usuarios WHERE id = :i"),
                      {"i": uid}).first()
    return linha[0] if linha else ""


def _acesso_desativado(s) -> bool:
    """O acesso desta sessão foi desativado DEPOIS de ela ter sido aberta?

    ⚑ 17/11/2026. Até aqui `usuarios.ativo` era conferido SÓ no login: quem já
    estava com a tela aberta continuava trabalhando mesmo depois de o acesso
    ser desligado — e isso vale para o desligamento (a virada do dia, que corta
    o login de quem saiu, por decisão dele) e para o bloqueio feito à mão pelo
    DP. Agora a sessão cai no clique seguinte.

    ⚠️ A CONSULTA É A MESMA de `_perfil_bruto`, por SQL direto e sem o ORM, e
    sem coluna que dependa de migração nova: esta função roda antes de TODA
    rota, inclusive da tela que aplica as migrações. Carregar o `Usuario` aqui
    é o impasse circular que derrubou o ERP em 02/09/2026.
    """
    from sqlalchemy import text as _text
    uid = session.get("erp_usuario_id")
    if not uid:
        return False
    linha = s.execute(_text("SELECT ativo FROM usuarios WHERE id = :i"),
                      {"i": uid}).first()
    return bool(linha) and linha[0] is False


def login_obrigatorio(fn):
    @wraps(fn)
    def _wrap(*a, **kw):
        if not session.get("erp_usuario_id"):
            if request.path.startswith("/erp/api/"):
                return jsonify({"ok": False, "erro": "Sessão expirada."}), 401
            # leva junto o endereço pedido, para voltar a ele depois de entrar
            return redirect(url_for("erp.pagina_login",
                                    proximo=request.full_path.rstrip("?")))
        return fn(*a, **kw)
    return _wrap


# ---------------------------------------------------------------------------
# Autorização: o padrão é NEGAR
#
# Toda rota do ERP declara a ação que exige. O que não declara não passa —
# `_guarda_permissao` recusa endpoint ausente do registro. É a inversão do
# default: esquecer fecha a rota, em vez de deixá-la aberta a todo mundo.
#
# Rota realmente pública declara isso por escrito, com o motivo, em
# `@permissao_publica(...)`. Silêncio nunca vale como liberação.
#
# Isto trata ALÇADA ("este perfil pode esta ação?"). Escopo de objeto ("pode
# NESTE registro?") é a outra metade, e mora em permissoes.exigir_*_no_escopo.
# ---------------------------------------------------------------------------
_REGISTRO_PERMISSOES: dict[str, dict[str, str]] = {}   # endpoint -> {método: ação}
_ENDPOINTS_PUBLICOS: dict[str, str] = {}               # endpoint -> motivo

# o Flask serve o CSS/JS do blueprint por este endpoint; não é rota de negócio
_ISENTOS = {"erp.static"}

_TODOS_METODOS = ("GET", "POST", "PUT", "PATCH", "DELETE")


def permissao(acao: str | None = None, **por_metodo: str):
    """Declara a ação exigida pela rota.

    `@permissao("pagar")` vale para todos os métodos. Quando um mesmo endpoint
    lê e escreve com sensibilidades diferentes, declare por método:
    `@permissao(GET="ver_erp", POST="configurar")`.
    """
    if acao is None and not por_metodo:
        raise ValueError("Declare a ação exigida pela rota.")
    mapa = {m: acao for m in _TODOS_METODOS} if acao else {}
    mapa.update({m.upper(): a for m, a in por_metodo.items()})
    for a in set(mapa.values()):
        if a not in PERMISSOES:
            raise ValueError(f"Ação desconhecida em @permissao: {a!r}")

    def _decorar(fn):
        _REGISTRO_PERMISSOES[f"erp.{fn.__name__}"] = mapa
        fn._permissao = mapa
        return fn
    return _decorar


def permissao_publica(motivo: str):
    """Rota sem autenticação. Exige motivo escrito — para a exceção ser uma
    decisão visível na revisão, e não um descuido."""
    def _decorar(fn):
        _ENDPOINTS_PUBLICOS[f"erp.{fn.__name__}"] = motivo
        fn._permissao_publica = motivo
        return fn
    return _decorar


@bp.errorhandler(ProgrammingError)
def _banco_atrasado(e: ProgrammingError):
    """Coluna ou tabela que o código espera e o banco ainda não tem.

    Acontece quando o código novo sobe antes de alguém apertar "Aplicar
    atualizações do banco". Em vez de "Internal Server Error", diz o que é e
    quem resolve. 503: o serviço está de pé, só o banco está atrasado."""
    logger.error("ERP: banco atrasado em relação ao código (%s) em %s",
                 str(e.orig or e).splitlines()[0], request.path)
    if request.path.startswith("/erp/api/"):
        return jsonify({"ok": False, "banco_atrasado": True,
                        "erro": "O banco está desatualizado em relação ao sistema. "
                                "Um administrador precisa aplicar as atualizações "
                                "em Configurações."}), 503
    return render_template("erp_banco_atrasado.html",
                           detalhe=str(e.orig or e).splitlines()[0],
                           e_admin=(session.get("erp_usuario_perfil") == "ADMIN")), 503


@bp.errorhandler(ErroNaoEncontrado)
def _fora_do_escopo(e: ErroNaoEncontrado):
    """Fora do escopo responde como inexistente — mesma resposta, mesmo status.

    Se aqui saísse 403 "sem permissão", a diferença entre 403 e 404 viraria um
    oráculo: bastaria varrer os ids para saber quais existem.
    """
    return jsonify({"ok": False, "erro": str(e) or "Não encontrado."}), 404


@bp.errorhandler(SemSaldoDeIa)
def _sem_saldo_de_ia(e: SemSaldoDeIa):
    """A pessoa gastou o teto de IA do mês dela.

    Errorhandler e não `except` em cada rota: são oito rotas que gastam IA
    hoje, e a nona seria a esquecida. 402 ("é preciso pagar") é o status que
    existe justamente para "a sua cota acabou" — não é erro do sistema nem
    falta de permissão, e a tela precisa saber diferenciar para mostrar o
    recado certo.
    """
    logger.info("ERP/ia: teto do mês atingido em %s", request.path)
    return jsonify({"ok": False, "sem_saldo_de_ia": True, "erro": str(e)}), 402


@bp.before_request
def _abrir_contexto_ia():
    """Quem está logado "assina" as chamadas de IA desta requisição. Zerado a
    cada requisição porque as threads do gunicorn são reaproveitadas."""
    from flask import g
    from app.apps.erp.core.comum.ia_custo import iniciar_contexto_requisicao
    g.erp_ia_token = iniciar_contexto_requisicao(session.get("erp_usuario_id"))


@bp.teardown_request
def _fechar_contexto_ia(_exc=None):
    from flask import g
    from app.apps.erp.core.comum.ia_custo import encerrar_contexto_requisicao
    encerrar_contexto_requisicao(g.pop("erp_ia_token", None))


@bp.before_request
def _guarda_permissao():
    endpoint = request.endpoint or ""
    if endpoint in _ISENTOS or endpoint in _ENDPOINTS_PUBLICOS:
        return None

    mapa = _REGISTRO_PERMISSOES.get(endpoint)
    if mapa is None:
        # Rota nova que ninguém declarou. Fecha e grita: é assim que o
        # esquecimento vira erro visível em vez de brecha silenciosa.
        logger.error("ERP/permissao: endpoint %s sem declaração — acesso negado", endpoint)
        return jsonify({"ok": False, "erro": "Rota sem permissão declarada."}), 403

    acao = mapa.get(request.method)
    if acao is None:
        return jsonify({"ok": False, "erro": "Método não liberado nesta rota."}), 405

    if not session.get("erp_usuario_id"):
        return None          # login_obrigatorio responde (401 ou redireciona)

    # O perfil vem por SQL DIRETO, nunca pelo ORM. Esta função roda antes de
    # TODA rota — inclusive da tela que aplica as migrações. Se carregasse o
    # objeto Usuario e o banco estivesse uma migração atrasado (coluna nova no
    # modelo, ainda não criada), quebraria aqui, e o botão que conserta o banco
    # ficaria inalcançável: o impasse circular que derrubou o ERP em 2026-09-02.
    with get_session() as s:
        # ⚑ ACESSO DESATIVADO DERRUBA A SESSÃO ABERTA (17/11/2026) — ver
        # `_acesso_desativado`. Vem ANTES de tudo: quem teve o acesso cortado
        # não tem perfil que valha.
        if _acesso_desativado(s):
            uid = session.get("erp_usuario_id")
            session.clear()
            logger.info("ERP/permissao: sessão do usuário %s encerrada — "
                        "acesso desativado", uid)
            if request.path.startswith("/erp/api/"):
                return jsonify({"ok": False, "erro":
                                "Seu acesso foi encerrado. Procure o DP."}), 401
            return redirect(url_for("erp.pagina_login"))
        perfil = _perfil_bruto(s)
        excecoes = _excecoes_brutas(s, session["erp_usuario_id"]) if perfil else {}
        # O PERFIL CADASTRADO (migração 065) decide, quando existe. Devolve
        # None enquanto a migração não rodou ou a pessoa não foi apontada para
        # perfil nenhum — e aí vale o cargo, como antes.
        acoes = (acoes_do_perfil_no_banco(s, session["erp_usuario_id"])
                 if perfil else None)
    if not perfil:
        return None          # usuário sumiu do banco: a rota responde
    try:
        perfil_enum = PerfilUsuario(perfil)
    except ValueError:
        logger.error("ERP/permissao: perfil desconhecido %r no usuário %s",
                     perfil, session.get("erp_usuario_id"))
        return jsonify({"ok": False, "erro": "Perfil de usuário inválido."}), 403
    if not decidir(perfil_enum, acao, excecoes, acoes):
        logger.warning("ERP/permissao: %s negado ao usuário %s (%s) em %s",
                       acao, session.get("erp_usuario_id"), perfil, endpoint)
        return _recusa_de_acesso(perfil_enum)
    return None


def _e_chamada_de_api() -> bool:
    """Quem está pedindo: um programa ou uma pessoa na frente da tela?

    Endereço de API responde JSON — é o que a tela e as integrações esperam.
    TELA responde tela: até 22/09/2026 a recusa vinha como JSON cru ocupando a
    janela inteira, sem menu e sem caminho de volta, com cara de defeito do
    sistema. Quem esbarrava nisso achava que tinha quebrado alguma coisa.
    """
    caminho = request.path or ""
    if caminho.startswith("/erp/api/"):
        return True
    # busca (XHR/fetch) pede JSON explicitamente; navegador pede HTML
    aceita = (request.headers.get("Accept") or "")
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return True
    return "application/json" in aceita and "text/html" not in aceita


def _recusa_de_acesso(perfil_enum):
    """A recusa, do jeito de quem perguntou."""
    if _e_chamada_de_api():
        return jsonify({"ok": False,
                        "erro": "Seu perfil não tem permissão para esta operação."}), 403
    from app.apps.erp.core.auth.permissoes import ROTULOS
    return render_template("erp_sem_acesso.html",
                           perfil=ROTULOS.get(perfil_enum, perfil_enum.value)), 403


def _destino_seguro(bruto: str | None) -> str | None:
    """O endereço para onde voltar depois de entrar — se for daqui de dentro.

    Aceitar qualquer endereço seria abrir a porta para o golpe clássico: um
    link que passa pela tela de entrada da BWS e joga a pessoa, já convencida
    de que está no sistema, num site de fora pedindo a senha. Por isso só
    passa caminho relativo que começa em /erp/ — sem "//", sem outro domínio.
    """
    destino = (bruto or "").strip()
    if not destino.startswith("/erp/") or destino.startswith("//"):
        return None
    if "\n" in destino or "\r" in destino or "\\" in destino:
        return None
    return destino


# ---------------------------------------------------------------------------
# O NÚMERO VIRA LINK — /erp/ir/<numero>
#
# Pedido do dono em 22/09/2026: *"eu encaminho via celular o número de um
# registro, e a pessoa só clica e puf, abre o sistema"* — do mesmo jeito que
# um card do Pipefy.
#
# As telas já abriam direto num registro, mas só pelo número INTERNO do banco,
# que ninguém vê. Aqui entra o número que o sistema imprime — 000123, PC-0001,
# CRECHE01 — e sai a tela certa, já aberta nele.
#
# Quem não está logado é levado ao login e VOLTA para cá depois de entrar.
# Quem não tem a ação recebe "não encontrado", e não "sem permissão": dizer
# "existe, mas você não pode" já entrega que o número existe, e varrer números
# mapearia o sistema sem abrir um registro.
# ---------------------------------------------------------------------------
@bp.route("/erp/ir/<path:numero>")
@login_obrigatorio
@permissao("ver_erp")
def ir_para_o_numero(numero: str):
    from app.apps.erp.core.auth.permissoes import exigir_registro_no_escopo, pode
    from app.apps.erp.core.comum import atalho

    def alcanca(s, usuario, achado) -> bool:
        """DUAS PENEIRAS, e as duas são necessárias. A ação responde "este
        perfil pode este tipo de registro?"; o escopo responde "pode ESTE
        registro?". Sem a segunda, quem mandasse números em sequência
        descobriria quais existem — e o número interno de cada um — sem abrir
        registro nenhum, que é o que "fora do escopo responde 404" impede."""
        if not pode(usuario, achado["destino"].acao):
            return False
        try:
            exigir_registro_no_escopo(s, usuario, achado["destino"].chave,
                                      achado["id"])
            return True
        except (ErroNaoEncontrado, ErroPermissao):
            return False

    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            achados = [a for a in atalho.achar(s, numero)
                       if alcanca(s, usuario, a)]
    except Exception as e:
        logger.exception("ERP: falha ao procurar o número %r", numero)
        return render_template("erp_numero.html", numero=atalho.limpar(numero),
                               achados=[], erro=recado_de_falha(e)), 500

    limpo = atalho.limpar(numero)
    if not achados:
        logger.info("ERP/ir: número %r não levou a nada", limpo)
        return render_template("erp_numero.html", numero=limpo,
                               achados=[], erro=None), 404
    if len(achados) == 1:
        a = achados[0]
        return redirect(atalho.endereco(a["destino"], a["id"], a["numero"]))
    # dois registros com o mesmo número: a pessoa escolhe, em vez de o
    # sistema adivinhar e abrir o errado calado
    return render_template("erp_numero.html", numero=limpo, erro=None, achados=[
        {"rotulo": a["destino"].rotulo, "numero": a["numero"],
         "endereco": atalho.endereco(a["destino"], a["id"], a["numero"])}
        for a in achados])


@bp.route("/erp/entrar", methods=["GET", "POST"])
@permissao_publica("tela de login — porta de entrada do ERP")
def pagina_login():
    # PARA ONDE A PESSOA IA (22/09/2026). Até aqui o login jogava todo mundo na
    # tela inicial, esquecendo o endereço pedido — e um link mandado por
    # WhatsApp morria na porta: quem clicava caía no início e tinha de caçar o
    # registro na mão. Agora o destino atravessa o login.
    proximo = _destino_seguro(request.args.get("proximo") or request.form.get("proximo"))
    if request.method == "GET":
        return render_template("erp_login.html", erro=None, proximo=proximo)
    email = (request.form.get("email") or "").strip()
    senha = request.form.get("senha") or ""
    try:
        with get_session() as s:
            usuario = autenticar(s, email, senha)
            session["erp_usuario_id"] = usuario.id
            session["erp_usuario_nome"] = usuario.nome
            session["erp_usuario_perfil"] = usuario.perfil.value
        logger.info("ERP: login de %s", email)
        return redirect(proximo or url_for("erp.pagina_inicio"))
    except ErroAutenticacao as e:
        return render_template("erp_login.html", erro=str(e), proximo=proximo), 401
    except Exception as e:  # falha de banco/config
        logger.exception("ERP: falha no login")
        return render_template("erp_login.html", proximo=proximo,
                               erro=f"Não foi possível conectar ao banco: {e}"), 500


# ---------------------------------------------------------------------------
# O ERP NO CELULAR — ícone na tela inicial, sem loja de aplicativo
#
# O dono pediu "poderíamos ter um aplicativo?". A resposta honesta: aplicativo
# nativo seriam DUAS bases de código (Android e iPhone), duas lojas, e revisão
# da Apple a cada correção — para mostrar as mesmas telas que já existem.
#
# O que resolve de verdade é isto: o navegador do celular passa a poder
# INSTALAR o ERP. Vira ícone na tela inicial, abre em tela cheia sem a barra de
# endereço, e é o mesmo sistema — publicou aqui, chegou no celular na hora.
#
# As duas rotas abaixo são PÚBLICAS por obrigação do navegador: ele busca o
# manifesto e o service worker ANTES de qualquer login, e se levar 302 para a
# tela de entrar, a instalação simplesmente não é oferecida. Nenhuma das duas
# devolve dado de negócio — uma é a ficha do ícone, a outra é código.
# ---------------------------------------------------------------------------
@bp.route("/erp/manifest.webmanifest")
@permissao_publica("o navegador busca o manifesto antes do login; sem ele o "
                   "celular não oferece instalar. Não devolve dado nenhum.")
def manifesto_pwa():
    icone = lambda nome: url_for("erp.static", filename=nome)
    return jsonify({
        "name": "ERP BWS Construções",
        "short_name": "ERP BWS",
        "description": "Financeiro, obras, suprimentos e contratos da BWS.",
        "lang": "pt-BR",
        "start_url": "/erp/inicio",
        "scope": "/erp/",
        "display": "standalone",
        "orientation": "portrait-primary",
        "background_color": "#0A1B2E",
        "theme_color": "#0A1B2E",
        "icons": [
            {"src": icone("icone-192.png"), "sizes": "192x192",
             "type": "image/png", "purpose": "any"},
            {"src": icone("icone-512.png"), "sizes": "512x512",
             "type": "image/png", "purpose": "any"},
            {"src": icone("icone-512-recortavel.png"), "sizes": "512x512",
             "type": "image/png", "purpose": "maskable"},
        ],
        "shortcuts": [
            {"name": "Perguntar", "url": "/erp/perguntar"},
            {"name": "Títulos", "url": "/erp/titulos"},
            {"name": "Obras", "url": "/erp/obras"},
        ],
    })


@bp.route("/erp/sw.js")
@permissao_publica("service worker: é código, não dado, e o navegador o busca "
                   "sem sessão. Servido daqui, e não de /erp/static, para o "
                   "alcance dele ser /erp/ — de dentro de static ele só "
                   "alcançaria os arquivos estáticos e a instalação falharia.")
def service_worker():
    from flask import send_from_directory
    resp = send_from_directory(bp.static_folder, "sw.js",
                               mimetype="application/javascript")
    # Sem isto o navegador recusa o alcance /erp/ para um arquivo servido
    # deste endereço, e nada funciona — sem mensagem de erro nenhuma.
    resp.headers["Service-Worker-Allowed"] = "/erp/"
    # O service worker não pode ser o próprio a envelhecer no cache: é ele que
    # decide o que fica guardado.
    resp.headers["Cache-Control"] = "no-cache"
    return resp


@bp.route("/erp/sair")
@permissao_publica("encerrar sessao nao pode exigir sessao valida")
def sair():
    session.pop("erp_usuario_id", None)
    session.pop("erp_usuario_nome", None)
    session.pop("erp_usuario_perfil", None)
    return redirect(url_for("erp.pagina_login"))


# ---------------------------------------------------------------------------
# Páginas
# ---------------------------------------------------------------------------
@bp.route("/erp/")
@bp.route("/erp/titulos")
@login_obrigatorio
@permissao("ver_titulos")
def pagina_titulos():
    return render_template("erp_titulos.html", **_contexto("titulos"))


@bp.route("/erp/inicio")
@login_obrigatorio
@permissao("ver_erp")
def pagina_inicio():
    """Porta de entrada: escolha do módulo, e o que tem prazo."""
    # A contagem da agenda é consulta curta e NÃO sincroniza: a porta de
    # entrada é a tela mais visitada do ERP, e recalcular tudo aqui pagaria o
    # preço em toda visita. Quem sincroniza é a própria tela da agenda.
    # Se a migração 051 ainda não rodou, a porta de entrada abre do mesmo
    # jeito — ela não pode depender do que veio depois dela.
    pode = _pode_agora(*ACOES_NA_TELA, *_acoes_das_abas())
    agenda = {}
    # O aviso da agenda só aparece para quem tem a agenda. Mostrar "2
    # obrigações vencidas" a quem não pode abrir a tela entrega informação de
    # uma área que o perfil dela não libera — e ainda por cima sem ela poder
    # fazer nada a respeito.
    if pode.get("ver_agenda"):
        try:
            from app.apps.erp.core.agenda import service as svc_agenda
            with get_session() as s:
                agenda = svc_agenda.contagem(s, usuario=_usuario_logado(s))
        except Exception:
            logger.warning("ERP/agenda: contagem indisponível na tela de início "
                           "(migração 051 pendente?)")
    # A porta de entrada mostra os MÓDULOS QUE ESTA PESSOA ABRE, e não a lista
    # inteira — é aqui que ela escolhe para onde ir, e oferecer uma área que
    # responde 403 é pior do que não mostrá-la.
    modulos_visiveis, _ = _menu_da_pessoa(pode)
    return render_template("erp_inicio.html", modulos=modulos_visiveis, modulo=None,
                           abas=[], aba_ativa="", agenda=agenda,
                           usuario_nome=session.get("erp_usuario_nome", ""),
                           usuario_perfil=session.get("erp_usuario_perfil", ""),
                           # `pode` é do molde comum a todas as telas: o
                           # encadeamento lê daqui para não oferecer link que
                           # a pessoa não consegue abrir.
                           pode=pode,
                           migracoes_pendentes=_migracoes_pendentes())


@bp.route("/erp/lancar")
@login_obrigatorio
@permissao("lancar")
def pagina_lancar():
    return render_template("erp_lancar.html", **_contexto("lancar"))


@bp.route("/erp/confirmar")
@login_obrigatorio
@permissao("ver_titulos")
def pagina_confirmar():
    return render_template("erp_confirmar.html", **_contexto("confirmar"))


@bp.route("/erp/pagamentos")
@login_obrigatorio
@permissao("pagar")
def pagina_pagamentos():
    return render_template("erp_pagamentos.html", **_contexto("pagamentos"))


@bp.route("/erp/empreitas")
@login_obrigatorio
@permissao("ver_empreitas")
def pagina_empreitas():
    return render_template("erp_empreitas.html", **_contexto("empreitas"))


@bp.route("/erp/suprimentos/locacoes")
@login_obrigatorio
@permissao("ver_locacoes")
def pagina_locacoes():
    """Locação de equipamento vive em SUPRIMENTOS, e não no financeiro.

    Quem demanda a locação é a obra, e quem atende é suprimentos — que resolve
    a mesma necessidade de três formas: remanejando o que já existe, comprando
    ou locando. A obrigação de pagar continua sendo do financeiro: a parcela
    vira título lá, e os dois lados se enxergam pelo número do contrato.
    """
    return render_template("erp_locacoes.html", **_contexto("locacoes"))


@bp.route("/erp/locacoes")
@login_obrigatorio
@permissao("ver_locacoes")
def pagina_locacoes_antiga():
    """O endereço antigo, de quando Locações ficava no Financeiro. Continua
    respondendo porque há link salvo e gente com a tela nos favoritos."""
    return redirect(url_for("erp.pagina_locacoes"))


@bp.route("/erp/dc")
@login_obrigatorio
@permissao("ver_pessoal")
def pagina_dc():
    return render_template("erp_dc.html", **_contexto("dc"))


@bp.route("/erp/colaboradores")
@login_obrigatorio
@permissao("ver_pessoal")
def pagina_colaboradores():
    # `ve_pessoal` decide se a área de jogar documento aparece. Não basta poder
    # arquivar: documento de pessoa tem sigilo PESSOAL, e oferecer "arquive o
    # ASO" a quem não vai conseguir abri-lo depois é convite à confusão.
    from app.apps.erp.core.arquivo.service import sigilos_visiveis
    with get_session() as _s:
        ve_pessoal = "PESSOAL" in sigilos_visiveis(_usuario_logado(_s))
    return render_template("erp_colaboradores.html", ve_pessoal=ve_pessoal,
                           **_contexto("colaboradores"))


@bp.route("/erp/colaboradores/cadastro/<int:colaborador_id>")
@login_obrigatorio
@permissao("editar_colaboradores")
def pagina_colaborador_embutido(colaborador_id: int):
    """O FORMULÁRIO DO CADASTRO SOZINHO, para aparecer DENTRO de outra tela.

    ⚑ 17/11/2026, pedido dele: *"quando estou vendo o pessoal pelo ponto e
    coloco pra abrir o cadastro, ele remete pra página de pessoal, quando
    deveria apenas abrir o modal correspondente sem necessidade de ir pra outro
    lado."*

    É a MESMA tela de Colaboradores e o MESMO formulário — não há um segundo
    cadastro. O que muda é que ela chega sem cabeçalho, sem filtro e sem lista
    (`embutido`), já com a janela da pessoa aberta. Quem a mostra (hoje a ficha
    do ponto) a põe numa janela e fica onde está.

    Custo, porque vale dizer: aberta assim ela NÃO carrega a lista de
    colaboradores — são 3 chamadas (obras, funções e opções do cadastro) em vez
    das 5 da tela inteira, e nenhuma delas traz as mais de mil pessoas.
    """
    from app.apps.erp.core.arquivo.service import sigilos_visiveis
    from app.apps.erp.core.auth.permissoes import exigir_colaborador_no_escopo
    from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado
    with get_session() as _s:
        usuario = _usuario_logado(_s)
        # Fora do alcance responde "não encontrado" (ver o CLAUDE.md) — mas
        # como isto é PÁGINA, e não API, a resposta é em português dentro da
        # janela. O errorhandler do blueprint devolve JSON, e quem abrisse a
        # janela leria `{"erro": ...}` na tela.
        try:
            exigir_colaborador_no_escopo(_s, usuario, colaborador_id)
        except ErroNaoEncontrado:
            return ("<!doctype html><meta charset=utf-8>"
                    "<div style=\"font:15px system-ui; padding:24px\">"
                    "Esta pessoa não está entre as que você acompanha.</div>"), 404
        ve_pessoal = "PESSOAL" in sigilos_visiveis(usuario)
    return render_template("erp_colaboradores.html", ve_pessoal=ve_pessoal,
                           embutido=True, abrir_cadastro=colaborador_id,
                           **_contexto("colaboradores"))


@bp.route("/erp/cargos")
@login_obrigatorio
@permissao("editar_colaboradores")
def pagina_cargos():
    """A tabela de cargos, convenções e pisos.

    É `editar_colaboradores` e não `ver_pessoal` porque a tela mostra o PISO
    SALARIAL de todos os cargos — a tabela de remuneração da empresa. Quem
    lança despesa de colaborador não precisa dela.
    """
    # `_contexto("config")`: desde 05/11/2026 esta tela pertence a
    # Configurações (o dono: *"isso aqui é para estar dentro de uma espécie de
    # configurações"*), e é a aba de Configurações que tem de ficar acesa lá
    # em cima — mesmo arranjo de Empresas.
    return render_template("erp_cargos.html", sub_aba="cargos",
                           **_contexto("config"))


@bp.route("/erp/conciliacao")
@login_obrigatorio
@permissao("conciliar")
def pagina_conciliacao():
    return render_template("erp_conciliacao.html", **_contexto("conciliacao"))


@bp.route("/erp/arquivo")
@login_obrigatorio
@permissao("ver_arquivo")
def pagina_arquivo():
    return render_template("erp_arquivo.html", **_contexto("arquivo"))


@bp.route("/erp/notas")
@login_obrigatorio
@permissao("ver_notas")
def pagina_notas():
    return render_template("erp_notas.html", **_contexto("notas"))


@bp.route("/erp/contratos")
@login_obrigatorio
@permissao("ver_contratos")
def pagina_contratos():
    """O quadro financeiro do contrato — medido, faturado, recebido."""
    return render_template("erp_contratos.html", **_contexto("contratos"))


@bp.route("/erp/acompanhamento")
@login_obrigatorio
@permissao("ver_acompanhamento")
def pagina_acompanhamento():
    """Os processos que correm fora da BWS: aditivo, licença, protocolo."""
    return render_template("erp_acompanhamento.html", **_contexto("acompanhamento"))


@bp.route("/erp/agenda")
@login_obrigatorio
@permissao("ver_agenda")
def pagina_agenda():
    """O calendário de obrigações — reajuste, certidão, locação e contrato."""
    return render_template("erp_agenda.html", **_contexto("agenda"))


@bp.route("/erp/notas-emitidas")
@login_obrigatorio
@permissao("ver_notas_emitidas")
def pagina_notas_emitidas():
    """As notas que a BWS emitiu — e o relatório que a contabilidade pede."""
    return render_template("erp_notas_emitidas.html", **_contexto("notas_emitidas"))


@bp.route("/erp/receber")
@login_obrigatorio
@permissao("receber")
def pagina_receber():
    return render_template("erp_receber.html", **_contexto("receber"))


@bp.route("/erp/obras")
@login_obrigatorio
@permissao("ver_obras")
def pagina_obras():
    # Os modelos de diário vêm da REGRA, não da tela: a lista está em
    # core/diario.py e é a mesma que o papel usa. Repetida aqui, um modelo
    # novo apareceria na impressão e não no cadastro.
    from app.apps.erp.core.diario import MODELOS as MODELOS_DE_DIARIO
    return render_template(
        "erp_obras.html",
        modelos_diario=[{"valor": k, **v} for k, v in MODELOS_DE_DIARIO.items()],
        **_contexto("obras"))


# ---------------------------------------------------------------------------
# Suprimentos — fase 1: os cadastros. A especificação está em SUPRIMENTOS.md.
# ---------------------------------------------------------------------------
@bp.route("/erp/api/suprimentos/cadastros")
@login_obrigatorio
@permissao("ver_suprimentos")
def api_suprimentos_cadastros():
    """Tudo que a tela de cadastros precisa, numa consulta só."""
    from sqlalchemy import select
    from app.apps.erp.core.suprimentos.pagamento import descrever
    from app.apps.erp.db.models.cadastros import (
        CondicaoPagamento, Fornecedor, Insumo, InsumoCategoria, Obra, UnidadeCompra,
    )
    with get_session() as s:
        unidades = s.scalars(select(UnidadeCompra).order_by(UnidadeCompra.ordem)).all()
        condicoes = s.scalars(select(CondicaoPagamento)
                              .order_by(CondicaoPagamento.ordem)).all()
        categorias = s.scalars(select(InsumoCategoria)
                               .order_by(InsumoCategoria.nome)).all()
        return jsonify({
            "ok": True,
            "unidades": [{"codigo": u.codigo, "descricao": u.descricao,
                          "ativo": u.ativo} for u in unidades],
            "condicoes": [{"id": c.id, "nome": c.nome,
                           "entrada_percentual": float(c.entrada_percentual or 0),
                           "dias": list(c.dias or []),
                           "em_palavras": descrever(c.entrada_percentual, c.dias),
                           "ativo": c.ativo} for c in condicoes],
            "categorias_insumo": [{"id": c.id, "codigo": c.codigo, "nome": c.nome}
                                  for c in categorias],
            # Obras e insumos vêm por aqui, e não pela tela de Configurações:
            # quem pede material não tem — nem deve ter — acesso àquela tela.
            "obras": [{"id": o.id, "codigo": o.codigo, "nome": o.nome}
                      for o in s.scalars(select(Obra).order_by(Obra.codigo)).all()
                      if getattr(o, "status", None) != "ENCERRADA"],
            "fornecedores": [{"id": f.id, "nome": f.razao_social,
                              "porte": f.porte.value if f.porte else None,
                              "regioes": list(f.regioes_atuacao or [])}
                             for f in s.scalars(select(Fornecedor)
                                                .order_by(Fornecedor.razao_social)).all()
                             if f.ativo is not False and f.e_fornecedor is not False],
            "insumos": [{"id": i.id, "codigo": i.codigo, "descricao": i.descricao,
                         "unidade": i.unidade}
                        for i in s.scalars(select(Insumo).order_by(Insumo.descricao)).all()
                        if i.ativo is not False],
        })


@bp.route("/erp/api/suprimentos/condicoes", methods=["POST"])
@login_obrigatorio
@permissao("administrar_insumos")
def api_condicao_pagamento():
    """Cadastra uma condição de pagamento como REGRA: quanto entra na hora e em
    quantos dias vencem as demais. Arranjo novo é uma linha, não código novo."""
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.core.suprimentos.pagamento import gerar_parcelas
    from app.apps.erp.db.models.cadastros import CondicaoPagamento
    from datetime import date as _date
    d = request.get_json(silent=True) or {}
    nome = (d.get("nome") or "").strip()
    if not nome:
        return jsonify({"ok": False, "erro": "Dê um nome à condição."}), 400
    try:
        entrada = Decimal(str(d.get("entrada_percentual") or 0))
        dias = sorted({int(x) for x in (d.get("dias") or [])})
    except (ValueError, TypeError, InvalidOperation):
        return jsonify({"ok": False, "erro": "Entrada e prazos têm de ser números."}), 400
    try:
        # Prova a regra antes de gravar: condição que não gera parcela nenhuma
        # só apareceria como defeito no primeiro pedido que a usasse.
        gerar_parcelas("1000.00", _date.today(), entrada, dias)
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            c = CondicaoPagamento(nome=nome, entrada_percentual=entrada, dias=dias,
                                  ordem=int(d.get("ordem") or 0))
            s.add(c)
            s.flush()
            registrar_evento(s, "condicao_pagamento", c.id, "CRIADA",
                             {"nome": nome, "entrada": str(entrada), "dias": dias},
                             atual.id if atual else None)
            s.commit()
            return jsonify({"ok": True, "id": c.id})
    except Exception as e:
        logger.exception("ERP: falha ao cadastrar condição de pagamento")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# Suprimentos — cadastros, em quatro telas em vez de uma
#
# A primeira versão empilhava carga de planilha, condição de pagamento, pedido
# de cadastro de insumo, unidades e categorias numa página só — e não havia
# como criar uma categoria de insumo, o que travava o módulo inteiro (sem
# categoria não se cadastra insumo, sem insumo não se pede material).
#
# Agora cada coisa tem o seu lugar: INSUMOS e FORNECEDORES são telas de
# gestão, com filtro à esquerda, busca e edição em linha; CONFIGURAÇÕES é o
# que o administrador mexe uma vez; IMPORTAÇÕES é a carga inicial.
# ---------------------------------------------------------------------------
def _pode_agora(*acoes: str) -> dict[str, bool]:
    """As ações que a pessoa logada tem AGORA, para a tela esconder o que ela
    não pode fazer.

    Não é a trava — a trava é o @permissao de cada rota. É só para não
    oferecer um botão que vai responder 403. Lê perfil e marcações por SQL
    direto, pelo mesmo motivo do resto da guarda.
    """
    from app.apps.erp.core.auth.permissoes import decidir
    try:
        with get_session() as s:
            perfil = _perfil_bruto(s)
            excecoes = _excecoes_brutas(s, session["erp_usuario_id"]) if perfil else {}
            do_perfil = acoes_do_perfil_no_banco(s, session["erp_usuario_id"])
        alvo = PerfilUsuario(perfil)
    except Exception:
        return {a: False for a in acoes}
    return {a: decidir(alvo, a, excecoes, do_perfil) for a in acoes}


def _contexto_cadastros(sub: str) -> dict:
    ctx = _contexto("sup_cadastros")
    ctx["sub_aba"] = sub
    return ctx


@bp.route("/erp/suprimentos/planejar")
@login_obrigatorio
@permissao("comprar")
def pagina_suprimentos_planejar():
    """O disparo automático: o sistema monta, o comprador confere e dispara."""
    return render_template("erp_suprimentos_planejar.html",
                           **_contexto("sup_planejar"))


@bp.route("/erp/suprimentos/insumos")
@login_obrigatorio
@permissao("ver_suprimentos")
def pagina_suprimentos_insumos():
    return render_template("erp_suprimentos_insumos.html",
                           **_contexto_cadastros("insumos"))


@bp.route("/erp/suprimentos/fornecedores")
@login_obrigatorio
@permissao("ver_suprimentos")
def pagina_suprimentos_fornecedores():
    return render_template("erp_suprimentos_fornecedores.html",
                           **_contexto_cadastros("fornecedores"))


@bp.route("/erp/suprimentos/configuracoes")
@login_obrigatorio
@permissao("administrar_insumos")
def pagina_suprimentos_configuracoes():
    return render_template("erp_suprimentos_config.html",
                           **_contexto_cadastros("config"))


@bp.route("/erp/suprimentos/importacoes")
@login_obrigatorio
@permissao("administrar_insumos")
def pagina_suprimentos_importacoes():
    return render_template("erp_suprimentos_importar.html",
                           **_contexto_cadastros("importar"))


@bp.route("/erp/suprimentos/cadastros")
@login_obrigatorio
@permissao("ver_suprimentos")
def pagina_suprimentos_cadastros():
    """Endereço antigo da tela única. Continua funcionando e leva a Insumos."""
    return redirect(url_for("erp.pagina_suprimentos_insumos"))


# ---------------------------------------------------------------------------
# Suprimentos — API dos cadastros
# ---------------------------------------------------------------------------
@bp.route("/erp/api/suprimentos/insumos", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_suprimentos", POST="administrar_insumos")
def api_suprimentos_insumos():
    """GET: a tela de gestão inteira (lista + filtros + indicadores).
    POST: cadastro direto, por quem administra Suprimentos."""
    from app.apps.erp.core.suprimentos import cadastro as svc
    try:
        with get_session() as s:
            if request.method == "GET":
                return jsonify({"ok": True, **svc.gerenciar_insumos(s)})
            atual = _usuario_logado(s)
            insumo = svc.criar_insumo(s, request.get_json(silent=True) or {}, atual)
            s.commit()
            return jsonify({"ok": True, "id": insumo.id, "codigo": insumo.codigo})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP/suprimentos: falha no cadastro de insumo")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/insumos/alteracoes")
@login_obrigatorio
@permissao("administrar_insumos")
def api_suprimentos_insumos_alteracoes():
    """As últimas correções feitas em insumos, com o que era antes.

    Existe porque a trilha sempre guardou isso e nenhuma tela mostrava — e foi
    exatamente disso que o dono precisou quando trocou a conta do plano de um
    insumo sem querer e não sabia qual tinha sido.
    """
    from app.apps.erp.core.suprimentos import cadastro as svc
    with get_session() as s:
        return jsonify({"ok": True, "alteracoes": svc.alteracoes_de_insumo(s)})


@bp.route("/erp/api/suprimentos/insumos/<int:insumo_id>", methods=["PATCH"])
@login_obrigatorio
@permissao("administrar_insumos")
def api_suprimentos_insumo_editar(insumo_id: int):
    """Correção de uma célula, como numa planilha: só o que veio muda."""
    from app.apps.erp.core.suprimentos import cadastro as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            insumo = svc.editar_insumo(s, insumo_id,
                                       request.get_json(silent=True) or {}, atual)
            s.commit()
            return jsonify({"ok": True, "id": insumo.id})
    except ErroNaoEncontrado as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao editar insumo")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/categorias-insumo", methods=["POST"])
@login_obrigatorio
@permissao("administrar_insumos")
def api_suprimentos_categoria_criar():
    """A categoria que faltava. Sem ela não havia insumo, e sem insumo não
    havia solicitação, cotação nem pedido."""
    from app.apps.erp.core.suprimentos import cadastro as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            c = svc.criar_categoria(s, request.get_json(silent=True) or {}, atual)
            s.commit()
            return jsonify({"ok": True, "id": c.id, "codigo": c.codigo})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao criar categoria de insumo")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/categorias-insumo/<int:categoria_id>",
          methods=["PATCH"])
@login_obrigatorio
@permissao("administrar_insumos")
def api_suprimentos_categoria_editar(categoria_id: int):
    from app.apps.erp.core.suprimentos import cadastro as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            c = svc.editar_categoria(s, categoria_id,
                                     request.get_json(silent=True) or {}, atual)
            s.commit()
            return jsonify({"ok": True, "id": c.id})
    except ErroNaoEncontrado as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao editar categoria de insumo")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/unidades", methods=["POST"])
@login_obrigatorio
@permissao("administrar_insumos")
def api_suprimentos_unidade_criar():
    from app.apps.erp.core.suprimentos import cadastro as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            u = svc.criar_unidade(s, request.get_json(silent=True) or {}, atual)
            s.commit()
            return jsonify({"ok": True, "codigo": u.codigo})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao criar unidade de compra")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/unidades/<codigo>", methods=["PATCH"])
@login_obrigatorio
@permissao("administrar_insumos")
def api_suprimentos_unidade_editar(codigo: str):
    from app.apps.erp.core.suprimentos import cadastro as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            u = svc.editar_unidade(s, codigo,
                                   request.get_json(silent=True) or {}, atual)
            s.commit()
            return jsonify({"ok": True, "codigo": u.codigo})
    except ErroNaoEncontrado as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao editar unidade de compra")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/condicoes/<int:condicao_id>", methods=["PATCH"])
@login_obrigatorio
@permissao("administrar_insumos")
def api_suprimentos_condicao_editar(condicao_id: int):
    """Só liga e desliga. A regra (entrada e prazos) não se edita: pedido
    antigo aponta para ela, e mudar a regra reescreveria o passado."""
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.db.models.cadastros import CondicaoPagamento
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            c = s.get(CondicaoPagamento, condicao_id, with_for_update=True,
                      populate_existing=True)
            if c is None:
                return jsonify({"ok": False,
                                "erro": "Condição de pagamento não encontrada."}), 404
            if "ativo" in d:
                c.ativo = bool(d.get("ativo"))
            if "ordem" in d:
                c.ordem = int(d.get("ordem") or 0)
            registrar_evento(s, "condicao_pagamento", c.id, "EDITADA",
                             {"ativo": c.ativo, "ordem": c.ordem},
                             atual.id if atual else None)
            s.commit()
            return jsonify({"ok": True, "id": c.id})
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao editar condição de pagamento")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/fornecedores", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_suprimentos", POST="administrar_fornecedores")
def api_suprimentos_fornecedores():
    """GET: a tela de gestão de fornecedores. POST: cadastro novo."""
    from app.apps.erp.core.suprimentos import fornecedores as svc
    try:
        with get_session() as s:
            if request.method == "GET":
                return jsonify({"ok": True, **svc.gerenciar(s)})
            atual = _usuario_logado(s)
            f = svc.criar(s, request.get_json(silent=True) or {}, atual)
            s.commit()
            return jsonify({"ok": True, "id": f.id})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP/suprimentos: falha no cadastro de fornecedor")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/fornecedores/<int:fornecedor_id>",
          methods=["PATCH"])
@login_obrigatorio
@permissao("administrar_fornecedores")
def api_suprimentos_fornecedor_editar(fornecedor_id: int):
    from app.apps.erp.core.suprimentos import fornecedores as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            f = svc.editar(s, fornecedor_id,
                           request.get_json(silent=True) or {}, atual)
            s.commit()
            return jsonify({"ok": True, "id": f.id})
    except ErroNaoEncontrado as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao editar fornecedor")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/fornecedores/<int:fornecedor_id>/contatos",
          methods=["POST"])
@login_obrigatorio
@permissao("administrar_fornecedores")
def api_suprimentos_fornecedor_contato(fornecedor_id: int):
    from app.apps.erp.core.suprimentos import fornecedores as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            c = svc.acrescentar_contato(s, fornecedor_id,
                                        request.get_json(silent=True) or {}, atual)
            s.commit()
            return jsonify({"ok": True, "id": c.id})
    except ErroNaoEncontrado as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao acrescentar contato")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/fornecedores/contatos/<int:contato_id>",
          methods=["POST"])
@login_obrigatorio
@permissao("administrar_fornecedores")
def api_suprimentos_fornecedor_contato_editar(contato_id: int):
    """Corrigir o vendedor — em especial a OBSERVAÇÃO, que é descoberta com o
    tempo e não no cadastro ("atende o interior", "só linha elétrica")."""
    from app.apps.erp.core.suprimentos import fornecedores as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            svc.editar_contato(s, contato_id,
                               request.get_json(silent=True) or {}, atual)
            s.commit()
            return jsonify({"ok": True})
    except ErroNaoEncontrado as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao editar contato")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/planejamento")
@login_obrigatorio
@permissao("comprar")
def api_suprimentos_planejamento():
    """O que o sistema PROPÕE cotar hoje — agrupado, priorizado e com quem vende.

    Não dispara nada, e é exatamente essa a linha: o sistema faz o trabalho
    braçal (varrer o pendente, agrupar, achar quem vende) e a decisão continua
    com gente, na tela, antes de qualquer e-mail sair.
    """
    from app.apps.erp.core.auth.permissoes import obras_de_registro_sem_autor
    from app.apps.erp.core.suprimentos import planejamento
    with get_session() as s:
        u = _usuario_logado(s)
        return jsonify({"ok": True, **planejamento.planejar(
            s, obras_permitidas=obras_de_registro_sem_autor(s, u))})


@bp.route("/erp/api/suprimentos/planejamento", methods=["POST"])
@login_obrigatorio
@permissao("comprar")
def api_suprimentos_planejamento_criar():
    """Transforma o plano CONFERIDO em cotações abertas.

    Cria as cotações e põe os fornecedores no mapa — e para por aí. O e-mail
    continua saindo por um clique na tela de Cotações: entre "montei para você"
    e "mandei sozinho" existe uma diferença que o dono há de querer manter.
    """
    from app.apps.erp.core.suprimentos import planejamento
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            u = _usuario_logado(s)
            r = planejamento.criar_cotacoes(s, d.get("blocos") or [], u)
            s.commit()
        return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/suprimentos/cobranca")
@login_obrigatorio
@permissao("comprar")
def api_suprimentos_cobranca():
    """O que o sistema ACHA que precisa ser cobrado — e nada mais que isso.

    Ele sabe que o e-mail saiu e que nenhum preço foi lançado. Não sabe se o
    fornecedor respondeu: pode ter respondido no WhatsApp. É por isso que a
    resposta traz o PORQUÊ de cada linha e dois caminhos de saída.
    """
    from app.apps.erp.core.suprimentos import cobranca
    with get_session() as s:
        return jsonify({"ok": True, **cobranca.sugerir(s)})


@bp.route("/erp/api/suprimentos/cobranca", methods=["POST"])
@login_obrigatorio
@permissao("comprar")
def api_suprimentos_cobrar():
    """Manda o lembrete para os fornecedores MARCADOS. Quem aperta é gente."""
    from app.apps.erp.core.suprimentos import cobranca
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            u = _usuario_logado(s)
            r = cobranca.cobrar(s, d.get("colunas") or [], u,
                                observacao=d.get("observacao") or "")
            s.commit()
        return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/suprimentos/cotacoes/fornecedores/<int:coluna_id>/resposta",
          methods=["POST"])
@login_obrigatorio
@permissao("comprar")
def api_cotacao_marcar_resposta(coluna_id: int):
    """"Ele já respondeu, foi por fora" — ou "esse não vai cotar".

    As duas tiram o fornecedor da cobrança, e são coisas diferentes na conta do
    histórico: quem avisa que não vai cotar respondeu; quem some, não.
    """
    from app.apps.erp.core.suprimentos import cobranca
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            u = _usuario_logado(s)
            if d.get("sem_interesse"):
                r = cobranca.marcar_sem_interesse(
                    s, coluna_id, motivo=d.get("motivo") or "", usuario=u)
            else:
                r = cobranca.marcar_resposta(
                    s, coluna_id, canal=d.get("canal") or "",
                    quem=d.get("quem") or "", usuario=u)
            s.commit()
        return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise


@bp.route("/erp/api/suprimentos/fornecedores/padronizar-regioes",
          methods=["POST"])
@login_obrigatorio
@permissao("administrar_fornecedores")
def api_suprimentos_padronizar_regioes():
    """Traduz o texto livre de "Região de Atuação" para algo que cruza com a
    obra. Com `?simular=1`, só relata.

    Roda na hora (não é fila): são 1.700 cadastros e nenhuma ida à rede.
    """
    from app.apps.erp.core.suprimentos import fornecedores as svc
    simular = str(request.args.get("simular") or "").strip() in ("1", "true", "sim")
    with get_session() as s:
        atual = _usuario_logado(s)
        r = svc.padronizar_regioes(s, atual, simular=simular)
        if simular:
            s.rollback()
        else:
            s.commit()
    return jsonify({"ok": True, **r})


@bp.route("/erp/api/suprimentos/fornecedores/<int:fornecedor_id>/regiao",
          methods=["POST"])
@login_obrigatorio
@permissao("administrar_fornecedores")
def api_suprimentos_definir_regiao(fornecedor_id: int):
    """Grava à mão até onde este fornecedor vende."""
    from app.apps.erp.core.suprimentos import fornecedores as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            r = svc.definir_regiao(s, fornecedor_id,
                                   request.get_json(silent=True) or {}, atual)
            s.commit()
        return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise


@bp.route("/erp/api/suprimentos/fornecedores/consulta-cnpj")
@login_obrigatorio
@permissao("administrar_fornecedores")
def api_suprimentos_consulta_cnpj():
    """O que a Receita sabe sobre este CNPJ — para o formulário preencher.

    Não grava nada, e também responde se o documento JÁ está cadastrado: é o
    que evita a pessoa digitar tudo para o banco recusar no fim.
    """
    from app.apps.erp.core.suprimentos import fornecedores as svc
    try:
        with get_session() as s:
            return jsonify({"ok": True, **svc.consultar_para_cadastro(
                s, request.args.get("cnpj", ""))})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/suprimentos/fornecedores/<int:fornecedor_id>/nome-oficial",
          methods=["POST"])
@login_obrigatorio
@permissao("administrar_fornecedores")
def api_suprimentos_adotar_nome_oficial(fornecedor_id: int):
    """Troca a razão social pela da Receita, guardando a antiga como fantasia."""
    from app.apps.erp.core.suprimentos import fornecedores as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            r = svc.adotar_nome_oficial(s, fornecedor_id, atual)
            s.commit()
        return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise


@bp.route("/erp/api/suprimentos/fornecedores/<int:fornecedor_id>",
          methods=["DELETE"])
@login_obrigatorio
@permissao("administrar_fornecedores")
def api_suprimentos_fornecedor_apagar(fornecedor_id: int):
    """Apaga o fornecedor — só quando ele nunca foi usado para nada.

    A recusa não é um "não pode" seco: diz ONDE ele aparece, para a pessoa
    entender por que o botão não serve e o que fazer em vez disso.
    """
    from app.apps.erp.core.suprimentos import fornecedores as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            r = svc.apagar(s, fornecedor_id, atual)
            s.commit()
        return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise


@bp.route("/erp/api/suprimentos/fornecedores/<int:fornecedor_id>/uso")
@login_obrigatorio
@permissao("administrar_fornecedores")
def api_suprimentos_fornecedor_uso(fornecedor_id: int):
    """Onde este fornecedor aparece. A tela pergunta ANTES de oferecer o
    apagamento, para não mostrar um botão que vai recusar."""
    from app.apps.erp.core.suprimentos import fornecedores as svc
    with get_session() as s:
        return jsonify({"ok": True, "usos": svc.uso_do_fornecedor(s, fornecedor_id)})


@bp.route("/erp/api/suprimentos/fornecedores/equalizar", methods=["POST"])
@login_obrigatorio
@permissao("administrar_fornecedores")
def api_suprimentos_equalizar():
    """Põe na fila a consulta à Receita para acertar o cadastro.

    Vai para a FILA por aritmética: a consulta pública aceita 3 CNPJs por
    minuto e são ~1.700 fornecedores. Um botão que trava a tela por dez horas
    não é um botão.
    """
    from app.apps.erp.core.comum import tarefas
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            u = _usuario_logado(s)
            t = tarefas.enfileirar_unico(
                s, "equalizar_fornecedores",
                {"limite": int(d.get("limite") or 120),
                 "todos": bool(d.get("todos"))},
                usuario=u, rotulo="Acertar o cadastro pela Receita")
            s.commit()
            # `enfileirar_unico` devolve None quando já havia um igual na fila:
            # a tela precisa saber a diferença entre "pus na fila" e "já
            # estava rodando", senão a pessoa aperta cinco vezes.
            return jsonify({"ok": True, "tarefa_id": t.id if t else None,
                            "ja_estava": t is None})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/suprimentos/fornecedores/contatos/<int:contato_id>",
          methods=["DELETE"])
@login_obrigatorio
@permissao("administrar_fornecedores")
def api_suprimentos_fornecedor_contato_remover(contato_id: int):
    from app.apps.erp.core.suprimentos import fornecedores as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            svc.remover_contato(s, contato_id, atual)
            s.commit()
            return jsonify({"ok": True})
    except ErroNaoEncontrado as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao remover contato")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/exemplo", methods=["GET", "POST", "DELETE"])
@login_obrigatorio
@permissao("administrar_insumos")
def api_suprimentos_exemplo():
    """Dados fictícios para simular o fluxo, e a remoção exata deles.

    GET diz se estão no sistema; POST traz; DELETE remove só o que foi trazido
    — e recusa inteiro se algo já tiver sido usado de verdade.
    """
    from app.apps.erp.core.suprimentos import exemplo as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if request.method == "GET":
                return jsonify({"ok": True, **svc.situacao(s)})
            if request.method == "POST":
                rel = svc.criar(s, atual)
                s.commit()
                quantos = " · ".join(
                    f"{n} {svc.ROTULOS.get(k, k)}" for k, n in rel["marcas"].items() if n)
                recado = f"Dados de exemplo criados: {quantos}."
                if rel["avisos"]:
                    recado += " Observações: " + "; ".join(rel["avisos"]) + "."
                return jsonify({"ok": True, "mensagem": recado, "relatorio": rel})
            rel = svc.remover(s, atual)
            s.commit()
            quantos = " · ".join(
                f"{n} {svc.ROTULOS.get(k, k)}" for k, n in rel["removidos"].items() if n)
            return jsonify({"ok": True,
                            "mensagem": f"Dados de exemplo removidos: {quantos or 'nada restava'}.",
                            "relatorio": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP/suprimentos: falha nos dados de exemplo")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/suprimentos")
@login_obrigatorio
@permissao("ver_suprimentos")
def pagina_suprimentos():
    return render_template("erp_suprimentos.html", **_contexto("sup_solicitacoes"))


@bp.route("/erp/api/suprimentos/solicitacoes", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_suprimentos", POST="solicitar_suprimento")
def api_suprimento_solicitacoes():
    """Os itens que a pessoa pode ver, e o registro de um pedido novo."""
    from app.apps.erp.core.suprimentos import solicitacao as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if request.method == "GET":
                itens = svc.listar_itens(
                    s, atual, status=request.args.get("status"),
                    obra_id=request.args.get("obra_id"),
                    busca=(request.args.get("busca") or "").strip())
                return jsonify({"ok": True, "itens": itens})
            sol = svc.criar(s, request.get_json(silent=True) or {}, atual)
            numero = sol.numero
            s.commit()
            return jsonify({"ok": True, "numero": numero})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP/suprimentos: falha na solicitação")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# --- Cotação e mapa ---------------------------------------------------------
# Tudo que MOSTRA PREÇO exige a ação "comprar": pela decisão do dono, quem pede
# material na obra acompanha a entrega, mas não vê preço nem fornecedor.
@bp.route("/erp/suprimentos/cotacoes")
@login_obrigatorio
@permissao("comprar")
def pagina_suprimentos_cotacoes():
    return render_template("erp_suprimentos_cotacoes.html", **_contexto("sup_cotacoes"))


@bp.route("/erp/suprimentos/precos")
@login_obrigatorio
@permissao("comprar")
def pagina_suprimentos_precos():
    return render_template("erp_suprimentos_precos.html", **_contexto("sup_precos"))


@bp.route("/erp/api/suprimentos/cotacoes", methods=["GET", "POST"])
@login_obrigatorio
@permissao("comprar")
def api_cotacoes():
    from sqlalchemy import select
    from app.apps.erp.core.suprimentos import cotacao as svc
    from app.apps.erp.db.models.cadastros import Cotacao
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if request.method == "GET":
                cotacoes = sorted(s.scalars(select(Cotacao)).all(),
                                  key=lambda c: c.id or 0, reverse=True)
                return jsonify({"ok": True, "cotacoes": [
                    {"id": c.id, "numero": c.numero, "titulo": c.titulo,
                     "status": c.status.value if c.status else "ABERTA",
                     "criado_em": c.criado_em.isoformat() if c.criado_em else None}
                    for c in cotacoes]})
            cot = svc.criar(s, request.get_json(silent=True) or {}, atual)
            saida = {"ok": True, "id": cot.id, "numero": cot.numero}
            s.commit()
            return jsonify(saida)
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP/suprimentos: falha na cotação")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/cotacoes/<int:cotacao_id>/mapa")
@login_obrigatorio
@permissao("comprar")
def api_cotacao_mapa(cotacao_id: int):
    from app.apps.erp.core.suprimentos import cotacao as svc
    with get_session() as s:
        return jsonify({"ok": True, "mapa": svc.montar_mapa(s, cotacao_id)})


@bp.route("/erp/api/suprimentos/cotacoes/<int:cotacao_id>/relatorio")
@login_obrigatorio
@permissao("comprar")
def api_cotacao_relatorio(cotacao_id: int):
    """Um dos quatro relatórios do mapa — os mesmos das abas R2 a R5 da
    planilha. `tipo` diz qual; `fornecedor` escolhe a coluna no de um só."""
    from app.apps.erp.core.suprimentos import cotacao as svc
    escolhido = request.args.get("fornecedor")
    try:
        with get_session() as s:
            return jsonify({"ok": True, "relatorio": svc.relatorio(
                s, cotacao_id, request.args.get("tipo") or "POR_ITEM",
                int(escolhido) if escolhido else None)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/suprimentos/cotacoes/<int:cotacao_id>/fornecedores",
          methods=["POST"])
@login_obrigatorio
@permissao("comprar")
def api_cotacao_fornecedor(cotacao_id: int):
    from app.apps.erp.core.suprimentos import cotacao as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            coluna = svc.adicionar_fornecedor(s, cotacao_id,
                                              request.get_json(silent=True) or {}, atual)
            saida = {"ok": True, "id": coluna.id}
            s.commit()
            return jsonify(saida)
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao acrescentar fornecedor ao mapa")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/cotacoes/<int:cotacao_id>/precos", methods=["POST"])
@login_obrigatorio
@permissao("comprar")
def api_cotacao_precos(cotacao_id: int):
    """Lança vários preços de uma vez — é assim que se preenche uma coluna
    inteira depois de ler a proposta do fornecedor."""
    from app.apps.erp.core.suprimentos import cotacao as svc
    d = request.get_json(silent=True) or {}
    lancamentos = d.get("precos") or []
    if not lancamentos:
        return jsonify({"ok": False, "erro": "Nenhum preço informado."}), 400
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            gravados, recusados = 0, []
            for linha in lancamentos:
                try:
                    svc.lancar_preco(s, int(linha["cotacao_fornecedor_id"]),
                                     int(linha["cotacao_item_id"]), linha.get("preco"),
                                     atual, origem=(linha.get("origem") or "DIGITADO"),
                                     observacao=linha.get("observacao") or "",
                                     herdado_de=linha.get("herdado_de"))
                    gravados += 1
                except ErroValidacao as e:
                    recusados.append({"item": linha.get("cotacao_item_id"),
                                      "motivo": str(e)})
            s.commit()
            return jsonify({"ok": True, "gravados": gravados, "recusados": recusados})
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao lançar preços")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/cotacoes/fornecedores/<int:coluna_id>/condicoes",
          methods=["POST"])
@login_obrigatorio
@permissao("comprar")
def api_cotacao_condicoes(coluna_id: int):
    """Grava pagamento, frete, desconto, entrega, prazo e validade da coluna.

    É o destino do que a leitura da proposta sugeriu — depois de alguém olhar.
    """
    from app.apps.erp.core.suprimentos import cotacao as svc
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            svc.atualizar_condicoes(s, coluna_id, d, atual)
            s.commit()
        return jsonify({"ok": True, "recado": "Condições gravadas."})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao gravar condições")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/cotacoes/fornecedores/<int:coluna_id>/proposta",
          methods=["POST"])
@login_obrigatorio
@permissao("comprar")
def api_ler_proposta(coluna_id: int):
    """Lê a proposta do fornecedor (arquivo ou texto colado) e devolve os
    preços casados com as linhas do mapa. Não grava preço: devolve sugestão."""
    from app.apps.erp.core.suprimentos import proposta
    arquivo = request.files.get("arquivo")
    texto = (request.form.get("texto") if arquivo is not None
             else (request.get_json(silent=True) or {}).get("texto")) or ""
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            leitura = proposta.ler(
                s, coluna_id,
                conteudo=arquivo.read() if arquivo is not None else None,
                nome_arquivo=getattr(arquivo, "filename", "") or "",
                texto=texto, usuario=atual)
            s.commit()
            return jsonify({"ok": True, "leitura": leitura})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao ler a proposta")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/cotacoes/<int:cotacao_id>/proposta", methods=["POST"])
@login_obrigatorio
@permissao("comprar")
def api_jogar_proposta_no_mapa(cotacao_id: int):
    """A proposta jogada no mapa, sem dizer de quem é: o sistema lê uma vez,
    reconhece o fornecedor pelo CNPJ ou pelo nome e devolve os preços casados
    com as linhas. Não grava preço — devolve sugestão para conferir."""
    from app.apps.erp.core.suprimentos import proposta
    arquivo = request.files.get("arquivo")
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Jogue o arquivo da proposta."}), 400
    try:
        with get_session() as s:
            leitura = proposta.ler_no_mapa(
                s, cotacao_id, conteudo=arquivo.read(),
                nome_arquivo=arquivo.filename or "", usuario=_usuario_logado(s))
            s.commit()
            return jsonify({"ok": True, "leitura": leitura})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao ler a proposta jogada no mapa")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# --- Pedido de compra e autorização -----------------------------------------
# A fila de pedidos serve a dois papéis — quem compra e quem autoriza —, e por
# isso tem ação própria (`ver_pedidos_compra`), que ambos ganham de graça. A
# alternativa (declarar "ver_suprimentos" e conferir outra coisa por dentro)
# faria a declaração mentir sobre quem entra, e há teste estrutural para isso.
@bp.route("/erp/suprimentos/pedidos")
@login_obrigatorio
@permissao("ver_pedidos_compra")
def pagina_suprimentos_pedidos():
    return render_template("erp_suprimentos_pedidos.html", **_contexto("sup_pedidos"))


@bp.route("/erp/api/suprimentos/pedidos", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_pedidos_compra", POST="comprar")
def api_pedidos():
    from app.apps.erp.core.suprimentos import pedido as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if request.method == "GET":
                return jsonify({"ok": True, "pedidos": svc.listar(
                    s, status=request.args.get("status"))})
            ped = svc.fechar_direto(s, request.get_json(silent=True) or {}, atual)
            saida = {"ok": True, "id": ped.id, "numero": ped.numero}
            s.commit()
            return jsonify(saida)
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP/suprimentos: falha no pedido de compra")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/cotacoes/<int:cotacao_id>/fechar", methods=["POST"])
@login_obrigatorio
@permissao("comprar")
def api_fechar_do_mapa(cotacao_id: int):
    """Fecha com um fornecedor os itens escolhidos no mapa. Um mesmo mapa gera
    vários pedidos, um por fornecedor."""
    from app.apps.erp.core.suprimentos import pedido as svc
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            ped = svc.fechar_do_mapa(s, cotacao_id,
                                     int(d.get("cotacao_fornecedor_id") or 0),
                                     [int(x) for x in (d.get("itens") or [])],
                                     d, atual)
            saida = {"ok": True, "id": ped.id, "numero": ped.numero}
            s.commit()
            return jsonify(saida)
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao fechar pedido do mapa")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/pedidos/<int:pedido_id>")
@login_obrigatorio
@permissao("ver_pedidos_compra")
def api_pedido(pedido_id: int):
    """O pedido com o mapa de origem embutido — quem autoriza precisa ver as
    alternativas que o comprador tinha, não só a escolha final."""
    from app.apps.erp.core.suprimentos import pedido as svc
    with get_session() as s:
        return jsonify({"ok": True, "pedido": svc.detalhar(s, pedido_id)})


@bp.route("/erp/api/suprimentos/pedidos/<int:pedido_id>/relatorio")
@login_obrigatorio
@permissao("comprar")
def api_pedido_relatorio(pedido_id: int):
    """O pedido como o fornecedor precisa ler: agrupado por endereço de entrega."""
    from app.apps.erp.core.suprimentos import pedido as svc
    with get_session() as s:
        return jsonify({"ok": True,
                        "relatorio": svc.relatorio_para_o_fornecedor(s, pedido_id)})


@bp.route("/erp/api/suprimentos/pedidos/<int:pedido_id>/envio",
          methods=["GET", "POST"])
@login_obrigatorio
@permissao("comprar")
def api_pedido_envio(pedido_id: int):
    """GET mostra o pedido como o fornecedor vai receber, antes de mandar.
    POST manda de verdade, pela conta de e-mail da empresa da obra.

    A rota é declarada ANTES da rota genérica de ações do pedido de propósito:
    "envio" é um segmento fixo e não pode cair no `<acao>` de autorizar/recusar,
    que exige outra permissão.
    """
    from app.apps.erp.core.suprimentos import envio as svc
    try:
        with get_session() as s:
            if request.method == "GET":
                return jsonify({"ok": True,
                                "envio": svc.preparar_pedido(s, pedido_id)})
            d = request.get_json(silent=True) or {}
            resultado = svc.disparar_pedido(s, pedido_id, d, _usuario_logado(s))
            s.commit()
            return jsonify({"ok": True, **resultado})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao mandar o pedido")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/pedidos/<int:pedido_id>/<acao>", methods=["POST"])
@login_obrigatorio
@permissao("autorizar_pedido")
def api_decidir_pedido(pedido_id: int, acao: str):
    from app.apps.erp.core.suprimentos import pedido as svc
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            envio_feito = None
            if acao == "autorizar":
                svc.autorizar(s, pedido_id, atual,
                              [int(x) for x in (d.get("itens_recusados") or [])])
                # ⚑ AUTORIZOU, SAIU PARA O FORNECEDOR (22/11/2026). O dono: "a
                # pessoa designada a autorizar, na hora que ela autoriza, o
                # pedido é disparado para o fornecedor. Aí é encaminhado o
                # e-mail e o anexo." A autorização é gravada ANTES e não
                # depende do envio: fornecedor sem e-mail ou conta de envio
                # fora do ar não desfaz a decisão — a tela diz que não saiu e
                # o botão de mandar continua em Pedidos.
                s.commit()
                try:
                    from app.apps.erp.core.suprimentos import envio as svc_envio
                    feito = svc_envio.disparar_pedido(s, pedido_id, {}, atual)
                    s.commit()
                    envio_feito = {"enviado": feito.get("situacao") == "ENVIADO",
                                   "para": feito.get("para") or [],
                                   "motivo": feito.get("erro")}
                except ErroValidacao as e:
                    s.rollback()
                    envio_feito = {"enviado": False, "para": [], "motivo": str(e)}
                except Exception as e:
                    s.rollback()
                    logger.exception("ERP/suprimentos: pedido autorizado, envio falhou")
                    envio_feito = {"enviado": False, "para": [], "motivo": recado_de_falha(e)}
                return jsonify({"ok": True, "envio": envio_feito})
            elif acao == "recusar":
                svc.recusar(s, pedido_id, d.get("motivo") or "", atual)
            elif acao == "cancelar":
                svc.cancelar(s, pedido_id, d.get("motivo") or "", atual)
            else:
                return jsonify({"ok": False, "erro": "Ação desconhecida."}), 400
            s.commit()
            return jsonify({"ok": True})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao decidir o pedido")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# --- Recebimento na obra ----------------------------------------------------
# Quem confere é a obra: a rota exige apenas "ver_suprimentos", e o escopo por
# obra já limita o que cada pessoa enxerga.
@bp.route("/erp/suprimentos/receber")
@login_obrigatorio
@permissao("receber_material")
def pagina_suprimentos_receber():
    return render_template("erp_suprimentos_receber.html",
                           **_contexto("sup_receber"))


@bp.route("/erp/api/suprimentos/recebimentos")
@login_obrigatorio
@permissao("receber_material")
def api_recebimentos_da_obra():
    """O que está para chegar nas obras da pessoa, o que chegou sem nota e —
    para ela acompanhar — o que ainda está sendo cotado ou autorizado."""
    from app.apps.erp.core.suprimentos import acompanhamento as svc_acomp
    from app.apps.erp.core.suprimentos import recebimento as svc
    with get_session() as s:
        usuario = _usuario_logado(s)
        antes = [i for i in svc_acomp.planejamento(s, usuario)["itens"]
                 if i["etapa"] in ("SEM_COTACAO", "EM_COTACAO", "A_AUTORIZAR")]
        # Para PEDIR material pelo portal: as obras da pessoa e as unidades.
        from sqlalchemy import select
        from app.apps.erp.core import portal as svc_portal
        from app.apps.erp.db.models.cadastros import UnidadeCompra
        from app.apps.erp.core.suprimentos import insumos as svc_insumos
        from app.apps.erp.core.suprimentos.solicitacao import obras_para_pedir
        para_pedir = obras_para_pedir(s, usuario)
        return jsonify({"ok": True, **svc.a_receber(s, usuario),
                        "em_andamento": antes,
                        "pode_lancar": pode(usuario, "lancar"),
                        "pode_pedir": pode(usuario, "solicitar_suprimento"),
                        "pode_liberar": pode(usuario, "liberar_solicitacao"),
                        "esperando_cadastro": svc_insumos.esperando_cadastro(s, usuario),
                        # só as obras DELA (cadastro + onde bate ponto) — 22/11/2026
                        "obras": [o for o in svc_portal.obras_para_escolher(s, usuario)
                                  if para_pedir is None or o["id"] in para_pedir],
                        "obra_sugerida": svc_portal.obra_sugerida(s, usuario),
                        "unidades": [u.codigo for u in s.scalars(
                            select(UnidadeCompra).order_by(UnidadeCompra.codigo)).all()
                            if getattr(u, "ativo", True) is not False]})


@bp.route("/erp/api/suprimentos/pedidos/<int:pedido_id>/receber-e-lancar",
          methods=["POST"])
@login_obrigatorio
@permissao("lancar")
def api_receber_e_lancar(pedido_id: int):
    """O procedimento único da obra: registra o que chegou E lança a conta a
    pagar, numa transação só. Exige `lancar` — quem só recebe usa a rota de
    recebimento, e a conta fica aparecendo como "falta lançar"."""
    from app.apps.erp.core.suprimentos import lancamento as svc
    from app.apps.erp.core.suprimentos.recebimento import exigir_pedido_no_escopo
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if not pode(usuario, "receber_material"):
                return jsonify({"ok": False,
                                "erro": "Você não registra recebimento de material."}), 403
            exigir_pedido_no_escopo(s, usuario, pedido_id)
            saida = svc.receber_e_lancar(s, usuario, pedido_id,
                                         request.get_json(silent=True) or {})
            s.commit()
            return jsonify({"ok": True, **saida})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao receber e lançar")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/recebimentos/<int:recebimento_id>/documentos",
          methods=["GET", "POST"])
@login_obrigatorio
@permissao("receber_material")
def api_documentos_do_recebimento(recebimento_id: int):
    """Nota, boleto e o que mais vier junto do material — vários por recebimento."""
    from app.apps.erp.core.suprimentos import recebimento as svc
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            rec = svc._recebimento_no_escopo(s, usuario, recebimento_id)
            svc.exigir_pedido_no_escopo(s, usuario, rec.pedido_id)
            if request.method == "GET":
                return jsonify({"ok": True, "documentos": svc.documentos_do_pedido(
                    s, rec.pedido_id, recebimento_id=rec.id)})
            arquivo = request.files.get("arquivo")
            if arquivo is None:
                return jsonify({"ok": False, "erro": "Escolha o arquivo."}), 400
            doc = svc.anexar(s, usuario, rec.id, arquivo.read(), arquivo.filename or "",
                             request.form.get("categoria") or "OUTRO")
            s.commit()
            return jsonify({"ok": True, "documento": doc})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/suprimentos/recebimentos/documento/<int:anexo_id>")
@login_obrigatorio
@permissao(GET="ver_suprimentos")
def baixar_documento_do_recebimento(anexo_id: int):
    """O arquivo anexado num recebimento. O recorte por obra é conferido pelo
    pedido a que o recebimento pertence."""
    from app.apps.erp.core.suprimentos import recebimento as svc
    with get_session() as s:
        usuario = _usuario_logado(s)
        svc.exigir_pedido_no_escopo(s, usuario, svc.pedido_do_documento(s, anexo_id))
        conteudo, nome, mime = svc.conteudo_do_documento(s, usuario, anexo_id)
    resposta = make_response(conteudo)
    resposta.headers["Content-Type"] = mime
    resposta.headers["Content-Disposition"] = f'inline; filename="{nome}"'
    return resposta


@bp.route("/erp/api/suprimentos/pedidos/a-lancar")
@login_obrigatorio
@permissao("lancar")
def api_pedidos_a_lancar():
    """Os pedidos de compra de um fornecedor que ainda têm conta a lançar —
    para a tela de Lançar perguntar "é deste pedido?"."""
    from app.apps.erp.core.suprimentos import lancamento as svc
    bruto = (request.args.get("fornecedor_id") or "").strip()
    if not bruto.isdigit():
        return jsonify({"ok": True, "pedidos": []})
    with get_session() as s:
        return jsonify({"ok": True, "pedidos": svc.pedidos_do_fornecedor_a_lancar(
            s, _usuario_logado(s), int(bruto))})


@bp.route("/erp/portal/material")
@login_obrigatorio
@permissao("receber_material")
def pagina_portal_material():
    """O MATERIAL DA OBRA, NO PORTAL (20/11/2026) — a tela enxuta, de celular,
    por onde a obra recebe. Usa as mesmas rotas de "Receber material" do ERP:
    são duas portas para a mesma regra, e não duas regras."""
    return render_template("erp_portal_material.html")


# ⚑ O POST DEIXOU DE SER `ver_suprimentos` em 20/11/2026. Quem só VÊ suprimentos
# (o perfil de consulta, o parceiro de fora da empresa) registrava recebimento
# — e sem recorte de obra. Agora é ação própria, e o pedido tem de ter item
# numa obra da pessoa.
@bp.route("/erp/api/suprimentos/pedidos/<int:pedido_id>/recebimento",
          methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_suprimentos", POST="receber_material")
def api_recebimento(pedido_id: int):
    from app.apps.erp.core.suprimentos import recebimento as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            svc.exigir_pedido_no_escopo(s, atual, pedido_id)
            if request.method == "GET":
                return jsonify({"ok": True, "situacao": svc.situacao(s, pedido_id)})
            # ⚑ NUNCA SÓ O RECEBIMENTO, para quem pode lançar (21/11/2026). O
            # dono: "nunca pode ser feito só o recebimento. Isso deixa um
            # vácuo." Quem lança pagamento recebe pelo procedimento único
            # (`receber-e-lancar`), que registra a chegada e a conta juntas —
            # ou a saída "a nota não veio", com o motivo escrito. Esta rota
            # fica só para quem NÃO lança: aí o recebimento entra, e fica
            # registrado por que veio sem conta.
            if pode(atual, "lancar"):
                return jsonify({"ok": False, "erro":
                    "O recebimento é registrado junto com a nota e o boleto. "
                    "Abra Suprimentos › Receber material e use \"Receber e "
                    "lançar a nota\"."}), 400
            rec = svc.registrar(s, pedido_id, request.get_json(silent=True) or {}, atual)
            from app.apps.erp.core.comum.auditoria import registrar_evento
            registrar_evento(s, "pedido_compra", pedido_id, "RECEBIDO_SEM_LANCAMENTO",
                             {"recebimento_id": rec.id,
                              "motivo": "quem recebeu não lança pagamentos"}, atual.id)
            saida = {"ok": True, "recebimento_id": rec.id}
            s.commit()
            return jsonify(saida)
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP/suprimentos: falha no recebimento")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/manutencao/limpeza/areas")
@login_obrigatorio
@permissao("configurar")
def api_limpeza_areas():
    """As áreas que podem ser zeradas, para a tela montar as caixinhas."""
    from app.apps.erp.core.manutencao import limpeza
    return jsonify({"ok": True, "areas": limpeza.catalogo(),
                    "preservado_sempre": sorted(limpeza.JAMAIS),
                    "frase": limpeza.FRASE_DE_CONFIRMACAO})


@bp.route("/erp/api/manutencao/limpeza", methods=["GET", "POST"])
@login_obrigatorio
@permissao("configurar")
def api_limpeza():
    """Zera o MOVIMENTO das áreas escolhidas. Cadastro nunca sai por aqui.

    GET conta o que sairia, sem tocar em nada; POST executa, e exige a frase
    digitada. Ver `core/manutencao/limpeza.py` para o desenho e as travas.
    """
    from app.apps.erp.core.manutencao import limpeza
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if request.method == "GET":
                areas = [a for a in (request.args.get("areas") or "").split(",") if a]
                return jsonify({"ok": True, "resumo": limpeza.resumo(s, areas)})
            d = request.get_json(silent=True) or {}
            relatorio = limpeza.zerar(s, d.get("areas") or [],
                                      d.get("confirmacao") or "", atual)
            s.commit()
            return jsonify({"ok": True, "relatorio": relatorio})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP/manutenção: falha ao zerar o movimento")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/pendencias")
@login_obrigatorio
@permissao("ver_suprimentos")
def api_pendencias():
    """O que ficou com saldo. Pendência tem tratamento mais urgente que
    solicitação nova: já foi pedida uma vez, e alguém está esperando."""
    from app.apps.erp.core.suprimentos import recebimento as svc
    with get_session() as s:
        return jsonify({"ok": True, "pendencias": svc.pendencias(s, _usuario_logado(s))})


@bp.route("/erp/api/suprimentos/precos")
@login_obrigatorio
@permissao("comprar")
def api_banco_de_precos():
    """O histórico de preços cotados e comprados, com o resumo do período."""
    from datetime import date as _date, timedelta as _td
    from app.apps.erp.core.suprimentos import cotacao as svc
    dias = request.args.get("dias")
    desde = None
    if dias:
        try:
            desde = _date.today() - _td(days=int(dias))
        except ValueError:
            desde = None
    with get_session() as s:
        return jsonify({"ok": True, "historico": svc.historico_de_precos(
            s, insumo_id=request.args.get("insumo_id"),
            fornecedor_id=request.args.get("fornecedor_id"),
            desde=desde, busca=(request.args.get("busca") or "").strip())})


@bp.route("/erp/api/suprimentos/ler-lista", methods=["POST"])
@login_obrigatorio
@permissao("solicitar_suprimento")
def api_suprimentos_ler_lista():
    """Cola a lista de materiais e a IA monta as linhas — para conferir, não
    para gravar. Nada entra no banco por aqui."""
    from app.apps.erp.core.suprimentos import leitura
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            return jsonify({"ok": True,
                            "leitura": leitura.ler_lista(s, d.get("texto") or "")})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao ler a lista colada")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/solicitacoes/<int:solicitacao_id>")
@login_obrigatorio
@permissao("ver_suprimentos")
def api_suprimento_solicitacao(solicitacao_id: int):
    """Uma solicitação com seus itens. Fora do alcance responde 404, nunca 403."""
    from app.apps.erp.core.suprimentos import solicitacao as svc
    with get_session() as s:
        return jsonify({"ok": True,
                        "solicitacao": svc.obter(s, solicitacao_id, _usuario_logado(s))})


@bp.route("/erp/api/suprimentos/itens/liberar", methods=["POST"])
@login_obrigatorio
@permissao("liberar_solicitacao")
def api_liberar_solicitacao():
    """O ok da engenharia no material pedido pela obra — ou a recusa, com motivo.
    O recorte por obra é conferido item a item, dentro do serviço."""
    from app.apps.erp.core.suprimentos import solicitacao as svc
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if d.get("recusar"):
                n = svc.recusar_liberacao(s, d.get("itens") or [], d.get("motivo") or "", atual)
            else:
                n = svc.liberar(s, d.get("itens") or [], atual)
            s.commit()
            return jsonify({"ok": True, "itens": n})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao liberar solicitação")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ⚑ SÓ O COMPRADOR MOVE O ITEM À MÃO (22/11/2026). Era `ver_suprimentos` — o
# que deixava perfil de consulta e parceiro levar um item a "entregue" e
# "recebido" sem pedido nem nota. O dono: "o compras até pode botar entregue,
# porque a ideia é ele saber que o material chegou (...) daí em diante, a
# pessoa que recebeu na obra vai fazer o recebimento."
@bp.route("/erp/api/suprimentos/itens/<int:item_id>/situacao", methods=["POST"])
@login_obrigatorio
@permissao("comprar")
def api_suprimento_situacao(item_id: int):
    """Move o item pelo fluxo. Só quem enxerga o item pode mexer nele."""
    from app.apps.erp.core.suprimentos import solicitacao as svc
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if not svc.enxerga_item(s, atual, item_id):
                raise ErroNaoEncontrado("Item não encontrado.")
            svc.mudar_situacao(s, item_id, d.get("status") or "", atual,
                               d.get("observacao") or "")
            s.commit()
            return jsonify({"ok": True})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao mudar situação do item")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/itens/<int:item_id>", methods=["PATCH"])
@login_obrigatorio
@permissao("comprar")
def api_suprimento_item_corrigir(item_id: int):
    """Corrige o item da solicitação: insumo, especificação, quantidade,
    unidade ou obra — com motivo obrigatório e registro de quem mudou."""
    from app.apps.erp.core.suprimentos import solicitacao as svc
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if item_id not in {i["id"] for i in svc.listar_itens(s, atual)}:
                raise ErroNaoEncontrado("Item não encontrado.")
            resultado = svc.editar_item(s, item_id, d, atual)
            s.commit()
            return jsonify({"ok": True, **resultado})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao corrigir item")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/itens/<int:item_id>/historico")
@login_obrigatorio
@permissao("ver_suprimentos")
def api_suprimento_item_historico(item_id: int):
    """As correções já feitas neste item. Fora do alcance responde 404."""
    from app.apps.erp.core.suprimentos import solicitacao as svc
    with get_session() as s:
        atual = _usuario_logado(s)
        if item_id not in {i["id"] for i in svc.listar_itens(s, atual)}:
            raise ErroNaoEncontrado("Item não encontrado.")
        return jsonify({"ok": True, "historico": svc.historico_do_item(s, item_id)})


@bp.route("/erp/api/suprimentos/insumos/solicitacoes", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_suprimentos", POST="solicitar_suprimento")
def api_solicitacoes_insumo():
    """Pedir o cadastro de um insumo que ainda não existe, e ver os pedidos."""
    from app.apps.erp.core.suprimentos import insumos as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if request.method == "GET":
                pendentes = str(request.args.get("pendentes") or "") in ("1", "true", "sim")
                return jsonify({"ok": True,
                                "solicitacoes": svc.listar(s, atual, pendentes)})
            pedido = svc.solicitar(s, request.get_json(silent=True) or {}, atual)
            s.commit()
            return jsonify({"ok": True, "id": pedido.id})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP/suprimentos: falha no pedido de cadastro de insumo")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/insumos/solicitacoes/<int:solicitacao_id>",
          methods=["POST"])
@login_obrigatorio
@permissao("administrar_insumos")
def api_decidir_solicitacao_insumo(solicitacao_id: int):
    """Cadastrar ou recusar. O nome final, a categoria e a conta do plano são
    de quem decide — é isso que mantém a base de insumos limpa."""
    from app.apps.erp.core.suprimentos import insumos as svc
    d = request.get_json(silent=True) or {}
    acao = (d.get("acao") or "").strip().lower()
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if acao == "cadastrar":
                insumo = svc.cadastrar(s, solicitacao_id, d, atual)
                resposta = {"ok": True, "insumo_id": insumo.id, "codigo": insumo.codigo}
            elif acao == "recusar":
                svc.recusar(s, solicitacao_id, d.get("motivo") or "", atual)
                resposta = {"ok": True}
            else:
                return jsonify({"ok": False,
                                "erro": "Diga se é para cadastrar ou recusar."}), 400
            s.commit()
            return jsonify(resposta)
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao decidir cadastro de insumo")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/importar/<tipo>", methods=["POST"])
@login_obrigatorio
@permissao("administrar_insumos")
def api_suprimentos_importar(tipo: str):
    """Carga de fornecedores ou insumos por CSV exportado da planilha.

    Com `?simular=1` só relata o que aconteceria — é a prévia que se confere
    antes de deixar gravar. Rodar duas vezes não duplica: fornecedor casa por
    CNPJ, insumo pela descrição.
    """
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.core.importadores import suprimentos as imp
    if tipo not in ("fornecedores", "insumos", "banco_precos"):
        return jsonify({"ok": False, "erro": "Tipo de carga desconhecido."}), 400
    arquivo = request.files.get("arquivo")
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Anexe o arquivo (CSV ou Excel)."}), 400
    conteudo = arquivo.read()
    if not conteudo:
        return jsonify({"ok": False, "erro": "Arquivo vazio."}), 400
    simular = str(request.args.get("simular") or "").strip() in ("1", "true", "sim")
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if tipo == "banco_precos":
                # O HISTÓRICO DE PREÇOS antigo (18/09/2026). Vive em módulo
                # próprio porque a regra de casar insumo é MAIS DURA aqui: um
                # preço no insumo errado mente sobre quanto o material custa, e
                # ninguém percebe.
                from app.apps.erp.core.importadores import banco_precos as imp_bp
                rel = (imp_bp.previa(s, conteudo) if simular
                       else imp_bp.importar(s, conteudo, atual))
            elif tipo == "fornecedores":
                rel = imp.importar_fornecedores_csv(s, conteudo, atual, simular=simular)
            else:
                # Criar categoria de insumo na carga é decisão do dono, marcada
                # na tela — nunca o padrão. Ver o porquê em `importar_insumos_csv`.
                criar_cats = str(request.args.get("criar_categorias") or "").strip() \
                    in ("1", "true", "sim")
                rel = imp.importar_insumos_csv(s, conteudo, atual, simular=simular,
                                               criar_categorias=criar_cats)
            if simular:
                s.rollback()
            else:
                registrar_evento(s, "importacao", 0, f"SUPRIMENTOS_{tipo.upper()}",
                                 rel, atual.id if atual else None)
                s.commit()
            logger.info("ERP/suprimentos: carga de %s (%s) — %s",
                        tipo, "prévia" if simular else "gravada", rel)
        return jsonify({"ok": True, "relatorio": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha na carga de %s", tipo)
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# Empresas — o CNPJ que executa a obra, compra e fatura (migração 038)
#
# A BWS passa a operar com mais de um. É daqui que sai o e-mail da cotação, a
# logo do relatório e, amanhã, a nota fiscal.
# ---------------------------------------------------------------------------
@bp.route("/erp/empresas")
@login_obrigatorio
@permissao("configurar")
def pagina_empresas():
    # `_contexto("config")`: a tela de Empresas pertence a Configurações, e é
    # a aba de Configurações que tem de ficar acesa lá em cima.
    return render_template("erp_empresas.html", sub_aba="empresas",
                           **_contexto("config"))


@bp.route("/erp/api/empresas", methods=["GET", "POST"])
@login_obrigatorio
@permissao("configurar")
def api_empresas():
    from app.apps.erp.core.cadastros import empresas as svc
    try:
        with get_session() as s:
            if request.method == "GET":
                return jsonify({"ok": True, **svc.gerenciar(s)})
            atual = _usuario_logado(s)
            e = svc.criar(s, request.get_json(silent=True) or {}, atual)
            s.commit()
            return jsonify({"ok": True, "id": e.id})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha no cadastro de empresa")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# A EMPRESA QUE NASCE DO CARTÃO CNPJ
#
# Dono, 17/09/2026: *"e eu conseguiria cadastrá-la a partir do Cartão CNPJ?"*.
# Mesmo caminho da obra que nasce do contrato: lê, mostra o que entendeu, cria e
# arquiva o documento na empresa — tudo na mesma transação.
# ---------------------------------------------------------------------------
@bp.route("/erp/api/empresas/documento/ler", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_nova_empresa_documento_ler():
    """Lê o Cartão CNPJ e diz que empresa ele criaria. Não grava nada."""
    from app.apps.erp.core.arquivo import leitura, preenchimento
    from app.apps.erp.core.documentos.leitor import ErroLeitura
    f = request.files.get("arquivo")
    if f is None:
        return jsonify({"ok": False, "erro": "Escolha o arquivo."}), 400
    # FORA do try: a recusa por teto tem resposta própria (402).
    _exigir_saldo_de_ia()
    try:
        conteudo = f.read()
        with get_session() as s:
            sugestao = leitura.sugerir(
                s, conteudo, f.filename or "arquivo",
                dica=(request.form.get("dica") or ""),
                extracao=preenchimento.instrucao_de_extracao_empresa(),
                dono_e_novo=True)
            sugestao["nome_original"] = f.filename or "arquivo"
            # O nome do arquivo depende da empresa, que ainda não existe.
            sugestao["nome_sugerido"] = ""
            cadastro = preenchimento.sugerir_para_nova_empresa(
                s, sugestao.get("tipo_codigo") or "", sugestao)
        return jsonify({"ok": True, "sugestao": sugestao, "cadastro": cadastro})
    except ErroLeitura as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP/empresas: falha ao ler o documento")
        return jsonify({"ok": False, "erro": f"Não deu para ler o documento: {e}"}), 500


@bp.route("/erp/api/empresas/documento", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_nova_empresa_documento():
    """Cria a empresa a partir do documento lido e arquiva o documento nela.

    Na MESMA transação: empresa criada com o cartão perdido, ou cartão guardado
    numa empresa que não chegou a existir, seriam os dois piores resultados.
    """
    from app.apps.erp.core.arquivo import service as svc_arq
    from app.apps.erp.core.cadastros import empresas as svc_emp
    f = request.files.get("arquivo")
    if f is None:
        return jsonify({"ok": False, "erro": "Escolha o arquivo."}), 400
    tipo = (request.form.get("tipo") or "").strip().upper()
    campos = json.loads(request.form.get("campos") or "{}")
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            empresa = svc_emp.criar(s, campos, usuario)
            s.flush()
            d = svc_arq.arquivar(
                s, f.read(), f.filename or "arquivo",
                tipo_codigo=tipo or "CARTAO-CNPJ", empresa_id=empresa.id,
                referencia=(request.form.get("referencia") or ""),
                emissao=(date.fromisoformat(request.form["emissao"])
                         if (request.form.get("emissao") or "").strip() else None),
                texto=(request.form.get("texto") or ""),
                resumo=(request.form.get("resumo") or ""),
                origem="IA", usuario=usuario)
            linha = svc_arq.ler(s, d)
            criada = {"id": empresa.id, "razao_social": empresa.razao_social,
                      "nome": empresa.nome_fantasia or empresa.razao_social,
                      "cnpj": empresa.cnpj}
            s.commit()
        return jsonify({"ok": True, "empresa": criada, "documento": linha})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP/empresas: falha ao criar empresa a partir de documento")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/empresas/<int:empresa_id>", methods=["PATCH"])
@login_obrigatorio
@permissao("configurar")
def api_empresa_editar(empresa_id: int):
    from app.apps.erp.core.cadastros import empresas as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            e = svc.editar(s, empresa_id, request.get_json(silent=True) or {}, atual)
            s.commit()
            return jsonify({"ok": True, "id": e.id})
    except ErroNaoEncontrado as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao editar empresa")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/empresas/<int:empresa_id>/conta-email", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_empresa_conta_email(empresa_id: int):
    """Servidor, porta, usuário, segurança e a senha (cifrada ao gravar).

    A senha só vem quando está sendo TROCADA — campo em branco mantém a que
    está lá. É o que evita apagar a senha ao corrigir a porta.
    """
    from app.apps.erp.core.cadastros import empresas as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            e = svc.definir_conta_de_email(s, empresa_id,
                                           request.get_json(silent=True) or {}, atual)
            s.commit()
            return jsonify({"ok": True, "id": e.id})
    except ErroNaoEncontrado as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao definir a conta de e-mail da empresa")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# CERTIFICADO DIGITAL (A1) da empresa
#
# O arquivo e a senha vão CIFRADOS. Não existe rota de download de propósito:
# certificado digital é a assinatura da empresa, e o que não tem porta não é
# arrombado. A tela mostra titular, validade e emissor — nunca os bytes.
# ---------------------------------------------------------------------------
@bp.route("/erp/api/empresas/<int:empresa_id>/certificado")
@login_obrigatorio
@permissao("configurar")
def api_empresa_certificado(empresa_id: int):
    from app.apps.erp.core.cadastros import certificado as svc
    with get_session() as s:
        return jsonify({"ok": True, "certificado": svc.ler(s, empresa_id),
                        "historico": svc.historico(s, empresa_id)})


@bp.route("/erp/api/empresas/<int:empresa_id>/certificado", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_empresa_certificado_guardar(empresa_id: int):
    """Guarda o A1. Abrir o arquivo é o que PROVA que a senha está certa."""
    from app.apps.erp.core.cadastros import certificado as svc
    arquivo = request.files.get("arquivo")
    if arquivo is None:
        return jsonify({"ok": False,
                        "erro": "Anexe o arquivo do certificado (.pfx ou .p12)."}), 400
    try:
        with get_session() as s:
            svc.guardar(s, empresa_id, arquivo.read(),
                        (request.form.get("senha") or ""),
                        nome_arquivo=(arquivo.filename or ""),
                        observacao=(request.form.get("observacao") or ""),
                        usuario=_usuario_logado(s))
            lido = svc.ler(s, empresa_id)
            s.commit()
        return jsonify({"ok": True, "certificado": lido})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/empresas/<int:empresa_id>/testar-email", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_empresa_testar_email(empresa_id: int):
    """Manda uma mensagem de teste. É o que transforma 'acho que configurei'
    em 'está configurado'."""
    from app.apps.erp.core.cadastros import empresas as svc
    from app.apps.erp.core.comum import email as correio
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            empresa = svc.obter(s, empresa_id)
            destino = (d.get("para") or "").strip() or getattr(atual, "email", "")
            registro = correio.testar_conta(s, empresa, destino, atual)
            saida = {"ok": True, "situacao": registro.situacao,
                     "erro": registro.erro, "para": destino}
            s.commit()
            return jsonify(saida)
    except ErroNaoEncontrado as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha no teste de e-mail")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/empresas/<int:empresa_id>/logo",
          methods=["GET", "POST", "DELETE"])
@login_obrigatorio
@permissao("configurar")
def api_empresa_logo(empresa_id: int):
    """A logo da empresa: GET devolve a imagem, POST troca, DELETE tira.

    Fica em "configurar" — e não numa ação ampla — porque hoje só a tela de
    Empresas mostra a logo. Quando o relatório ao fornecedor passar a
    carregá-la, quem abre relatório vai precisar ler esta rota; nessa hora a
    ação muda E a rota ganha conferência de escopo, porque a regra do ERP é
    que rota com número de registro aberta a perfil restrito confere se o
    registro é dele (tests/test_permissoes_rotas.py). Abrir agora, sem
    ninguém precisar, seria abrir por antecipação.
    """
    from flask import Response
    from app.apps.erp.core.cadastros import empresas as svc
    try:
        with get_session() as s:
            if request.method == "GET":
                empresa = svc.obter(s, empresa_id)
                if not empresa.logo:
                    raise ErroNaoEncontrado("Esta empresa não tem logo.")
                return Response(empresa.logo,
                                mimetype=empresa.logo_mime or "image/png",
                                headers={"Cache-Control": "private, max-age=300"})
            atual = _usuario_logado(s)
            if request.method == "DELETE":
                svc.remover_logo(s, empresa_id, atual)
                s.commit()
                return jsonify({"ok": True})
            arquivo = request.files.get("arquivo")
            if arquivo is None:
                return jsonify({"ok": False, "erro": "Anexe o arquivo da logo."}), 400
            svc.definir_logo(s, empresa_id, arquivo.filename or "logo",
                             arquivo.mimetype or "", arquivo.read(), atual)
            s.commit()
            return jsonify({"ok": True})
    except ErroNaoEncontrado as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha na logo da empresa")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/obras/<int:obra_id>/logo-empresa")
@login_obrigatorio
@permissao("ver_erp")
def api_obra_logo_empresa(obra_id: int):
    """A logo da EMPRESA que executa esta obra — só para mostrar.

    ⚑ POR QUE NÃO USAR A ROTA DA EMPRESA: aquela exige "configurar", porque
    recebe o número de uma empresa qualquer. Esta recebe o número de uma OBRA
    e confere o escopo dela, então quem pode ver a obra pode ver o cabeçalho
    que vai sair no diário dela. Sem isto, a ficha da obra mostraria um
    quadrado quebrado no lugar da logo para quem não configura o sistema.

    Só GET: TROCAR a logo continua sendo coisa do cadastro da empresa, porque
    ela vale para todas as obras daquele CNPJ.
    """
    from flask import Response
    from sqlalchemy import text as _text
    from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
    try:
        with get_session() as s:
            exigir_obra_no_escopo(s, _usuario_logado(s), obra_id)
            linha = s.execute(_text("""
                SELECT e.logo, e.logo_mime FROM obras o
                  JOIN empresas e ON e.id = o.empresa_id
                 WHERE o.id = :o"""), {"o": obra_id}).first()
            if linha is None or not linha[0]:
                raise ErroNaoEncontrado("Esta obra não tem logo de empresa.")
            return Response(bytes(linha[0]), mimetype=linha[1] or "image/png",
                            headers={"Cache-Control": "private, max-age=300"})
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha na logo da empresa da obra %s", obra_id)
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/obras/<int:obra_id>/logo-contratante",
          methods=["GET", "POST", "DELETE"])
@login_obrigatorio
@permissao(GET="ver_erp", POST="configurar", DELETE="configurar")
def api_obra_logo_contratante(obra_id: int):
    """A logo do ÓRGÃO que contrata esta obra: GET mostra, POST troca, DELETE tira.

    Ela entra no cabeçalho do diário impresso junto com a da empresa — padrão
    que ele pediu em 09/11/2026, vendo o modelo da Prefeitura de Abreu e Lima:
    *"usar esse padrão, logo do órgão e logo da prefeitura."*

    ⚑ O GET é "ver_erp" e NÃO "configurar", e a diferença é prática: quem
    imprime o diário todo dia é o administrativo da obra, que não configura
    nada. Ler a imagem do cabeçalho de uma obra que ele já pode ver não expõe
    nada; exigir "configurar" para isso é que deixaria o papel sem cabeçalho
    na mão de quem trabalha com ele.

    E a rota confere o ESCOPO da obra, porque recebe número de registro: obra
    fora do alcance responde 404 "não encontrado", nunca 403.
    """
    from flask import Response
    from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
    from app.apps.erp.core.cadastros import obras as svc
    from app.apps.erp.db.models.cadastros import Obra
    try:
        with get_session() as s:
            exigir_obra_no_escopo(s, _usuario_logado(s), obra_id)
            if request.method == "GET":
                obra = s.get(Obra, obra_id)
                if obra is None or not obra.contratante_logo:
                    raise ErroNaoEncontrado("Esta obra não tem logo do contratante.")
                return Response(obra.contratante_logo,
                                mimetype=obra.contratante_logo_mime or "image/png",
                                headers={"Cache-Control": "private, max-age=300"})
            atual = _usuario_logado(s)
            if request.method == "DELETE":
                svc.remover_logo_contratante(s, obra_id, atual)
                s.commit()
                return jsonify({"ok": True})
            arquivo = request.files.get("arquivo")
            if arquivo is None:
                return jsonify({"ok": False, "erro": "Anexe o arquivo da logo."}), 400
            svc.definir_logo_contratante(s, obra_id, arquivo.mimetype or "",
                                         arquivo.read(), atual)
            s.commit()
            return jsonify({"ok": True})
    except ErroNaoEncontrado:
        raise                # recusa de escopo e logo ausente viram 404
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha na logo do contratante da obra %s", obra_id)
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/obras/<int:obra_id>/empresa", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_obra_empresa(obra_id: int):
    """Liga a obra à empresa que a executa. Sem isso o disparo da cotação não
    sabe por qual e-mail sair."""
    from app.apps.erp.core.cadastros import empresas as svc
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            svc.definir_empresa_da_obra(s, obra_id, d.get("empresa_id"), atual)
            s.commit()
            return jsonify({"ok": True})
    except ErroNaoEncontrado as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao ligar obra e empresa")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# Disparar a cotação — e provar que disparou
# ---------------------------------------------------------------------------
@bp.route("/erp/api/suprimentos/cotacoes/<int:cotacao_id>/envio",
          methods=["GET", "POST"])
@login_obrigatorio
@permissao("comprar")
def api_cotacao_envio(cotacao_id: int):
    """GET mostra quem recebe, de qual empresa sai e o texto que vai.
    POST manda — um envio e um registro por fornecedor."""
    from app.apps.erp.core.suprimentos import envio as svc
    try:
        with get_session() as s:
            if request.method == "GET":
                return jsonify({"ok": True, **svc.preparar(s, cotacao_id)})
            atual = _usuario_logado(s)
            rel = svc.disparar(s, cotacao_id,
                               request.get_json(silent=True) or {}, atual)
            s.commit()
            return jsonify({"ok": True, **rel})
    except ErroNaoEncontrado as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao disparar a cotação")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/cotacoes/<int:cotacao_id>/envios")
@login_obrigatorio
@permissao("comprar")
def api_cotacao_envios(cotacao_id: int):
    """Tudo que já saiu desta cotação, com o texto exato e o resultado."""
    from app.apps.erp.core.comum import email as correio
    with get_session() as s:
        return jsonify({"ok": True,
                        "envios": correio.historico(s, "cotacao", cotacao_id)})


@bp.route("/erp/relatorios")
@login_obrigatorio
@permissao("ver_relatorios")
def pagina_relatorios():
    return render_template("erp_relatorios.html", **_contexto("relatorios"))


def _data_ou_nada(bruto: str | None):
    """Data vinda da tela, aceitando vazio e lixo sem estourar."""
    try:
        return date.fromisoformat((bruto or "").strip())
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Perguntar — o catálogo de perguntas respondido por CÓDIGO
#
# A rota é POR GRUPO de pergunta, e não uma só que despacha tudo. É o que
# permite cada grupo declarar a SUA ação e a declaração continuar verdadeira:
# uma rota única teria de conferir permissão por dentro, pergunta a pergunta,
# e aí a ação declarada mentiria.
#
# O grupo "financeiro" vive sob `ver_erp` + ESCOPO — todo operador pode
# perguntar, e cada um recebe a conta feita apenas sobre o que já poderia ver
# na tela de Títulos. Grupo novo (suprimentos, contratos) ganha rota própria.
# ---------------------------------------------------------------------------
@bp.route("/erp/perguntar")
@login_obrigatorio
@permissao("ver_erp")
def pagina_perguntar():
    return render_template("erp_perguntar.html", **_contexto("perguntar"))


@bp.route("/erp/api/perguntas/financeiro")
@login_obrigatorio
@permissao("ver_erp")
def api_perguntas_financeiro():
    """A lista do que dá para perguntar — sem tocar no banco."""
    from app.apps.erp.core.perguntas import catalogo
    return jsonify({"ok": True, "perguntas": catalogo.para_a_tela("financeiro")})


@bp.route("/erp/api/perguntar/financeiro", methods=["POST"])
@login_obrigatorio
@permissao("ver_erp")
def api_perguntar_financeiro():
    """Responde uma pergunta do grupo financeiro, no escopo de quem perguntou."""
    from app.apps.erp.core.perguntas import catalogo
    d = request.get_json(silent=True) or {}
    chave = (d.get("chave") or "").strip()
    if chave not in {p["chave"] for p in catalogo.do_grupo("financeiro")}:
        # Pergunta de outro grupo respondida aqui passaria por cima da ação
        # daquele grupo. Fora do alcance responde "não encontrado".
        raise ErroNaoEncontrado("Pergunta desconhecida neste grupo.")
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if atual is None:
                return jsonify({"ok": False, "erro": "Sessão expirada."}), 401
            resposta = catalogo.responder(chave, s, atual, d.get("parametros") or {})
        return jsonify({"ok": True, "resposta": resposta})
    except ErroNaoEncontrado:
        raise
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao responder a pergunta %s", chave)
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/perguntar/entender", methods=["POST"])
@login_obrigatorio
@permissao("ver_erp")
def api_entender_pergunta():
    """Da frase escrita para a pergunta que o sistema sabe responder.

    ELA NÃO RESPONDE NADA e não toca em dado nenhum — só diz QUAL pergunta é,
    e de que grupo. É por isso que ela pode ser aberta a todo operador sem
    mentir na declaração: quem responde continua sendo a rota do grupo, com a
    ação dela. Uma rota que recebesse a frase e já devolvesse o número teria
    de conferir permissão por dentro, pergunta a pergunta.

    O QUE ELA NÃO ENTENDE É GUARDADO. Essa lista é a coisa mais útil que sai
    daqui: é o que as pessoas querem saber e o sistema ainda não sabe
    responder — ou seja, o que construir em seguida.
    """
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.core.perguntas import catalogo, entender as svc_entender

    d = request.get_json(silent=True) or {}
    texto = (d.get("texto") or "").strip()
    if not texto:
        return jsonify({"ok": False, "erro": "Escreva a pergunta."}), 400
    # A última pergunta respondida nesta conversa, quando houver. É o que
    # permite "e da obra Triunfo?" — e a tela AVISA quando repete a anterior.
    leitura = svc_entender.entender(texto, catalogo.para_a_tela(),
                                    chave_anterior=(d.get("anterior") or "").strip())
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            registrar_evento(
                s, "pergunta", 0,
                "ENTENDIDA" if leitura["entendi"] else "NAO_ENTENDIDA",
                {"texto": texto[:500], "chave": leitura.get("chave"),
                 "confianca": leitura.get("confianca")},
                atual.id if atual else None)
            s.commit()
    except Exception:                       # pragma: no cover - log não trava
        logger.exception("ERP: falha ao registrar a pergunta escrita")
    return jsonify({"ok": True, "leitura": leitura})


@bp.route("/erp/api/perguntar/ouvir", methods=["POST"])
@login_obrigatorio
@permissao("ver_erp")
def api_pergunta_por_audio():
    """O áudio vira TEXTO. Só isso.

    A rota não responde pergunta nenhuma e não consulta dado de negócio — ela
    devolve a frase transcrita, que a tela põe na caixa de escrita para a
    pessoa LER antes de mandar responder. É esse passo que protege do erro de
    transcrição: "a pagar" e "apagar" soam igual, e uma pergunta mal ouvida
    respondida em silêncio seria o pior defeito possível aqui.

    Por não tocar em dado, ela pode exigir `ver_erp` sem mentir: quem responde
    continua sendo a rota do grupo, com a ação do grupo.
    """
    from app.apps.erp.core.perguntas.audio import ErroAudio, transcrever

    arquivo = request.files.get("audio")
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Não chegou áudio nenhum."}), 400
    # FORA do try: a recusa por teto tem resposta própria (402), e um
    # `except Exception` a transformaria em 'falha do sistema'.
    _exigir_saldo_de_ia()
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            usuario_id = atual.id if atual else None
        lido = transcrever(arquivo.read(), arquivo.filename or "pergunta.webm",
                           segundos=request.form.get("segundos"),
                           usuario_id=usuario_id)
        return jsonify({"ok": True, **lido})
    except ErroAudio as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao ouvir a pergunta")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/perguntar/documento", methods=["POST"])
@login_obrigatorio
@permissao("ver_erp")
def api_pergunta_com_documento():
    """Leitura de um documento ANEXADO à pergunta.

    ATENÇÃO À DIFERENÇA, que a tela também precisa deixar clara: tudo o mais
    nesta área é calculado pelo sistema sobre o banco, por código testado.
    AQUI a resposta vem da IA lendo o arquivo — e a IA lê errado às vezes.
    A diferença é aceitável porque **o documento está na mão de quem
    perguntou**: dá para conferir olhando o papel, o que não acontece com um
    total somado sobre dez mil lançamentos.

    Reusa o mesmo leitor do Arquivo (`core/documentos/leitor.py`), com os
    mesmos tetos de tamanho e o mesmo registro de consumo. Não grava nada: se
    a pessoa quiser guardar o documento, o caminho continua sendo o Arquivo.
    """
    from app.apps.erp.core.documentos.leitor import ErroLeitura, ler_documento

    arquivo = request.files.get("documento")
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Não chegou arquivo nenhum."}), 400
    nome = arquivo.filename or "documento"
    # A recusa do leitor é escrita para a tela do Arquivo ("preencha os campos
    # manualmente") e não quer dizer nada aqui, onde não há campo nenhum. Esta
    # tela tem o seu próprio jeito de dizer a mesma coisa.
    if not os.getenv("OPENAI_API_KEY", "").strip():
        return jsonify({"ok": False, "erro":
                        "A leitura de documento não está ligada neste sistema "
                        "(falta a chave do serviço). As perguntas sobre o que "
                        "já está no ERP continuam funcionando normalmente."}), 400
    # FORA do try: a recusa por teto tem resposta própria (402), e um
    # `except Exception` a transformaria em 'falha do sistema'.
    _exigir_saldo_de_ia()
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            usuario_id = atual.id if atual else None
        from app.apps.erp.core.comum import ia_custo
        with ia_custo.contexto(operacao="pergunta_com_documento",
                               usuario_id=usuario_id, referencia=nome[:120]):
            lido = ler_documento(arquivo.read(), nome)
        return jsonify({"ok": True, "documento": _resumo_do_documento(lido, nome)})
    except ErroLeitura as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao ler o documento anexado")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


def _resumo_do_documento(lido: dict, nome: str) -> dict:
    """O que interessa numa resposta de tela, sem despejar o retorno inteiro.

    DUAS REGRAS AQUI, e as duas já custaram defeito neste repositório:

    1. **Campo vazio vira AUSENTE, não "0" nem "—".** Preencher buraco com
       valor padrão é o jeito mais fácil de o sistema afirmar o que não sabe.
    2. **Nada sai no formato do banco.** "NFSE", "2026-09-02", "12480.00" e
       "11222333000144" são o computador falando. Quem formata é o servidor,
       por `core/comum/formato.py`, que é o mesmo lugar de onde saem o
       relatório e o PDF — assim o mesmo valor não aparece de dois jeitos em
       duas telas.
    """
    from app.apps.erp.core.comum.formato import (
        _dinheiro_br, data_br, documento_por_extenso,
    )
    from app.apps.erp.core.documentos.leitor import rotulo_do_tipo

    def dinheiro(v):
        formatado = _dinheiro_br(v)
        return f"R$ {formatado}" if formatado else ""

    # O terceiro item diz como formatar; o quarto, se é NÚMERO OU CÓDIGO — que
    # a tela escreve em fonte de largura fixa, para os dígitos alinharem e
    # dar para conferir de olho. Frase em fonte de máquina de escrever fica
    # difícil de ler, então nome e descrição ficam de fora.
    campos = [
        ("tipo_documento", "Tipo", rotulo_do_tipo, False),
        ("emitente_nome", "Quem emitiu", None, False),
        ("emitente_documento", "CNPJ/CPF de quem emitiu",
         documento_por_extenso, True),
        ("numero_documento", "Número", None, True),
        ("data_emissao", "Emissão", data_br, True),
        ("vencimento", "Vencimento", data_br, True),
        ("competencia", "Competência", data_br, True),
        ("valor_total", "Valor total", dinheiro, True),
        ("valor_liquido", "Valor líquido", dinheiro, True),
        ("descricao", "Descrição", None, False),
        ("obra_mencionada", "Obra citada", None, True),
        ("municipio_emissao", "Município", None, False),
    ]
    linhas = []
    for chave, rotulo, formatar, e_dado in campos:
        bruto = str(lido.get(chave) or "").strip()
        if bruto in ("", "None"):
            continue
        linhas.append({"rotulo": rotulo, "dados": e_dado,
                       "valor": (formatar(bruto) if formatar else bruto)})
    return {
        "arquivo": nome,
        "linhas": linhas,
        "observacoes": (lido.get("observacoes") or "").strip(),
        "modelo": lido.get("modelo") or "",
    }


@bp.route("/erp/api/perguntar/agendar", methods=["POST"])
@login_obrigatorio
@permissao("ver_erp")
def api_agendar_pergunta():
    """"Me manda isso toda segunda."

    A ação é `ver_erp` porque agendar para SI MESMO não dá acesso a nada novo:
    o relatório roda com a permissão de quem recebe, que é a mesma pessoa.
    Agendar para OUTRA pessoa é outra história, e o core exige `gerir_usuarios`
    para isso — senão qualquer um mandaria um relatório no nome do diretor, que
    rodaria com a permissão dele.
    """
    from app.apps.erp.core.perguntas import agendadas as svc

    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if atual is None:
                return jsonify({"ok": False, "erro": "Sessão expirada."}), 401
            a = svc.agendar(
                s, atual, chave=(d.get("chave") or "").strip(),
                titulo=(d.get("titulo") or "").strip(),
                parametros=d.get("parametros") or {},
                frequencia=(d.get("frequencia") or "SEMANAL"),
                dia=d.get("dia"), canal=(d.get("canal") or "TELEGRAM"),
                so_se_houver=bool(d.get("so_se_houver")),
                para_usuario_id=d.get("para_usuario_id"))
            quando = svc.quando_por_extenso(a.frequencia, a.dia)
            s.commit()
        return jsonify({"ok": True, "quando": quando,
                        "aviso": ("Combinado. Vou mandar pelo Telegram — "
                                  "confira se o seu telefone está no cadastro, "
                                  "senão não tenho por onde mandar.")})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao agendar a pergunta")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/perguntar/agendados")
@login_obrigatorio
@permissao("ver_erp")
def api_perguntas_agendadas():
    from app.apps.erp.core.perguntas import agendadas as svc
    with get_session() as s:
        atual = _usuario_logado(s)
        if atual is None:
            return jsonify({"ok": False, "erro": "Sessão expirada."}), 401
        return jsonify({"ok": True, "agendados": svc.listar(s, atual)})


@bp.route("/erp/api/perguntar/agendados/<int:agendada_id>/desligar",
          methods=["POST"])
@login_obrigatorio
@permissao("ver_erp")
def api_desligar_agendada(agendada_id: int):
    """Desliga um relatório automático SEU.

    O escopo é conferido no core: relatório de outra pessoa responde "não
    encontrado", nunca "sem permissão" — dizer "sem permissão" para um número
    que existe confirma que ele existe.
    """
    from app.apps.erp.core.auth.permissoes import exigir_agendada_no_escopo
    from app.apps.erp.core.perguntas import agendadas as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if atual is None:
                return jsonify({"ok": False, "erro": "Sessão expirada."}), 401
            exigir_agendada_no_escopo(s, atual, agendada_id)
            svc.desligar(s, atual, agendada_id)
            s.commit()
        return jsonify({"ok": True})
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha ao desligar o relatório automático")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/perguntas/nao-entendidas")
@login_obrigatorio
@permissao("ver_uso_da_equipe")
def api_perguntas_nao_entendidas():
    """O que perguntaram e o sistema não soube responder.

    É a lista do que falta construir, escrita pelas próprias pessoas que usam
    o ERP — vale mais que qualquer suposição minha sobre o que elas precisam.
    """
    from sqlalchemy import text as _sql
    try:
        with get_session() as s:
            linhas = s.execute(_sql("""
                SELECT e.detalhe->>'texto' AS texto,
                       COUNT(*)            AS vezes,
                       MAX(e.criado_em)    AS ultima
                  FROM eventos e
                 WHERE e.entidade_tipo = 'pergunta'
                   AND e.acao = 'NAO_ENTENDIDA'
                 GROUP BY e.detalhe->>'texto'
                 ORDER BY vezes DESC, ultima DESC
                 LIMIT 100""")).all()
        return jsonify({"ok": True, "perguntas": [
            {"texto": t, "vezes": v,
             "ultima": u.astimezone().strftime("%d/%m/%Y") if u else None}
            for t, v, u in linhas]})
    except Exception as e:
        logger.exception("ERP: falha ao ler as perguntas não entendidas")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/perguntas/suprimentos")
@login_obrigatorio
@permissao("ver_suprimentos")
def api_perguntas_suprimentos():
    from app.apps.erp.core.perguntas import catalogo
    return jsonify({"ok": True, "perguntas": catalogo.para_a_tela("suprimentos")})


@bp.route("/erp/api/perguntar/suprimentos", methods=["POST"])
@login_obrigatorio
@permissao("ver_suprimentos")
def api_perguntar_suprimentos():
    """Grupo de suprimentos: catálogo de insumos, preços e fila de pedidos.

    O CATÁLOGO é cadastro da empresa e não se recorta por obra. Já a fila de
    pedidos passa pelo mesmo filtro por pessoa da tela de Solicitações — quem
    é preso a uma obra vê só a dela.
    """
    from app.apps.erp.core.perguntas import catalogo
    d = request.get_json(silent=True) or {}
    chave = (d.get("chave") or "").strip()
    if chave not in {p["chave"] for p in catalogo.do_grupo("suprimentos")}:
        raise ErroNaoEncontrado("Pergunta desconhecida neste grupo.")
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if atual is None:
                return jsonify({"ok": False, "erro": "Sessão expirada."}), 401
            resposta = catalogo.responder(chave, s, atual, d.get("parametros") or {})
        return jsonify({"ok": True, "resposta": resposta})
    except ErroNaoEncontrado:
        raise
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao responder a pergunta %s", chave)
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/perguntas/obras")
@login_obrigatorio
@permissao("ver_erp")
def api_perguntas_obras():
    from app.apps.erp.core.perguntas import catalogo
    return jsonify({"ok": True, "perguntas": catalogo.para_a_tela("obras")})


@bp.route("/erp/api/perguntar/obras", methods=["POST"])
@login_obrigatorio
@permissao("ver_erp")
def api_perguntar_obras():
    """Grupo de obras: as conferências de cadastro, prazo e garantia.

    Mesma ação do grupo financeiro (`ver_erp`) e mesmo recorte por obra
    designada. O que separa os dois é o ASSUNTO, não a permissão.

    Este grupo NÃO responde custo nem resultado de obra — essas palavras ainda
    não têm uma definição combinada com o dono, e responder seria escolher uma
    leitura por ele em silêncio.
    """
    from app.apps.erp.core.perguntas import catalogo
    d = request.get_json(silent=True) or {}
    chave = (d.get("chave") or "").strip()
    if chave not in {p["chave"] for p in catalogo.do_grupo("obras")}:
        raise ErroNaoEncontrado("Pergunta desconhecida neste grupo.")
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if atual is None:
                return jsonify({"ok": False, "erro": "Sessão expirada."}), 401
            resposta = catalogo.responder(chave, s, atual, d.get("parametros") or {})
        return jsonify({"ok": True, "resposta": resposta})
    except ErroNaoEncontrado:
        raise
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao responder a pergunta %s", chave)
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/perguntas/documentos")
@login_obrigatorio
@permissao("ver_arquivo")
def api_perguntas_documentos():
    from app.apps.erp.core.perguntas import catalogo
    return jsonify({"ok": True, "perguntas": catalogo.para_a_tela("documentos")})


@bp.route("/erp/api/perguntar/documentos", methods=["POST"])
@login_obrigatorio
@permissao("ver_arquivo")
def api_perguntar_documentos():
    """O que está ESCRITO nos documentos da empresa.

    A ação é `ver_arquivo`, e não `ver_erp` como os outros grupos: quem não
    pode abrir o acervo também não pode perguntar o que está escrito dentro
    dele. Por dentro, a busca passa pelo MESMO recorte da tela do Arquivo —
    faixa de sigilo mais obra designada.

    Esta é a única família de perguntas que não calcula nada: ela devolve
    trechos, com o documento de onde saíram.
    """
    from app.apps.erp.core.perguntas import catalogo
    d = request.get_json(silent=True) or {}
    chave = (d.get("chave") or "").strip()
    if chave not in {p["chave"] for p in catalogo.do_grupo("documentos")}:
        raise ErroNaoEncontrado("Pergunta desconhecida neste grupo.")
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if atual is None:
                return jsonify({"ok": False, "erro": "Sessão expirada."}), 401
            resposta = catalogo.responder(chave, s, atual, d.get("parametros") or {})
        return jsonify({"ok": True, "resposta": resposta})
    except ErroNaoEncontrado:
        raise
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao procurar nos documentos")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/documentos/<int:documento_id>/arquivo", methods=["POST"])
@login_obrigatorio
@permissao("arquivar")
def api_documento_substituir_arquivo(documento_id: int):
    """Troca o ARQUIVO de um documento já arquivado, mantendo o registro.

    Dono, 17/09/2026: *"anexei uma ART e depois percebi que era o documento
    errado, e queria substituir"*. O tipo, o dono e as datas continuam; o
    arquivo anterior é apagado e a trilha guarda o retrato dele.

    Mesma ação de guardar (`arquivar`) e mesmo recorte por escopo: ter a ação
    não é alcançar ESTE documento.
    """
    from app.apps.erp.core.arquivo import service as svc_arq
    f = request.files.get("arquivo")
    if f is None:
        return jsonify({"ok": False, "erro": "Escolha o arquivo novo."}), 400
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            svc_arq.exigir_documento_no_escopo(s, usuario, documento_id)
            doc = svc_arq.substituir_arquivo(
                s, documento_id, f.read(), f.filename or "arquivo",
                motivo=(request.form.get("motivo") or ""), usuario=usuario)
            linha = svc_arq.ler(s, doc)
            s.commit()
        return jsonify({"ok": True, "documento": linha})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/documentos/<int:documento_id>/datas", methods=["POST"])
@login_obrigatorio
@permissao("arquivar")
def api_documento_corrigir_datas(documento_id: int):
    """Corrige as datas de um documento já arquivado.

    Dono, 17/09/2026: *"eu vi um contrato que está dando que está vencido, mas
    na verdade está vencido porque eu escrevi a validade errado"*. Sem isto, uma
    data digitada errada ficava errada para sempre — e o aviso de vencimento
    junto com ela, o que ensina a equipe a ignorar aviso.

    A ação é `arquivar`, a mesma de guardar o documento, e por dentro ainda
    passa por `exigir_documento_no_escopo`: ter a ação não é alcançar ESTE
    documento. Fora do recorte responde "não encontrado", nunca "sem permissão".
    """
    from app.apps.erp.core.arquivo import service as svc_arq
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            svc_arq.exigir_documento_no_escopo(s, usuario, documento_id)
            doc = svc_arq.corrigir_datas(
                s, documento_id, emissao=d.get("emissao"),
                validade=d.get("validade"), competencia=d.get("competencia"),
                referencia=d.get("referencia"), usuario=usuario)
            linha = svc_arq.ler(s, doc)
            s.commit()
        return jsonify({"ok": True, "documento": linha})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/documentos/<int:documento_id>/perguntar", methods=["POST"])
@login_obrigatorio
@permissao("ver_arquivo")
def api_perguntar_sobre_documento(documento_id: int):
    """Perguntar sobre UM documento do acervo — a IA lê o texto dele.

    A ação é `ver_arquivo`, a mesma da tela do acervo, e por dentro ainda passa
    por `exigir_documento_no_escopo`: ter a ação não é alcançar ESTE documento.
    Fora do recorte responde 'não encontrado', nunca 'sem permissão'.

    A resposta traz os trechos que o SISTEMA conferiu dentro do documento —
    ver o porquê inteiro em `core/perguntas/documentos.py`.
    """
    from app.apps.erp.core.perguntas import documentos as svc_doc

    d = request.get_json(silent=True) or {}
    pergunta = (d.get("pergunta") or "").strip()
    # FORA do try: a recusa por teto tem resposta própria (402), e o
    # `except Exception` lá embaixo a transformaria em 'falha do sistema'.
    _exigir_saldo_de_ia()
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if atual is None:
                return jsonify({"ok": False, "erro": "Sessão expirada."}), 401
            resposta = svc_doc.perguntar_sobre(s, atual,
                                               documento_id=documento_id,
                                               pergunta=pergunta)
            s.commit()
        return jsonify({"ok": True, "resposta": resposta})
    except ErroNaoEncontrado:
        raise
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao perguntar sobre o documento %s",
                         documento_id)
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/perguntas/contratos")
@login_obrigatorio
@permissao("ver_contratos")
def api_perguntas_contratos():
    from app.apps.erp.core.perguntas import catalogo
    return jsonify({"ok": True, "perguntas": catalogo.para_a_tela("contratos")})


@bp.route("/erp/api/perguntar/contratos", methods=["POST"])
@login_obrigatorio
@permissao("ver_contratos")
def api_perguntar_contratos():
    """Grupo de contratos: medição, faturamento e recebimento.

    Rota separada da do financeiro porque a AÇÃO é outra — e mais estreita. O
    quadro mostra o contrato de ponta a ponta e não se recorta por obra
    designada sem mentir no total.
    """
    from app.apps.erp.core.perguntas import catalogo
    d = request.get_json(silent=True) or {}
    chave = (d.get("chave") or "").strip()
    if chave not in {p["chave"] for p in catalogo.do_grupo("contratos")}:
        raise ErroNaoEncontrado("Pergunta desconhecida neste grupo.")
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if atual is None:
                return jsonify({"ok": False, "erro": "Sessão expirada."}), 401
            resposta = catalogo.responder(chave, s, atual, d.get("parametros") or {})
        return jsonify({"ok": True, "resposta": resposta})
    except ErroNaoEncontrado:
        raise
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao responder a pergunta %s", chave)
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# Trabalho no sistema — a trilha de auditoria lida como entrega
#
# São TRÊS rotas e não uma, e isso é de propósito. A própria semana é aberta a
# todo operador; a semana dos OUTROS exige `ver_uso_da_equipe`. Juntar as duas
# numa rota só obrigaria a conferir por dentro qual é o caso — e rota que
# declara uma ação e confere outra faz a declaração mentir.
# ---------------------------------------------------------------------------
@bp.route("/erp/uso")
@login_obrigatorio
@permissao("ver_erp")
def pagina_uso():
    return render_template("erp_uso.html", **_contexto("uso"))


@bp.route("/erp/api/uso/minha-semana")
@login_obrigatorio
@permissao("ver_erp")
def api_minha_semana():
    """A semana de QUEM PEDIU. Não aceita o número de outra pessoa — é o que
    permite esta rota ser aberta a todo operador sem abrir a dos outros."""
    from app.apps.erp.core.comum import uso as svc_uso
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if atual is None:
                return jsonify({"ok": False, "erro": "Sessão expirada."}), 401
            dados = svc_uso.semana_da_pessoa(
                s, atual.id, de=_data_ou_nada(request.args.get("de")),
                ate=_data_ou_nada(request.args.get("ate")))
            dados["nome"] = atual.nome
            # O passo a passo do PRÓPRIO dia também é de todo operador: quem
            # não pode ver o próprio trabalho em detalhe não consegue conferir
            # o que a tela diz sobre ele.
            dia = _data_ou_nada(request.args.get("dia"))
            detalhe = svc_uso.detalhe_do_dia(s, atual.id, dia) if dia else []
        return jsonify({"ok": True, "semana": dados, "detalhe": detalhe})
    except Exception as e:
        logger.exception("ERP: falha ao ler a própria semana")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/uso/equipe")
@login_obrigatorio
@permissao("ver_uso_da_equipe")
def api_uso_da_equipe():
    """Uma linha por pessoa no período, mais o que o sistema fez sozinho."""
    from app.apps.erp.core.comum import uso as svc_uso
    try:
        with get_session() as s:
            dados = svc_uso.semana_da_equipe(
                s, de=_data_ou_nada(request.args.get("de")),
                ate=_data_ou_nada(request.args.get("ate")))
        return jsonify({"ok": True, "equipe": dados})
    except Exception as e:
        logger.exception("ERP: falha ao ler o trabalho da equipe")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/uso/pessoa/<int:usuario_id>")
@login_obrigatorio
@permissao("ver_uso_da_equipe")
def api_uso_da_pessoa(usuario_id: int):
    """A semana de uma pessoa, e o passo a passo de um dia dela."""
    from app.apps.erp.core.comum import uso as svc_uso
    from app.apps.erp.db.models.cadastros import Usuario as U
    try:
        with get_session() as s:
            pessoa = s.get(U, usuario_id)
            if pessoa is None:
                raise ErroNaoEncontrado("Operador não encontrado.")
            semana = svc_uso.semana_da_pessoa(
                s, usuario_id, de=_data_ou_nada(request.args.get("de")),
                ate=_data_ou_nada(request.args.get("ate")))
            semana["nome"] = pessoa.nome
            dia = _data_ou_nada(request.args.get("dia"))
            detalhe = svc_uso.detalhe_do_dia(s, usuario_id, dia) if dia else []
        return jsonify({"ok": True, "semana": semana, "detalhe": detalhe})
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha ao ler o trabalho da pessoa")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/prestacao")
@login_obrigatorio
@permissao("ver_fundo_fixo")
def pagina_prestacao():
    return render_template("erp_prestacao.html", **_contexto("prestacao"))


@bp.route("/erp/importar")
@login_obrigatorio
@permissao("importar")
def pagina_importar():
    return render_template("erp_importar.html", **_contexto("importar"))


@bp.route("/erp/configuracoes")
@login_obrigatorio
@permissao("configurar")
def pagina_config():
    return render_template("erp_config.html", **_contexto("config"))


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------
def _colaboradores_do_titulo(s, t) -> list[dict]:
    """Quem são as pessoas por trás deste pagamento."""
    from sqlalchemy import select as _sel
    from app.apps.erp.db.models.cadastros import Colaborador
    from app.apps.erp.db.models.financeiro import TituloColaborador
    saida = []
    if getattr(t, "colaborador_id", None):
        c = s.get(Colaborador, t.colaborador_id)
        if c is not None:
            saida.append({"id": c.id, "nome": c.nome, "cpf": c.cpf,
                          "valor": float(t.valor_liquido)})
    for v in s.scalars(_sel(TituloColaborador).where(
            TituloColaborador.titulo_id == t.id)).all():
        c = s.get(Colaborador, v.colaborador_id)
        if c is not None:
            saida.append({"id": c.id, "nome": c.nome, "cpf": c.cpf,
                          "valor": float(v.valor) if v.valor else None,
                          "observacao": v.observacao})
    return saida


def _explicar_status(s, t) -> str:
    """Em português: por que este título está parado onde está."""
    from app.apps.erp.db.models.financeiro import StatusTitulo as _S
    if t.status == _S.AGUARDANDO_AVAL:
        solicitante = s.get(Usuario, t.solicitante_id)
        perfil = solicitante.perfil.value.replace("_", " ").lower() if solicitante else "?"
        return (f"Lançado por {solicitante.nome if solicitante else '—'} ({perfil}), "
                f"que exige confirmação de uma segunda pessoa. Aguarda o aval de um "
                f"supervisor da obra, gestor de obras ou diretor financeiro — na aba Confirmar.")
    if t.status == _S.AGUARDANDO_APROVACAO:
        return ("Passou na análise e aguarda a liberação para pagamento pelo financeiro "
                "(botão 'Liberar p/ pagamento' na lista).")
    if t.status == _S.BLOQUEADO:
        return (f"A análise automática apontou risco {t.score_risco}. Revise os apontamentos "
                f"abaixo; quem tem alçada pode liberar assumindo o registro.")
    if t.status == _S.APROVADO:
        return "Liberado — aparece na aba Pagamentos, pronto para ser pago."
    if t.status == _S.DEVOLVIDO:
        return "Devolvido a quem lançou; veja o motivo no histórico."
    if t.status == _S.PAGO_PARCIAL:
        return "Parte das parcelas já foi paga; o restante segue em Pagamentos."
    return ""


def _serializar(t, hoje: date, ver_pagamento: bool = True) -> dict:
    venc = min((p.vencimento for p in t.parcelas), default=None)
    obras = sorted({r.obra.codigo for r in t.rateios if r.obra})
    return {
        "obra": " + ".join(obras) if obras else "",
        "id": t.id,
        "numero_sp": t.numero_sp,
        "fornecedor": t.fornecedor.razao_social,
        "descricao": t.descricao,
        "categoria": f"{t.categoria.codigo} · {t.categoria.descricao}",
        "valor_liquido": float(t.valor_liquido),
        "competencia": t.competencia.strftime("%m/%Y"),
        "vencimento": venc.isoformat() if venc else None,
        "vencido": bool(venc and venc < hoje and t.status.value in _ABERTOS),
        "parcelas": len(t.parcelas),
        "risco": t.score_risco or 0,
        "status": t.status.value,
        "dedutibilidade": (t.dedutibilidade.value if hasattr(t.dedutibilidade, "value")
                           else str(t.dedutibilidade)),
        "exige_aval": bool(getattr(t, "exige_aval", False)),
        "avalizado": bool(getattr(t, "avalizado_em", None)),
        "ver_pagamento": ver_pagamento,
        # ENCADEAMENTO: os números que ligam este título aos cadastros. São
        # inteiros que já estão carregados — não custam consulta nenhuma.
        "fornecedor_id": t.fornecedor_id,
        "categoria_id": t.categoria_id,
        "obra_ids": sorted({r.obra_id for r in t.rateios if r.obra_id}),
        "pedido_id": getattr(t, "pedido_id", None),
        # A competência em AAAA-MM: é o que o bloco de documentos precisa para
        # montar a pasta que o cliente pede junto com a medição.
        "competencia_iso": t.competencia.strftime("%Y-%m"),
        "numero_medicao": getattr(t, "numero_medicao", None),
    }


@bp.route("/erp/api/titulos")
@login_obrigatorio
@permissao("ver_erp")
def api_titulos():
    busca = (request.args.get("busca") or "").strip()
    status = [s for s in (request.args.get("status") or "").split(",") if s]
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            from app.apps.erp.core.titulos.aval import pode_ver_dados_pagamento
            ver_pg = pode_ver_dados_pagamento(usuario)
            filtros = {"busca": busca, "status": status, "usuario": usuario}
            # A PÁGINA e as SOMAS saem da MESMA consulta filtrada. Antes o
            # filtro de situação era aplicado em Python DEPOIS de trazer os 500
            # títulos mais novos — filtrar por "bloqueado" não achava nada se
            # os 500 mais novos não tivessem nenhum —, e as somas do topo
            # somavam só esses 500 e se apresentavam como "total".
            pag = svc_titulos.pagina_de_titulos(
                s, pagina=request.args.get("pagina"),
                tamanho=request.args.get("tamanho"), **filtros)
            resumo = svc_titulos.somar_titulos(s, **filtros)
            hoje = date.today()
            linhas = [_serializar(t, hoje, ver_pg) for t in pag["itens"]]
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao listar títulos")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500

    return jsonify({"ok": True, "titulos": linhas, "resumo": resumo,
                    "pagina": {k: pag[k] for k in
                               ("pagina", "tamanho", "total", "paginas",
                                "tem_mais", "de", "ate", "resumo")}})


def _conta_do_credor_do_titulo(s, t) -> dict | None:
    """A conta bancária que este título vai pagar, com a situação dela.

    Só o necessário para o card decidir se mostra o botão de homologar: id,
    situação e se QUEM ESTÁ OLHANDO pode homologar. Dado bancário em si não
    passa por aqui — ele tem tela própria, com permissão própria.
    """
    from app.apps.erp.core.auth.permissoes import pode
    from app.apps.erp.core.cadastros import fornecedores as svc_forn
    from app.apps.erp.db.models.cadastros import FornecedorConta

    if not getattr(t, "fornecedor_conta_id", None):
        return None
    conta = s.get(FornecedorConta, t.fornecedor_conta_id)
    if conta is None:
        return None
    atual = _usuario_logado(s)
    autor = svc_forn.quem_cadastrou_a_conta(s, conta.id)
    proprio = autor is not None and atual is not None and autor == atual.id
    return {
        "id": conta.id,
        "status": conta.status.value if conta.status else None,
        "forma": conta.forma.value if conta.forma else None,
        "posso_homologar": bool(pode(atual, "homologar_conta_credor")) and not proprio,
        "eu_que_cadastrei": proprio,
    }


def _pedido_do_titulo(s, t) -> dict | None:
    """O pedido de compra que originou o título, para virar link na ficha.

    Só o número e o id — a ficha do pedido inteiro mora em Suprimentos, e é
    para lá que o elo leva.
    """
    if not getattr(t, "pedido_id", None):
        return None
    from app.apps.erp.db.models.financeiro import Pedido
    p = s.get(Pedido, t.pedido_id)
    return {"id": p.id, "numero": p.numero} if p else None


def _origem_do_titulo(s, t) -> dict | None:
    """O contrato que originou o título, quando ele nasceu de uma locação.

    Devolve o suficiente para a tela dizer o que é e mandar a pessoa para a
    gestão: número do contrato, locadora, obra, competência e o endereço da
    tela de Locações.
    """
    parcela_id = getattr(t, "locacao_parcela_id", None)
    if not parcela_id:
        return None
    from app.apps.erp.db.models.cadastros import Fornecedor, Obra
    from app.apps.erp.db.models.financeiro import ContratoLocacao, LocacaoParcela
    parcela = s.get(LocacaoParcela, parcela_id)
    if parcela is None:
        return None
    contrato = s.get(ContratoLocacao, parcela.contrato_id)
    if contrato is None:
        return None
    obra = s.get(Obra, contrato.obra_id) if contrato.obra_id else None
    forn = s.get(Fornecedor, contrato.fornecedor_id) if contrato.fornecedor_id else None
    return {
        "tipo": "LOCACAO",
        "rotulo": "Locação de equipamento",
        "contrato": contrato.numero,
        "contrato_externo": contrato.numero_externo,
        "contrato_id": contrato.id,
        "locadora": getattr(forn, "razao_social", ""),
        "obra": getattr(obra, "codigo", ""),
        "competencia": parcela.competencia.strftime("%m/%Y"),
        "valor_previsto": float(parcela.valor_previsto or 0),
        "situacao_contrato": contrato.status,
        "link": url_for("erp.pagina_locacoes") + f"?contrato={contrato.id}",
    }


# ---------------------------------------------------------------------------
# Exportar o que está na tela
#
# UMA ROTA SÓ, e não uma por tela. A tela já tem as linhas filtradas na frente
# do usuário — ela manda de volta o que está mostrando, e aqui isso vira .xlsx
# ou .pdf. Assim "exportar" passa a existir em toda tela sem escrever código
# de servidor nenhum para cada uma, e o arquivo é exatamente o que a pessoa
# estava vendo, com os filtros dela impressos no cabeçalho.
# ---------------------------------------------------------------------------
LIMITE_EXPORTACAO = 50000


@bp.route("/erp/api/exportar", methods=["POST"])
@login_obrigatorio
@permissao("ver_erp")
def api_exportar():
    """Recebe o que está na tela e devolve a planilha ou o PDF."""
    from app.apps.erp.core.comum import exportar as svc

    d = request.get_json(silent=True) or {}
    formato = (d.get("formato") or "xlsx").lower()
    if formato not in ("xlsx", "pdf"):
        return jsonify({"ok": False, "erro": "Formato deve ser xlsx ou pdf."}), 400

    linhas = d.get("linhas") or []
    if len(linhas) > LIMITE_EXPORTACAO:
        return jsonify({"ok": False,
                        "erro": f"São {len(linhas)} linhas. Filtre um pouco antes "
                                f"de exportar — o teto é {LIMITE_EXPORTACAO}."}), 400
    colunas = d.get("colunas") or []
    if not colunas:
        return jsonify({"ok": False, "erro": "Nada para exportar."}), 400

    titulo = (d.get("titulo") or "Relatório").strip()[:120]
    quem = session.get("erp_usuario_nome", "")
    try:
        if formato == "xlsx":
            conteudo = svc.para_excel(
                titulo, colunas, linhas, subtitulo=(d.get("subtitulo") or "")[:200],
                filtros=d.get("filtros"), quem=quem)
            tipo = ("application/vnd.openxmlformats-officedocument"
                    ".spreadsheetml.sheet")
        else:
            conteudo = svc.para_pdf(
                titulo, colunas, linhas, subtitulo=(d.get("subtitulo") or "")[:200],
                filtros=d.get("filtros"), quem=quem, paisagem=d.get("paisagem"))
            tipo = "application/pdf"
    except Exception as e:
        logger.exception("ERP: falha ao exportar %s em %s", titulo, formato)
        return jsonify({"ok": False, "erro": f"Não deu para gerar o arquivo: {e}"}), 500

    from flask import Response
    nome = svc.nome_de_arquivo(titulo, formato)
    return Response(conteudo, mimetype=tipo, headers={
        "Content-Disposition": f'attachment; filename="{nome}"',
        "Content-Length": str(len(conteudo)),
    })


@bp.route("/erp/api/titulos/<int:titulo_id>")
@login_obrigatorio
@permissao("ver_erp")
def api_titulo_detalhe(titulo_id: int):
    from sqlalchemy import select
    from app.apps.erp.db.models.cadastros import ContaBancaria
    from app.apps.erp.db.models.financeiro import Analise, Evento, Parcela
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_titulo_no_escopo
            exigir_titulo_no_escopo(s, _usuario_logado(s), titulo_id)
            t = svc_titulos.obter(s, titulo_id)
            analise = s.scalars(select(Analise).where(Analise.titulo_id == t.id)
                                .order_by(Analise.executada_em.desc())).first()
            eventos = s.scalars(select(Evento).where(
                Evento.entidade_tipo == "titulo", Evento.entidade_id == t.id)
                .order_by(Evento.criado_em)).all()
            from app.apps.erp.core.titulos.aval import (
                historico_avais, pode_ver_dados_pagamento,
            )
            ver_pg = pode_ver_dados_pagamento(_usuario_logado(s))
            from app.apps.erp.core.documentos.armazenamento import listar as listar_anexos
            from app.apps.erp.db.models.financeiro import Pagamento as _Pg
            pagamentos = s.execute(
                select(_Pg, ContaBancaria.descricao, Usuario.nome)
                .join(Parcela, Parcela.id == _Pg.parcela_id)
                .join(ContaBancaria, ContaBancaria.id == _Pg.conta_bancaria_id, isouter=True)
                .join(Usuario, Usuario.id == _Pg.executado_por, isouter=True)
                .where(Parcela.titulo_id == t.id)
                .order_by(_Pg.data_pagamento)).all()
            solicitante = s.get(Usuario, t.solicitante_id)
            dados = {
                "cabecalho": _serializar(t, date.today(), ver_pg),
                # DE ONDE ESTE TÍTULO VEIO. Sem isto, a conta de aluguel que
                # chega todo mês é uma despesa órfã: quem confere não sabe de
                # qual contrato é, nem se o valor bate com o que está em obra.
                "origem": _origem_do_titulo(s, t),
                "pode_editar": ver_pg,
                "avais": historico_avais(s, t.id),
                "solicitante": solicitante.nome if solicitante else "—",
                "modalidade": getattr(t, "modalidade", "NORMAL"),
                "porque_status": _explicar_status(s, t),
                # ENCADEAMENTO: o pedido de compra que originou este título.
                "pedido": _pedido_do_titulo(s, t),
                "anexos": listar_anexos(s, "titulo", t.id),
                "colaboradores": _colaboradores_do_titulo(s, t),
                "pagamentos": [{
                    "id": pg.id, "parcela_id": pg.parcela_id,
                    "data": pg.data_pagamento.isoformat(),
                    "valor": float(pg.valor_pago),
                    "meio": pg.meio.value if hasattr(pg.meio, "value") else str(pg.meio),
                    "conta": conta or "—", "por": quem or "sistema",
                    "comprovante_id": pg.comprovante_anexo_id,
                } for pg, conta, quem in pagamentos],
                "bruto": float(t.valor_bruto),
                "retencoes_total": float(t.valor_retencoes),
                "dedutivel": t.dedutivel,
                "forma_pagamento": t.forma_pagamento.value if ver_pg else "—",
                "parcelas": [{"parcela_id": p.id, "numero": p.numero,
                              "vencimento": p.vencimento.strftime("%d/%m/%Y"),
                              "valor": float(p.valor), "status": p.status.value,
                              "boleto": ((p.linha_digitavel or "")[:24] if ver_pg
                                         else ("informado" if p.linha_digitavel else ""))}
                             for p in t.parcelas],
                "rateios": [{"obra": f"{r.obra.codigo} · {r.obra.nome}",
                             "obra_id": r.obra_id,
                             "categoria_id": getattr(r, "categoria_id", None),
                             "categoria": (f"{r.categoria.codigo} · {r.categoria.descricao}"
                                           if getattr(r, "categoria", None) else None),
                             "descricao": r.descricao,
                             "valor": float(r.valor),
                             "percentual": float(r.percentual or 0)} for r in t.rateios],
                "retencoes": [{"tipo": r.tipo.value, "base": float(r.base_calculo),
                               "aliquota": float(r.aliquota), "valor": float(r.valor)}
                              for r in t.retencoes],
                "criticas": (analise.criticas if analise else []) or [],
                # A CONTA DO CREDOR que este título usa, e se ela ainda espera
                # conferência (22/09/2026). É o que permite ao card mostrar o
                # botão de homologar bem onde a pessoa esbarra no bloqueio, em
                # vez de mandá-la procurar a tela do credor e voltar.
                "conta_do_credor": _conta_do_credor_do_titulo(s, t),
                "trilha": [{"quando": e.criado_em.strftime("%d/%m/%Y %H:%M"),
                            "acao": e.acao, "detalhe": e.detalhe} for e in eventos],
            }
        return jsonify({"ok": True, "titulo": dados})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha no detalhe do título %s", titulo_id)
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/titulos/<int:titulo_id>/cancelar", methods=["POST"])
@login_obrigatorio
@permissao("cancelar_titulo")
def api_cancelar_titulo(titulo_id: int):
    """Cancela UM título, com motivo — e avisa quem lançou.

    A ação é `cancelar_titulo`, própria, e não `aprovar`: desde 12/09/2026 quem
    LANÇOU cancela o próprio lançamento sem depender do financeiro, por decisão
    do dono — *"liberado o lançamento que não está baixado ou conciliado"*.
    Quem tem `lancar` recebe esta ação por implicação.

    Por dentro, o serviço confere o ESCOPO (fora do recorte responde "não
    encontrado") e, para quem não aprova, que o título seja dele.

    O aviso sai DEPOIS do commit, de propósito: mensagem enviada sobre um
    cancelamento que não gravou seria pior que aviso nenhum.
    """
    from app.apps.erp.core.auth.permissoes import exigir_titulo_no_escopo

    d = request.get_json(silent=True) or {}
    motivo = (d.get("motivo") or "").strip()
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if usuario is None:
                return jsonify({"ok": False, "erro": "Sessão expirada."}), 401
            # O escopo aparece AQUI, e não só lá dentro, porque é aqui que a
            # varredura estrutural da suíte procura — rota que recebe o número
            # de um registro e é aberta a perfil preso a obra ou a autoria tem
            # de conferir o recorte. O serviço confere de novo; a repetição é
            # barata e a ausência seria invisível.
            exigir_titulo_no_escopo(s, usuario, titulo_id)
            t = svc_titulos.cancelar(s, titulo_id, motivo, usuario)
            numero = t.numero_sp
            quem_id = usuario.id
            s.commit()
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except (ErroValidacao, ErroPermissao) as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao cancelar o título %s", titulo_id)
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500

    aviso = {}
    try:
        from app.apps.erp.core import notificacoes
        with get_session() as s:
            aviso = notificacoes.avisar_cancelamento(
                s, titulo_id, motivo=motivo, cancelado_por=s.get(Usuario, quem_id))
            s.commit()
    except Exception:
        # O cancelamento já está gravado. Aviso que não saiu fica no histórico
        # de notificações como falha, e dá para reenviar — derrubar a resposta
        # por causa dele faria a pessoa cancelar de novo.
        logger.exception("ERP: título %s cancelado, mas o aviso não saiu", numero)
        aviso = {"ok": False, "motivo": "o aviso não saiu; o cancelamento foi gravado"}

    return jsonify({"ok": True, "numero_sp": numero, "aviso": aviso})


# ---------------------------------------------------------------------------
# ENCAMINHAR INFORMAÇÃO (12/09/2026)
#
# Pedido do dono: um botão para mandar a informação de um lançamento — ou de um
# documento do acervo — para um operador ou para um número avulso, por
# WhatsApp. Ver o porquê inteiro, e as três travas, em `core/encaminhar.py`.
# ---------------------------------------------------------------------------
@bp.route("/erp/api/encaminhar/destinos")
@login_obrigatorio
@permissao("encaminhar")
def api_encaminhar_destinos():
    """Os operadores que a tela oferece como destino."""
    from app.apps.erp.core import encaminhar as svc

    with get_session() as s:
        return jsonify({"ok": True, "pessoas": svc.destinos_possiveis(s)})


@bp.route("/erp/api/encaminhar/<tipo>/<int:registro_id>/previa")
@login_obrigatorio
@permissao("encaminhar")
def api_encaminhar_previa(tipo: str, registro_id: int):
    """O texto que SERÁ enviado — última chance de ver que é o errado."""
    from app.apps.erp.core import encaminhar as svc

    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if atual is None:
                return jsonify({"ok": False, "erro": "Sessão expirada."}), 401
            svc.exigir_registro_no_escopo(s, atual, tipo, registro_id)
            return jsonify({"ok": True, **svc.montar(s, atual, tipo=tipo,
                                                     registro_id=registro_id)})
    except ErroNaoEncontrado:
        raise
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha na prévia do encaminhamento")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/encaminhar/<tipo>/<int:registro_id>", methods=["POST"])
@login_obrigatorio
@permissao("encaminhar")
def api_encaminhar(tipo: str, registro_id: int):
    """Dispara o encaminhamento e devolve o que saiu e o que não saiu."""
    from app.apps.erp.core import encaminhar as svc

    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if atual is None:
                return jsonify({"ok": False, "erro": "Sessão expirada."}), 401
            svc.exigir_registro_no_escopo(s, atual, tipo, registro_id)
            r = svc.enviar(s, atual, tipo=tipo, registro_id=registro_id,
                           usuarios=[int(x) for x in (d.get("usuarios") or [])],
                           numeros=[str(x) for x in (d.get("numeros") or [])],
                           com_anexo=bool(d.get("com_anexo")),
                           observacao=str(d.get("observacao") or ""))
            s.commit()
        return jsonify({"ok": True, "resultado": r})
    except ErroNaoEncontrado:
        raise
    except (ErroValidacao, ErroPermissao) as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao encaminhar %s %s", tipo, registro_id)
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/titulos/acao", methods=["POST"])
@login_obrigatorio
@permissao("aprovar")
def api_acao_lote():
    payload = request.get_json(silent=True) or {}
    acao = (payload.get("acao") or "").strip()
    ids = payload.get("ids") or []
    motivo = (payload.get("motivo") or "").strip()
    # "reanalisar" (22/09/2026) é a saída do beco do título BLOQUEADO por conta
    # do credor ainda não homologada: homologa-se a conta e manda reanalisar,
    # em vez de refazer o lançamento inteiro. Ele NÃO aprova — devolve o título
    # para a fila de aprovação, que continua sendo de outra pessoa.
    if acao not in ("aprovar", "devolver", "cancelar", "reanalisar"):
        return jsonify({"ok": False, "erro": f"Ação inválida: {acao!r}"}), 400
    if not ids:
        return jsonify({"ok": False, "erro": "Nenhum título selecionado."}), 400

    oks, erros = [], []
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if usuario is None:
                return jsonify({"ok": False, "erro": "Sessão expirada."}), 401
            for tid in ids:
                try:
                    # SELEÇÃO ESTRAGADA vira recado, não erro 500 (22/09/2026).
                    try:
                        tid = int(tid)
                    except (TypeError, ValueError):
                        erros.append({"id": tid, "erro": "Seleção inválida — "
                                      "recarregue a lista e escolha de novo."})
                        continue
                    if acao == "aprovar":
                        t = svc_titulos.aprovar(s, tid, usuario)
                    elif acao == "devolver":
                        t = svc_titulos.devolver(s, tid, motivo, usuario)
                    elif acao == "reanalisar":
                        t = svc_titulos.reanalisar(s, tid, usuario)
                    else:
                        t = svc_titulos.cancelar(s, tid, motivo, usuario)
                    oks.append(t.numero_sp)
                except (ErroValidacao, ErroPermissao) as e:
                    erros.append({"id": tid, "erro": str(e)})
            s.commit()
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha na ação em lote %s", acao)
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500
    logger.info("ERP: %s em lote — %d ok, %d com erro", acao, len(oks), len(erros))
    return jsonify({"ok": True, "processados": oks, "erros": erros})


# ---------------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------------
@bp.route("/erp/api/config")
@login_obrigatorio
@permissao("configurar")
def api_config():
    from sqlalchemy import select
    from app.apps.erp.db.models.cadastros import (
        Categoria, ContaBancaria, Empresa, Obra)
    try:
        with get_session() as s:
            cats = s.scalars(select(Categoria).order_by(Categoria.ordem, Categoria.codigo)).all()
            obras = s.scalars(select(Obra).order_by(Obra.codigo)).all()
            # As empresas entram AQUI, e não numa chamada própria: a tela de
            # relatórios já busca este payload, e a rota de empresas exige
            # "configurar" — o filtro ficaria vazio calado para quem só olha
            # relatório.
            empresas = s.scalars(select(Empresa).order_by(Empresa.razao_social)).all()
            contas = s.scalars(select(ContaBancaria).order_by(ContaBancaria.descricao)).all()
            usuarios = s.scalars(select(Usuario).order_by(Usuario.nome)).all()
            dados = {
                "empresas": [{"id": e.id,
                              "nome": e.nome_fantasia or e.razao_social}
                             for e in empresas],
                "categorias": [{
                    "id": c.id, "codigo": c.codigo, "descricao": c.descricao,
                    "natureza": getattr(c, "natureza", "RESULTADO"),
                    "grupo_codigo": c.grupo_codigo or "0",
                    "grupo_nome": c.grupo_nome or "Sem grupo",
                    "subgrupo_codigo": c.subgrupo_codigo or "",
                    "subgrupo_nome": c.subgrupo_nome or "",
                    "uso": c.descricao_uso or "",
                    "sugestao_dedutivel": c.dedutivel_padrao,
                    "ativo": c.ativo,
                    "substituida_por": c.substituida_por_id,
                    "personalizada": c.personalizada,
                    "tipos": [(t.value if hasattr(t, "value") else str(t)).split("_", 1)[0]
                              for t in (c.tipos_permitidos or [])],
                } for c in cats],
                "obras": [{
                    "id": o.id, "codigo": o.codigo, "nome": o.nome,
                    "objeto": (o.objeto or "")[:120], "cliente": o.cliente,
                    "municipio": o.municipio, "uf": o.uf, "contrato": o.contrato,
                    "status": o.status,
                } for o in obras],
                # A EMPRESA DONA vai junto (migração 080): é o que permite a
                # tela oferecer só as contas do CNPJ certo na hora de pagar, em
                # vez de misturar as de todas as empresas numa lista só.
                "contas": [{"id": b.id, "descricao": b.descricao, "banco": b.banco_codigo,
                            "agencia": b.agencia, "conta": b.conta, "ativo": b.ativo,
                            "empresa_id": b.empresa_id}
                           for b in contas],
                "usuarios": [{"nome": u.nome, "email": u.email,
                              "perfil": u.perfil.value, "ativo": u.ativo} for u in usuarios],
            }
        return jsonify({"ok": True, "dados": dados})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao carregar configurações")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/config/categoria", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_nova_categoria():
    from app.apps.erp.core.cadastros import categorias as svc_cat
    dados = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            svc_cat.criar(s, {
                "codigo": dados.get("codigo"), "descricao": dados.get("descricao"),
                "natureza": dados.get("natureza") or "RESULTADO",
                "dedutivel_padrao": (dados.get("dedutivel_padrao") or "sim") == "sim",
            }, usuario)
            s.commit()
        return jsonify({"ok": True})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao criar categoria")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/config/categorias/ajeitar", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_ajeitar_categorias():
    """Põe no grupo certo as contas que nasceram sem grupo. Deduz pelo código."""
    from app.apps.erp.core.cadastros import categorias as svc_cat
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            r = svc_cat.ajeitar_sem_grupo(s, usuario)
            s.commit()
        return jsonify({"ok": True, "resultado": r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao ajeitar as contas sem grupo")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/config/conta", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_nova_conta():
    """Cadastra a conta da empresa — com a chave Pix, se ela já estiver à mão."""
    from app.apps.erp.core.cadastros import contas as svc_contas
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            conta = svc_contas.criar(s, d, usuario)
            criada = {"id": conta.id, "descricao": conta.descricao}
            s.commit()
        return jsonify({"ok": True, "conta": criada})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao criar conta bancária")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# A LISTA DE BANCOS (código FEBRABAN/COMPE)
#
# Pedido do dono em 10/09/2026: em vez de digitar "237" de cabeça, escolher o
# banco pelo nome. A lista embutida funciona sozinha e sem internet; o botão
# troca pela oficial do Banco Central.
# ---------------------------------------------------------------------------
@bp.route("/erp/api/bancos")
@login_obrigatorio
@permissao("ver_erp")
def api_bancos():
    from app.apps.erp.core.cadastros import bancos
    with get_session() as s:
        return jsonify({"ok": True, "bancos": bancos.listar(s),
                        "estado": bancos.estado(s)})


@bp.route("/erp/api/bancos/atualizar", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_bancos_atualizar():
    """Troca a lista pela oficial do Banco Central. Pelo botão, nunca sozinha."""
    from app.apps.erp.core.cadastros import bancos
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            r = bancos.atualizar(s, usuario)
            estado = bancos.estado(s)
            s.commit()
        return jsonify({"ok": True, "resultado": r, "estado": estado})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao atualizar a lista de bancos")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# Importação
# ---------------------------------------------------------------------------
@bp.route("/erp/api/importar/pipefy", methods=["POST"])
@login_obrigatorio
@permissao("importar")
def api_importar_pipefy():
    """Enfileira a importação e volta na hora.

    Antes, cem cards — cada um com consulta ao Pipefy e anexos para baixar —
    rodavam enquanto a pessoa esperava, segurando uma das quatro linhas de
    atendimento do serviço. Todo mundo sentia o sistema pesado e ninguém sabia
    por quê. Agora o clique só põe na fila; a tela acompanha o andamento.
    """
    from app.apps.erp.core.comum import tarefas, trabalhos
    from app.apps.erp.core.importadores.pipefy_cards import extrair_ids

    d = request.get_json(silent=True) or {}
    ids = extrair_ids(d.get("texto") or "")
    if not ids:
        return jsonify({"ok": False, "erro": "Nenhum ID de card reconhecido no texto colado."}), 400
    if len(ids) > trabalhos.MAX_CARDS:
        return jsonify({"ok": False, "erro": f"{len(ids)} cards de uma vez — "
                        f"importe em blocos de até {trabalhos.MAX_CARDS}."}), 400
    try:
        trabalhos.registrar_todos()
        with get_session() as s:
            usuario = _usuario_logado(s)
            t = tarefas.enfileirar(
                s, "importar_pipefy",
                {"ids": ids,
                 "usuario_id": usuario.id if usuario else None,
                 "categoria_padrao_id": (int(d["categoria_padrao_id"])
                                         if d.get("categoria_padrao_id") else None),
                 "obra_padrao_id": (int(d["obra_padrao_id"])
                                    if d.get("obra_padrao_id") else None),
                 "criar_fornecedor": bool(d.get("criar_fornecedor", True)),
                 "baixar_anexos": bool(d.get("baixar_anexos", True))},
                rotulo=f"Importar {len(ids)} card(s) do Pipefy", usuario=usuario)
            linha = tarefas.ler(s, t.id)
            s.commit()
        return jsonify({"ok": True, "tarefa": linha})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao enfileirar a importação do Pipefy")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/arquivo/texto-dos-documentos", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_texto_dos_documentos():
    """Deixa legíveis os documentos que foram arquivados sem o texto de dentro.

    Só quem configura o sistema chega aqui: é trabalho sobre o acervo INTEIRO,
    sem recorte por obra — quem dispara não está lendo documento nenhum, está
    mandando o sistema extrair texto de todos. Ler continua passando pelo
    recorte de sempre, na hora de perguntar.

    Não gasta IA: é a camada de texto do próprio PDF.
    """
    from app.apps.erp.core.comum import tarefas, trabalhos

    d = request.get_json(silent=True) or {}
    try:
        trabalhos.registrar_todos()
        with get_session() as s:
            usuario = _usuario_logado(s)
            t = tarefas.enfileirar(
                s, "texto_dos_documentos",
                {"limite": int(d.get("limite") or trabalhos.LOTE_DE_TEXTOS)},
                rotulo="Deixar documentos legíveis para perguntas",
                usuario=usuario)
            linha = tarefas.ler(s, t.id)
            s.commit()
        return jsonify({"ok": True, "tarefa": linha})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao enfileirar a leitura dos documentos")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# TRABALHO EM SEGUNDO PLANO (migração 055)
#
# O que não cabe no tempo de um clique. A fila é do ERP inteiro; estas rotas
# só a mostram e deixam cancelar ou repetir.
# ---------------------------------------------------------------------------
@bp.route("/erp/api/tarefas")
@login_obrigatorio
@permissao("ver_erp")
def api_tarefas():
    from app.apps.erp.core.comum import tarefas
    todas = request.args.get("todas") == "1"
    with get_session() as s:
        usuario = _usuario_logado(s)
        # Ver a fila INTEIRA é ver o que os outros pediram: exige a mesma ação
        # de quem administra o sistema. Sem isso, cada um vê o que enfileirou.
        if todas and not _pode_agora("configurar").get("configurar"):
            todas = False
        return jsonify({"ok": True, **tarefas.listar(
            s, usuario=(None if todas else usuario),
            situacao=(request.args.get("situacao") or "").strip().upper(),
            limite=request.args.get("limite", type=int) or 40)})


@bp.route("/erp/api/tarefas/<int:tarefa_id>")
@login_obrigatorio
@permissao("ver_erp")
def api_tarefa(tarefa_id: int):
    from app.apps.erp.core.auth.permissoes import exigir_tarefa_no_escopo
    from app.apps.erp.core.comum import tarefas
    with get_session() as s:
        exigir_tarefa_no_escopo(s, _usuario_logado(s), tarefa_id)
        return jsonify({"ok": True, "tarefa": tarefas.ler(s, tarefa_id)})


@bp.route("/erp/api/tarefas/<int:tarefa_id>/<acao>", methods=["POST"])
@login_obrigatorio
@permissao("ver_erp")
def api_tarefa_acao(tarefa_id: int, acao: str):
    from app.apps.erp.core.auth.permissoes import exigir_tarefa_no_escopo
    from app.apps.erp.core.comum import tarefas, trabalhos
    if acao not in ("cancelar", "repetir"):
        return jsonify({"ok": False, "erro": f"Ação inválida: {acao!r}"}), 400
    try:
        trabalhos.registrar_todos()
        with get_session() as s:
            usuario = _usuario_logado(s)
            exigir_tarefa_no_escopo(s, usuario, tarefa_id)
            if acao == "cancelar":
                tarefas.cancelar(s, tarefa_id, usuario=usuario)
            else:
                tarefas.tentar_de_novo(s, tarefa_id, usuario=usuario)
            linha = tarefas.ler(s, tarefa_id)
            s.commit()
        return jsonify({"ok": True, "tarefa": linha})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500


# ---------------------------------------------------------------------------
# TRAZER DO PIPEFY O CADASTRO DAS PESSOAS (10/11/2026)
#
# Ele: *"tem o número do card do pipe, certo? Então, tu não consegue fazer logo
# essa importação, não? Faz a leitura do card, baixa os documentos, importa
# para o ERP."*
#
# Duas rotas: uma SÓ OLHA (quantos têm card, se o servidor tem o token) e outra
# põe o trabalho na fila. A conferência sem gravar é o padrão; gravar exige
# dizer `gravar: true` com todas as letras.
# ---------------------------------------------------------------------------
@bp.route("/erp/api/importar/pipefy-cadastro/situacao")
@login_obrigatorio
@permissao("importar")
def api_pipefy_cadastro_situacao():
    from app.apps.erp.core.pipefy import cadastro_importacao as imp
    try:
        with get_session() as s:
            return jsonify({"ok": True, "dados": imp.situacao(s)})
    except Exception as e:
        logger.exception("ERP: falha ao olhar a situação da importação do Pipefy")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# Quantos cards por rodada. O mesmo espírito do limite da importação de SPs:
# uma rodada que não termina antes da renovação do serviço no Render é uma
# rodada perdida no meio.
TETO_DE_CARDS_POR_RODADA = 300


@bp.route("/erp/api/importar/planilha-cadastro", methods=["POST"])
@login_obrigatorio
@permissao("importar")
def api_importar_planilha_cadastro():
    """Põe na fila a leitura da planilha "Dados Documentos" e o preenchimento
    dos cadastros.

    ⚑ SEM CARD NENHUM — e foi ele quem apontou, em 17/11/2026: *"mas por que
    rodar com o card dela se quero que puxe da planilha?"* O card servia para
    achar a pessoa e baixar os documentos; a planilha tem o CPF e acha
    sozinha. Exigir o número do card para trazer dado que está na planilha era
    pedir 575 números para nada.

    Os DOCUMENTOS continuam vindo do Pipefy (`/erp/api/importar/pipefy-cadastro`):
    é o que a planilha não tem.
    """
    import re

    from app.apps.erp.core.comum import tarefas, trabalhos

    d = request.get_json(silent=True) or {}
    gravar = bool(d.get("gravar"))
    # Lista opcional de CPFs: é como se começa, com dois ou três.
    escrito = str(d.get("cpfs") or "").strip()
    cpfs = [c for c in re.findall(r"\d[\d.\-]{9,}", escrito)]
    cpfs = [re.sub(r"\D", "", c) for c in cpfs]
    cpfs = [c for c in cpfs if len(c) == 11]
    # ⚑ ESCREVEU E EU NÃO ENTENDI ⇒ RECUSA, nunca "a planilha inteira".
    #
    # Ele, em 17/11/2026: *"tem mais gente no ERP do que eu coloquei para
    # importar. Só quero os que eu coloquei para importar."*
    #
    # Era este o defeito, e ele é do tipo mais caro: campo em branco quer
    # dizer "a planilha inteira", e CPF que o sistema não conseguia ler virava
    # campo em branco. Ou seja, um CPF digitado com um dígito faltando
    # transformava "importe estas três pessoas" em "importe as 700" — sem uma
    # linha de aviso.
    #
    # Agora a diferença entre "não pedi ninguém" e "pedi e você não entendeu"
    # é explícita, e a segunda para o trabalho antes de ele começar.
    if escrito and not cpfs:
        return jsonify({"ok": False, "erro":
                        "Você escreveu algo no campo de CPF e eu não achei "
                        "nenhum CPF de 11 dígitos ali. Confira o que digitou. "
                        "Para trazer a planilha inteira, deixe o campo "
                        "totalmente em branco."}), 400
    try:
        trabalhos.registrar_todos()
        with get_session() as s:
            usuario = _usuario_logado(s)
            t = tarefas.enfileirar(
                s, "importar_cadastro_planilha",
                {"gravar": gravar, "cpfs": cpfs,
                 # ⚑ CADASTRAR QUEM FALTA É OPT-IN (17/11/2026). Criar gente é
                 # a única coisa que esta importação faz que não se desfaz com
                 # outra importação — então não pode ser padrão. Ele achou o
                 # ERP com mais gente do que pediu, e tinha razão.
                 "criar": bool(d.get("criar")),
                 # ⚑ LIMPAR A BASE: apaga quem está no ERP e não está na
                 # lista. Ele pediu em 17/11/2026 (*"o CPF que não tiver
                 # dentro da minha lista de importação, eu quero que seja
                 # excluído dessa base de dados"*). Só vale COM lista — o
                 # importador recusa sem ela, porque lista vazia quer dizer
                 # "a planilha inteira" e isso apagaria o cadastro todo.
                 "so_estes": bool(d.get("so_estes")) and bool(cpfs),
                 "usuario_id": usuario.id if usuario else None},
                rotulo=(("Conferir sem gravar" if not gravar
                         else "Trazer da planilha")
                        + (f": {len(cpfs)} pessoa(s)" if cpfs
                           else ": a planilha inteira")
                        + (" (cadastrando quem falta)" if d.get("criar") else "")
                        + (" + APAGANDO quem não está na lista"
                           if d.get("so_estes") and cpfs else "")),
                usuario=usuario)
            linha = tarefas.ler(s, t.id)
            s.commit()
        return jsonify({"ok": True, "tarefa": linha})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/importar/pipefy-cadastro", methods=["POST"])
@login_obrigatorio
@permissao("importar")
def api_importar_pipefy_cadastro():
    """Põe na fila a leitura dos cards e a busca dos documentos.

    ⚑ VAI PARA A FILA, não roda no clique: são dezenas de pessoas com até oito
    arquivos cada. No clique, isso seguraria uma das quatro linhas de
    atendimento do serviço por minutos."""
    from app.apps.erp.core.comum import tarefas, trabalhos
    from app.apps.erp.core.pipefy import cadastro_importacao as imp
    from app.apps.erp.core.pipefy import cliente as cli

    d = request.get_json(silent=True) or {}
    gravar = bool(d.get("gravar"))
    if not cli.tem_token():
        return jsonify({"ok": False, "erro": (
            "Este servidor não tem o token do Pipefy configurado "
            "(PIPEFY_API_TOKEN). Sem ele não dá para ler os cards.")}), 400
    # ⚑ A FILA É A LISTA QUE ELE COLA (13/11/2026). Antes eu varria o pipe
    # inteiro e casava pelo CPF — e trazia card cancelado e card repetido, que
    # não interessam. Ele: *"eu vou listar os cards que eu quero que você
    # busque. E você faz a pesquisa card a card e puxa as informações."*
    pedidos = imp.ids_dos_cards(d.get("cards"))
    if not pedidos:
        return jsonify({"ok": False, "erro": (
            "Cole os números dos cards que devem ser buscados.")}), 400
    if len(pedidos) > TETO_DE_CARDS_POR_RODADA:
        return jsonify({"ok": False, "erro": (
            f"{len(pedidos)} cards de uma vez — faça em blocos de até "
            f"{TETO_DE_CARDS_POR_RODADA}.")}), 400
    try:
        trabalhos.registrar_todos()
        with get_session() as s:
            usuario = _usuario_logado(s)
            t = tarefas.enfileirar(
                s, "importar_cadastro_pipefy",
                {"simular": not gravar, "cards": pedidos,
                 "regravar": bool(d.get("regravar")),
                 "usuario_id": usuario.id if usuario else None},
                rotulo=(("Conferir sem gravar: " if not gravar
                         else "Trazer do Pipefy: ")
                        + f"{len(pedidos)} card(s)"),
                usuario=usuario)
            linha = tarefas.ler(s, t.id)
            s.commit()
        return jsonify({"ok": True, "tarefa": linha})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao enfileirar a importação do cadastro")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# AS PERGUNTAS QUE A IMPORTAÇÃO DEIXOU — e quem responde é gente
#
# Ele, em 13/11/2026: *"quando houver divergência entre o que a IA lê e o que
# está cadastrado, aí você questiona. E o humano decide."*
# ---------------------------------------------------------------------------
@bp.route("/erp/api/cadastro/divergencias")
@login_obrigatorio
@permissao("ver_pessoal")
def api_cadastro_divergencias():
    from app.apps.erp.core.pipefy import cadastro_importacao as imp
    try:
        with get_session() as s:
            return jsonify({"ok": True, "perguntas": imp.perguntas_abertas(s)})
    except Exception as e:
        logger.exception("ERP: falha ao listar as divergências do cadastro")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/cadastro/divergencias/<int:divergencia_id>",
          methods=["POST"])
@login_obrigatorio
@permissao("editar_colaboradores")
def api_cadastro_divergencia_decidir(divergencia_id: int):
    """Alguém escolheu qual versão vale — e o cadastro passa a dizer isso."""
    from app.apps.erp.core.pipefy import cadastro_importacao as imp

    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            r = imp.decidir(s, divergencia_id,
                            str(d.get("fonte") or ""), usuario)
            s.commit()
        return jsonify({"ok": True, "resultado": r})
    except ErroNaoEncontrado as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao decidir a divergência")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/importar/csv", methods=["POST"])
@login_obrigatorio
@permissao("importar")
def api_importar_csv():
    from app.apps.erp.core.importadores.planilhas import (
        importar_categorias_csv, importar_obras_csv,
    )
    arquivo = request.files.get("arquivo")
    tipo = (request.form.get("tipo") or "").strip()
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Arquivo não enviado."}), 400
    if tipo not in ("obras", "categorias"):
        return jsonify({"ok": False, "erro": f"Tipo inválido: {tipo!r}."}), 400
    try:
        conteudo = arquivo.read()
        with get_session() as s:
            usuario = _usuario_logado(s)
            fn = importar_obras_csv if tipo == "obras" else importar_categorias_csv
            rel = fn(s, conteudo, usuario)
            s.commit()
        return jsonify({"ok": True, "relatorio": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha na importação de CSV")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/importar/ofx", methods=["POST"])
@login_obrigatorio
@permissao("importar")
def api_importar_ofx():
    from app.apps.erp.core.pagamentos import service as svc_pag
    from app.apps.erp.core.pagamentos.ofx import ErroOFX
    arquivo = request.files.get("arquivo")
    conta_id = request.form.get("conta_id")
    if arquivo is None or not conta_id:
        return jsonify({"ok": False, "erro": "Envie o arquivo e escolha a conta."}), 400
    try:
        conteudo = arquivo.read()
        with get_session() as s:
            usuario = _usuario_logado(s)
            rel = svc_pag.importar_ofx(s, conteudo, int(conta_id), usuario)
            s.commit()
        return jsonify({"ok": True, "relatorio": rel})
    except ErroOFX as e:
        return jsonify({"ok": False, "erro": f"Arquivo OFX inválido: {e}"}), 400
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha na importação de OFX")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/config/plano/instalar", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_instalar_plano():
    """Grava o plano financeiro padrão da BWS direto no banco (idempotente)."""
    from app.apps.erp.core.cadastros.plano_padrao import aplicar_plano
    from app.apps.erp.db.models.cadastros import PerfilUsuario
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if usuario.perfil not in (PerfilUsuario.ADMIN, PerfilUsuario.FINANCEIRO):
                return jsonify({"ok": False, "erro": "Restrito a FINANCEIRO/ADMIN."}), 403
            rel = aplicar_plano(s, usuario)
            s.commit()
        return jsonify({"ok": True, "relatorio": rel})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao instalar plano financeiro")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/config/categoria/substituir", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_substituir_categoria():
    """Aposenta uma conta e remaneja seus títulos para outra."""
    from app.apps.erp.core.cadastros.plano_padrao import substituir_categoria
    from app.apps.erp.db.models.cadastros import PerfilUsuario
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if usuario.perfil not in (PerfilUsuario.ADMIN, PerfilUsuario.FINANCEIRO):
                return jsonify({"ok": False, "erro": "Restrito a FINANCEIRO/ADMIN."}), 403
            rel = substituir_categoria(s, int(d.get("origem_id")),
                                       int(d.get("destino_id")), usuario)
            s.commit()
        return jsonify({"ok": True, "relatorio": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao substituir categoria")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/titulos/dedutibilidade", methods=["POST"])
@login_obrigatorio
@permissao("reclassificar")
def api_definir_dedutibilidade():
    """Define a dedutibilidade do título — decisão do financeiro (ou da IA),
    tomada a partir do documento, não da categoria."""
    from datetime import datetime, timezone
    from decimal import Decimal
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.db.models.financeiro import StatusDedutibilidade, Titulo
    d = request.get_json(silent=True) or {}
    ids = d.get("ids") or []
    situacao = (d.get("situacao") or "").strip().upper()
    motivo = (d.get("motivo") or "").strip()
    if situacao not in {s.value for s in StatusDedutibilidade}:
        return jsonify({"ok": False, "erro": f"Situação inválida: {situacao!r}"}), 400
    if situacao in ("INDEDUTIVEL", "PARCIAL") and len(motivo) < 10:
        return jsonify({"ok": False, "erro": "Indedutível ou parcial exige motivo "
                                             "(mínimo 10 caracteres)."}), 400
    valor = d.get("valor_dedutivel")
    processados, erros = [], []
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            for tid in ids:
                t = s.get(Titulo, int(tid))
                if t is None:
                    erros.append({"id": tid, "erro": "título não encontrado"})
                    continue
                if situacao == "PARCIAL" and not valor:
                    erros.append({"id": tid, "erro": "informe o valor dedutível"})
                    continue
                t.dedutibilidade = StatusDedutibilidade(situacao)
                t.dedutivel = situacao in ("DEDUTIVEL", "PARCIAL")
                t.dedutibilidade_valor = (Decimal(str(valor).replace(",", "."))
                                          if situacao == "PARCIAL" else None)
                t.dedutibilidade_motivo = motivo or None
                t.dedutibilidade_por = usuario.id
                t.dedutibilidade_em = datetime.now(timezone.utc)
                t.dedutibilidade_origem = d.get("origem") or "HUMANO"
                registrar_evento(s, "titulo", t.id, "DEDUTIBILIDADE_DEFINIDA",
                                 {"situacao": situacao, "motivo": motivo,
                                  "valor": str(valor) if valor else None}, usuario.id)
                processados.append(t.numero_sp)
            s.commit()
        return jsonify({"ok": True, "processados": processados, "erros": erros})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao definir dedutibilidade")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/config/categoria/<int:categoria_id>", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_editar_categoria(categoria_id: int):
    """Renomeia/ajusta uma conta. A edição marca a conta como personalizada —
    reinstalar o plano padrão não sobrescreve mais o texto dela."""
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.db.models.cadastros import Categoria, PerfilUsuario, TipoTitulo
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if usuario.perfil not in (PerfilUsuario.ADMIN, PerfilUsuario.FINANCEIRO):
                return jsonify({"ok": False, "erro": "Restrito a FINANCEIRO/ADMIN."}), 403
            cat = s.get(Categoria, categoria_id)
            if cat is None:
                return jsonify({"ok": False, "erro": "Categoria não encontrada."}), 404
            antes = {"codigo": cat.codigo, "descricao": cat.descricao,
                     "natureza": cat.natureza, "ativo": cat.ativo}

            if "descricao" in d:
                nova_desc = (d.get("descricao") or "").strip()
                if len(nova_desc) < 3:
                    return jsonify({"ok": False, "erro": "Descrição muito curta."}), 400
                cat.descricao = nova_desc
            if "codigo" in d and (d.get("codigo") or "").strip():
                novo_cod = d["codigo"].strip()
                if novo_cod != cat.codigo:
                    from sqlalchemy import select as _sel
                    if s.scalars(_sel(Categoria).where(Categoria.codigo == novo_cod)).first():
                        return jsonify({"ok": False, "erro": f"Já existe conta com o código {novo_cod}."}), 400
                    cat.codigo = novo_cod
            if "descricao_uso" in d:
                cat.descricao_uso = (d.get("descricao_uso") or "").strip() or None
            if "natureza" in d:
                nat = (d.get("natureza") or "").strip().upper()
                if nat not in ("RESULTADO", "FLUXO"):
                    return jsonify({"ok": False, "erro": "Natureza deve ser RESULTADO ou FLUXO."}), 400
                cat.natureza = nat
            if "grupo_nome" in d and (d.get("grupo_nome") or "").strip():
                cat.grupo_nome = d["grupo_nome"].strip()
            if "subgrupo_nome" in d and (d.get("subgrupo_nome") or "").strip():
                cat.subgrupo_nome = d["subgrupo_nome"].strip()
            if "tipos" in d and isinstance(d["tipos"], list):
                try:
                    cat.tipos_permitidos = [TipoTitulo(t) for t in d["tipos"]]
                except ValueError as e:
                    return jsonify({"ok": False, "erro": f"Tipo inválido: {e}"}), 400
            if "ativo" in d:
                cat.ativo = bool(d["ativo"])

            cat.personalizada = True
            s.flush()
            registrar_evento(s, "categoria", cat.id, "EDITADA",
                             {"antes": antes, "depois": {
                                 "codigo": cat.codigo, "descricao": cat.descricao,
                                 "natureza": cat.natureza, "ativo": cat.ativo}}, usuario.id)
            s.commit()
        return jsonify({"ok": True})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao editar categoria %s", categoria_id)
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/manutencao/banco")
@login_obrigatorio
@permissao("configurar")
def api_estado_banco():
    from app.apps.erp.core.comum.migracoes import listar_estado
    try:
        return jsonify({"ok": True, "estado": listar_estado()})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/manutencao/banco/aplicar", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_aplicar_migracoes():
    """Botão de atualização do banco — restrito a ADMIN."""
    from app.apps.erp.core.comum.migracoes import aplicar_pendentes
    try:
        with get_session() as s:
            perfil = _perfil_bruto(s)      # SQL direto: funciona com o banco atrasado
            if perfil != "ADMIN":
                return jsonify({"ok": False, "erro": "Restrito ao ADMIN."}), 403
        rel = aplicar_pendentes()
        logger.info("ERP: %s migração(ões) aplicada(s) por usuário %s",
                    len(rel["aplicadas"]), session.get("erp_usuario_id"))
        return jsonify({"ok": True, "relatorio": rel})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao aplicar migrações")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/config/depara")
@login_obrigatorio
@permissao("configurar")
def api_listar_depara():
    from app.apps.erp.core.cadastros.depara import listar
    try:
        with get_session() as s:
            return jsonify({"ok": True, "depara": listar(s)})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao listar de-para")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/config/depara/instalar", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_instalar_depara():
    from app.apps.erp.core.cadastros.depara import instalar_depara_padrao
    from app.apps.erp.db.models.cadastros import PerfilUsuario
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if usuario.perfil not in (PerfilUsuario.ADMIN, PerfilUsuario.FINANCEIRO):
                return jsonify({"ok": False, "erro": "Restrito a FINANCEIRO/ADMIN."}), 403
            rel = instalar_depara_padrao(s, usuario)
            s.commit()
        return jsonify({"ok": True, "relatorio": rel})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao instalar de-para")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/config/depara/definir", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_definir_depara():
    from app.apps.erp.core.cadastros.depara import definir
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            definir(s, int(d.get("depara_id")), int(d.get("categoria_id")), usuario)
            s.commit()
        return jsonify({"ok": True})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao definir tradução")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# Lançamento
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# CADASTRAR O CREDOR SEM SAIR DO LANÇAMENTO — 22/09/2026
#
# Pedido do dono: *"se o credor não tem cadastro, o ERP deve avisar e tem que
# ser permitido o cadastro já a partir da tela."*
#
# É a mesma regra de desenho que ele já tinha dado para empresa e obra: UMA
# PORTA, e o atalho DENTRO do formulário. Mandar a pessoa sair do lançamento,
# ir a Cadastros, voltar e recomeçar é onde o lançamento é abandonado no meio.
#
# A CONTA BANCÁRIA VEM JUNTO, E NASCE PENDENTE — e é aqui que mora a decisão
# que importa. A homologação em duas pessoas existe contra o golpe da troca de
# conta: quem lança não pode ser quem aprova o destino do dinheiro. Deixar o
# lançador criar conta JÁ HOMOLOGADA destruiria esse controle.
#
# Então a conta entra PENDENTE: o lançamento é concluído, o título nasce
# BLOQUEADO pela crítica C2 (que já existia), e alguém com alçada homologa e
# reanalisa. O trabalho não se perde e o controle não cai.
# ---------------------------------------------------------------------------
@bp.route("/erp/api/credores", methods=["POST"])
@login_obrigatorio
@permissao("lancar")
def api_credor_no_lancamento():
    """Cadastra o credor a partir da tela de lançamento, com conta pendente."""
    from app.apps.erp.core.cadastros import fornecedores as svc_forn
    from app.apps.erp.core.cadastros.validadores import somente_digitos
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            doc = somente_digitos(d.get("cnpj_cpf") or "")
            # Documento já cadastrado não é erro para quem está lançando: é a
            # resposta que ele procurava. Devolve o que existe em vez de mandar
            # procurar.
            ja = svc_forn.obter_por_documento(s, doc) if doc else None
            if ja is not None:
                # CREDOR QUE JÁ EXISTE MAS AINDA NÃO TEM FORMA DE PAGAMENTO
                # (22/09/2026, achado ao simular a cadeia inteira). Antes a
                # rota devolvia o cadastro e DESCARTAVA em silêncio a conta
                # que a pessoa acabou de digitar — ela via "pronto", salvava,
                # e o título travava do mesmo jeito. É exatamente o caso que
                # o dono descreveu: *"preciso voltar pra cadastrar a forma de
                # pgt"*.
                conta_nova = _conta_pendente_do_credor(s, ja, d)
                if conta_nova is not None:
                    s.commit()
                return jsonify({"ok": True, "ja_existia": True,
                                "conta_pendente_id": conta_nova,
                                "credor": _credor_para_lancamento(s, ja)})
            forn = svc_forn.criar(s, {
                "tipo_pessoa": "PF" if len(doc) == 11 else "PJ",
                "cnpj_cpf": doc,
                "razao_social": (d.get("razao_social") or "").strip(),
                "nome_fantasia": (d.get("nome_fantasia") or "").strip() or None,
                "email": (d.get("email") or "").strip() or None,
                "telefone": (d.get("telefone") or "").strip() or None,
                "origem": "LANCAMENTO"}, usuario)

            conta_criada = _conta_pendente_do_credor(s, forn, d)
            s.commit()
            return jsonify({"ok": True, "ja_existia": False,
                            "conta_pendente_id": conta_criada,
                            "credor": _credor_para_lancamento(s, forn)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao cadastrar credor no lançamento")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# A HOMOLOGAÇÃO DA CONTA DO CREDOR — a porta que faltava (22/09/2026)
#
# Achado ao simular a cadeia inteira de ponta a ponta: a regra de negócio
# `homologar_conta` existia desde sempre e NÃO TINHA QUEM A CHAMASSE — nenhuma
# rota, nenhum botão, nenhum teste. Na prática isso travava o ERP inteiro num
# banco novo: todo título pago por Pix ou TED nasce BLOQUEADO pela crítica C2
# enquanto a conta do credor não estiver homologada, e não havia como
# homologar. O lançamento chegava ao fim e morria ali.
#
# É o segundo lado da porta que o cadastro do credor pela tela de lançamento
# abriu: lá a conta nasce PENDENTE de propósito, aqui ela é conferida por
# outra pessoa e liberada. As duas metades juntas é que fazem o controle
# valer — uma sem a outra é ou um beco, ou um convite ao golpe da troca de
# conta.
# ---------------------------------------------------------------------------
@bp.route("/erp/api/credores/contas/pendentes")
@login_obrigatorio
@permissao("homologar_conta_credor")
def api_contas_pendentes():
    """A fila de contas de credor esperando conferência.

    Sem esta lista a pendência ficaria invisível: ela só apareceria para quem
    esbarrasse num título bloqueado, e o dinheiro pararia sem ninguém saber
    por quê.
    """
    from sqlalchemy import select
    from app.apps.erp.core.cadastros import fornecedores as svc_forn
    from app.apps.erp.db.models.cadastros import (
        Fornecedor, FornecedorConta, StatusConta, Usuario as _U,
    )
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            contas = s.scalars(select(FornecedorConta).where(
                FornecedorConta.status == StatusConta.PENDENTE)).all()
            nomes = {f.id: f.razao_social for f in s.scalars(select(Fornecedor)).all()}
            pessoas = {u.id: u.nome for u in s.scalars(select(_U)).all()}
            fila = []
            for ct in sorted(contas, key=lambda c: c.id or 0, reverse=True):
                autor = svc_forn.quem_cadastrou_a_conta(s, ct.id)
                fila.append({
                    "id": ct.id, "fornecedor_id": ct.fornecedor_id,
                    "credor": nomes.get(ct.fornecedor_id, ""),
                    "forma": ct.forma.value if ct.forma else None,
                    "identificacao": ct.pix_chave or
                    f"{ct.banco_codigo or ''}/{ct.agencia or ''}/{ct.conta or ''}",
                    "titular": ct.titular_nome, "titular_doc": ct.titular_doc,
                    "cadastrada_por": pessoas.get(autor, ""),
                    # quem cadastrou não homologa: a tela já mostra o botão
                    # apagado, em vez de deixar a pessoa clicar e tomar recusa.
                    "posso_homologar": not (autor is not None and atual is not None
                                            and autor == atual.id),
                    "criado_em": ct.criado_em.isoformat() if ct.criado_em else None,
                })
        return jsonify({"ok": True, "contas": fila})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao listar contas pendentes")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/credores/contas/<int:conta_id>/homologar", methods=["POST"])
@login_obrigatorio
@permissao("homologar_conta_credor")
def api_homologar_conta_credor(conta_id: int):
    """Libera a conta do credor para pagamento, conferida por outra pessoa.

    Aceita `reanalisar_titulo_id`: o título que estava bloqueado por causa
    desta conta volta para a fila de aprovação na MESMA ida. Sem isso a
    pessoa homologa, acha que resolveu, e o título continua parado — o beco
    que o dono descreveu, só que uma casa adiante.
    """
    from app.apps.erp.core.cadastros import fornecedores as svc_forn
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            conta = svc_forn.homologar_conta(s, conta_id, usuario)
            resposta = {"ok": True, "conta_id": conta.id,
                        "status": conta.status.value}
            alvo = d.get("reanalisar_titulo_id")
            if alvo:
                try:
                    titulo = svc_titulos.reanalisar(s, int(alvo), usuario)
                    resposta["titulo"] = {"id": titulo.id,
                                          "numero_sp": titulo.numero_sp,
                                          "status": titulo.status.value}
                except (ErroValidacao, ErroPermissao) as e:
                    # a homologação vale de qualquer jeito: ela é o ato
                    # principal, e desfazê-la por causa da reanálise seria
                    # perder o trabalho conferido.
                    resposta["aviso_reanalise"] = str(e)
            s.commit()
        return jsonify(resposta)
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao homologar conta do credor")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


def _conta_pendente_do_credor(s, forn, d: dict):
    """A conta bancária que veio junto do cadastro pela tela de lançamento.

    Nasce PENDENTE sempre — é o controle inteiro: quem lança pode DIZER para
    onde o credor recebe, mas não pode LIBERAR esse destino. Devolve o id da
    conta criada, ou None quando não veio forma de pagamento nenhuma.
    """
    from app.apps.erp.core.cadastros.validadores import somente_digitos
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.db.models.cadastros import (
        FormaPagamento, FornecedorConta, StatusConta,
    )

    forma = (d.get("conta_forma") or "").strip().upper()
    if forma not in ("PIX", "TED"):
        return None
    conta = FornecedorConta(
        fornecedor_id=forn.id, forma=FormaPagamento(forma),
        titular_nome=forn.razao_social, titular_doc=forn.cnpj_cpf,
        status=StatusConta.PENDENTE)
    if forma == "PIX":
        chave = (d.get("pix_chave") or "").strip()
        if not chave:
            raise ErroValidacao("Informe a chave Pix do credor.")
        conta.pix_tipo = (d.get("pix_tipo") or "ALEATORIA").strip().upper()
        conta.pix_chave = chave
    else:
        banco = somente_digitos(d.get("banco_codigo") or "")
        agencia = (d.get("agencia") or "").strip()
        numero = (d.get("conta") or "").strip()
        if not (banco and agencia and numero):
            raise ErroValidacao("Para TED informe banco, agência e conta.")
        conta.banco_codigo, conta.agencia, conta.conta = banco, agencia, numero
    # a conta repetida não vira segunda conta pendente: seriam duas filas de
    # conferência para o mesmo dado, e quem confere não saberia qual liberar.
    igual = next((c for c in forn.contas
                  if c.forma == conta.forma and c.status == StatusConta.PENDENTE
                  and (c.pix_chave or "") == (conta.pix_chave or "")
                  and (c.banco_codigo or "") == (conta.banco_codigo or "")
                  and (c.agencia or "") == (conta.agencia or "")
                  and (c.conta or "") == (conta.conta or "")), None)
    if igual is not None:
        return igual.id
    s.add(conta)
    s.flush()
    # QUEM CADASTROU fica na trilha — é o que a homologação lê depois para
    # recusar que a mesma pessoa libere a própria conta.
    usuario = _usuario_logado(s)
    registrar_evento(s, "fornecedor_conta", conta.id, "CRIADA",
                     {"fornecedor_id": forn.id, "forma": forma,
                      "origem": "LANCAMENTO"},
                     usuario.id if usuario else None)
    s.flush()
    # a lista de contas do credor foi lida acima (a busca por repetida), então
    # ela ficou velha: sem esta linha o cadastro acabado de gravar não apareceria
    # na resposta, e a tela diria que nada foi criado.
    s.expire(forn, ["contas"])
    return conta.id


def _credor_para_lancamento(s, forn) -> dict:
    """O credor no MESMO formato que /erp/api/lancamento/dados devolve.

    Uma função só, para a tela poder encaixar o recém-criado na lista sem
    recarregar tudo — e para os dois formatos não divergirem com o tempo.
    """
    from app.apps.erp.db.models.cadastros import StatusConta
    return {
        "id": forn.id, "nome": forn.razao_social, "documento": forn.cnpj_cpf,
        "situacao_rfb": forn.situacao_rfb,
        "contas": [{"id": ct.id, "forma": ct.forma.value,
                    "identificacao": ct.pix_chave or
                    f"{ct.banco_codigo}/{ct.agencia}/{ct.conta}"}
                   for ct in forn.contas if ct.status == StatusConta.HOMOLOGADA],
        "contas_pendentes": [{"id": ct.id, "forma": ct.forma.value}
                             for ct in forn.contas
                             if ct.status == StatusConta.PENDENTE],
    }


@bp.route("/erp/api/lancamento/dados")
@login_obrigatorio
@permissao("lancar")
def api_dados_lancamento():
    """Cadastros necessários ao formulário, já filtrados para uso."""
    from sqlalchemy import select
    from app.apps.erp.db.models.cadastros import (
        Categoria, Fornecedor, Obra, StatusConta,
    )
    try:
        with get_session() as s:
            forns = s.scalars(select(Fornecedor).where(Fornecedor.ativo.is_(True))
                              .order_by(Fornecedor.razao_social)).all()
            obras = s.scalars(select(Obra).where(Obra.status == "ATIVA")
                              .order_by(Obra.codigo)).all()
            from app.apps.erp.core.cadastros.sugestao import categorias_do_usuario
            permitidas = categorias_do_usuario(s, _usuario_logado(s))
            stmt_cat = select(Categoria).where(Categoria.ativo.is_(True))
            if permitidas:
                stmt_cat = stmt_cat.where(Categoria.id.in_(permitidas))
            cats = s.scalars(stmt_cat.order_by(Categoria.ordem, Categoria.codigo)).all()
            dados = {
                "fornecedores": [{
                    "id": f.id, "nome": f.razao_social, "documento": f.cnpj_cpf,
                    "situacao_rfb": f.situacao_rfb,
                    "contas": [{"id": ct.id, "forma": ct.forma.value,
                                "identificacao": ct.pix_chave or
                                f"{ct.banco_codigo}/{ct.agencia}/{ct.conta}"}
                               for ct in f.contas if ct.status == StatusConta.HOMOLOGADA],
                } for f in forns],
                "obras": [{"id": o.id, "codigo": o.codigo, "nome": o.nome} for o in obras],
                "categorias": [{
                    "id": c.id, "codigo": c.codigo, "descricao": c.descricao,
                    "grupo": c.grupo_nome or "", "subgrupo": c.subgrupo_nome or "",
                    "uso": c.descricao_uso or "", "natureza": c.natureza,
                    "tipos": [(t.value if hasattr(t, "value") else str(t))
                              for t in (c.tipos_permitidos or [])],
                } for c in cats],
            }
        return jsonify({"ok": True, "dados": dados})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao carregar dados do lançamento")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/lancamento/ler-documento", methods=["POST"])
@login_obrigatorio
@permissao("lancar")
def api_ler_documento():
    """Lê o anexo (XML/PDF/imagem) e devolve os campos sugeridos."""
    from sqlalchemy import select
    from app.apps.erp.core.documentos.leitor import ErroLeitura, ler_documento
    from app.apps.erp.core.cadastros.validadores import somente_digitos
    from app.apps.erp.db.models.cadastros import Fornecedor, Obra
    arquivo = request.files.get("arquivo")
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Nenhum arquivo enviado."}), 400
    from app.apps.erp.core.comum.ia_custo import contexto
    # FORA do try: a recusa por teto tem resposta própria (402), e um
    # `except Exception` a transformaria em 'falha do sistema'.
    _exigir_saldo_de_ia()
    try:
        with contexto(operacao="leitura_documento"):
            lido = ler_documento(arquivo.read(), arquivo.filename or "",
                                 dica_usuario=(request.form.get("dica") or ""))
    except ErroLeitura as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao ler documento")
        return jsonify({"ok": False, "erro": f"Falha ao ler o documento: {e}"}), 500

    # amarra ao que já existe no cadastro
    try:
        with get_session() as s:
            doc = somente_digitos(lido.get("emitente_documento") or "")
            forn = None
            if doc:
                forn = s.scalars(select(Fornecedor).where(Fornecedor.cnpj_cpf == doc)).first()
            lido["fornecedor_id"] = forn.id if forn else None
            lido["fornecedor_nome_cadastro"] = forn.razao_social if forn else None
            from app.apps.erp.core.cadastros.sugestao import sugerir_categoria, sugerir_obra
            usuario = _usuario_logado(s)
            obra_txt = (lido.get("obra_mencionada") or "").strip()
            if obra_txt:
                o = s.scalars(select(Obra).where(Obra.codigo == obra_txt.upper())).first()
                lido["obra_id"] = o.id if o else None
            if not lido.get("obra_id"):
                sug_obra = sugerir_obra(s, lido)
                if sug_obra:
                    lido["obra_id"] = sug_obra["obra_id"]
                    lido["obra_sugerida"] = sug_obra
            lido["categoria_sugerida"] = sugerir_categoria(
                s, lido, usuario, lido.get("fornecedor_id"))
            # nota de débito da locadora: acha o contrato e a parcela sozinho
            from app.apps.erp.core.locacoes import identificar_contrato
            achado = identificar_contrato(s, lido, lido.get("fornecedor_id"))
            if achado:
                lido["locacao_identificada"] = achado
    except Exception:
        logger.exception("ERP: falha ao casar documento com cadastros")
    return jsonify({"ok": True, "documento": lido})


@bp.route("/erp/api/lancamento/checar", methods=["POST"])
@login_obrigatorio
@permissao("lancar")
def api_checar_duplicidade():
    """Crítica de duplicidade antes de gravar."""
    from app.apps.erp.core.titulos.duplicidade import checar
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            from app.apps.erp.core.titulos.enquadramento import avaliar
            critica = checar(s, d)
            critica["enquadramento"] = avaliar(s, d, _usuario_logado(s))
            return jsonify({"ok": True, "critica": critica})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha na crítica de duplicidade")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/lancamento", methods=["POST"])
@login_obrigatorio
@permissao("lancar")
def api_criar_titulo():
    """Grava o título. A crítica de duplicidade roda de novo aqui — o que
    bloqueia na tela não pode passar por chamada direta."""
    from app.apps.erp.core.titulos import service as svc
    from app.apps.erp.core.titulos.duplicidade import checar
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            critica = checar(s, {
                "fornecedor_id": d.get("fornecedor_id"),
                "valor": d.get("valor_bruto"), "parcelas": d.get("parcelas") or [],
                "competencia": d.get("competencia"), "descricao": d.get("descricao"),
                "categoria_id": d.get("categoria_id"),
                "documento_numero": d.get("documento_numero")})
            if critica["bloqueios"] and not d.get("forcar"):
                return jsonify({"ok": False, "erro": "Lançamento bloqueado por duplicidade.",
                                "critica": critica}), 409
            usuario = _usuario_logado(s)
            # credor lido do documento e ainda não cadastrado: cadastra agora,
            # no salvamento — não antes, para não criar fornecedor de rascunho
            if not d.get("fornecedor_id") and d.get("emitente_documento"):
                from app.apps.erp.core.cadastros import fornecedores as svc_forn
                from app.apps.erp.core.cadastros.validadores import somente_digitos
                from app.apps.erp.db.models.cadastros import Fornecedor as _F
                from sqlalchemy import select as _sel
                doc = somente_digitos(d["emitente_documento"])
                existente = s.scalars(_sel(_F).where(_F.cnpj_cpf == doc)).first()
                if existente is not None:
                    d["fornecedor_id"] = existente.id
                else:
                    try:
                        novo = svc_forn.criar(s, {
                            "tipo_pessoa": "PJ" if len(doc) == 14 else "PF",
                            "cnpj_cpf": doc,
                            "razao_social": (d.get("emitente_nome") or "").strip(),
                            "municipio": d.get("municipio_emissao") or None,
                        }, usuario)
                        s.flush()
                        d["fornecedor_id"] = novo.id
                        logger.info("ERP: credor %s cadastrado no salvamento do título", doc)
                    except ErroValidacao as e:
                        return jsonify({"ok": False,
                                        "erro": f"Não foi possível cadastrar o credor "
                                                f"automaticamente: {e}"}), 400
            if not d.get("tipo"):
                from app.apps.erp.core.titulos.derivacao import derivar_por_contexto
                from app.apps.erp.db.models.cadastros import Categoria
                cat = s.get(Categoria, int(d.get("categoria_id") or 0))
                if cat is None:
                    return jsonify({"ok": False,
                                    "erro": "Escolha a conta do plano financeiro."}), 400
                tipo = derivar_por_contexto(cat, d)
                d["tipo"] = tipo.value
                if tipo.value == "T14_EXCECAO_SEM_NOTA" and not d.get("justificativa_excecao"):
                    d["justificativa_excecao"] = (
                        f"Lançamento sem documento fiscal vinculado — "
                        f"registrado por {usuario.email}.")
            # ⚑ A NOTA NÃO ENTRA SOLTA SE HÁ MATERIAL DESTE FORNECEDOR RECEBIDO
            # E SEM CONTA (21/11/2026, noite). A pergunta do dono: "ficou o
            # pedido recebido, pendente de lançamento de nota, e se alguém
            # tentar lançar a nota sem o pedido, como é que a gente vai
            # garantir que esse pedido seja vinculado depois?" Assim: a tela
            # já oferecia o pedido; agora o servidor EXIGE a resposta — ou a
            # pessoa escolhe o pedido, ou declara que a nota não é de nenhum
            # deles (e a declaração fica registrada).
            pedidos_sem_conta = []
            if not d.get("pedido_compra_id") and d.get("fornecedor_id"):
                try:
                    with s.begin_nested():
                        from app.apps.erp.core.suprimentos import lancamento as _lanc
                        pedidos_sem_conta = [
                            p for p in _lanc.pedidos_do_fornecedor_a_lancar(
                                s, usuario, int(d["fornecedor_id"])) if p["chegou"]]
                except Exception:      # suprimentos fora de uso: o lançamento segue
                    logger.exception("ERP: não consegui procurar pedidos do credor")
                    pedidos_sem_conta = []
                if pedidos_sem_conta and not d.get("nao_e_de_pedido"):
                    return jsonify({
                        "ok": False, "escolher_pedido": True, "pedidos": pedidos_sem_conta,
                        "erro": "Este fornecedor tem material recebido e ainda sem conta "
                                "lançada. Diga de qual pedido é esta nota."}), 409
            titulo = svc.criar_titulo(s, d, usuario)
            if pedidos_sem_conta:
                from app.apps.erp.core.comum.auditoria import registrar_evento
                registrar_evento(s, "titulo", titulo.id, "NOTA_DECLARADA_SEM_PEDIDO",
                                 {"pedidos_recusados": [p["numero"] for p in pedidos_sem_conta],
                                  "declarado_por": usuario.email}, usuario.id)
            # ⚑ LANÇADO A PARTIR DO RECEBIMENTO DE UM PEDIDO DE COMPRA
            # (20/11/2026): o título nasce pelo caminho de sempre, e SÓ ENTÃO
            # é ligado ao pedido — a previsão de pagamento do pedido é baixada
            # e o coordenador passa a ver a conta. Erro aqui desfaz o título
            # inteiro (mesma transação): vínculo pela metade seria pior.
            if d.get("pedido_compra_id"):
                from app.apps.erp.core.suprimentos import lancamento as svc_lanc
                svc_lanc.vincular(s, titulo, int(d["pedido_compra_id"]), usuario,
                                  recebimento_id=d.get("recebimento_id") or None)
            # ⚑ MATERIAL SEM PEDIDO DE COMPRA (21/11/2026): com a chave ligada
            # em Suprimentos › Painel, o lançamento exige o motivo e nasce
            # bloqueado até alguém com alçada autorizar. Desligada, não faz nada.
            from app.apps.erp.core.suprimentos import fora_do_fluxo
            retido = fora_do_fluxo.conferir(s, titulo, d, usuario)
            if d.get("chave_acesso"):
                titulo.chave_acesso_nfe = str(d["chave_acesso"])[:44] or None
            if d.get("cno_documento"):
                titulo.cno_documento = str(d["cno_documento"])[:30] or None
            if critica["alertas"]:
                from app.apps.erp.core.comum.auditoria import registrar_evento
                registrar_evento(s, "titulo", titulo.id, "ALERTAS_DUPLICIDADE_ACEITOS",
                                 {"alertas": critica["alertas"],
                                  "confirmado_por": usuario.email}, usuario.id)
            s.commit()
            return jsonify({"ok": True, "titulo": {
                "id": titulo.id, "numero_sp": titulo.numero_sp,
                "status": titulo.status.value, "risco": titulo.score_risco,
                "fora_do_fluxo": bool(retido),
                "fora_do_fluxo_trava": bool(retido) and fora_do_fluxo.estado(s)[0] == "TRAVAR"}})
    except ErroValidacao as e:
        from app.apps.erp.core.suprimentos.fora_do_fluxo import PrecisaDoMotivo
        if isinstance(e, PrecisaDoMotivo):
            # nada foi gravado: a tela pergunta o motivo e manda de novo
            return jsonify({"ok": False, "erro": str(e), "precisa_motivo": True}), 409
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao criar título")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# Pagamentos e lotes
# ---------------------------------------------------------------------------
@bp.route("/erp/api/pagamentos/agenda")
@login_obrigatorio
@permissao("pagar")
def api_agenda():
    """Parcelas liberadas aguardando pagamento."""
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload
    from app.apps.erp.db.models.cadastros import ContaBancaria
    from app.apps.erp.db.models.financeiro import (
        LoteItem, Parcela, StatusParcela, StatusTitulo, Titulo,
    )
    try:
        with get_session() as s:
            parcelas = s.scalars(
                select(Parcela).join(Titulo, Parcela.titulo_id == Titulo.id)
                .where(Parcela.status.in_([StatusParcela.ABERTA, StatusParcela.AGENDADA]),
                       Titulo.status.in_([StatusTitulo.APROVADO, StatusTitulo.PAGO_PARCIAL]))
                .options(selectinload(Parcela.titulo).selectinload(Titulo.fornecedor),
                         selectinload(Parcela.titulo).selectinload(Titulo.rateios))
                .order_by(Parcela.vencimento)).all()
            em_lote = {i.parcela_id for i in s.scalars(select(LoteItem)).all()}
            hoje = date.today()

            # DE QUE CONTA SAI CADA PAGAMENTO. Pedido do dono: "às vezes a gente
            # quer filtrar o que tem pra pagar nessa conta, o que tem pra pagar
            # na outra — às vezes é mais fácil do que filtrar por obra". A conta
            # vem da OBRA do rateio; título rateado entre obras de contas
            # diferentes aparece nos dois filtros, que é o certo.
            from app.apps.erp.db.models.cadastros import Obra as _Obra
            conta_da_obra = {o.id: o.conta_bancaria_id
                             for o in s.scalars(select(_Obra)).all()}
            nome_da_conta = {c.id: c.descricao for c in s.scalars(
                select(ContaBancaria)).all()}

            def _contas_da_parcela(p):
                ids = {conta_da_obra.get(r.obra_id) for r in p.titulo.rateios}
                return sorted({i for i in ids if i})

            itens = [{
                "parcela_id": p.id, "titulo_id": p.titulo.id,
                "numero_sp": p.titulo.numero_sp, "parcela": p.numero,
                "credor": p.titulo.fornecedor.razao_social,
                "descricao": p.titulo.descricao,
                "obra": " + ".join(sorted({r.obra.codigo for r in p.titulo.rateios if r.obra})),
                "valor": float(p.valor), "vencimento": p.vencimento.isoformat(),
                "atrasada": p.vencimento < hoje,
                "forma": p.titulo.forma_pagamento.value,
                "tem_boleto": bool(p.linha_digitavel),
                "em_lote": p.id in em_lote,
                "contas_da_obra": _contas_da_parcela(p),
                "contas_nomes": [nome_da_conta.get(i, "") for i in _contas_da_parcela(p)],
                # A EMPRESA QUE PAGA (migração 080): é com ela que a tela
                # decide quais contas oferecer, em vez de listar as de todos
                # os CNPJs juntas.
                "empresa_id": p.titulo.empresa_id,
            } for p in parcelas]
            from app.apps.erp.db.models.cadastros import Empresa as _Emp
            nome_empresa = {e.id: (e.nome_fantasia or e.razao_social)
                            for e in s.scalars(select(_Emp)).all()}
            for item in itens:
                item["empresa_nome"] = nome_empresa.get(item["empresa_id"], "")
            contas = [{"id": c.id, "descricao": c.descricao,
                       "empresa_id": c.empresa_id,
                       "empresa_nome": nome_empresa.get(c.empresa_id, "")}
                      for c in s.scalars(select(ContaBancaria)
                                         .where(ContaBancaria.ativo.is_(True))).all()]
        return jsonify({"ok": True, "parcelas": itens, "contas": contas})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha na agenda de pagamentos")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/pagamentos/detalhe/<int:parcela_id>")
@login_obrigatorio
@permissao("ver_dados_pagamento")
def api_detalhe_pagamento(parcela_id: int):
    from app.apps.erp.core.auth.permissoes import exigir_parcela_no_escopo
    from app.apps.erp.core.pagamentos.lotes import dados_pagamento
    try:
        with get_session() as s:
            exigir_parcela_no_escopo(s, _usuario_logado(s), parcela_id)
            return jsonify({"ok": True, "pagamento": dados_pagamento(s, parcela_id)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except ErroNaoEncontrado:
        raise            # vira 404 no errorhandler, nao 500
    except Exception as e:
        logger.exception("ERP: falha no detalhe de pagamento")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/pagamentos/baixar", methods=["POST"])
@login_obrigatorio
@permissao("pagar")
def api_baixar():
    """Registra o pagamento de uma ou várias parcelas."""
    from app.apps.erp.core.auth.permissoes import exigir_parcela_no_escopo
    from app.apps.erp.core.pagamentos import service as svc_pag
    d = request.get_json(silent=True) or {}
    itens = d.get("itens") or []
    conta_id = d.get("conta_bancaria_id")
    data_pg = d.get("data_pagamento") or date.today().isoformat()
    if not itens or not conta_id:
        return jsonify({"ok": False, "erro": "Informe as parcelas e a conta de saída."}), 400
    ok, erros, confirmar = [], [], []
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            for it in itens:
                try:
                    # Parcela sem número não é falha do sistema (22/09/2026):
                    # era um erro 500 com o recado de "falha do sistema", que
                    # assusta e não diz o que fazer.
                    try:
                        parcela_id = int(it.get("parcela_id"))
                    except (TypeError, ValueError):
                        raise ErroValidacao(
                            "Escolha de novo as parcelas a pagar — a seleção se perdeu.")
                    # escopo do OBJETO: ter alçada para pagar não autoriza a
                    # pagar a parcela da obra de outro. Fora do escopo responde
                    # "não encontrado", nunca "sem permissão".
                    exigir_parcela_no_escopo(s, usuario, parcela_id)
                    pg = svc_pag.registrar_pagamento(
                        s, parcela_id=parcela_id,
                        conta_bancaria_id=int(conta_id),
                        data_pagamento=date.fromisoformat(data_pg),
                        valor_pago=it.get("valor_pago") or it.get("valor"),
                        usuario=usuario,
                        confirmar_outra_empresa=bool(d.get("confirmar_outra_empresa")))
                    ok.append({"parcela_id": pg.parcela_id, "valor": float(pg.valor_pago),
                               "pagamento_id": pg.id})
                # PAGAR POR OUTRA EMPRESA não é erro: é uma decisão, e ela
                # volta em campo PRÓPRIO para a tela perguntar uma vez e
                # reenviar confirmado. Misturado com os erros, viraria uma
                # linha vermelha que a pessoa lê como "deu problema".
                except svc_pag.ErroEmpresaDiferente as e:
                    confirmar.append({"parcela_id": it.get("parcela_id"),
                                      "pergunta": str(e)})
                except (ErroValidacao, ErroPermissao) as e:
                    erros.append({"parcela_id": it.get("parcela_id"), "erro": str(e)})
            s.commit()
            avisos = []
            if d.get("avisar", True):
                from app.apps.erp.core.notificacoes import avisar_baixa
                for item in ok:
                    try:
                        avisos.append(avisar_baixa(s, item["pagamento_id"]))
                    except Exception as e:      # aviso não derruba a baixa
                        logger.warning("ERP: aviso falhou (%s)", e)
                s.commit()
        return jsonify({"ok": True, "pagas": ok, "erros": erros,
                        "confirmar": confirmar, "avisos": avisos})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao baixar pagamentos")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/lotes")
@login_obrigatorio
@permissao("pagar")
def api_lotes():
    from app.apps.erp.core.pagamentos.lotes import listar
    try:
        with get_session() as s:
            return jsonify({"ok": True, "lotes": listar(s)})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao listar lotes")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/lotes/<int:lote_id>")
@login_obrigatorio
@permissao("pagar")
def api_lote_detalhe(lote_id: int):
    from app.apps.erp.core.pagamentos.lotes import detalhar
    try:
        with get_session() as s:
            return jsonify({"ok": True, "lote": detalhar(s, lote_id)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404


@bp.route("/erp/api/lotes/criar", methods=["POST"])
@login_obrigatorio
@permissao("pagar")
def api_criar_lote():
    from app.apps.erp.core.pagamentos.lotes import adicionar_parcelas, criar
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            lote = criar(s, d, usuario)
            rel = {"incluidas": [], "recusadas": [], "nao_encontradas": []}
            ids = list(d.get("parcela_ids") or [])
            # o lote nasce com as SPs: coladas como texto ou marcadas na tabela
            if d.get("texto"):
                from app.apps.erp.core.pagamentos.lotes import (
                    extrair_ids_sp, parcelas_por_sp,
                )
                numeros = extrair_ids_sp(d["texto"])
                achado = parcelas_por_sp(s, numeros) if numeros else {}
                ids += [x["parcela_id"] for x in achado.get("parcelas", [])
                        if x.get("status") != "PAGA"]
                rel["nao_encontradas"] = achado.get("nao_encontradas", [])
            if ids:
                achadas = adicionar_parcelas(s, lote.id, sorted(set(ids)), usuario)
                rel.update({k: v for k, v in achadas.items()})
            s.commit()
            return jsonify({"ok": True, "lote_id": lote.id, "relatorio": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao criar lote")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/lotes/<int:lote_id>/parcelas", methods=["POST"])
@login_obrigatorio
@permissao("pagar")
def api_lote_parcelas(lote_id: int):
    from app.apps.erp.core.pagamentos.lotes import adicionar_parcelas, remover_parcela
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if d.get("remover"):
                remover_parcela(s, lote_id, int(d["remover"]), usuario)
                s.commit()
                return jsonify({"ok": True})
            rel = adicionar_parcelas(s, lote_id, d.get("parcela_ids") or [], usuario)
            s.commit()
            return jsonify({"ok": True, "relatorio": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao alterar lote")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/lotes/<int:lote_id>/status", methods=["POST"])
@login_obrigatorio
@permissao("pagar")
def api_lote_status(lote_id: int):
    from app.apps.erp.core.pagamentos.lotes import mudar_status
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            mudar_status(s, lote_id, (d.get("status") or "").upper(), usuario)
            s.commit()
        return jsonify({"ok": True})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/lotes/por-sp", methods=["POST"])
@login_obrigatorio
@permissao("pagar")
def api_lote_por_sp():
    """Recebe o texto colado (a mensagem que volta do solicitante) e devolve
    as parcelas correspondentes."""
    from app.apps.erp.core.pagamentos.lotes import extrair_ids_sp, parcelas_por_sp
    d = request.get_json(silent=True) or {}
    numeros = extrair_ids_sp(d.get("texto") or "")
    if not numeros:
        return jsonify({"ok": False, "erro": "Nenhum número de SP reconhecido no texto."}), 400
    try:
        with get_session() as s:
            return jsonify({"ok": True, "reconhecidos": numeros, **parcelas_por_sp(s, numeros)})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao buscar SPs coladas")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# Conciliação
# ---------------------------------------------------------------------------
@bp.route("/erp/api/conciliacao/painel")
@login_obrigatorio
@permissao("conciliar")
def api_conciliacao_painel():
    from sqlalchemy import select
    from app.apps.erp.core.pagamentos.conciliacao import painel
    from app.apps.erp.db.models.cadastros import ContaBancaria
    conta = request.args.get("conta_id", type=int)
    try:
        with get_session() as s:
            # A EMPRESA vai junto na conciliação também (migração 080):
            # escolher a conta errada aqui joga o extrato de uma empresa dentro
            # de outra, e a conciliação passa a tentar casar lançamentos que
            # nunca vão casar.
            from app.apps.erp.db.models.cadastros import Empresa as _EmpC
            _nome_emp = {e.id: (e.nome_fantasia or e.razao_social)
                         for e in s.scalars(select(_EmpC)).all()}
            contas = [{"id": c.id, "descricao": c.descricao,
                       "empresa_id": c.empresa_id,
                       "empresa_nome": _nome_emp.get(c.empresa_id, "")}
                      for c in s.scalars(select(ContaBancaria)
                                         .where(ContaBancaria.ativo.is_(True))).all()]
            return jsonify({"ok": True, "painel": painel(s, conta), "contas": contas})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha no painel de conciliação")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/conciliacao/executar", methods=["POST"])
@login_obrigatorio
@permissao("conciliar")
def api_conciliar():
    from app.apps.erp.core.pagamentos.conciliacao import conciliar_automatico
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            rel = conciliar_automatico(
                s, conta_bancaria_id=d.get("conta_id") or None, usuario=usuario)
            s.commit()
        return jsonify({"ok": True, "relatorio": rel})
    except IntegrityError:
        # outra pessoa executou a conciliação no mesmo instante: a restrição
        # única barrou a repetição. Nada gravado; é só rodar de novo.
        return jsonify({"ok": False, "erro": "Outra conciliação acabou de rodar. "
                        "Recarregue a tela e execute de novo."}), 409
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha na conciliação")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/conciliacao/manual", methods=["POST"])
@login_obrigatorio
@permissao("conciliar")
def api_conciliar_manual():
    from app.apps.erp.core.pagamentos.conciliacao import conciliar_manual
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            # CASAR EXIGE OS DOIS LADOS (22/09/2026). Sem eles, ou com a
            # seleção estragada, isto estourava como erro 500 — e quem estava
            # conciliando lia "falha do sistema" em vez de "escolha o
            # pagamento e a linha do extrato".
            try:
                pagamento_id = int(d["pagamento_id"])
                extrato_id = int(d["extrato_id"])
            except (KeyError, TypeError, ValueError):
                return jsonify({"ok": False, "erro":
                                "Escolha o pagamento E a linha do extrato para "
                                "casar os dois."}), 400
            conciliar_manual(s, pagamento_id, extrato_id, usuario,
                             d.get("observacao", ""))
            s.commit()
        return jsonify({"ok": True})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except IntegrityError:
        # duas pessoas casando a mesma linha no mesmo instante: a restrição
        # única do banco barrou a segunda. Nada gravado.
        return jsonify({"ok": False, "erro": "Alguém acabou de conciliar esta "
                        "linha. Atualize a tela para ver o casamento que "
                        "ficou."}), 409
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha na conciliação manual")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# Relatórios
# ---------------------------------------------------------------------------
@bp.route("/erp/api/relatorios", methods=["POST"])
@login_obrigatorio
@permissao("ver_relatorios")
def api_relatorios():
    from app.apps.erp.core.relatorios import (
        analitico, curva_abc, dre_gerencial, fluxo_de_caixa, resumo)
    d = request.get_json(silent=True) or {}
    tipo = (d.get("tipo") or "resumo").strip()
    filtros = d.get("filtros") or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if tipo == "dre":
                return jsonify({"ok": True, "dre": dre_gerencial(s, filtros, usuario)})
            if tipo == "analitico":
                return jsonify({"ok": True,
                                "linhas": analitico(s, filtros, usuario)})
            if tipo == "abc":
                return jsonify({"ok": True,
                                "resumo": curva_abc(s, d.get("dimensao") or "credor",
                                                    filtros, usuario)})
            if tipo == "fluxo":
                return jsonify({"ok": True, "fluxo": fluxo_de_caixa(
                    s, filtros, usuario,
                    periodos=int(d.get("periodos") or 13),
                    passo=(d.get("passo") or "semana"),
                    saldo_inicial=float(d.get("saldo_inicial") or 0))})
            return jsonify({"ok": True,
                            "resumo": resumo(s, d.get("dimensao") or "grupo", filtros,
                                             usuario)})
    except ValueError as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha no relatório")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/relatorios/csv", methods=["POST"])
@login_obrigatorio
@permissao("ver_relatorios")
def api_relatorios_csv():
    from flask import Response
    from app.apps.erp.core.relatorios import analitico, para_csv, resumo
    d = request.get_json(silent=True) or {}
    filtros = d.get("filtros") or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if (d.get("tipo") or "") == "analitico":
                linhas = analitico(s, filtros, usuario)
                colunas = [("numero_sp", "SP"), ("competencia", "Competência"),
                           ("credor", "Credor"), ("descricao", "Descrição"),
                           ("grupo", "Grupo"), ("conta", "Conta"), ("obra", "Obra"),
                           ("valor", "Valor"), ("vencimento", "Vencimento"),
                           ("pagamento", "Pagamento"), ("situacao", "Situação"),
                           ("dedutibilidade", "Dedutibilidade")]
                nome = "erp_analitico.csv"
            else:
                r = resumo(s, d.get("dimensao") or "grupo", filtros, usuario)
                linhas = r["linhas"]
                colunas = [("chave", r["rotulo"]), ("titulos", "Títulos"),
                           ("total", "Total"), ("pago", "Pago"), ("aberto", "Em aberto"),
                           ("percentual", "%")]
                nome = f"erp_{d.get('dimensao') or 'grupo'}.csv"
            conteudo = para_csv(linhas, colunas)
        return Response(conteudo.encode("utf-8-sig"), mimetype="text/csv",
                        headers={"Content-Disposition": f'attachment; filename="{nome}"'})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao exportar CSV")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# Extrato, movimentações e recebimentos
# ---------------------------------------------------------------------------
@bp.route("/erp/api/conciliacao/extrato")
@login_obrigatorio
@permissao("conciliar")
def api_extrato():
    from app.apps.erp.core.pagamentos.conciliacao import extrato_detalhado
    try:
        with get_session() as s:
            linhas = extrato_detalhado(
                s, conta_bancaria_id=request.args.get("conta_id", type=int),
                situacao=request.args.get("situacao", "todos"))
        return jsonify({"ok": True, "linhas": linhas})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao listar extrato")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/conciliacao/candidatos/<int:extrato_id>")
@login_obrigatorio
@permissao("conciliar")
def api_candidatos(extrato_id: int):
    from app.apps.erp.core.pagamentos.conciliacao import candidatos_para_extrato
    try:
        with get_session() as s:
            return jsonify({"ok": True, "candidatos": candidatos_para_extrato(s, extrato_id)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404


@bp.route("/erp/api/movimentacoes", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="conciliar", POST="conciliar")
def api_movimentacoes():
    from app.apps.erp.core.titulos.receber import (
        TIPOS_MOVIMENTO, criar_movimentacao, listar_movimentacoes,
    )
    if request.method == "GET":
        try:
            with get_session() as s:
                return jsonify({"ok": True, "movimentacoes": listar_movimentacoes(s),
                                "tipos": [{"chave": k, "rotulo": v[0], "conta": v[1],
                                           "exige_origem": v[2], "exige_destino": v[3]}
                                          for k, v in TIPOS_MOVIMENTO.items()]})
        except ErroNaoEncontrado:
            raise        # recusa de escopo vira 404, nunca 500
        except Exception as e:
            logger.exception("ERP: falha ao listar movimentações")
            return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            mov = criar_movimentacao(s, d, usuario)
            s.commit()
            return jsonify({"ok": True, "movimentacao_id": mov.id,
                            "neutra": mov.neutra, "par_id": mov.par_id})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao criar movimentação")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/receber", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="receber", POST="receber")
def api_receber():
    from app.apps.erp.core.titulos.receber import criar_medicao, listar_receber
    if request.method == "GET":
        try:
            with get_session() as s:
                return jsonify({"ok": True, "titulos": listar_receber(s, {
                    "obra_id": request.args.get("obra_id", type=int),
                    "status": request.args.get("status")})})
        except ErroNaoEncontrado:
            raise        # recusa de escopo vira 404, nunca 500
        except Exception as e:
            logger.exception("ERP: falha ao listar recebíveis")
            return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            t = criar_medicao(s, d, usuario)
            s.commit()
            return jsonify({"ok": True, "titulo": {"id": t.id, "numero_sp": t.numero_sp,
                                                   "liquido": float(t.valor_liquido)}})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao lançar medição")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/receber/baixar", methods=["POST"])
@login_obrigatorio
@permissao("receber")
def api_receber_baixar():
    from app.apps.erp.core.titulos.receber import registrar_recebimento
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            rel = registrar_recebimento(
                s, parcela_id=int(d["parcela_id"]),
                conta_bancaria_id=int(d["conta_bancaria_id"]),
                data_recebimento=date.fromisoformat(d.get("data") or date.today().isoformat()),
                valor=d.get("valor"), notas_fiscais=d.get("notas_fiscais") or [],
                usuario=usuario)
            s.commit()
        return jsonify({"ok": True, "recebimento": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha no recebimento")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# Ajustes: reclassificar e desfazer
# ---------------------------------------------------------------------------
@bp.route("/erp/api/titulos/<int:titulo_id>/reclassificar", methods=["POST"])
@login_obrigatorio
@permissao("reclassificar")
def api_reclassificar(titulo_id: int):
    """Troca conta do plano e/ou obra mesmo com o título pago e conciliado."""
    from app.apps.erp.core.titulos.ajustes import reclassificar
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            rel = reclassificar(s, titulo_id,
                                categoria_id=d.get("categoria_id"),
                                rateios=d.get("rateios"),
                                motivo=d.get("motivo", ""), usuario=usuario)
            s.commit()
        return jsonify({"ok": True, "relatorio": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao reclassificar")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/titulos/<int:titulo_id>/desfazer", methods=["GET", "POST"])
@login_obrigatorio
@permissao("desfazer")
def api_desfazer(titulo_id: int):
    """GET diz o que será desfeito; POST desfaz conciliação e baixa de uma vez."""
    from app.apps.erp.core.titulos.ajustes import desfazer_em_cadeia, diagnosticar_desfazer
    try:
        if request.method == "GET":
            with get_session() as s:
                return jsonify({"ok": True, "diagnostico": diagnosticar_desfazer(s, titulo_id)})
        d = request.get_json(silent=True) or {}
        with get_session() as s:
            usuario = _usuario_logado(s)
            rel = desfazer_em_cadeia(s, titulo_id, d.get("motivo", ""), usuario,
                                     ate=(d.get("ate") or "APROVADO").upper())
            s.commit()
        return jsonify({"ok": True, "relatorio": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao desfazer")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# CHAVES PIX DA CONTA — e o bloco pronto para copiar
#
# O uso é o que o dono descreveu: não é pagar por aqui, é COPIAR E MANDAR
# quando alguém pede os dados da empresa. Por isso a listagem já vem com o
# texto montado — copiar campo por campo é onde se erra um dígito.
# ---------------------------------------------------------------------------
@bp.route("/erp/api/contas/detalhe")
@login_obrigatorio
@permissao("ver_erp")
def api_contas_detalhe():
    from app.apps.erp.core.cadastros import contas as svc_contas
    with get_session() as s:
        return jsonify({"ok": True, "contas": svc_contas.listar(s),
                        "tipos_pix": svc_contas.TIPOS_PIX})


@bp.route("/erp/api/contas/<int:conta_id>/empresa", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_conta_empresa(conta_id: int):
    """Marca de quem é uma conta que nasceu antes da migração 080.

    Existe porque as contas antigas ficaram SEM empresa de propósito: atribuir
    sozinho a empresa padrão seria adivinhar em cima de dado bancário. São
    poucas, e quem sabe de quem é cada uma é o dono.
    """
    from app.apps.erp.core.cadastros import contas as svc_contas
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            svc_contas.definir_empresa(s, conta_id, d.get("empresa_id"),
                                       usuario=_usuario_logado(s))
            s.commit()
            return jsonify({"ok": True, "contas": svc_contas.listar(s)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/contas/<int:conta_id>/pix", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_conta_pix(conta_id: int):
    from app.apps.erp.core.cadastros import contas as svc_contas
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            svc_contas.acrescentar_pix(
                s, conta_id, tipo=(d.get("tipo") or ""), chave=(d.get("chave") or ""),
                descricao=(d.get("descricao") or ""), usuario=_usuario_logado(s))
            s.commit()
            return jsonify({"ok": True, "contas": svc_contas.listar(s)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/contas/pix/<int:chave_id>", methods=["DELETE"])
@login_obrigatorio
@permissao("configurar")
def api_conta_pix_remover(chave_id: int):
    from app.apps.erp.core.cadastros import contas as svc_contas
    try:
        with get_session() as s:
            svc_contas.remover_pix(s, chave_id, usuario=_usuario_logado(s))
            s.commit()
            return jsonify({"ok": True, "contas": svc_contas.listar(s)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


# ---------------------------------------------------------------------------
# EMISSÃO DE NOTA POR EMPRESA
#
# São duas empresas, em municípios diferentes, uma por API e outra manual — por
# isso município, endereço e token são CADASTRO, não constante no código.
# ---------------------------------------------------------------------------
@bp.route("/erp/api/empresas/<int:empresa_id>/emissao", methods=["GET", "POST"])
@login_obrigatorio
@permissao("configurar")
def api_empresa_emissao(empresa_id: int):
    from app.apps.erp.core.cadastros import emissao as svc_emissao
    from app.apps.erp.db.models.cadastros import Empresa
    try:
        with get_session() as s:
            if request.method == "POST":
                empresa = svc_emissao.definir(s, empresa_id,
                                              request.get_json(silent=True) or {},
                                              usuario=_usuario_logado(s))
                s.commit()
            else:
                empresa = s.get(Empresa, empresa_id)
                if empresa is None:
                    raise ErroNaoEncontrado("Empresa não encontrada.")
            return jsonify({"ok": True, "emissao": svc_emissao.ler(s, empresa)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/contas")
@login_obrigatorio
@permissao("ver_erp")
def api_contas_bancarias():
    """As contas bancárias da empresa, para escolher a da obra.

    A tela de Obras pedia esta lista e recebia "não encontrado" — o campo
    "conta bancária" do cadastro da obra abria SEMPRE vazio, sem erro na tela,
    porque a chamada morria dentro de um `try`. Quem cadastrava obra não tinha
    como escolher a conta.

    Lê e só lê, então basta `ver_erp`: quem CRIA conta continua precisando de
    `configurar`, em /erp/api/config/conta.
    """
    from sqlalchemy import select
    from app.apps.erp.db.models.cadastros import ContaBancaria
    with get_session() as s:
        contas = s.scalars(select(ContaBancaria)
                           .order_by(ContaBancaria.descricao)).all()
        return jsonify({"ok": True, "contas": [
            {"id": c.id, "descricao": c.descricao, "banco": c.banco_codigo,
             "agencia": c.agencia, "conta": c.conta, "ativo": c.ativo,
             "empresa_id": c.empresa_id}
            for c in contas if c.ativo]})


@bp.route("/erp/api/obras/nova", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_criar_obra():
    """Cria a obra. É o ÚNICO lugar que cria obra à mão.

    Existiam dois formulários — um em Configurações, com oito campos, e outro
    no painel de Obras, com cinco. A mesma obra nascia diferente conforme a
    porta de entrada, e quem entrava pela porta curta não sabia que a outra
    existia. O dono viu isso em 10/09/2026 e resolveu: *"se a gente tem o
    painel de obras, não tem mais que ter obras em administração"*. Ficou um
    formulário só, no painel — e Configurações passa a apontar para lá.
    """
    from app.apps.erp.core.cadastros import obras as svc_obra
    dados = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            obra = svc_obra.criar(s, dados, usuario)
            criada = {"id": obra.id, "codigo": obra.codigo, "nome": obra.nome}
            s.commit()
        return jsonify({"ok": True, "obra": criada})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao criar obra")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/obras/<int:obra_id>", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_erp", POST="configurar")
def api_obra(obra_id: int):
    """Cadastro completo da obra: identificação, endereço, contrato e tributação."""
    from sqlalchemy import select
    from sqlalchemy import text as _text
    from app.apps.erp.core.titulos.tributacao import resumo_tributacao
    from app.apps.erp.db.models.cadastros import Obra, ObraAditivo
    from app.apps.erp.core.comum.auditoria import registrar_evento
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
            exigir_obra_no_escopo(s, _usuario_logado(s), obra_id)
            obra = s.get(Obra, obra_id)
            if obra is None:
                return jsonify({"ok": False, "erro": "Obra não encontrada."}), 404
            if request.method == "POST":
                usuario = _usuario_logado(s)
                d = request.get_json(silent=True) or {}
                # SEGURO-GARANTIA entra na lista em 22/09/2026, com a caução,
                # a validade da apólice e o código do departamento no Omie.
                # Os quatro campos APARECIAM na tela de cadastro da obra, a
                # pessoa preenchia, o sistema respondia "Salvo." — e nada era
                # gravado, porque não estavam nesta lista. Mesmo defeito do
                # código da obra, que o dono achou usando: o que não está aqui
                # é descartado em silêncio, e a tela recarrega mostrando vazio.
                texto = ("nome objeto cliente cnpj_cliente contrato municipio uf cno endereco "
                         "bairro numero_endereco complemento cep responsavel_tecnico art_rrt "
                         # ⚑ `crea_obra` ENTRA EM 14/11/2026, e é o mesmo
                         # defeito do seguro-garantia: a coluna existe desde a
                         # migração, o DIÁRIO IMPRIME o CREA — e não havia
                         # onde digitá-lo. O papel saía com o campo vazio e
                         # ninguém sabia por quê. Ele perguntou: *"me explica
                         # de onde é que está vindo o responsável técnico,
                         # número do registro profissional e a empresa."*
                         "crea_obra "
                         # ⚑ O CENTRO DE CUSTO DA CONTABILIDADE (16/11/2026).
                         # É o código que o escritório usa para a MESMA obra
                         # ("164.01" onde a BWS diz "094"). As duas
                         # nomenclaturas convivem por decisão do dono; é por
                         # este campo que a ficha do Fortes acha a obra da
                         # pessoa. Obra sem ele faz a ficha entrar sem obra.
                         "centro_custo_contabil "
                         "engenheiro_fiscal ordem_servico indice_reajuste regime_obra "
                         "observacoes_fiscais orgao_resumido status seguro_garantia "
                         # O CONTRATANTE NO PAPEL DO DIÁRIO (09/11/2026): a
                         # unidade é a linha abaixo do órgão ("Secretaria de
                         # Obras e Defesa Civil") e o modelo é o formato de
                         # diário que ESTE contrato exige.
                         "contratante_unidade diario_modelo").split()
                if "conta_bancaria_id" in d:
                    obra.conta_bancaria_id = (int(d["conta_bancaria_id"])
                                              if d["conta_bancaria_id"] else None)
                numeros = ("valor_contrato latitude longitude "
                           "aliquota_iss aliquota_iss_pct pct_servico_iss "
                           "pct_servico_inss caucao_pct").split()
                datas = ("vigencia_inicio vigencia_fim data_base_orcamento data_ordem_servico "
                         "data_inicio data_termino seguro_vigencia_fim").split()
                booleanos = "iss_retido inss_retido aceita_deducao_material".split()
                from decimal import Decimal, InvalidOperation
                from datetime import date as _date
                antes = {"aliquota_iss": str(obra.aliquota_iss_pct),
                         "valor_contrato": str(obra.valor_contrato)}
                for campo in texto:
                    if campo in d:
                        setattr(obra, campo, (str(d[campo]).strip() or None))
                for campo in numeros:
                    if campo in d:
                        bruto = str(d[campo]).strip()
                        if campo in ("latitude", "longitude"):
                            # coordenada não é moeda: o ponto é decimal, não milhar
                            v = bruto.replace(",", ".")
                        elif "," in bruto:
                            v = bruto.replace(".", "").replace(",", ".")
                        else:
                            v = bruto
                        try:
                            setattr(obra, campo, Decimal(v) if v else None)
                        except InvalidOperation:
                            return jsonify({"ok": False,
                                            "erro": f"Valor inválido em {campo}."}), 400
                for campo in datas:
                    if campo in d:
                        v = str(d[campo]).strip()
                        try:
                            setattr(obra, campo, _date.fromisoformat(v) if v else None)
                        except ValueError:
                            # DATA QUE NÃO EXISTE não é falha do sistema
                            # (22/09/2026): "31/02/2026" ou uma data escrita no
                            # formato brasileiro caía como erro 500, com o
                            # recado de "falha do sistema" — que não diz à
                            # pessoa que basta corrigir o dia.
                            return jsonify({"ok": False, "erro":
                                            f"Data inválida em {campo}: {v!r}. "
                                            f"Use dia/mês/ano que existam."}), 400
                for campo in booleanos:
                    if campo in d:
                        setattr(obra, campo, bool(d[campo]))
                if "federais_retidos" in d:
                    obra.federais_retidos = [str(x).upper() for x in (d["federais_retidos"] or [])]
                if "prazo_execucao_dias" in d:
                    # prazo negativo passava direto e virava "-5 dias de
                    # execução" no cadastro (22/09/2026). Não é dado, é engano
                    # de digitação — e melhor dizer isso na hora.
                    try:
                        dias = int(str(d["prazo_execucao_dias"] or 0).strip() or 0)
                    except ValueError:
                        return jsonify({"ok": False, "erro":
                                        "Prazo de execução tem de ser um número de dias."}), 400
                    if dias < 0:
                        return jsonify({"ok": False, "erro":
                                        "Prazo de execução não pode ser negativo."}), 400
                    obra.prazo_execucao_dias = dias or None
                if "conta_recebimento_id" in d:
                    obra.conta_recebimento_id = d["conta_recebimento_id"] or None
                # O projeto que agrupa a obra (migração 066). Vazio = sem
                # projeto, que é o caso comum.
                if "projeto_id" in d:
                    obra.projeto_id = int(d["projeto_id"]) if d["projeto_id"] else None
                # CÓDIGO DO DEPARTAMENTO NO OMIE. Fica de fora da lista de
                # texto porque é ÚNICO no banco: repetido, o salvamento
                # estouraria com erro de banco em vez de dizer o que houve.
                if "codigo_omie_depto" in d:
                    novo_depto = (str(d["codigo_omie_depto"]) or "").strip() or None
                    if novo_depto and novo_depto != obra.codigo_omie_depto:
                        from app.apps.erp.db.models.cadastros import Obra as _ObD
                        ja = s.scalars(select(_ObD).where(
                            _ObD.codigo_omie_depto == novo_depto,
                            _ObD.id != obra.id)).first()
                        if ja is not None:
                            return jsonify({"ok": False, "erro":
                                            f"O departamento {novo_depto} do Omie já está "
                                            f"na obra {ja.codigo}."}), 400
                    obra.codigo_omie_depto = novo_depto
                # A EMPRESA DA OBRA, editável DEPOIS de criada (17/09/2026):
                # *"depois de criada a obra, tem como selecionar a empresa
                # fácil?"*. Não tinha — o campo só existia na criação, e as
                # obras que nasceram soltas não tinham conserto pela tela.
                # O CÓDIGO DA OBRA, editável (22/09/2026). O dono: *"tentei
                # alterar o código de uma obra, ele salva, mas quando volta
                # pro cadastro não muda."*
                #
                # O defeito era silencioso e vale entender o formato: a tela
                # SEMPRE mandou o `codigo`, e a lista de campos que esta rota
                # aceita simplesmente não o incluía. O que não está na lista é
                # descartado sem reclamar — então o salvamento respondia "ok",
                # a tela recarregava do banco, e o código voltava o mesmo. Erro
                # que responde sucesso é pior que erro que responde erro.
                #
                # Trocar o código é RENOMEAR, não criar outra obra: os
                # lançamentos apontam para o número interno dela e seguem
                # intactos. Mas o código aparece em relatório, em planilha e
                # na cabeça das pessoas — por isso a troca fica registrada na
                # trilha com o de e o para.
                if "codigo" in d:
                    novo_codigo = (str(d["codigo"]) or "").strip().upper()
                    if not novo_codigo:
                        return jsonify({"ok": False,
                                        "erro": "O código da obra não pode ficar vazio."}), 400
                    if novo_codigo != (obra.codigo or "").upper():
                        from app.apps.erp.db.models.cadastros import Obra as _Ob
                        ja = s.scalars(select(_Ob).where(
                            _Ob.codigo == novo_codigo, _Ob.id != obra.id)).first()
                        if ja is not None:
                            return jsonify({"ok": False, "erro":
                                            f"Já existe outra obra com o código "
                                            f"{novo_codigo} ({ja.nome})."}), 400
                        registrar_evento(s, "obra", obra.id, "CODIGO_ALTERADO",
                                         {"de": obra.codigo, "para": novo_codigo},
                                         usuario.id if usuario else None)
                        obra.codigo = novo_codigo
                if "empresa_id" in d:
                    from app.apps.erp.db.models.cadastros import Empresa
                    novo_id = int(d["empresa_id"]) if d["empresa_id"] else None
                    if novo_id is not None and s.get(Empresa, novo_id) is None:
                        return jsonify({"ok": False,
                                        "erro": "Empresa não encontrada."}), 400
                    if novo_id != obra.empresa_id:
                        registrar_evento(
                            s, "obra", obra.id, "EMPRESA_DEFINIDA",
                            {"de": obra.empresa_id, "para": novo_id},
                            usuario.id if usuario else None)
                    obra.empresa_id = novo_id
                s.flush()
                registrar_evento(s, "obra", obra.id, "ATUALIZADA",
                                 {"codigo": obra.codigo, "antes": antes}, usuario.id)
                s.commit()

            aditivos = s.scalars(select(ObraAditivo)
                                 .where(ObraAditivo.obra_id == obra.id)
                                 .order_by(ObraAditivo.numero)).all()
            acrescimo = sum(float(a.valor) for a in aditivos)
            dias_extra = sum(a.dias for a in aditivos)
            dados = {c: getattr(obra, c) for c in (
                "id codigo nome objeto cliente cnpj_cliente contrato municipio uf cno endereco "
                "bairro numero_endereco complemento cep responsavel_tecnico art_rrt "
                # Volta junto, senão o formulário remonta vazio e o salvamento
                # seguinte APAGA — o defeito do seguro-garantia, de novo.
                "crea_obra "
                # Volta junto, senão o formulário remonta vazio e o
                # salvamento seguinte APAGA — o defeito do seguro-garantia.
                "centro_custo_contabil "
                "engenheiro_fiscal ordem_servico indice_reajuste regime_obra status "
                "observacoes_fiscais orgao_resumido codigo_omie_depto ref_pipefy "
                # Voltam junto, senão o formulário remonta vazio e o
                # salvamento seguinte apaga — o defeito do seguro-garantia.
                "contratante_unidade diario_modelo "
                "iss_retido inss_retido aceita_deducao_material prazo_execucao_dias "
                # SEGURO-GARANTIA, VALIDADE E CAUÇÃO voltam junto (22/09/2026),
                # pelo mesmo motivo da conta: gravados e nunca devolvidos, o
                # formulário remontava vazio e o salvamento seguinte apagava.
                "seguro_garantia seguro_vigencia_fim caucao_pct "
                # A CONTA DE PAGAMENTO DA OBRA volta junto (22/09/2026). Ela era
                # gravada e nunca devolvida: o formulário remontava a caixinha
                # vazia, e o salvamento seguinte — que manda o campo em branco —
                # APAGAVA a conta sem ninguém pedir. Mesmo defeito do código da
                # obra, só que pior, porque perdia informação em silêncio.
                "conta_bancaria_id conta_recebimento_id projeto_id empresa_id").split()}
            for campo in ("valor_contrato", "latitude", "longitude",
                          "aliquota_iss", "aliquota_iss_pct",
                          "pct_servico_iss", "pct_servico_inss", "caucao_pct"):
                v = getattr(obra, campo, None)
                dados[campo] = float(v) if v is not None else None
            for campo in ("vigencia_inicio", "vigencia_fim", "data_base_orcamento",
                          "data_ordem_servico", "data_inicio", "data_termino",
                          "seguro_vigencia_fim"):
                v = getattr(obra, campo, None)
                dados[campo] = v.isoformat() if v else None
            dados["federais_retidos"] = list(obra.federais_retidos or [])
            # SÓ O "TEM OU NÃO TEM": a imagem da logo não volta no JSON do
            # cadastro. Meio megabyte em base64 a cada abertura de ficha é
            # peso puro — a tela precisa saber se há logo para mostrar o
            # quadrinho, e a imagem vem pela rota dela.
            dados["tem_logo_contratante"] = bool(obra.contratante_logo)
            # A logo da EMPRESA mora no cadastro dela, mas quem monta o
            # cabeçalho do diário está aqui, na obra — e ele pediu em
            # 09/11/2026 que desse para resolver as duas no mesmo lugar. A
            # tela mostra o quadrinho e manda para a rota da empresa; o dado
            # continua sendo um só, da empresa, e serve aos outros documentos
            # que o sistema vier a emitir.
            dados["tem_logo_empresa"] = bool(
                obra.empresa_id and s.execute(
                    _text("SELECT logo IS NOT NULL FROM empresas WHERE id = :e"),
                    {"e": obra.empresa_id}).scalar())
            dados["resumo_tributacao"] = resumo_tributacao(obra)
            dados["valor_vigente"] = round(float(obra.valor_contrato or 0) + acrescimo, 2)
            dados["aditivos"] = [{
                "id": a.id, "numero": a.numero, "tipo": a.tipo, "valor": float(a.valor),
                "dias": a.dias, "objeto": a.objeto,
                "data_assinatura": a.data_assinatura.isoformat() if a.data_assinatura else None,
                "nova_vigencia_fim": (a.nova_vigencia_fim.isoformat()
                                      if a.nova_vigencia_fim else None)} for a in aditivos]
            dados["dias_aditivados"] = dias_extra
        return jsonify({"ok": True, "obra": dados})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha no cadastro da obra %s", obra_id)
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/obras/<int:obra_id>/responsaveis", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_erp", POST="configurar")
def api_obra_responsaveis(obra_id: int):
    """Quem responde por esta obra — o outro lado da mesma ligação.

    O dono pediu para marcar pelos dois lados, "porque facilita o manuseio do
    sistema". Aceita mais de uma pessoa: as duas recebem a cobrança.
    """
    from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
    from app.apps.erp.core.cadastros import vinculos
    with get_session() as s:
        exigir_obra_no_escopo(s, _usuario_logado(s), obra_id)
        if request.method == "GET":
            return jsonify({"ok": True,
                            "operadores": vinculos.operadores_da_obra(s, obra_id)})
        d = request.get_json(silent=True) or {}
        r = vinculos.definir_responsaveis_da_obra(s, obra_id, d.get("responsaveis"))
        s.commit()
        return jsonify({"ok": True, **r,
                        "operadores": vinculos.operadores_da_obra(s, obra_id)})


@bp.route("/erp/api/obras/<int:obra_id>/aditivos", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_aditivo(obra_id: int):
    from decimal import Decimal
    from datetime import date as _date
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.db.models.cadastros import Obra, ObraAditivo
    d = request.get_json(silent=True) or {}
    numero = (d.get("numero") or "").strip()
    tipo = (d.get("tipo") or "VALOR").upper()
    if not numero:
        return jsonify({"ok": False, "erro": "Informe o número do aditivo."}), 400
    if tipo not in ("VALOR", "PRAZO", "VALOR_E_PRAZO", "REAJUSTE", "SUPRESSAO"):
        return jsonify({"ok": False, "erro": f"Tipo inválido: {tipo}"}), 400
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
            exigir_obra_no_escopo(s, _usuario_logado(s), obra_id)
            usuario = _usuario_logado(s)
            obra = s.get(Obra, obra_id)
            if obra is None:
                return jsonify({"ok": False, "erro": "Obra não encontrada."}), 404
            valor = Decimal(str(d.get("valor") or 0).replace(".", "").replace(",", "."))
            if tipo == "SUPRESSAO" and valor > 0:
                valor = -valor                      # supressão reduz o contrato
            nova_vig = (_date.fromisoformat(d["nova_vigencia_fim"])
                        if d.get("nova_vigencia_fim") else None)
            s.add(ObraAditivo(
                obra_id=obra.id, numero=numero, tipo=tipo, valor=valor,
                dias=int(d.get("dias") or 0), nova_vigencia_fim=nova_vig,
                data_assinatura=(_date.fromisoformat(d["data_assinatura"])
                                 if d.get("data_assinatura") else None),
                objeto=(d.get("objeto") or "").strip() or None, criado_por=usuario.id))
            if nova_vig:
                obra.vigencia_fim = nova_vig       # o aditivo de prazo move a vigência
            s.flush()
            registrar_evento(s, "obra", obra.id, "ADITIVO_REGISTRADO", {
                "numero": numero, "tipo": tipo, "valor": str(valor),
                "dias": int(d.get("dias") or 0)}, usuario.id)
            s.commit()
        return jsonify({"ok": True})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao registrar aditivo")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/obras/<int:obra_id>/tributacao", methods=["POST"])
@login_obrigatorio
@permissao("ver_erp")
def api_simular_tributacao(obra_id: int):
    """Simula as retenções de uma medição com o cadastro fiscal da obra."""
    from app.apps.erp.core.titulos.tributacao import calcular
    from app.apps.erp.db.models.cadastros import Obra
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
            exigir_obra_no_escopo(s, _usuario_logado(s), obra_id)
            obra = s.get(Obra, obra_id)
            if obra is None:
                return jsonify({"ok": False, "erro": "Obra não encontrada."}), 404
            calc = calcular(obra, d.get("valor") or 0,
                            sem_deducao=bool(d.get("sem_deducao")),
                            pct_servico_iss=d.get("pct_servico_iss"),
                            pct_servico_inss=d.get("pct_servico_inss"),
                            aliquota_iss=d.get("aliquota_iss"))
        return jsonify({"ok": True, "calculo": calc.como_dict()})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao simular tributação")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/pagamentos/comprovante", methods=["POST"])
@login_obrigatorio
@permissao("pagar")
def api_comprovante():
    """Baixa por comprovante: lê o PDF/foto do banco, acha o título e baixa."""
    from app.apps.erp.core.pagamentos.comprovante import processar_comprovante
    arquivo = request.files.get("arquivo")
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Envie o comprovante."}), 400
    try:
        conteudo = arquivo.read()
        with get_session() as s:
            usuario = _usuario_logado(s)
            rel = processar_comprovante(
                s, conteudo, arquivo.filename or "comprovante.pdf",
                conta_bancaria_id=request.form.get("conta_id", type=int),
                baixar_automatico=(request.form.get("automatico", "1") != "0"),
                origem="TELA", usuario=usuario)
            s.commit()
            if rel.get("situacao") == "BAIXADO" and rel.get("baixa"):
                from app.apps.erp.core.notificacoes import avisar_baixa
                from app.apps.erp.db.models.financeiro import Pagamento
                from sqlalchemy import select as _sel
                pg = s.scalars(_sel(Pagamento).where(
                    Pagamento.parcela_id == rel["baixa"]["parcela_id"])
                    .order_by(Pagamento.id.desc())).first()
                if pg is not None:
                    try:
                        rel["aviso"] = avisar_baixa(s, pg.id)
                        s.commit()
                    except ErroNaoEncontrado:
                        raise        # recusa de escopo vira 404, nunca 500
                    except Exception as e:
                        logger.warning("ERP: aviso do comprovante falhou (%s)", e)
        return jsonify({"ok": True, "resultado": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao processar comprovante")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/pagamentos/comprovantes/lote", methods=["POST"])
@permissao_publica("chamada pelo cenario do Make, que recebe os comprovantes por "
                   "e-mail; a guarda e o ERP_COMPROVANTE_SECRET no corpo do pedido, "
                   "como nos demais modulos do monorepo")
def api_comprovantes_lote():
    """A porta do e-mail: um lote de comprovantes, vindo de fora.

    O Make já recebe o e-mail e separa os anexos — ele é o carteiro. Esta rota
    é o destino. O MIOLO É O MESMO da tela: mesmo leitor, mesmo casamento,
    MESMA TRAVA contra baixar duas vezes. É isso que garante que o comprovante
    dê o mesmo resultado venha por onde vier.

    Formato esperado (o mesmo que o Make já monta hoje):
        {"secret": "...", "attachments": [{"filename": "...", "base64": "..."}]}
    """
    import base64 as _b64
    import os
    from app.apps.erp.core.pagamentos.comprovante import processar_comprovante

    esperado = os.getenv("ERP_COMPROVANTE_SECRET", "").strip()
    dados = request.get_json(silent=True) or {}
    if not esperado:
        return jsonify({"ok": False, "erro": "ERP_COMPROVANTE_SECRET não "
                        "configurado no ambiente. A porta do e-mail não abre "
                        "sem ele."}), 503
    if dados.get("secret") != esperado:
        logger.warning("ERP/comprovantes: lote recusado, segredo inválido")
        return jsonify({"ok": False, "erro": "Segredo inválido."}), 403

    anexos = dados.get("attachments") or dados.get("anexos") or []
    if not anexos:
        return jsonify({"ok": False, "erro": "Nenhum comprovante no lote."}), 400

    resultados = []
    with get_session() as s:
        for anexo in anexos[:50]:          # teto: lote gigante é engano, não uso
            nome = (anexo.get("filename") or anexo.get("nome")
                    or "comprovante.pdf")
            try:
                conteudo = _b64.b64decode(anexo.get("base64") or anexo.get("conteudo") or "")
            except Exception:
                resultados.append({"arquivo": nome, "situacao": "ILEGIVEL",
                                   "mensagem": "Anexo não pôde ser decodificado."})
                continue
            if not conteudo:
                resultados.append({"arquivo": nome, "situacao": "ILEGIVEL",
                                   "mensagem": "Anexo vazio."})
                continue
            try:
                r = processar_comprovante(s, conteudo, nome, origem="MAKE",
                                          baixar_automatico=True, usuario=None)
                r["arquivo"] = nome
                resultados.append(r)
                s.commit()
            except ErroValidacao as e:
                s.rollback()
                resultados.append({"arquivo": nome, "situacao": "ILEGIVEL",
                                   "mensagem": str(e)})
            except Exception as e:                       # pragma: no cover
                s.rollback()
                logger.exception("ERP/comprovantes: falha em %s", nome)
                resultados.append({"arquivo": nome, "situacao": "FALHOU",
                                   "mensagem": str(e)})

    return jsonify({"ok": True, "total": len(resultados),
                    "resumo": _resumo_do_lote(resultados),
                    "resultados": resultados})


def _resumo_do_lote(resultados: list) -> dict:
    """O relatório do lote — pedido do dono: "a tela já dá um relatório do que
    foi resolvido, o que foi detectado e o que não"."""
    contagem: dict[str, int] = {}
    for r in resultados:
        chave = r.get("situacao") or "?"
        contagem[chave] = contagem.get(chave, 0) + 1
    return contagem


@bp.route("/erp/api/pagamentos/comprovante/confirmar", methods=["POST"])
@login_obrigatorio
@permissao("pagar")
def api_comprovante_confirmar():
    from app.apps.erp.core.pagamentos.comprovante import confirmar_baixa
    arquivo = request.files.get("arquivo")
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            rel = confirmar_baixa(
                s, parcela_id=int(request.form["parcela_id"]),
                conta_bancaria_id=int(request.form["conta_id"]),
                data_pagamento=request.form.get("data") or date.today().isoformat(),
                conteudo=(arquivo.read() if arquivo else None),
                nome_arquivo=(arquivo.filename if arquivo else ""), usuario=usuario)
            s.commit()
        return jsonify({"ok": True, "baixa": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao confirmar baixa por comprovante")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/anexos/<entidade>/<int:entidade_id>", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_erp", POST="lancar")
def api_anexos(entidade: str, entidade_id: int):
    """Anexos guardados no próprio banco, comprimidos."""
    from app.apps.erp.core.auth.permissoes import exigir_entidade_no_escopo
    from app.apps.erp.core.documentos.armazenamento import listar, salvar
    if entidade not in ("titulo", "obra", "movimentacao", "fornecedor",
                        "medicao", "contrato_servico"):
        return jsonify({"ok": False, "erro": f"Entidade inválida: {entidade}"}), 400
    try:
        with get_session() as s:
            exigir_entidade_no_escopo(s, _usuario_logado(s), entidade, entidade_id)
            if request.method == "GET":
                return jsonify({"ok": True, "anexos": listar(s, entidade, entidade_id)})
            arquivo = request.files.get("arquivo")
            if arquivo is None:
                return jsonify({"ok": False, "erro": "Envie o arquivo."}), 400
            usuario = _usuario_logado(s)
            a = salvar(s, arquivo.read(), arquivo.filename or "arquivo",
                       entidade_tipo=entidade, entidade_id=entidade_id,
                       categoria=request.form.get("categoria", "OUTRO"),
                       descricao=request.form.get("descricao", ""), usuario=usuario)
            s.commit()
            return jsonify({"ok": True, "anexo": {
                "id": a.id, "nome": a.nome_arquivo,
                "tamanho_kb": round((a.tamanho_bytes or 0) / 1024, 1),
                "original_kb": round((a.tamanho_original or 0) / 1024, 1)}})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise            # vira 404 no errorhandler, nao 500
    except Exception as e:
        logger.exception("ERP: falha no anexo")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/anexo/<int:anexo_id>")
@login_obrigatorio
@permissao("ver_erp")
def baixar_anexo(anexo_id: int):
    """Serve o arquivo — venha ele do banco ou do Google Drive.

    QUEM ENTREGA O DOCUMENTO É ESTA ROTA, sempre. Mesmo com o arquivo morando
    no Drive, o link do Drive não vai para a tela: é aqui que a permissão e o
    escopo são conferidos, e link do Drive não passa por nenhum dos dois.
    """
    from flask import Response
    from app.apps.erp.core.auth.permissoes import exigir_anexo_no_escopo
    from app.apps.erp.core.documentos.armazenamento import conteudo_de, obter
    try:
        with get_session() as s:
            exigir_anexo_no_escopo(s, _usuario_logado(s), anexo_id)
            a = obter(s, anexo_id)
            dados = conteudo_de(s, a)
            if not dados:
                return jsonify({"ok": False,
                                "erro": "Anexo antigo sem conteúdo no banco."}), 404
            return Response(dados, mimetype=a.mime_type or "application/octet-stream",
                            headers={"Content-Disposition":
                                     f'inline; filename="{a.nome_arquivo}"'})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404


# ---------------------------------------------------------------------------
# CORRIGIR A ETIQUETA DO DOCUMENTO — dono, 23/09/2026:
# *"caso eu adicione um documento de forma equivocada e precise alterar, não
# tem opção pra isso. Ou até mesmo excluir algo que esteja errado."*
#
# Excluir já existia na API, mas SÓ a tela do Arquivo oferecia o botão — nas
# fichas (obra, título, medição) o documento entrava e não saía mais. Corrigir
# não existia em lugar nenhum.
#
# É `lancar` e confere escopo do anexo pelo mesmo motivo do DELETE: quem não
# alcança a obra não alcança o papel dela.
# ---------------------------------------------------------------------------
@bp.route("/erp/api/anexos/<int:anexo_id>", methods=["PATCH"])
@login_obrigatorio
@permissao("lancar")
def api_corrigir_anexo(anexo_id: int):
    from app.apps.erp.core.auth.permissoes import exigir_anexo_no_escopo
    from app.apps.erp.core.documentos.armazenamento import corrigir
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            exigir_anexo_no_escopo(s, usuario, anexo_id)
            a = corrigir(s, anexo_id, usuario,
                         descricao=d.get("descricao"),
                         categoria=d.get("categoria"))
            resposta = {"id": a.id, "nome": a.nome_arquivo,
                        "categoria": a.categoria_anexo or "OUTRO",
                        "descricao": a.descricao or ""}
            s.commit()
        return jsonify({"ok": True, "anexo": resposta})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise            # fora do escopo vira 404, nunca 403
    except Exception as e:
        logger.exception("ERP: falha ao corrigir o anexo")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/anexos/<int:anexo_id>", methods=["DELETE"])
@login_obrigatorio
@permissao("lancar")
def api_excluir_anexo(anexo_id: int):
    from app.apps.erp.core.auth.permissoes import exigir_anexo_no_escopo
    from app.apps.erp.core.documentos.armazenamento import excluir
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            exigir_anexo_no_escopo(s, usuario, anexo_id)
            excluir(s, anexo_id, usuario)
            s.commit()
        return jsonify({"ok": True})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404


# ---------------------------------------------------------------------------
# ONDE FICAM OS DOCUMENTOS — banco ou Google Drive
#
# É `configurar` porque muda onde o documento da empresa mora; e é `configurar`
# também para MOVER, porque mover é operação que apaga bytes do banco depois de
# conferir. Nada disso é coisa de quem só lança.
# ---------------------------------------------------------------------------
@bp.route("/erp/api/anexos/armazenamento", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="configurar", POST="configurar")
def api_anexos_armazenamento():
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.core.documentos import drive
    from app.apps.erp.core.documentos.armazenamento import a_mover
    from app.apps.erp.db.models.cadastros import Parametro
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if request.method == "POST":
                d = request.get_json(silent=True) or {}
                def _guardar(chave, valor):
                    linha = s.get(Parametro, chave)
                    if linha is None:
                        linha = Parametro(chave=chave, valor="")
                        s.add(linha)
                    linha.valor = valor
                    linha.atualizado_por = usuario.id
                _guardar(drive.CHAVE_PASTA, drive.id_da_pasta(d.get("pasta")))
                _guardar(drive.CHAVE_IMPERSONAR, (d.get("impersonar") or "").strip())
                _guardar(drive.CHAVE_LIGADO, "1" if d.get("ligado") else "0")
                registrar_evento(s, "configuracao", 0, "ANEXO_ARMAZENAMENTO", {
                    "pasta": drive.id_da_pasta(d.get("pasta")),
                    "ligado": bool(d.get("ligado"))}, usuario.id)
                s.commit()
            return jsonify({"ok": True, "dados": {**drive.configuracao(s), **a_mover(s)}})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/anexos/armazenamento/testar", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_anexos_testar():
    from app.apps.erp.core.documentos import drive
    with get_session() as s:
        r = drive.testar(s)
    if not r.get("ok"):
        return jsonify({"ok": False, "erro": r.get("erro", "falhou")}), 400
    return jsonify(r)


@bp.route("/erp/api/anexos/armazenamento/mover", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_anexos_mover():
    from app.apps.erp.core.documentos.armazenamento import mover_para_drive
    try:
        with get_session() as s:
            r = mover_para_drive(s, limite=int(request.args.get("limite", 25)))
            s.commit()
        return jsonify({"ok": True, "dados": r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/anexos/armazenamento/reorganizar", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_anexos_reorganizar():
    """Põe na pasta certa o que já está no Drive fora do lugar (migração 070).

    Move, nunca copia e nunca apaga: no Drive, mover é trocar o pai do arquivo
    — o id, o histórico e o link continuam os mesmos.
    """
    from app.apps.erp.core.documentos.armazenamento import reorganizar_no_drive
    try:
        with get_session() as s:
            r = reorganizar_no_drive(s, limite=int(request.args.get("limite", 50)))
            s.commit()
        return jsonify({"ok": True, "dados": r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


# ---------------------------------------------------------------------------
# O CRUZAMENTO DE NOTAS FISCAIS
#
# Nota × pedido × título × prestação de fundo fixo. A especificação inteira,
# ditada pelo dono, está em `NOTAS_FISCAIS.md`. O espírito é o da conciliação
# bancária: o sistema cruza o que consegue e expõe o duvidoso para uma pessoa.
# ---------------------------------------------------------------------------
@bp.route("/erp/api/notas")
@login_obrigatorio
@permissao("ver_notas")
def api_notas():
    from app.apps.erp.core.notas import cruzamento
    def _data(nome):
        try:
            return date.fromisoformat(request.args.get(nome) or "")
        except ValueError:
            return None
    with get_session() as s:
        dados = cruzamento.listar(
            s, conferencia=(request.args.get("conferencia") or "").strip(),
            empresa_id=(int(request.args["empresa_id"])
                        if request.args.get("empresa_id") else None),
            desde=_data("desde"), ate=_data("ate"),
            busca=(request.args.get("busca") or "").strip(),
            usuario=_usuario_logado(s))
    return jsonify({"ok": True, **dados})


@bp.route("/erp/api/notas/<int:nota_id>")
@login_obrigatorio
@permissao("ver_notas")
def api_nota(nota_id: int):
    """A nota com os CANDIDATOS de cada ponta — é o que a pessoa olha para decidir."""
    from app.apps.erp.core.notas import cruzamento
    from app.apps.erp.db.models.financeiro import DocumentoFiscal
    with get_session() as s:
        # Mesmo recorte da listagem: abrir pelo número não pode alcançar o que
        # a tela não mostra. Fora do recorte responde igual a inexistente.
        cruzamento.exigir_nota_no_escopo(s, _usuario_logado(s), nota_id)
        nota = s.get(DocumentoFiscal, nota_id)
        if nota is None:
            raise ErroNaoEncontrado("Nota não encontrada.")
        return jsonify({"ok": True, "nota": cruzamento.ler(s, nota, com_candidatos=True)})


@bp.route("/erp/api/notas/<int:nota_id>/ligar", methods=["POST"])
@login_obrigatorio
@permissao("cruzar_notas")
def api_nota_ligar(nota_id: int):
    from app.apps.erp.core.notas import cruzamento
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            nota = cruzamento.ligar(
                s, nota_id, usuario=_usuario_logado(s),
                titulo_id=(int(d["titulo_id"]) if d.get("titulo_id") else None),
                pedido_id=(int(d["pedido_id"]) if d.get("pedido_id") else None),
                item_id=(int(d["item_id"]) if d.get("item_id") else None))
            s.commit()
        return jsonify({"ok": True, "nota": nota})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/notas/<int:nota_id>/desligar", methods=["POST"])
@login_obrigatorio
@permissao("cruzar_notas")
def api_nota_desligar(nota_id: int):
    from app.apps.erp.core.notas import cruzamento
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            nota = cruzamento.desligar(s, nota_id, o_que=(d.get("o_que") or ""),
                                       usuario=_usuario_logado(s))
            s.commit()
        return jsonify({"ok": True, "nota": nota})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/notas/<int:nota_id>/marcar", methods=["POST"])
@login_obrigatorio
@permissao("cruzar_notas")
def api_nota_marcar(nota_id: int):
    from app.apps.erp.core.notas import cruzamento
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            nota = cruzamento.marcar(s, nota_id,
                                     conferencia=(d.get("conferencia") or "").strip(),
                                     motivo=(d.get("motivo") or ""),
                                     usuario=_usuario_logado(s))
            s.commit()
        return jsonify({"ok": True, "nota": nota})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/notas/sugerir", methods=["POST"])
@login_obrigatorio
@permissao("cruzar_notas")
def api_notas_sugerir():
    """Casa sozinho só o que é prova (a chave de acesso). O resto fica para gente."""
    from app.apps.erp.core.notas import cruzamento
    with get_session() as s:
        r = cruzamento.sugerir(s)
        s.commit()
    return jsonify({"ok": True, "dados": r})


@bp.route("/erp/api/notas/pedidos")
@login_obrigatorio
@permissao("ver_notas")
def api_notas_por_pedido():
    """O outro lado: quanto de cada pedido já veio em nota, e quanto falta."""
    from app.apps.erp.core.notas import cruzamento
    with get_session() as s:
        return jsonify({"ok": True, "pedidos": cruzamento.por_pedido(s)})


@bp.route("/erp/api/notas/importar", methods=["POST"])
@login_obrigatorio
@permissao("cruzar_notas")
def api_notas_importar():
    """Os XMLs que o serviço de monitoramento já baixa — soltos ou num .zip."""
    from app.apps.erp.core.notas.importar import importar_lote
    arquivos = [(f.filename or "arquivo.xml", f.read())
                for f in request.files.getlist("arquivos")]
    try:
        with get_session() as s:
            r = importar_lote(s, arquivos, usuario=_usuario_logado(s))
            s.commit()
        return jsonify({"ok": True, "dados": r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


# ---------------------------------------------------------------------------
# O ARQUIVO DE DOCUMENTOS DA EMPRESA
#
# Especificação em `GESTAO_DOCUMENTOS.md`. Quem separa o que cada pessoa vê NÃO
# é a ação declarada — é o SIGILO do tipo de documento e o escopo por obra,
# aplicados dentro do serviço. Por isso `ver_arquivo` é largo: certidão e
# contrato social são o que todo mundo precisa e ninguém acha.
# ---------------------------------------------------------------------------
@bp.route("/erp/api/arquivo/tipos")
@login_obrigatorio
@permissao("ver_arquivo")
def api_arquivo_tipos():
    from app.apps.erp.core.arquivo import catalogo
    with get_session() as s:
        return jsonify({"ok": True, "tipos": catalogo.listar(s),
                        "grupos": catalogo.GRUPOS})


@bp.route("/erp/api/arquivo/tipos/aplicar", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_arquivo_tipos_aplicar():
    """Traz o catálogo inicial. Cria o que falta e NUNCA apaga."""
    from app.apps.erp.core.arquivo import catalogo
    with get_session() as s:
        r = catalogo.aplicar(s)
        s.commit()
    return jsonify({"ok": True, "dados": r})


@bp.route("/erp/api/arquivo")
@login_obrigatorio
@permissao("ver_arquivo")
def api_arquivo():
    from app.apps.erp.core.arquivo import service as svc_arq
    def _comp():
        bruto = (request.args.get("competencia") or "").strip()
        try:
            return date.fromisoformat(bruto + "-01") if len(bruto) == 7 else None
        except ValueError:
            return None
    with get_session() as s:
        dados = svc_arq.listar(
            s, usuario=_usuario_logado(s),
            tipo=(request.args.get("tipo") or "").strip(),
            grupo=(request.args.get("grupo") or "").strip(),
            empresa_id=(int(request.args["empresa_id"]) if request.args.get("empresa_id") else None),
            obra_id=(int(request.args["obra_id"]) if request.args.get("obra_id") else None),
            # ⚑ FILTRAR POR PESSOA (16/11/2026). O serviço já sabia fazer isso;
            # a rota é que não deixava pedir. Sem ela, a ficha do colaborador
            # não tinha como listar os documentos dele — e foi exatamente o que
            # o dono viu: *"eu falei que o documento não estava aparecendo no
            # cadastro dele, mas quando eu vim na administração arquivo,
            # apareceu."* O documento estava certo o tempo todo; a ficha é que
            # nunca mostrou nenhum.
            colaborador_id=request.args.get("colaborador_id", type=int),
            competencia=_comp(),
            situacao=(request.args.get("situacao") or "").strip(),
            busca=(request.args.get("busca") or "").strip())
    return jsonify({"ok": True, **dados})


# ---------------------------------------------------------------------------
# OS BLOCOS — o que sempre é pedido junto
#
# O bloco aponta para TIPOS, não para documentos: é isso que faz o bloco fiscal
# de agosto e o de setembro serem o MESMO bloco, com recortes diferentes.
# ---------------------------------------------------------------------------
@bp.route("/erp/api/arquivo/blocos")
@login_obrigatorio
@permissao("ver_arquivo")
def api_arquivo_blocos():
    from app.apps.erp.core.arquivo import blocos
    with get_session() as s:
        return jsonify({"ok": True, "blocos": blocos.listar(s)})


@bp.route("/erp/api/arquivo/blocos/aplicar", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_arquivo_blocos_aplicar():
    from app.apps.erp.core.arquivo import blocos
    with get_session() as s:
        r = blocos.aplicar(s)
        s.commit()
    return jsonify({"ok": True, "dados": r})


def _recorte_do_pedido():
    """Empresa, obra e competência vindos da URL — usados pelos dois endereços."""
    bruto = (request.args.get("competencia") or "").strip()
    try:
        comp = date.fromisoformat(bruto + "-01") if len(bruto) == 7 else None
    except ValueError:
        comp = None
    return {
        "empresa_id": (int(request.args["empresa_id"])
                       if request.args.get("empresa_id") else None),
        "obra_id": (int(request.args["obra_id"])
                    if request.args.get("obra_id") else None),
        "competencia": comp,
    }


@bp.route("/erp/api/arquivo/blocos/<codigo>/conferir")
@login_obrigatorio
@permissao("ver_arquivo")
def api_arquivo_bloco_conferir(codigo: str):
    """O que o bloco tem e o que falta — ANTES de baixar.

    Existe separado do download de propósito: ver a falta na tela é melhor que
    descobri-la abrindo o arquivo compactado.
    """
    from app.apps.erp.core.arquivo import blocos
    try:
        with get_session() as s:
            return jsonify({"ok": True, "resultado": blocos.montar(
                s, codigo, usuario=_usuario_logado(s), **_recorte_do_pedido())})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/arquivo/blocos/<codigo>/baixar")
@login_obrigatorio
@permissao("ver_arquivo")
def api_arquivo_bloco_baixar(codigo: str):
    from flask import Response
    from app.apps.erp.core.arquivo import blocos
    try:
        with get_session() as s:
            nome, dados, _ = blocos.gerar_zip(
                s, codigo, usuario=_usuario_logado(s), **_recorte_do_pedido())
        return Response(dados, mimetype="application/zip",
                        headers={"Content-Disposition": f'attachment; filename="{nome}"'})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/arquivo/vencendo")
@login_obrigatorio
@permissao("ver_arquivo")
def api_arquivo_vencendo():
    """O que vence nos próximos dias — a pergunta que dá valor ao arquivo."""
    from app.apps.erp.core.arquivo import service as svc_arq
    with get_session() as s:
        dias = int(request.args.get("dias", 30))
        return jsonify({"ok": True,
                        "documentos": svc_arq.vencendo(s, dias=dias,
                                                       usuario=_usuario_logado(s))})


@bp.route("/erp/api/arquivo", methods=["POST"])
@login_obrigatorio
@permissao("arquivar")
def api_arquivo_guardar():
    from app.apps.erp.core.arquivo import service as svc_arq
    f = request.files.get("arquivo")
    if f is None:
        return jsonify({"ok": False, "erro": "Escolha o arquivo."}), 400

    def _data(nome):
        try:
            return date.fromisoformat(request.form.get(nome) or "")
        except ValueError:
            return None

    def _comp():
        bruto = (request.form.get("competencia") or "").strip()
        try:
            return date.fromisoformat(bruto + "-01") if len(bruto) == 7 else None
        except ValueError:
            return None

    def _num(nome):
        bruto = (request.form.get(nome) or "").strip()
        return int(bruto) if bruto.isdigit() else None

    try:
        with get_session() as s:
            d = svc_arq.arquivar(
                s, f.read(), f.filename or "arquivo",
                tipo_codigo=(request.form.get("tipo") or ""),
                empresa_id=_num("empresa_id"), obra_id=_num("obra_id"),
                colaborador_id=_num("colaborador_id"),
                fornecedor_id=_num("fornecedor_id"),
                competencia=_comp(), referencia=(request.form.get("referencia") or ""),
                emissao=_data("emissao"), validade=_data("validade"),
                observacao=(request.form.get("observacao") or ""),
                # Vindos da leitura automática, quando houve. Guardar o texto
                # aqui é o que torna possível buscar DENTRO do documento
                # depois, sem reprocessar o arquivo.
                texto=(request.form.get("texto") or ""),
                resumo=(request.form.get("resumo") or ""),
                origem=("IA" if request.form.get("texto") or request.form.get("resumo")
                        else "TELA"),
                usuario=_usuario_logado(s))
            linha = svc_arq.ler(s, d)
            s.commit()
        return jsonify({"ok": True, "documento": linha})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/arquivo/donos")
@login_obrigatorio
@permissao("arquivar")
def api_arquivo_donos():
    """As listas de donos que a tela do Arquivo precisa oferecer.

    Existe separada das telas de Pessoal e de Suprimentos porque quem arquiva
    não necessariamente pode abrir aquelas telas — e sem estas listas os tipos
    de documento de PESSOA e de PARCEIRO ficavam sem onde pendurar.

    A lista de colaboradores só sai para quem enxerga documento PESSOAL (a
    mesma faixa de sigilo do módulo): nome de empregado é dado de pessoa.
    """
    from app.apps.erp.core.arquivo.service import sigilos_visiveis
    from app.apps.erp.core.auth.permissoes import obras_do_usuario
    from app.apps.erp.db.models.cadastros import Colaborador, Empresa, Fornecedor, Obra
    from sqlalchemy import select as _sel
    try:
        with get_session() as s:
            u = _usuario_logado(s)
            empresas = [{"id": e.id, "nome": e.nome_fantasia or e.razao_social}
                        for e in s.scalars(_sel(Empresa).order_by(Empresa.razao_social)).all()]
            minhas = obras_do_usuario(s, u) if u is not None else None
            stmt = _sel(Obra).order_by(Obra.codigo)
            if minhas is not None:
                stmt = stmt.where(Obra.id.in_(minhas or [-1]))
            obras = [{"id": o.id, "nome": f"{o.codigo} · {o.nome}"}
                     for o in s.scalars(stmt).all()]
            fornecedores = [{"id": f.id, "nome": f.nome_fantasia or f.razao_social}
                            for f in s.scalars(_sel(Fornecedor)
                                               .order_by(Fornecedor.razao_social)).all()]
            colaboradores = []
            if "PESSOAL" in sigilos_visiveis(u):
                colaboradores = [{"id": c.id, "nome": c.nome}
                                 for c in s.scalars(_sel(Colaborador)
                                                    .order_by(Colaborador.nome)).all()]
        return jsonify({"ok": True, "empresas": empresas, "obras": obras,
                        "fornecedores": fornecedores, "colaboradores": colaboradores})
    except Exception as e:
        logger.exception("ERP/arquivo: falha ao listar donos")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/arquivo/ler", methods=["POST"])
@login_obrigatorio
@permissao("arquivar")
def api_arquivo_ler():
    """A IA lê o documento e devolve o formulário preenchido — sem guardar.

    Guardar continua sendo um ato da pessoa: documento arquivado no tipo
    errado some do conjunto que o cliente pede na medição, e ninguém percebe
    até o dia da entrega.
    """
    from app.apps.erp.core.arquivo import leitura
    from app.apps.erp.core.documentos.leitor import ErroLeitura
    f = request.files.get("arquivo")
    if f is None:
        return jsonify({"ok": False, "erro": "Escolha o arquivo."}), 400
    # FORA do try: a recusa por teto tem resposta própria (402), e um
    # `except Exception` a transformaria em 'falha do sistema'.
    _exigir_saldo_de_ia()
    try:
        conteudo = f.read()
        with get_session() as s:
            sugestao = leitura.sugerir(s, conteudo, f.filename or "arquivo",
                                       dica=(request.form.get("dica") or ""))
        return jsonify({"ok": True, "sugestao": sugestao})
    except ErroLeitura as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP/arquivo: falha na leitura automática")
        return jsonify({"ok": False, "erro": f"Não deu para ler o documento: {e}"}), 500


# ---------------------------------------------------------------------------
# DOCUMENTO DA OBRA: arquivar e preencher o cadastro num gesto só
#
# Pedido do dono em 10/09/2026, e o princípio que ele tirou dele: *"matariamos
# duas ações... cadastros e arquivo estarem associados quando fizer sentido"*.
# ---------------------------------------------------------------------------
@bp.route("/erp/api/obras/<int:obra_id>/documento/ler", methods=["POST"])
@login_obrigatorio
@permissao("arquivar")
def api_obra_documento_ler(obra_id: int):
    """Lê o documento e diz o que ele arquivaria E o que preencheria. Não grava."""
    from app.apps.erp.core.arquivo import leitura, preenchimento
    from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
    from app.apps.erp.core.documentos.leitor import ErroLeitura
    f = request.files.get("arquivo")
    if f is None:
        return jsonify({"ok": False, "erro": "Escolha o arquivo."}), 400
    # FORA do try: a recusa por teto tem resposta própria (402), e um
    # `except Exception` a transformaria em 'falha do sistema'.
    _exigir_saldo_de_ia()
    try:
        conteudo = f.read()
        with get_session() as s:
            exigir_obra_no_escopo(s, _usuario_logado(s), obra_id)
            sugestao = leitura.sugerir(
                s, conteudo, f.filename or "arquivo",
                dica=(request.form.get("dica") or ""),
                extracao=preenchimento.instrucao_de_extracao())
            # O dono é a obra em que a pessoa está — não se deduz de novo. E o
            # nome do arquivo sai DELA, senão a prévia promete um nome e o
            # arquivamento entrega outro.
            sugestao["obra_id"] = obra_id
            sugestao["nome_original"] = f.filename or "arquivo"
            sugestao["nome_sugerido"] = preenchimento.nome_para_obra(
                s, obra_id, sugestao)
            cadastro = preenchimento.sugerir_para_obra(
                s, obra_id, sugestao.get("tipo_codigo") or "", sugestao)
        return jsonify({"ok": True, "sugestao": sugestao, "cadastro": cadastro})
    except ErroLeitura as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP/obras: falha ao ler o documento da obra %s", obra_id)
        return jsonify({"ok": False, "erro": f"Não deu para ler o documento: {e}"}), 500


@bp.route("/erp/api/obras/<int:obra_id>/documento", methods=["POST"])
@login_obrigatorio
@permissao("arquivar")
def api_obra_documento_guardar(obra_id: int):
    """Arquiva o documento E preenche os campos confirmados, numa transação só.

    Se o preenchimento falhar, o arquivamento vai junto: guardar o arquivo e
    deixar o cadastro pela metade seria o pior dos dois mundos — a pessoa
    acharia que tinha feito e não teria.
    """
    from app.apps.erp.core.arquivo import preenchimento
    from app.apps.erp.core.arquivo import service as svc_arq
    from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
    f = request.files.get("arquivo")
    if f is None:
        return jsonify({"ok": False, "erro": "Escolha o arquivo."}), 400
    tipo = (request.form.get("tipo") or "").strip().upper()
    if not tipo:
        return jsonify({"ok": False, "erro": "Escolha o tipo do documento."}), 400

    def _data(nome):
        try:
            return date.fromisoformat(request.form.get(nome) or "")
        except ValueError:
            return None

    def _comp():
        bruto = (request.form.get("competencia") or "").strip()
        try:
            return date.fromisoformat(bruto + "-01") if len(bruto) == 7 else None
        except ValueError:
            return None

    campos = json.loads(request.form.get("campos") or "{}")
    aditivo = json.loads(request.form.get("aditivo") or "null")
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            exigir_obra_no_escopo(s, usuario, obra_id)
            d = svc_arq.arquivar(
                s, f.read(), f.filename or "arquivo", tipo_codigo=tipo,
                obra_id=obra_id, competencia=_comp(),
                referencia=(request.form.get("referencia") or ""),
                emissao=_data("emissao"), validade=_data("validade"),
                observacao=(request.form.get("observacao") or ""),
                texto=(request.form.get("texto") or ""),
                resumo=(request.form.get("resumo") or ""),
                origem="IA", usuario=usuario)
            preenchido = preenchimento.aplicar_na_obra(
                s, obra_id, campos, tipo_codigo=tipo, documento_id=d.id,
                usuario=usuario)
            novo_aditivo = None
            if aditivo:
                a = preenchimento.criar_aditivo(
                    s, obra_id, aditivo, anexo_id=d.anexo_id, usuario=usuario)
                novo_aditivo = {"id": a.id, "numero": a.numero, "tipo": a.tipo,
                                "valor": float(a.valor), "dias": a.dias}
            linha = svc_arq.ler(s, d)
            s.commit()
        return jsonify({"ok": True, "documento": linha,
                        "preenchido": preenchido, "aditivo": novo_aditivo})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500


# ---------------------------------------------------------------------------
# CRIAR A OBRA A PARTIR DO DOCUMENTO
#
# Pedido do dono em 10/09/2026: *"nós havíamos conversado sobre a criação de
# obras a partir de um documento, da leitura de um documento. Então isso
# ficaria associado a obras."*
#
# A ação declarada é `configurar`, e é a certa: o que estas rotas fazem de
# irreversível é CRIAR obra — arquivar o documento é consequência, não o
# assunto. Quem pode criar obra pode guardar o contrato que a criou.
# ---------------------------------------------------------------------------
@bp.route("/erp/api/obras/documento/ler", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_nova_obra_documento_ler():
    """Lê o documento e diz que obra ele criaria. Não grava nada."""
    from app.apps.erp.core.arquivo import leitura, preenchimento
    from app.apps.erp.core.documentos.leitor import ErroLeitura
    f = request.files.get("arquivo")
    if f is None:
        return jsonify({"ok": False, "erro": "Escolha o arquivo."}), 400
    # FORA do try: a recusa por teto tem resposta própria (402), e um
    # `except Exception` a transformaria em 'falha do sistema'.
    _exigir_saldo_de_ia()
    try:
        conteudo = f.read()
        with get_session() as s:
            sugestao = leitura.sugerir(
                s, conteudo, f.filename or "arquivo",
                dica=(request.form.get("dica") or ""),
                extracao=preenchimento.instrucao_de_extracao(),
                dono_e_novo=True)
            sugestao["nome_original"] = f.filename or "arquivo"
            # O nome do arquivo só se sabe depois que a obra existe (ele usa o
            # código dela). Prometer um nome agora seria prometer errado.
            sugestao["nome_sugerido"] = ""
            cadastro = preenchimento.sugerir_para_nova_obra(
                s, sugestao.get("tipo_codigo") or "", sugestao)
        return jsonify({"ok": True, "sugestao": sugestao, "cadastro": cadastro})
    except ErroLeitura as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP/obras: falha ao ler documento de obra nova")
        return jsonify({"ok": False, "erro": f"Não deu para ler o documento: {e}"}), 500


@bp.route("/erp/api/obras/documento", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_nova_obra_documento():
    """Cria a obra, arquiva o documento nela e preenche o que foi confirmado.

    Tudo na mesma transação. Obra criada com o documento perdido, ou documento
    guardado numa obra que não chegou a existir, seriam os dois piores
    resultados possíveis — e sem transação única os dois são possíveis.
    """
    from app.apps.erp.core.arquivo import preenchimento
    from app.apps.erp.core.arquivo import service as svc_arq
    from app.apps.erp.core.cadastros import obras as svc_obra
    f = request.files.get("arquivo")
    if f is None:
        return jsonify({"ok": False, "erro": "Escolha o arquivo."}), 400
    tipo = (request.form.get("tipo") or "").strip().upper()
    if not tipo:
        return jsonify({"ok": False, "erro": "Escolha o tipo do documento."}), 400
    codigo = (request.form.get("codigo") or "").strip().upper()
    nome = (request.form.get("nome") or "").strip()
    if not codigo or not nome:
        return jsonify({"ok": False,
                        "erro": "Código e nome da obra são obrigatórios."}), 400

    def _data(campo):
        try:
            return date.fromisoformat(request.form.get(campo) or "")
        except ValueError:
            return None

    def _comp():
        bruto = (request.form.get("competencia") or "").strip()
        try:
            return date.fromisoformat(bruto + "-01") if len(bruto) == 7 else None
        except ValueError:
            return None

    campos = json.loads(request.form.get("campos") or "{}")
    aditivo = json.loads(request.form.get("aditivo") or "null")
    confirmada = (request.form.get("confirmar_duplicada") or "") == "1"
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            # A guarda contra duplicar olha o que VAI ser gravado, não o que a
            # leitura sugeriu: a pessoa pode ter corrigido o CNO na tela.
            parecidas = preenchimento.obras_parecidas(
                s, cno=campos.get("cno") or campos.get("cei_obra"),
                contrato=campos.get("contrato"))
            if parecidas and not confirmada:
                return jsonify({
                    "ok": False, "parecidas": parecidas,
                    "erro": "Já existe obra cadastrada com esses mesmos dados. "
                            "Duas obras para o mesmo contrato partem o histórico "
                            "em dois e nenhum relatório fecha."}), 400

            obra = svc_obra.criar(s, {"codigo": codigo, "nome": nome,
                                      "empresa_id": request.form.get("empresa_id"),
                                      "origem": "DOCUMENTO"}, usuario)
            s.flush()
            d = svc_arq.arquivar(
                s, f.read(), f.filename or "arquivo", tipo_codigo=tipo,
                obra_id=obra.id, competencia=_comp(),
                referencia=(request.form.get("referencia") or ""),
                emissao=_data("emissao"), validade=_data("validade"),
                observacao=(request.form.get("observacao") or ""),
                texto=(request.form.get("texto") or ""),
                resumo=(request.form.get("resumo") or ""),
                origem="IA", usuario=usuario)
            preenchido = preenchimento.aplicar_na_obra(
                s, obra.id, campos, tipo_codigo=tipo, documento_id=d.id,
                usuario=usuario)
            novo_aditivo = None
            if aditivo:
                a = preenchimento.criar_aditivo(
                    s, obra.id, aditivo, anexo_id=d.anexo_id, usuario=usuario)
                novo_aditivo = {"id": a.id, "numero": a.numero, "tipo": a.tipo,
                                "valor": float(a.valor), "dias": a.dias}
            linha = svc_arq.ler(s, d)
            criada = {"id": obra.id, "codigo": obra.codigo, "nome": obra.nome}
            s.commit()
        return jsonify({"ok": True, "obra": criada, "documento": linha,
                        "preenchido": preenchido, "aditivo": novo_aditivo})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP/obras: falha ao criar obra a partir de documento")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/colaboradores/documento/ler", methods=["POST"])
@login_obrigatorio
@permissao("arquivar")
def api_nova_pessoa_documento_ler():
    """Lê o documento e diz que cadastro de pessoa ele criaria. Não grava nada.

    Gêmea da de obra nova e da de empresa nova. Faltava só a de pessoa, e a
    falta aparecia na tela: o formulário de colaborador novo prometia o bloco
    de jogar o documento e não tinha bloco nenhum.

    Sem `<int:colaborador_id>` não há objeto a conferir no escopo — a ação
    `arquivar` decide sozinha quem entra, e nada é gravado aqui.
    """
    from app.apps.erp.core.arquivo import leitura, preenchimento
    from app.apps.erp.core.documentos.leitor import ErroLeitura
    f = request.files.get("arquivo")
    if f is None:
        return jsonify({"ok": False, "erro": "Escolha o arquivo."}), 400
    # FORA do try: a recusa por teto tem resposta própria (402), e um
    # `except Exception` a transformaria em 'falha do sistema'.
    _exigir_saldo_de_ia()
    try:
        conteudo = f.read()
        with get_session() as s:
            sugestao = leitura.sugerir(
                s, conteudo, f.filename or "arquivo",
                dica=(request.form.get("dica") or ""),
                extracao=preenchimento.instrucao_de_extracao_pessoa(),
                dono_e_novo=True)
            sugestao["colaborador_id"] = None
            sugestao["nome_original"] = f.filename or "arquivo"
            # O nome do arquivo usa o nome e o CPF da pessoa, que só se sabem
            # depois que ela existe. Prometer um nome agora seria prometer
            # errado.
            sugestao["nome_sugerido"] = ""
            cadastro = preenchimento.sugerir_para_nova_pessoa(
                s, sugestao.get("tipo_codigo") or "", sugestao)
        return jsonify({"ok": True, "sugestao": sugestao, "cadastro": cadastro})
    except ErroLeitura as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP/pessoal: falha ao ler documento de pessoa nova")
        return jsonify({"ok": False, "erro": f"Não deu para ler o documento: {e}"}), 500


@bp.route("/erp/api/colaboradores/<int:colaborador_id>/documento/ler", methods=["POST"])
@login_obrigatorio
@permissao("arquivar")
def api_colaborador_documento_ler(colaborador_id: int):
    """Lê o documento da pessoa: o que arquivaria e o que preencheria."""
    from app.apps.erp.core.arquivo import leitura, preenchimento
    from app.apps.erp.core.auth.permissoes import exigir_colaborador_no_escopo
    from app.apps.erp.core.documentos.leitor import ErroLeitura
    f = request.files.get("arquivo")
    if f is None:
        return jsonify({"ok": False, "erro": "Escolha o arquivo."}), 400
    # FORA do try: a recusa por teto tem resposta própria (402), e um
    # `except Exception` a transformaria em 'falha do sistema'.
    _exigir_saldo_de_ia()
    try:
        conteudo = f.read()
        with get_session() as s:
            exigir_colaborador_no_escopo(s, _usuario_logado(s), colaborador_id)
            sugestao = leitura.sugerir(
                s, conteudo, f.filename or "arquivo",
                dica=(request.form.get("dica") or ""),
                extracao=preenchimento.instrucao_de_extracao_pessoa())
            sugestao["colaborador_id"] = colaborador_id
            sugestao["nome_original"] = f.filename or "arquivo"
            sugestao["nome_sugerido"] = preenchimento.nome_para_colaborador(
                s, colaborador_id, sugestao)
            cadastro = preenchimento.sugerir_para_colaborador(
                s, colaborador_id, sugestao.get("tipo_codigo") or "", sugestao)
        return jsonify({"ok": True, "sugestao": sugestao, "cadastro": cadastro})
    except ErroLeitura as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP/pessoal: falha ao ler o documento do colaborador %s",
                         colaborador_id)
        return jsonify({"ok": False, "erro": f"Não deu para ler o documento: {e}"}), 500


@bp.route("/erp/api/colaboradores/<int:colaborador_id>/documento", methods=["POST"])
@login_obrigatorio
@permissao("arquivar")
def api_colaborador_documento_guardar(colaborador_id: int):
    """Arquiva o documento E preenche o cadastro da pessoa, numa transação só."""
    from app.apps.erp.core.arquivo import preenchimento
    from app.apps.erp.core.arquivo import service as svc_arq
    from app.apps.erp.core.auth.permissoes import exigir_colaborador_no_escopo
    f = request.files.get("arquivo")
    if f is None:
        return jsonify({"ok": False, "erro": "Escolha o arquivo."}), 400
    tipo = (request.form.get("tipo") or "").strip().upper()
    if not tipo:
        return jsonify({"ok": False, "erro": "Escolha o tipo do documento."}), 400

    def _data(nome):
        try:
            return date.fromisoformat(request.form.get(nome) or "")
        except ValueError:
            return None

    def _comp():
        bruto = (request.form.get("competencia") or "").strip()
        try:
            return date.fromisoformat(bruto + "-01") if len(bruto) == 7 else None
        except ValueError:
            return None

    campos = json.loads(request.form.get("campos") or "{}")
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            exigir_colaborador_no_escopo(s, usuario, colaborador_id)
            d = svc_arq.arquivar(
                s, f.read(), f.filename or "arquivo", tipo_codigo=tipo,
                colaborador_id=colaborador_id, competencia=_comp(),
                referencia=(request.form.get("referencia") or ""),
                emissao=_data("emissao"), validade=_data("validade"),
                observacao=(request.form.get("observacao") or ""),
                texto=(request.form.get("texto") or ""),
                resumo=(request.form.get("resumo") or ""),
                origem="IA", usuario=usuario)
            preenchido = preenchimento.aplicar_no_colaborador(
                s, colaborador_id, campos, tipo_codigo=tipo, documento_id=d.id,
                usuario=usuario)
            linha = svc_arq.ler(s, d)
            s.commit()
        return jsonify({"ok": True, "documento": linha, "preenchido": preenchido})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500


@bp.route("/erp/api/arquivo/<int:documento_id>", methods=["DELETE"])
@login_obrigatorio
@permissao("arquivar")
def api_arquivo_excluir(documento_id: int):
    from app.apps.erp.core.arquivo import service as svc_arq
    try:
        with get_session() as s:
            svc_arq.excluir(s, documento_id, _usuario_logado(s))
            s.commit()
        return jsonify({"ok": True})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404


# ---------------------------------------------------------------------------
# MEDIÇÃO: tipo, correlação, protocolo — e o quadro do contrato
#
# O tipo é catálogo EDITÁVEL e o número é texto livre porque quem manda na
# nomenclatura é o ÓRGÃO, não o ERP.
# ---------------------------------------------------------------------------
@bp.route("/erp/api/medicoes/tipos")
@login_obrigatorio
@permissao("ver_contratos")
def api_medicao_tipos():
    from app.apps.erp.core.titulos import medicao as svc_med
    with get_session() as s:
        return jsonify({"ok": True, "tipos": svc_med.listar_tipos(s)})


@bp.route("/erp/api/medicoes/tipos/aplicar", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_medicao_tipos_aplicar():
    from app.apps.erp.core.titulos import medicao as svc_med
    with get_session() as s:
        r = svc_med.aplicar_tipos(s)
        s.commit()
    return jsonify({"ok": True, "dados": r})


@bp.route("/erp/api/medicoes/<int:titulo_id>/classificar", methods=["POST"])
@login_obrigatorio
@permissao("receber")
def api_medicao_classificar(titulo_id: int):
    from app.apps.erp.core.auth.permissoes import exigir_titulo_no_escopo
    from app.apps.erp.core.titulos import medicao as svc_med
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            exigir_titulo_no_escopo(s, _usuario_logado(s), titulo_id)
            t = svc_med.classificar(
                s, titulo_id, tipo=(d.get("tipo") or ""),
                medicao_de_id=(int(d["medicao_de_id"]) if d.get("medicao_de_id") else None),
                usuario=_usuario_logado(s))
            linha = svc_med.ler(s, t)
            s.commit()
        return jsonify({"ok": True, "medicao": linha})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/medicoes/<int:titulo_id>/protocolo", methods=["POST"])
@login_obrigatorio
@permissao("receber")
def api_medicao_protocolo(titulo_id: int):
    from app.apps.erp.core.auth.permissoes import exigir_titulo_no_escopo
    from app.apps.erp.core.titulos import medicao as svc_med
    d = request.get_json(silent=True) or {}
    def _quando():
        try:
            return date.fromisoformat(d.get("em") or "")
        except ValueError:
            return None
    try:
        with get_session() as s:
            exigir_titulo_no_escopo(s, _usuario_logado(s), titulo_id)
            t = svc_med.protocolar(s, titulo_id, numero=(d.get("numero") or ""),
                                   em=_quando(), usuario=_usuario_logado(s))
            linha = svc_med.ler(s, t)
            s.commit()
        return jsonify({"ok": True, "medicao": linha})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/contratos/quadro")
@login_obrigatorio
@permissao("ver_contratos")
def api_contratos_quadro():
    """A lista de contratos, com o essencial antes de abrir o quadro."""
    from app.apps.erp.core.titulos import quadro as svc_quadro
    with get_session() as s:
        obra_id = (int(request.args["obra_id"]) if request.args.get("obra_id") else None)
        return jsonify({"ok": True,
                        "contratos": svc_quadro.listar_contratos(s, obra_id=obra_id)})


@bp.route("/erp/api/contratos/<int:contrato_id>/quadro")
@login_obrigatorio
@permissao("ver_contratos")
def api_contrato_quadro(contrato_id: int):
    """O quadro financeiro: as medições e os sete totais."""
    from app.apps.erp.core.titulos import quadro as svc_quadro
    try:
        with get_session() as s:
            return jsonify({"ok": True, **svc_quadro.quadro(s, contrato_id)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404


# ---------------------------------------------------------------------------
# NOTAS EMITIDAS — o controle da numeração e o relatório da contabilidade
#
# Tela IRMÃ da de títulos a receber, não a mesma: uma medição pode virar duas
# notas, e uma nota pode ser cancelada e substituída sem o título mudar.
# ---------------------------------------------------------------------------
@bp.route("/erp/api/notas-emitidas")
@login_obrigatorio
@permissao("ver_notas_emitidas")
def api_notas_emitidas():
    from app.apps.erp.core.notas_emitidas import listagem as svc
    def _data(chave):
        try:
            return date.fromisoformat(request.args.get(chave) or "")
        except ValueError:
            return None
    with get_session() as s:
        return jsonify({"ok": True, **svc.listar(
            s,
            empresa_id=(int(request.args["empresa_id"])
                        if request.args.get("empresa_id") else None),
            obra_id=(int(request.args["obra_id"])
                     if request.args.get("obra_id") else None),
            situacao=(request.args.get("situacao") or "").strip().upper(),
            ambiente=(request.args.get("ambiente") or "").strip().upper(),
            serie=(request.args.get("serie") or "").strip(),
            desde=_data("desde"), ate=_data("ate"),
            busca=(request.args.get("busca") or ""))})


@bp.route("/erp/api/notas-emitidas/conferencia")
@login_obrigatorio
@permissao("ver_notas_emitidas")
def api_notas_emitidas_conferencia():
    """A sequência está inteira? Buraco é diferente de queimado."""
    from app.apps.erp.core.notas_emitidas import numeracao as svc
    from app.apps.erp.db.models.cadastros import Empresa
    try:
        with get_session() as s:
            empresa = s.get(Empresa, int(request.args.get("empresa_id") or 0))
            if empresa is None:
                raise ErroValidacao("Escolha a empresa para conferir a numeração.")
            return jsonify({"ok": True, "conferencia": svc.conferir(
                s, empresa,
                serie=(request.args.get("serie") or "").strip() or None,
                ambiente=(request.args.get("ambiente") or "").strip().upper() or None)})
    except (ErroValidacao, ValueError) as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/notas-emitidas/registrar", methods=["POST"])
@login_obrigatorio
@permissao("emitir_nota")
def api_nota_emitida_registrar():
    """A nota que saiu pelo PORTAL da prefeitura, registrada aqui.

    Sem isto a conferência acusa buraco na numeração e ninguém sabe por quê.
    """
    from decimal import Decimal as _Dec
    from app.apps.erp.core.notas_emitidas import listagem as svc
    d = request.get_json(silent=True) or {}
    def _quando():
        try:
            return date.fromisoformat(d.get("emissao") or "")
        except ValueError:
            return None
    try:
        with get_session() as s:
            nota = svc.registrar_manual(
                s, empresa_id=int(d.get("empresa_id") or 0),
                titulo_id=(int(d["titulo_id"]) if d.get("titulo_id") else None),
                numero_nota=(d.get("numero_nota") or ""),
                emissao=_quando(),
                valor_bruto=(_Dec(str(d["valor_bruto"])) if d.get("valor_bruto") else None),
                retencoes=(d.get("retencoes") or {}),
                observacao=(d.get("observacao") or ""),
                usuario=_usuario_logado(s))
            linha = svc.ler(s, nota)
            s.commit()
        return jsonify({"ok": True, "nota": linha})
    except (ErroValidacao, ValueError) as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/medicoes/<int:titulo_id>/emitir")
@login_obrigatorio
@permissao("emitir_nota")
def api_medicao_preparar_emissao(titulo_id: int):
    """Tudo que o portal da prefeitura pergunta, num bloco só. NÃO emite."""
    from app.apps.erp.core.auth.permissoes import exigir_titulo_no_escopo
    from app.apps.erp.core.notas_emitidas import manual as svc
    try:
        with get_session() as s:
            exigir_titulo_no_escopo(s, _usuario_logado(s), titulo_id)
            return jsonify({"ok": True, "emissao": svc.preparar(s, titulo_id)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/medicoes/<int:titulo_id>/emitir", methods=["POST"])
@login_obrigatorio
@permissao("emitir_nota")
def api_medicao_registrar_emissao(titulo_id: int):
    """A nota voltou do portal: registra com as retenções calculadas."""
    from app.apps.erp.core.auth.permissoes import exigir_titulo_no_escopo
    from app.apps.erp.core.notas_emitidas import listagem as svc_lista
    from app.apps.erp.core.notas_emitidas import manual as svc
    d = request.get_json(silent=True) or {}
    def _quando():
        try:
            return date.fromisoformat(d.get("emissao") or "")
        except ValueError:
            return None
    try:
        with get_session() as s:
            exigir_titulo_no_escopo(s, _usuario_logado(s), titulo_id)
            nota = svc.registrar(
                s, titulo_id, numero_nota=(d.get("numero_nota") or ""),
                emissao=_quando(), valor_bruto=d.get("valor_bruto"),
                retencoes=d.get("retencoes"),
                codigo_verificacao=(d.get("codigo_verificacao") or ""),
                chave_acesso=(d.get("chave_acesso") or ""),
                observacao=(d.get("observacao") or ""),
                usuario=_usuario_logado(s))
            linha = svc_lista.ler(s, nota)
            s.commit()
        return jsonify({"ok": True, "nota": linha})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/medicoes/<int:titulo_id>/emitir-automatico")
@login_obrigatorio
@permissao("emitir_nota")
def api_medicao_conferir_emissao(titulo_id: int):
    """O que falta para esta medição virar nota sozinha. NÃO emite nada.

    Roda antes de qualquer número ser tomado: cadastro incompleto vira lista de
    pendências na tela, e não um número de nota queimado por engano.
    """
    from app.apps.erp.core.auth.permissoes import exigir_titulo_no_escopo
    from app.apps.erp.core.notas_emitidas import automatica as svc
    try:
        with get_session() as s:
            exigir_titulo_no_escopo(s, _usuario_logado(s), titulo_id)
            return jsonify({"ok": True, "conferencia": svc.conferir(s, titulo_id)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500


@bp.route("/erp/api/medicoes/<int:titulo_id>/emitir-automatico", methods=["POST"])
@login_obrigatorio
@permissao("emitir_nota")
def api_medicao_emitir_automatico(titulo_id: int):
    """Põe a emissão na fila. Entre enviar e a prefeitura responder passam-se
    de segundos a dois minutos — tempo demais para segurar uma tela."""
    from app.apps.erp.core.auth.permissoes import exigir_titulo_no_escopo
    from app.apps.erp.core.comum import tarefas, trabalhos
    from app.apps.erp.core.notas_emitidas import automatica as svc
    from app.apps.erp.db.models.financeiro import Titulo
    d = request.get_json(silent=True) or {}
    try:
        trabalhos.registrar_todos()
        with get_session() as s:
            usuario = _usuario_logado(s)
            exigir_titulo_no_escopo(s, usuario, titulo_id)
            # Confere ANTES de enfileirar: assim a pessoa vê o que falta na
            # hora, em vez de descobrir minutos depois num trabalho falhado.
            check = svc.conferir(s, titulo_id)
            if not check["pode"]:
                return jsonify({"ok": False, "erro": "A nota não pode sair ainda.",
                                "faltas": check["faltas"]}), 400
            titulo = s.get(Titulo, titulo_id)
            t = tarefas.enfileirar(
                s, "emitir_nota",
                {"titulo_id": titulo_id, "valor": d.get("valor"),
                 "observacao": (d.get("observacao") or ""),
                 "usuario_id": usuario.id if usuario else None},
                rotulo=f"Emitir a nota da medição {titulo.numero_medicao or titulo.numero_sp}",
                usuario=usuario)
            linha = tarefas.ler(s, t.id)
            s.commit()
        return jsonify({"ok": True, "tarefa": linha})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500


@bp.route("/erp/api/fornecedores/<int:fornecedor_id>/receita", methods=["POST"])
@login_obrigatorio
@permissao("administrar_fornecedores")
def api_fornecedor_receita(fornecedor_id: int):
    """Consulta o CNPJ e completa o que estiver em branco no cadastro.

    Completa, não sobrescreve: correção feita à mão vale mais do que dado de
    terceiro. É o que destrava a emissão de nota — a declaração exige o
    endereço de quem recebe o serviço.
    """
    from app.apps.erp.core.cadastros import fornecedores as svc
    try:
        with get_session() as s:
            r = svc.preencher_pela_receita(s, fornecedor_id, _usuario_logado(s))
            s.commit()
        return jsonify({"ok": True, **r})
    except svc.ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/notas-emitidas/ler", methods=["POST"])
@login_obrigatorio
@permissao("emitir_nota")
def api_nota_emitida_ler():
    """A IA lê o PDF da nota que o portal devolveu e sugere os campos."""
    from app.apps.erp.core.documentos.leitor import ErroLeitura
    from app.apps.erp.core.notas_emitidas import manual as svc
    arquivo = request.files.get("arquivo")
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Anexe o PDF da nota."}), 400
    try:
        lido = svc.ler_nota_emitida(arquivo.read(), arquivo.filename or "nota.pdf")
    except ErroLeitura as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    return jsonify({"ok": True, "lido": lido})


@bp.route("/erp/api/notas-emitidas/<int:nota_id>/cancelar", methods=["POST"])
@login_obrigatorio
@permissao("emitir_nota")
def api_nota_emitida_cancelar(nota_id: int):
    """Cancelar exige MOTIVO — número de nota fiscal não se apaga, se explica."""
    from app.apps.erp.core.notas_emitidas import listagem as svc
    from app.apps.erp.core.notas_emitidas import numeracao
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            nota = numeracao.cancelar(s, nota_id, motivo=(d.get("motivo") or ""),
                                      usuario=_usuario_logado(s))
            linha = svc.ler(s, nota)
            s.commit()
        return jsonify({"ok": True, "nota": linha})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


# ---------------------------------------------------------------------------
# REAJUSTE — a data-base do contrato, os índices e a previsão
#
# O índice normal é o INCC-DI, e a tabela é mantida pelo próprio sistema pela
# série 192 do Banco Central. A coleta é pelo BOTÃO, nunca no start.
# ---------------------------------------------------------------------------
@bp.route("/erp/api/indices")
@login_obrigatorio
@permissao("ver_contratos")
def api_indices():
    from app.apps.erp.core.indices import bcb
    with get_session() as s:
        return jsonify({"ok": True,
                        "catalogo": bcb.listar_indices(),
                        **bcb.listar(s, request.args.get("codigo") or bcb.PADRAO)})


@bp.route("/erp/api/indices/atualizar", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_indices_atualizar():
    """Traz do Banco Central o que falta. Nunca sobrescreve linha lançada à mão."""
    from app.apps.erp.core.indices import bcb
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            r = bcb.atualizar(s, (d.get("codigo") or bcb.PADRAO),
                              usuario=_usuario_logado(s))
            s.commit()
        return jsonify({"ok": True, "resumo": r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/indices/lancar", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_indices_lancar():
    """O número do boletim da FGV, digitado — o INCC-DI do mês só sai lá pelo 25."""
    from app.apps.erp.core.indices import bcb
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            bcb.lancar_manual(
                s, codigo=(d.get("codigo") or bcb.PADRAO),
                competencia=date.fromisoformat((d.get("competencia") or "") + "-01"
                                               if len(d.get("competencia") or "") == 7
                                               else (d.get("competencia") or "")),
                variacao_pct=d.get("variacao_pct"),
                usuario=_usuario_logado(s))
            s.commit()
        return jsonify({"ok": True})
    except (ErroValidacao, ValueError) as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


# ---------------------------------------------------------------------------
# ACOMPANHAMENTO — os processos que correm fora da BWS (migração 072)
#
# O escopo é o da OBRA, pelo mesmo `obras_do_usuario` da listagem de obras.
# Processo fora do escopo responde 404 "não encontrado", nunca 403: dizer "sem
# permissão" para um número que existe confirma que ele existe.
# ---------------------------------------------------------------------------
@bp.route("/erp/api/acompanhamento")
@login_obrigatorio
@permissao("ver_acompanhamento")
def api_acompanhamento():
    """A fila: o que está pendente, de quem está mais perto de virar problema."""
    from app.apps.erp.core.acompanhamento import processos as svc
    # `obras_de_registro_sem_autor` e NÃO `obras_do_usuario`: processo não é
    # recortável por autoria (quem abriu não é quem toca), e para quem enxerga
    # "só o que eu lancei" a segunda devolveria None — "sem filtro de obra".
    from app.apps.erp.core.auth.permissoes import obras_de_registro_sem_autor
    with get_session() as s:
        u = _usuario_logado(s)
        r = svc.listar(
            s, obras_permitidas=obras_de_registro_sem_autor(s, u),
            obra_id=request.args.get("obra_id", type=int),
            responsavel_id=request.args.get("responsavel_id", type=int),
            tipo=request.args.get("tipo"),
            incluir_encerrados=request.args.get("encerrados") == "1")
        return jsonify({"ok": True, "tipos": svc.TIPOS, "situacoes": svc.SITUACOES,
                        **r})


@bp.route("/erp/api/acompanhamento", methods=["POST"])
@login_obrigatorio
@permissao("tocar_processo")
def api_acompanhamento_criar():
    """Abre o processo. Obrigatórios: assunto, tipo e UM dono (obra ou empresa)."""
    from app.apps.erp.core.acompanhamento import processos as svc
    from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo_sem_autoria
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            u = _usuario_logado(s)
            if d.get("obra_id"):
                # O MESMO recorte da listagem: abrir processo numa obra que a
                # pessoa depois não consegue listar seria criar o invisível.
                exigir_obra_no_escopo_sem_autoria(s, u, int(d["obra_id"]))
            p = svc.criar(s, d, u)
            numero, pid = p.numero, p.id
            s.commit()
        return jsonify({"ok": True, "id": pid, "numero": numero})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/acompanhamento/<int:processo_id>")
@login_obrigatorio
@permissao("ver_acompanhamento")
def api_acompanhamento_detalhe(processo_id: int):
    from app.apps.erp.core.acompanhamento import processos as svc
    from app.apps.erp.core.auth.permissoes import exigir_processo_no_escopo
    with get_session() as s:
        exigir_processo_no_escopo(s, _usuario_logado(s), processo_id)
        return jsonify({"ok": True, "processo": svc.detalhe(s, processo_id),
                        "tipos": svc.TIPOS, "situacoes": svc.SITUACOES})


@bp.route("/erp/api/acompanhamento/<int:processo_id>", methods=["POST"])
@login_obrigatorio
@permissao("tocar_processo")
def api_acompanhamento_atualizar(processo_id: int):
    from app.apps.erp.core.acompanhamento import processos as svc
    from app.apps.erp.core.auth.permissoes import exigir_processo_no_escopo
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            u = _usuario_logado(s)
            exigir_processo_no_escopo(s, u, processo_id)
            svc.atualizar(s, processo_id, d, u)
            s.commit()
        return jsonify({"ok": True})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/acompanhamento/<int:processo_id>/andamento", methods=["POST"])
@login_obrigatorio
@permissao("tocar_processo")
def api_acompanhamento_andamento(processo_id: int):
    """Uma frase, e pronto — data e autor entram sozinhos."""
    from app.apps.erp.core.acompanhamento import processos as svc
    from app.apps.erp.core.auth.permissoes import exigir_processo_no_escopo
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            u = _usuario_logado(s)
            exigir_processo_no_escopo(s, u, processo_id)
            svc.lancar_andamento(s, processo_id, d, u)
            s.commit()
        return jsonify({"ok": True})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/acompanhamento/<int:processo_id>/passo", methods=["POST"])
@login_obrigatorio
@permissao("tocar_processo")
def api_acompanhamento_passo(processo_id: int):
    """Marca, desmarca, acrescenta ou apaga um passo sugerido.

    Nenhuma destas ações trava coisa alguma — a lista lembra, não barra. Se um
    dia aparecer validação aqui, o módulo virou o SEI, que foi o contraexemplo
    que o dono deu.
    """
    from app.apps.erp.core.acompanhamento import processos as svc
    from app.apps.erp.core.auth.permissoes import exigir_processo_no_escopo
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            u = _usuario_logado(s)
            exigir_processo_no_escopo(s, u, processo_id)
            # O passo pertence ao processo, e é por isso que ele é conferido
            # aqui: mandar o id de um passo de OUTRO processo não pode virar
            # uma porta lateral para fora do escopo.
            passo_id = d.get("passo_id")
            if passo_id is not None:
                meus = {x.id for x in svc.passos_do_processo(s, processo_id)}
                if int(passo_id) not in meus:
                    raise ErroNaoEncontrado("Passo não encontrado.")
            if d.get("acao") == "acrescentar":
                svc.acrescentar_passo(s, processo_id, d.get("texto") or "", u)
            elif d.get("acao") == "apagar":
                svc.apagar_passo(s, int(passo_id))
            else:
                svc.marcar_passo(s, int(passo_id), bool(d.get("feito")), u)
            s.commit()
        return jsonify({"ok": True})
    except (ErroValidacao, ValueError, TypeError) as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/acompanhamento/<int:processo_id>/oficio")
@login_obrigatorio
@permissao("tocar_processo")
def api_acompanhamento_oficio_rascunho(processo_id: int):
    """O rascunho do ofício, com o que o ERP já sabe preenchido.

    Não numera nada: numerar um rascunho que a pessoa vai descartar deixaria
    buraco na sequência, e buraco em sequência de ofício é pergunta que o órgão
    faz.
    """
    from app.apps.erp.core.acompanhamento import oficios
    from app.apps.erp.core.auth.permissoes import exigir_processo_no_escopo
    with get_session() as s:
        exigir_processo_no_escopo(s, _usuario_logado(s), processo_id)
        return jsonify({"ok": True, "rascunho": oficios.montar(s, processo_id),
                        "gerados": oficios.listar_do_processo(s, processo_id)})


@bp.route("/erp/api/acompanhamento/<int:processo_id>/oficio", methods=["POST"])
@login_obrigatorio
@permissao("tocar_processo")
def api_acompanhamento_oficio_gerar(processo_id: int):
    """Numera, monta o PDF, arquiva e lança o andamento — numa transação só."""
    from app.apps.erp.core.acompanhamento import oficios
    from app.apps.erp.core.auth.permissoes import exigir_processo_no_escopo
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            u = _usuario_logado(s)
            exigir_processo_no_escopo(s, u, processo_id)
            o = oficios.gerar(s, processo_id, d, u)
            saida = {"numero": o.numero, "documento_id": o.documento_id,
                     "anexo_id": (oficios.listar_do_processo(s, processo_id) or
                                  [{}])[0].get("anexo_id")}
            s.commit()
        return jsonify({"ok": True, **saida})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/acompanhamento/<int:processo_id>/deferir")
@login_obrigatorio
@permissao("tocar_processo")
def api_acompanhamento_deferir_proposta(processo_id: int):
    """O que o ERP PROPÕE atualizar na obra. Propõe — não aplica."""
    from app.apps.erp.core.acompanhamento import processos as svc
    from app.apps.erp.core.auth.permissoes import exigir_processo_no_escopo
    with get_session() as s:
        exigir_processo_no_escopo(s, _usuario_logado(s), processo_id)
        return jsonify({"ok": True,
                        "proposta": svc.proposta_de_deferimento(s, processo_id)})


@bp.route("/erp/api/acompanhamento/<int:processo_id>/deferir", methods=["POST"])
@login_obrigatorio
@permissao("tocar_processo")
def api_acompanhamento_deferir(processo_id: int):
    """Registra o aditivo na obra a partir do processo deferido.

    Exige `configurar`? NÃO — e é decisão: quem toca o processo é quem sabe que
    o termo saiu. O que protege o contrato é a CONFIRMAÇÃO na tela (o sistema
    propõe, a pessoa confirma) e a trilha, não uma segunda permissão que faria
    a pessoa certa depender da agenda de outra.
    """
    from app.apps.erp.core.acompanhamento import processos as svc
    from app.apps.erp.core.auth.permissoes import exigir_processo_no_escopo
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            u = _usuario_logado(s)
            exigir_processo_no_escopo(s, u, processo_id)
            r = svc.aplicar_deferimento(s, processo_id, d, u)
            s.commit()
        return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/acompanhamento/demora")
@login_obrigatorio
@permissao("ver_acompanhamento")
def api_acompanhamento_demora():
    """Quanto cada órgão demora, por tipo de processo."""
    from app.apps.erp.core.acompanhamento import processos as svc
    from app.apps.erp.core.auth.permissoes import obras_de_registro_sem_autor
    with get_session() as s:
        u = _usuario_logado(s)
        return jsonify({"ok": True,
                        **svc.demora_por_orgao(
                            s, obras_permitidas=obras_de_registro_sem_autor(s, u))})


@bp.route("/erp/api/acompanhamento/assumir", methods=["POST"])
@login_obrigatorio
@permissao("tocar_processo")
def api_acompanhamento_assumir():
    """O botão do caso das férias: vários processos para uma pessoa de uma vez."""
    from app.apps.erp.core.acompanhamento import processos as svc
    from app.apps.erp.core.auth.permissoes import exigir_processo_no_escopo
    d = request.get_json(silent=True) or {}
    ids = [int(i) for i in (d.get("ids") or [])]
    try:
        with get_session() as s:
            u = _usuario_logado(s)
            for pid in ids:
                exigir_processo_no_escopo(s, u, pid)
            quantos = svc.assumir(s, ids, int(d.get("responsavel_id") or u.id), u)
            s.commit()
        return jsonify({"ok": True, "trocados": quantos})
    except (ErroValidacao, ValueError) as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/indices/boletim", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_indices_boletim():
    """A tabela do boletim, colada de uma vez — variações E número-índice.

    Antes disto, alimentar a tabela com o boletim do dono exigia duas telas
    desconexas: digitar a variação mês a mês e depois informar o número de um
    mês para a régua bater. Ele já tem a tabela inteira copiada da fonte
    (18/09/2026: *"os índices vêm nesse formato"*) — colar resolve os dois.
    """
    from app.apps.erp.core.indices import bcb
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            r = bcb.importar_boletim(
                s, codigo=(d.get("codigo") or bcb.PADRAO),
                texto=d.get("texto") or "",
                ancorar=d.get("ancorar") is not False,
                usuario=_usuario_logado(s),
                simular=bool(d.get("simular")))
            if d.get("simular"):
                s.rollback()
            else:
                s.commit()
        return jsonify({"ok": True, "relatorio": r})
    except (ErroValidacao, ValueError) as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/indices/ancora", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_indices_ancora():
    """Pendura a série no número oficial de um mês — ou tira a âncora.

    O número-índice é acumulado pelo sistema a partir da variação, e acumular
    exige escolher onde a régua começa. Sem isso o número não bate com o do
    boletim, e o dono não tem como conferir (migração 069).
    """
    from app.apps.erp.core.indices import bcb
    d = request.get_json(silent=True) or {}
    codigo = (d.get("codigo") or bcb.PADRAO)
    try:
        with get_session() as s:
            u = _usuario_logado(s)
            if d.get("remover"):
                bcb.limpar_ancora(s, codigo=codigo, usuario=u)
            else:
                mes = (d.get("competencia") or "").strip()
                bcb.definir_ancora(
                    s, codigo=codigo,
                    competencia=date.fromisoformat(mes + "-01" if len(mes) == 7
                                                   else mes),
                    numero_indice=d.get("numero_indice"),
                    observacao=d.get("observacao") or "", usuario=u)
            s.commit()
        return jsonify({"ok": True})
    except (ErroValidacao, ValueError) as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/contratos/<int:contrato_id>/reajuste")
@login_obrigatorio
@permissao("ver_contratos")
def api_contrato_reajuste(contrato_id: int):
    """A previsão de reajuste de cada medição do contrato, e a soma."""
    from app.apps.erp.core.indices import reajuste as svc
    try:
        with get_session() as s:
            return jsonify({"ok": True, **svc.previsao_do_contrato(s, contrato_id)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404


@bp.route("/erp/api/contratos/<int:contrato_id>/reajuste", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_contrato_reajuste_config(contrato_id: int):
    """A data-base do contrato: do orçamento ou da proposta. Muda por contrato."""
    from app.apps.erp.core.indices import reajuste as svc
    from app.apps.erp.db.models.cadastros import Contrato
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            c = s.get(Contrato, contrato_id)
            if c is None:
                raise ErroValidacao("Contrato não encontrado.")
            if d.get("data_base"):
                c.data_base = date.fromisoformat(d["data_base"])
            elif "data_base" in d:
                c.data_base = None
            if "data_base_origem" in d:
                c.data_base_origem = (d.get("data_base_origem") or "").strip().upper() or None
            if d.get("indice_reajuste") is not None:
                c.indice_reajuste = (d.get("indice_reajuste") or "").strip().upper() or None
            if d.get("reajuste_meses"):
                c.reajuste_meses = int(d["reajuste_meses"])
            s.flush()
            saida = svc.previsao_do_contrato(s, contrato_id)
            s.commit()
        return jsonify({"ok": True, **saida})
    except (ErroValidacao, ValueError) as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/medicoes/<int:titulo_id>/reajuste", methods=["POST"])
@login_obrigatorio
@permissao("receber")
def api_medicao_gerar_reajuste(titulo_id: int):
    """A previsão vira título a receber — com o valor EDITÁVEL, porque quem
    fecha o número é o órgão, não o sistema."""
    from app.apps.erp.core.auth.permissoes import exigir_titulo_no_escopo
    from app.apps.erp.core.indices import reajuste as svc
    from app.apps.erp.core.titulos import medicao as svc_med
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            exigir_titulo_no_escopo(s, _usuario_logado(s), titulo_id)
            novo = svc.gerar_titulo(s, titulo_id, valor=d.get("valor"),
                                    numero_medicao=(d.get("numero_medicao") or ""),
                                    usuario=_usuario_logado(s))
            linha = svc_med.ler(s, novo)
            s.commit()
        return jsonify({"ok": True, "medicao": linha})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


# ---------------------------------------------------------------------------
# AGENDA — o calendário de obrigações
#
# Quatro coisas construídas antes dela esperavam um lugar para avisar:
# aniversário de reajuste, conferência de locação, certidão vencendo e fim da
# vigência do contrato. A sincronização é recalculada, não acumulada.
# ---------------------------------------------------------------------------
@bp.route("/erp/api/agenda")
@login_obrigatorio
@permissao("ver_agenda")
def api_agenda_obrigacoes():
    from app.apps.erp.core.agenda import service as svc
    from app.apps.erp.core.comum import tarefas, trabalhos
    with get_session() as s:
        # A agenda ABRE com o que já está calculado, e manda recalcular por
        # trás. Antes ela recalculava tudo — obras, contratos, certidões,
        # locações, certificados — com a pessoa olhando a tela em branco, e
        # isso cresce junto com a empresa. Agenda que só atualiza no botão é
        # agenda desatualizada, então o recálculo continua acontecendo sozinho:
        # só deixou de ser na frente de quem abriu.
        tarefa = None
        if request.args.get("sincronizar", "1") != "0":
            try:
                trabalhos.registrar_todos()
                usuario = _usuario_logado(s)
                # `enfileirar_unico`: dez pessoas abrindo a agenda não criam
                # dez recálculos iguais na fila.
                nova = tarefas.enfileirar_unico(
                    s, "sincronizar_agenda",
                    {"usuario_id": usuario.id if usuario else None},
                    rotulo="Atualizar os avisos da agenda", usuario=usuario,
                    frescor_minutos=10)
                tarefa = tarefas.ler(s, nova.id) if nova is not None else None
                s.commit()
            except Exception:
                logger.warning("ERP/agenda: não deu para enfileirar o recálculo",
                               exc_info=True)
                s.rollback()
        return jsonify({"ok": True, "tarefa": tarefa, **svc.listar(
            s,
            usuario=_usuario_logado(s),
            situacao=(request.args.get("situacao") or "ABERTO").strip().upper(),
            origem=(request.args.get("origem") or "").strip().upper(),
            obra_id=(int(request.args["obra_id"]) if request.args.get("obra_id") else None),
            incluir_futuros=request.args.get("futuros") == "1")})


@bp.route("/erp/api/agenda/<int:evento_id>", methods=["POST", "DELETE"])
@login_obrigatorio
@permissao("tratar_agenda")
def api_agenda_evento(evento_id: int):
    """Resolver, dispensar (com motivo), reabrir — ou apagar a anotação."""
    from app.apps.erp.core.agenda import service as svc
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if request.method == "DELETE":
                svc.apagar_manual(s, evento_id, usuario=usuario)
                s.commit()
                return jsonify({"ok": True})
            acao = (d.get("acao") or "resolver").strip().lower()
            if acao == "reabrir":
                svc.reabrir(s, evento_id, usuario=usuario)
            else:
                svc.resolver(s, evento_id, observacao=(d.get("observacao") or ""),
                             dispensar=(acao == "dispensar"), usuario=usuario)
            s.commit()
        return jsonify({"ok": True})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/agenda/anotar", methods=["POST"])
@login_obrigatorio
@permissao("tratar_agenda")
def api_agenda_anotar():
    """A anotação que ninguém deduz: 'entregar a declaração no dia 20'."""
    from app.apps.erp.core.agenda import service as svc
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            svc.criar_manual(
                s, titulo=(d.get("titulo") or ""),
                quando=date.fromisoformat(d.get("quando") or ""),
                detalhe=(d.get("detalhe") or ""),
                avisar_dias=int(d.get("avisar_dias") or 7),
                obra_id=(int(d["obra_id"]) if d.get("obra_id") else None),
                usuario=_usuario_logado(s))
            s.commit()
        return jsonify({"ok": True})
    except (ErroValidacao, ValueError) as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/saude")
@login_obrigatorio
@permissao("configurar")
def api_saude():
    """O termômetro: tempo por tela, memória, e o que ocupa o banco.

    Fica com o ADMIN porque é a tela que embasa decisão de GASTAR — trocar de
    plano, subir o banco —, e porque mostra o tamanho de cada tabela.
    """
    from app.apps.erp.core.comum import saude
    # Desce o que ainda está na memória antes de ler: sem isto a tela mostraria
    # tudo menos o minuto que acabou de passar, que é justamente o que a pessoa
    # foi conferir depois de achar o sistema lento.
    saude.gravar()
    with get_session() as s:
        dias = 7
        try:
            dias = max(1, min(int(request.args.get("dias") or 7), 90))
        except ValueError:
            pass
        return jsonify({"ok": True, **saude.panorama(s, dias=dias)})


def _perfis_cadastrados(s) -> list[dict]:
    """Os perfis de acesso cadastrados, para a caixa de escolha do operador.

    Devolve lista vazia — e não erro — enquanto a migração 065 não rodou: a
    tela de operadores continua funcionando com o cargo antigo, que é o que
    segura o sistema na janela entre publicar e apertar o botão.
    """
    try:
        from app.apps.erp.core.auth import perfis as svc_perfis
        return svc_perfis.listar(s)
    except Exception:
        logger.warning("ERP: perfis indisponíveis (migração 065 pendente?)")
        return []


def _projetos_cadastrados(s) -> list[dict]:
    """Os projetos, para a caixa de seleção do alcance do operador."""
    try:
        from app.apps.erp.core.cadastros import projetos as svc_projetos
        return [{"id": p["id"], "codigo": p["codigo"], "nome": p["nome"],
                 "quantas_obras": p["quantas_obras"]}
                for p in svc_projetos.listar(s)]
    except Exception:
        logger.warning("ERP: projetos indisponíveis (migração 066 pendente?)")
        return []


def _empresas_cadastradas(s) -> list[dict]:
    """As empresas do grupo, para a caixa de seleção do alcance do operador."""
    from sqlalchemy import select as _select
    from app.apps.erp.db.models.cadastros import Empresa
    try:
        return [{"id": e.id, "nome": e.nome_fantasia or e.razao_social}
                for e in s.scalars(_select(Empresa).order_by(Empresa.razao_social)).all()]
    except Exception:
        return []


def _aplicar_perfil_cadastrado(u, d: dict, s=None, autor=None) -> None:
    """Grava o perfil de acesso e a marca "enxerga todas as obras".

    As duas coisas são diferentes de propósito, e foi o dono quem separou:
    *"só que tem uma diferença do banco, porque tem a questão da obra"*. O
    perfil diz O QUE a pessoa faz; as obras, ONDE. Duas pessoas do mesmo perfil
    acompanham obras diferentes.

    E desde a migração 095 a pessoa pode ter MAIS DE UM perfil: um principal e
    quantos acréscimos forem precisos. Eles SOMAM acesso, nunca tiram.
    """
    if "perfil_id" in d:
        valor = d.get("perfil_id")
        u.perfil_id = int(valor) if valor not in (None, "", 0, "0") else None
    if "ve_todas_as_obras" in d:
        u.ve_todas_as_obras = bool(d.get("ve_todas_as_obras"))
    if s is not None and "perfis_extras" in d:
        _gravar_perfis_extras(s, u, d.get("perfis_extras") or [], autor)


def _perfis_extras_do_usuario(s, usuario_id: int) -> list[int]:
    """Os perfis ACRESCENTADOS a esta pessoa (migração 095).

    Protegida como todo o resto do acesso: enquanto a tabela não existir, a
    resposta é "nenhum", e o cadastro continua abrindo.
    """
    from sqlalchemy import text as _text
    try:
        return [linha[0] for linha in s.execute(_text(
            "SELECT perfil_id FROM usuario_perfis WHERE usuario_id = :i "
            "ORDER BY perfil_id"), {"i": usuario_id}).all()]
    except Exception:
        return []


def _gravar_perfis_extras(s, u, pedidos, autor) -> None:
    """Troca a lista de perfis acrescentados desta pessoa.

    ⚑ O PERFIL PRINCIPAL NUNCA ENTRA AQUI. Se ele entrasse, trocar o principal
    deixaria para trás um acréscimo invisível com o perfil velho — e a pessoa
    continuaria com um acesso que a tela diz que ela não tem mais.
    """
    from sqlalchemy import text as _text
    from app.apps.erp.core.comum.auditoria import registrar_evento
    try:
        novos = {int(x) for x in pedidos if str(x).strip() not in ("", "0", "None")}
    except (TypeError, ValueError):
        raise ErroValidacao("Perfil de acesso inválido na lista.")
    novos.discard(u.perfil_id or 0)
    atuais = set(_perfis_extras_do_usuario(s, u.id))
    if novos == atuais:
        return
    validos = {linha[0] for linha in s.execute(_text(
        "SELECT id FROM perfis WHERE ativo")).all()}
    desconhecidos = novos - validos
    if desconhecidos:
        raise ErroValidacao("Perfil de acesso que não existe mais foi pedido.")
    s.execute(_text("DELETE FROM usuario_perfis WHERE usuario_id = :i"),
              {"i": u.id})
    for pid in sorted(novos):
        s.execute(_text(
            "INSERT INTO usuario_perfis (usuario_id, perfil_id, criado_por) "
            "VALUES (:u, :p, :a)"),
            {"u": u.id, "p": pid, "a": autor.id if autor else None})
    # Mudança de ACESSO é das que têm de deixar rastro: fica no registro quem
    # mudou, quando, e de quais perfis para quais.
    registrar_evento(s, "usuario", u.id, "PERFIS_EXTRAS_ALTERADOS",
                     {"antes": sorted(atuais), "depois": sorted(novos)},
                     autor.id if autor else None)


@bp.route("/erp/api/usuarios", methods=["GET", "POST"])
@login_obrigatorio
@permissao("gerir_usuarios")
def api_usuarios():
    """Cadastro de operadores. Só o ADMIN mexe."""
    from sqlalchemy import select
    from app.apps.erp.core.auth.permissoes import ROTULOS, escopo_visao, exigir
    from app.apps.erp.core.auth.service import criar_usuario
    from app.apps.erp.core.comum import ia_custo as _ia_custo
    from app.apps.erp.core.cadastros.validadores import cpf_valido, somente_digitos
    from app.apps.erp.db.models.cadastros import (
        EscopoVisao, Obra, PerfilUsuario, Usuario, UsuarioCategoria, UsuarioObra,
    )
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if request.method == "GET":
                usuarios = s.scalars(select(Usuario).order_by(Usuario.nome)).all()
                from app.apps.erp.core.cadastros import vinculos as _vinc
                alcance = {u.id: _vinc.alcance_do_operador(s, u.id) for u in usuarios}
                vinculos: dict[int, list[str]] = {}
                responde_por: dict[int, list[int]] = {}
                for v in s.scalars(select(UsuarioObra)).all():
                    obra = s.get(Obra, v.obra_id)
                    vinculos.setdefault(v.usuario_id, []).append(
                        obra.codigo if obra else str(v.obra_id))
                    if v.responsavel:
                        responde_por.setdefault(v.usuario_id, []).append(v.obra_id)
                return jsonify({"ok": True, "usuarios": [{
                    "id": u.id, "nome": u.nome, "email": u.email,
                    "cpf": u.cpf, "telefone": u.telefone,
                    "ff_autorizado": u.ff_autorizado,
                    "ff_teto_item": float(u.ff_teto_item) if u.ff_teto_item else None,
                    "ff_teto_prestacao": (float(u.ff_teto_prestacao)
                                          if u.ff_teto_prestacao else None),
                    "ff_saldo_adiantamento": float(u.ff_saldo_adiantamento or 0),
                    "categorias": [x.categoria_id for x in s.scalars(
                        select(UsuarioCategoria).where(
                            UsuarioCategoria.usuario_id == u.id)).all()],
                    "perfil": u.perfil.value,
                    "perfil_rotulo": ROTULOS.get(u.perfil, u.perfil.value),
                    # O perfil CADASTRADO (migração 065) e as obras da pessoa.
                    # É aqui que o dono mexe agora — o cargo acima ficou de
                    # herança e sai quando nada mais o ler.
                    "perfil_id": getattr(u, "perfil_id", None),
                    # Os perfis ACRESCENTADOS (migração 095). Eles somam ao
                    # principal; a tela mostra os dois juntos.
                    "perfis_extras": _perfis_extras_do_usuario(s, u.id),
                    "ve_todas_as_obras": getattr(u, "ve_todas_as_obras", None),
                    # O alcance dito por PROJETO e por EMPRESA (migração 066).
                    # A obra continua sendo a unidade: estes dois são jeitos de
                    # nomear um conjunto de obras, resolvido na consulta.
                    "projetos": alcance.get(u.id, {}).get("projetos", []),
                    "empresas": alcance.get(u.id, {}).get("empresas", []),
                    "escopo_visao": escopo_visao(u).value,
                    # Teto de IA do mês desta pessoa e quanto dele já foi.
                    # Nulo = sem limite, e a tela escreve isso com todas as
                    # letras — caixa vazia parece esquecimento, não escolha.
                    "teto_ia_usd": (float(u.teto_ia_usd)
                                    if u.teto_ia_usd is not None else None),
                    "ia_do_mes": _ia_custo.situacao_da_pessoa(s, u.id),
                    "ativo": u.ativo, "obras": vinculos.get(u.id, []),
                    # por quais obras esta pessoa RESPONDE (recebe a conferência
                    # mensal e a cobrança do agente) — é a mesma marca que
                    # aparece no cadastro da obra
                    "obras_responsavel": responde_por.get(u.id, []),
                } for u in usuarios], "perfis": [
                    {"chave": p.value, "rotulo": ROTULOS.get(p, p.value)}
                    for p in PerfilUsuario],
                    "perfis_cadastrados": _perfis_cadastrados(s),
                    "projetos_cadastrados": _projetos_cadastrados(s),
                    "empresas_cadastradas": _empresas_cadastradas(s)})

            exigir(atual, "gerir_usuarios")
            d = request.get_json(silent=True) or {}
            cpf = somente_digitos(d.get("cpf") or "")
            if cpf and not cpf_valido(cpf):
                return jsonify({"ok": False, "erro": "CPF inválido."}), 400
            # SEM SENHA DIGITADA, A CONTA NASCE COM UMA SORTEADA E JOGADA
            # FORA — e quem define é a pessoa, pelo convite (09/11/2026).
            # Pedido dele: *"eu vou cadastrar o e-mail da pessoa, configurar o
            # perfil, e disparar um e-mail para ela gravar a senha dela."*
            # Isso é melhor por segurança, não só por comodidade: hoje alguém
            # escolhe a senha pela pessoa e manda por WhatsApp.
            import secrets as _secrets
            convidar = not (d.get("senha") or "").strip()
            u = criar_usuario(s, nome=d.get("nome") or "", email=d.get("email") or "",
                              senha=(d.get("senha") or _secrets.token_urlsafe(32)),
                              perfil=d.get("perfil") or "CONSULTA",
                              criado_por=atual)
            u.cpf = cpf or None
            u.telefone = somente_digitos(d.get("telefone") or "") or None
            u.observacoes = (d.get("observacoes") or "").strip() or None
            # O flush vem ANTES porque os perfis acrescentados precisam do id
            # da pessoa, e no cadastro novo ele só existe depois dele.
            s.flush()
            _aplicar_perfil_cadastrado(u, d, s, atual)
            if d.get("projetos") or d.get("empresas"):
                from app.apps.erp.core.cadastros import vinculos as _vinc
                s.flush()
                _vinc.definir_alcance_do_operador(
                    s, u.id, projetos=d.get("projetos") or [],
                    empresas=d.get("empresas") or [])
            # Escopo ausente ou desconhecido cai no mais restritivo.
            try:
                u.escopo_visao = EscopoVisao(d.get("escopo_visao"))
            except ValueError:
                u.escopo_visao = EscopoVisao.PROPRIOS
            for obra_id in (d.get("obras") or []):
                s.add(UsuarioObra(usuario_id=u.id, obra_id=int(obra_id)))
            # O CONVITE SAI AQUI, no cadastro novo e sem senha digitada.
            convite = {"enviado": False, "erro": "", "link": ""}
            if convidar and (u.email or "").strip():
                from app.apps.erp.core.auth import convites as _convites
                convite = _convites.enviar(s, u.id,
                                           link_base=_link_do_convite(),
                                           autor=atual)
            s.commit()
            # O RESULTADO VOLTA PARA A TELA, inclusive quando NÃO saiu:
            # cadastrar dez pessoas e descobrir depois que nenhuma recebeu o
            # link é o tipo de silêncio que faz perder uma manhã.
            return jsonify({"ok": True, "usuario_id": u.id,
                            "convite": convite})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ValueError as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha no cadastro de operador")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/usuarios/<int:usuario_id>", methods=["POST"])
@login_obrigatorio
@permissao("gerir_usuarios")
def api_editar_usuario(usuario_id: int):
    from sqlalchemy import select
    from app.apps.erp.core.auth.permissoes import exigir
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.core.cadastros.validadores import somente_digitos
    from app.apps.erp.db.models.cadastros import (
        EscopoVisao, PerfilUsuario, Usuario, UsuarioObra,
    )
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            exigir(atual, "gerir_usuarios")
            u = s.get(Usuario, usuario_id)
            if u is None:
                return jsonify({"ok": False, "erro": "Operador não encontrado."}), 404
            for campo in ("nome", "telefone", "observacoes"):
                if campo in d:
                    valor = (d[campo] or "").strip()
                    setattr(u, campo, somente_digitos(valor) if campo == "telefone" else valor or None)
            # ⚑ O CPF SE CORRIGE NA EDIÇÃO (23/11/2026). A tela mostrava o campo
            # aberto, mas só o cadastro NOVO gravava — ele digitou, salvou, e o
            # CPF continuou em branco. Vazio não apaga o que já existe.
            if "cpf" in d:
                from app.apps.erp.core.cadastros.validadores import cpf_valido as _cpf_ok
                cpf_novo = somente_digitos(d.get("cpf") or "")
                if cpf_novo and not _cpf_ok(cpf_novo):
                    return jsonify({"ok": False, "erro": "CPF inválido."}), 400
                if cpf_novo:
                    u.cpf = cpf_novo
            if d.get("perfil"):
                u.perfil = PerfilUsuario(d["perfil"])
            if "escopo_visao" in d:
                # Ampliar o alcance é escolha explícita; valor estranho fecha.
                try:
                    u.escopo_visao = EscopoVisao(d["escopo_visao"])
                except ValueError:
                    return jsonify({"ok": False,
                                    "erro": "Escopo de visão inválido."}), 400
            if "ativo" in d:
                u.ativo = bool(d["ativo"])
            if d.get("senha"):
                u.senha_hash = gerar_hash(d["senha"])
            if "ff_autorizado" in d:
                u.ff_autorizado = bool(d["ff_autorizado"])
            from decimal import Decimal as _D, InvalidOperation as _IE
            if "teto_ia_usd" in d:
                # Campo VAZIO = sem limite. É escolha, não esquecimento: quem
                # deixa em branco está dizendo "esta pessoa pode gastar à
                # vontade", e a tela avisa isso na hora de salvar.
                bruto = str(d["teto_ia_usd"] or "").replace("US$", "").strip()
                bruto = bruto.replace(".", "").replace(",", ".") if "," in bruto else bruto
                if not bruto:
                    u.teto_ia_usd = None
                else:
                    try:
                        valor = _D(bruto).quantize(_D("0.01"))
                    except _IE:
                        return jsonify({"ok": False, "erro":
                                        "Limite de IA inválido — informe um valor "
                                        "em dólar, como 5 ou 10,50."}), 400
                    if valor < 0:
                        return jsonify({"ok": False, "erro":
                                        "O limite de IA não pode ser negativo."}), 400
                    u.teto_ia_usd = valor
            for campo in ("ff_teto_item", "ff_teto_prestacao"):
                if campo in d:
                    valor = str(d[campo] or "").replace(".", "").replace(",", ".").strip()
                    try:
                        setattr(u, campo, _D(valor) if valor else None)
                    except _IE:
                        return jsonify({"ok": False,
                                        "erro": f"Valor inválido em {campo}."}), 400
            _aplicar_perfil_cadastrado(u, d, s, atual)
            if "projetos" in d or "empresas" in d:
                from app.apps.erp.core.cadastros import vinculos as _vinc
                _vinc.definir_alcance_do_operador(
                    s, u.id,
                    projetos=d.get("projetos") if "projetos" in d else None,
                    empresas=d.get("empresas") if "empresas" in d else None)
            if "obras" in d:
                # Um caminho só, compartilhado com a tela da OBRA. Antes daqui
                # esta rota apagava todos os vínculos e recriava — o que
                # apagava, sem avisar, a marca de quem responde pela obra.
                from app.apps.erp.core.cadastros import vinculos
                vinculos.definir_obras_do_operador(
                    s, u.id, d.get("obras") or [],
                    d.get("obras_responsavel") if "obras_responsavel" in d else None)
            if "categorias" in d:
                from app.apps.erp.db.models.cadastros import UsuarioCategoria
                for v in s.scalars(select(UsuarioCategoria).where(
                        UsuarioCategoria.usuario_id == u.id)).all():
                    s.delete(v)
                s.flush()
                for cat_id in (d.get("categorias") or []):
                    s.add(UsuarioCategoria(usuario_id=u.id, categoria_id=int(cat_id)))
            s.commit()
        return jsonify({"ok": True})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroValidacao as e:
        # Recado de gente para pedido errado — perfil que não existe mais,
        # por exemplo. Sem isto virava 500, e a tela dizia "falha no sistema"
        # para uma coisa que a pessoa consegue corrigir sozinha.
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao editar operador")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/colaboradores/buscar")
@login_obrigatorio
@permissao("ver_pessoal")
def api_colaboradores_buscar():
    """Acha a pessoa por PARTE DO NOME ou pelo CPF. Nunca devolve a lista toda.

    Pedido dele em 10/11/2026, abrindo uma solicitação: *"vamos buscar a
    pessoa na nossa base de dados, digitando algum dado, um CPF, e já poder
    visualizar o funcionário. Talvez pela LGPD a gente não possa expor uma
    lista de funcionários, mas a gente começar a digitar algum nome, ele já
    começar a ir aparecendo."*

    ⚑ EXIGE TRÊS LETRAS E DEVOLVE NO MÁXIMO OITO, e as duas coisas são a
    resposta à preocupação dele. Antes, a tela de abrir solicitação carregava
    **o cadastro inteiro** num `<select>`: qualquer pessoa com acesso ao
    Pessoal via os 575 nomes de uma vez, e bastava abrir o código da página
    para copiar a lista. Com busca, só aparece quem já se sabia procurar.

    ⚑ O CPF É PROCURADO POR DÍGITOS, e não pelo que foi digitado: quem digita
    "123.456" não acharia nada se a comparação fosse literal, porque o banco
    guarda só os números.

    O ESCOPO VALE AQUI TAMBÉM: quem é preso a obra só acha gente das obras
    dele. É o mesmo `aplicar_escopo` da listagem — regra de escopo escrita
    duas vezes diverge no primeiro mês.
    """
    from sqlalchemy import or_, select as _select

    from app.apps.erp.core.auth.permissoes import obras_do_usuario
    from app.apps.erp.core.cadastros.validadores import somente_digitos
    from app.apps.erp.db.models.cadastros import Colaborador, Obra
    procurado = (request.args.get("q") or "").strip()
    digitos = somente_digitos(procurado)
    if len(procurado) < 3 and len(digitos) < 3:
        return jsonify({"ok": True, "pessoas": [], "curto": True})
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            consulta = _select(Colaborador)
            filtros = [Colaborador.nome.ilike(f"%{procurado}%")]
            if digitos:
                filtros.append(Colaborador.cpf.like(f"%{digitos}%"))
                filtros.append(Colaborador.matricula.ilike(f"%{digitos}%"))
            consulta = consulta.where(or_(*filtros)).order_by(Colaborador.nome)
            # O ESCOPO POR OBRA. `None` quer dizer "alcança todas"; uma lista
            # vazia quer dizer "nenhuma", e aí não acha ninguém — que é o
            # certo, e não o contrário.
            permitidas = obras_do_usuario(s, atual)
            if permitidas is not None:
                consulta = consulta.where(
                    Colaborador.obra_id.in_(list(permitidas) or [-1]))
            achados = list(s.scalars(consulta.limit(8)).all())
            # ⚑ O VÍNCULO VAI JUNTO (15/11/2026). Ele: *"na hora que
            # selecionar o colaborador, a gente já sabe que tipo de
            # colaborador ele é. Isso o sistema já tem que informar: dados
            # básicos ali, prestador tal, iniciou tal, tipo de vínculo dele,
            # se é carteira de trabalho, se é prestador — isso já tem que
            # aparecer para facilitar para quem está pedindo o desligamento."*
            #
            # E com ele o CONTRATO DE EXPERIÊNCIA, que é o que mais muda o
            # pedido: muda a forma do desligamento e tira o aviso prévio.
            from app.apps.erp.core import pessoal_experiencia as svc_exp
            return jsonify({"ok": True, "pessoas": [{
                "id": c.id, "nome": c.nome, "cpf": c.cpf,
                "situacao": c.situacao or "ATIVO",
                # A OBRA VAI JUNTO porque é o que distingue dois homônimos —
                # e homônimo em obra é comum.
                "obra": (s.get(Obra, c.obra_id).codigo
                         if c.obra_id and s.get(Obra, c.obra_id) else ""),
                "vinculo": c.vinculo or "",
                "tipo_contrato": c.tipo_contrato or "",
                "admissao": str(c.admissao) if c.admissao else None,
                "cnpj_admissao": c.cnpj_admissao or "",
                "experiencia": svc_exp.situacao(c.admissao, c.contrato_experiencia),
            } for c in achados]})
    except Exception as e:
        logger.exception("ERP: falha na busca de colaborador")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/colaboradores/<int:colaborador_id>/foto")
@login_obrigatorio
@permissao("ver_pessoal")
def api_colaborador_foto(colaborador_id: int):
    """A foto da pessoa — e ela VEM DO PONTO, não de um anexo.

    Decisão dele em 10/11/2026: *"a foto vai ser alimentada pelo ponto (…) a
    primeira foto do ponto que ele começar a usar, ele coloca como cadastro."*
    Ninguém mais sobe foto de colaborador: ela já existe, batida todo dia.

    Confere o escopo porque recebe número de registro, e responde 404 quando
    não há foto — a tela esconde o quadro em vez de mostrar imagem quebrada.
    """
    from flask import Response
    from app.apps.erp.core.auth.permissoes import exigir_colaborador_no_escopo
    from app.apps.erp.core.cadastros import pessoas as svc
    try:
        with get_session() as s:
            exigir_colaborador_no_escopo(s, _usuario_logado(s), colaborador_id)
            foto = svc.foto_do_ponto(s, colaborador_id)
            if foto is None:
                raise ErroNaoEncontrado("Esta pessoa ainda não tem foto do ponto.")
            return Response(foto["conteudo"], mimetype=foto["mime"],
                            headers={"Cache-Control": "private, max-age=600"})
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha ao servir a foto da pessoa %s", colaborador_id)
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


def _liberar_acesso(s, c, *, autor, email=None, perfil=None, perfil_id=None,
                    obra_id=None, obra_ids=None, todas_as_obras=False,
                    convidar=True) -> dict:
    """CRIA O ACESSO de um colaborador — a ÚNICA implementação disto.

    ⚑ POR QUE É UMA FUNÇÃO E NÃO SÓ A ROTA (17/11/2026): a fila de pedidos
    vindos do ponto precisa liberar acesso, e a tentação era escrever um
    segundo caminho que faz "quase a mesma coisa". Foi exatamente a cobrança
    dele naquele dia — *"três botões para importar pessoas (…) às vezes não
    compreendo a lógica de complicar"*. Duas implementações do mesmo ato
    divergem: uma manda convite, a outra esquece; uma liga a obra, a outra
    não. Então há uma, e as duas portas chamam esta.

    NÃO dá commit: quem chama decide quando fechar a transação (a fila precisa
    marcar o pedido como atendido na MESMA transação — senão existe um instante
    em que o acesso foi criado e o pedido continua na fila).
    """
    from sqlalchemy import select as sa_select

    from app.apps.erp.core.auth import convites as svc_convites
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.core.cadastros import pessoas as svc
    from app.apps.erp.db.models.cadastros import (
        PerfilUsuario, Usuario, UsuarioObra,
    )
    import secrets as _secrets

    email = (email or c.email or "").strip().lower()
    if "@" not in email:
        return {"ok": False, "erro":
                "Informe o e-mail da pessoa — é por ele que ela entra e "
                "recebe o convite."}
    if (c.situacao or "").upper() == "DESLIGADO":
        return {"ok": False, "erro":
                "Esta pessoa está desligada. Readmita antes de liberar o "
                "acesso."}
    operador = s.scalars(
        sa_select(Usuario).where(Usuario.email == email)).first()
    if operador is None:
        # SENHA SORTEADA E JOGADA FORA: a conta nasce sem senha utilizável, e
        # quem define é a pessoa, pelo convite. Nasce com uma senha conhecida
        # seria uma porta aberta até alguém trocar.
        operador = Usuario(
            nome=c.nome, email=email,
            senha_hash=gerar_hash(_secrets.token_urlsafe(32)),
            perfil=PerfilUsuario.CONSULTA, ativo=True,
            telefone=(c.telefone or None))
        s.add(operador)
        s.flush()
    elif not operador.ativo:
        operador.ativo = True
    # ⚑ O CPF VEM DO CADASTRO (23/11/2026). Ele, liberando uma pessoa da fila:
    # *"O CPF já era para ele buscar e preencher automaticamente, não
    # aconteceu."* Nome, e-mail e telefone vinham; o CPF ficava em branco na
    # tela de operadores. Só preenche se estiver vazio — não troca o que alguém
    # já digitou.
    if not (operador.cpf or "").strip() and (c.cpf or "").strip():
        operador.cpf = c.cpf
    # O CARGO, quando quem libera disse qual. A fila do portal manda
    # ADMINISTRATIVO_OBRA; a ficha, nada, e aí fica o CONSULTA de sempre.
    if perfil:
        try:
            operador.perfil = PerfilUsuario(str(perfil).strip().upper())
        except ValueError:
            return {"ok": False, "erro": f"Perfil desconhecido: {perfil}."}
    if perfil_id:
        operador.perfil_id = int(perfil_id)
    svc.ligar_ao_operador(s, c.id, operador.id, autor)
    # A OBRA DA PESSOA vira o alcance dela, que é o caso comum de quem
    # trabalha em obra: sem isto o acesso nasce sem enxergar nada e alguém
    # teria de ir marcar na outra tela. `obra_id` de fora (a obra em que ela
    # bate ponto) tem preferência sobre a do cadastro, que envelhece.
    # ⚑ VÁRIAS OBRAS DESDE 18/11/2026. Ele, fechando o ciclo da liberação:
    # *"o administrador vai liberar a pessoa, mas tem que definir o acesso
    # dele, o perfil, as obras, etc e tal."*
    #
    # Antes era UMA obra — a do cadastro — e quem respondia por três ficava
    # vendo uma. `obra_ids` vazio mantém o de sempre (a obra do cadastro), e é
    # o que faz a liberação continuar sendo um clique para o caso comum.
    if todas_as_obras:
        operador.ve_todas_as_obras = True
    # ⚠️ LISTA VAZIA É ESCOLHA, não omissão: quem desmarcou tudo na tela quis
    # liberar o acesso sem obra nenhuma (um acesso de escritório, por
    # exemplo). Por isso a diferença entre `None` (ninguém disse nada — vale o
    # padrão de sempre) e `[]` (disseram "nenhuma").
    if obra_ids is None:
        escolhidas = [int(obra_id)] if obra_id else (
            [int(c.obra_id)] if c.obra_id else [])
    else:
        escolhidas = [int(x) for x in obra_ids if x]
    alvo = escolhidas[0] if escolhidas else None
    if not getattr(operador, "ve_todas_as_obras", False):
        for uma in escolhidas:
            ja = s.scalars(sa_select(UsuarioObra).where(
                UsuarioObra.usuario_id == operador.id,
                UsuarioObra.obra_id == uma)).first()
            if ja is None:
                s.add(UsuarioObra(usuario_id=operador.id, obra_id=uma))
    convite = {"enviado": False, "erro": "", "link": ""}
    if convidar:
        convite = svc_convites.enviar(
            s, operador.id, link_base=_link_do_convite(), autor=autor)
    return {"ok": True, "usuario_id": operador.id, "email": email,
            "obra_id": alvo, "obra_ids": escolhidas,
            "todas_as_obras": bool(getattr(operador, "ve_todas_as_obras", False)),
            "convite": convite}


@bp.route("/erp/api/colaboradores/<int:colaborador_id>/acesso",
          methods=["POST", "DELETE"])
@login_obrigatorio
@permissao("gerir_usuarios")
def api_colaborador_acesso(colaborador_id: int):
    """LIBERAR O ACESSO de um colaborador ao ERP, num passo só.

    É a resposta à pergunta dele: *"estando tudo certo, o que eu vou precisar
    é só cadastrar as pessoas e liberar o acesso?"* — sim, e é aqui.

    O que acontece num clique: cria o operador com o e-mail e o perfil
    escolhidos, **liga ao cadastro do colaborador** e manda o convite para a
    pessoa gravar a própria senha. Se o operador já existir com aquele
    e-mail, não duplica: liga ao que existe.

    ⚑ EXIGE `gerir_usuarios`, e não `editar_colaboradores`: criar acesso ao
    sistema é ato de administrador, não de quem opera o DP. Quem cadastra a
    pessoa e quem dá a chave da casa podem ser a mesma pessoa, mas não é o
    cadastro que decide isso.

    DELETE desfaz a LIGAÇÃO, não o operador. Apagar o operador levaria junto a
    autoria de tudo o que ele lançou.
    """
    from app.apps.erp.core.cadastros import pessoas as svc
    from app.apps.erp.db.models.cadastros import Colaborador
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            c = s.get(Colaborador, colaborador_id)
            if c is None:
                raise ErroNaoEncontrado("Colaborador não encontrado.")
            if request.method == "DELETE":
                svc.ligar_ao_operador(s, colaborador_id, None, atual)
                s.commit()
                return jsonify({"ok": True})

            d = request.get_json(silent=True) or {}
            r = _liberar_acesso(s, c, autor=atual, email=d.get("email"),
                                perfil=d.get("perfil"),
                                perfil_id=d.get("perfil_id"),
                                obra_id=d.get("obra_id"),
                                # A rota inteira já exige `gerir_usuarios`,
                                # então quem chega aqui pode escolher obra e
                                # alcance — não há o que filtrar de novo.
                                obra_ids=d.get("obra_ids"),
                                todas_as_obras=bool(d.get("todas_as_obras")),
                                convidar=d.get("convidar", True))
            if not r["ok"]:
                return jsonify(r), 400
            s.commit()
            return jsonify(r)
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except Exception as e:
        logger.exception("ERP: falha ao liberar acesso do colaborador %s",
                         colaborador_id)
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ===========================================================================
# A FILA DE PEDIDOS DE ACESSO AO PORTAL (17/11/2026)
#
# Ele: *"que tal a partir do ponto eu requerer a liberação do acesso ao
# portal? Ela já tem o cadastro no ponto, e lá pelo ponto ela vai informar o
# e-mail dela."*
#
# O que esta fila resolve: o acesso só nasce com e-mail, e o e-mail da gente de
# obra o sistema não tem — ninguém pede e-mail para contratar. A pessoa informa
# no ponto (onde ela já está autenticada com CPF e PIN) e o DP decide aqui.
#
# ⚑ LIBERAR CHAMA `_liberar_acesso`, a MESMA função da ficha da pessoa. Não há
# segundo caminho de criar acesso — ver o comentário dela.
# ===========================================================================
@bp.route("/erp/api/portal/pedidos-de-acesso")
@login_obrigatorio
@permissao("liberar_acesso_portal")
def api_pedidos_de_acesso():
    from app.apps.erp.core.auth import pedidos_de_portal as fila
    try:
        with get_session() as s:
            conn = s.connection()
            status = (request.args.get("status") or "PENDENTE").upper()
            # ⚑ QUEM ADMINISTRA O ACESSO VAI DIRETO CONFIGURAR (18/11/2026).
            # Ele, fechando o ciclo: *"no ato da liberação já abrir a tela de
            # configurações do cadastro da pessoa (…) lá será designado o
            # perfil dela, obras de acesso e etc."*
            #
            # A resposta diz só SE esta pessoa administra operadores. A tela
            # usa isso para mandar quem libera direto para Configurações ›
            # Usuários, com o operador aberto. A lista de perfis NÃO vem aqui:
            # ela já existe lá, e mandá-la nesta resposta era uma consulta por
            # abertura da tela de Pessoal para desenhar uma lista que acabou
            # morando em outro lugar.
            #
            # Quem apenas resolve a fila (o DP) não administra: para ele o
            # acesso nasce preso à obra do cadastro e a tela não muda.
            from app.apps.erp.core.auth.permissoes import pode_com_banco
            atual = _usuario_logado(s)
            administra = bool(atual is not None
                              and pode_com_banco(s, atual, "gerir_usuarios"))
            return jsonify({"ok": True,
                            "pedidos": fila.listar(conn, status=status),
                            "pendentes": fila.quantos_pendentes(conn),
                            "administra_acesso": administra,
                            "disponivel": fila.tabela_existe(conn)})
    except Exception as e:
        logger.exception("ERP: falha ao listar os pedidos de acesso ao portal")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/portal/pedidos-de-acesso/<int:pedido_id>",
          methods=["POST"])
@login_obrigatorio
@permissao("liberar_acesso_portal")
def api_pedido_de_acesso_decidir(pedido_id: int):
    """LIBERAR ou RECUSAR um pedido.

    Liberar cria o acesso com o perfil escolhido (padrão
    ADMINISTRATIVO_OBRA — quem pede acesso ao portal pela obra é quem abre
    pedidos de lá), liga a obra em que a pessoa bate ponto e manda o convite
    de primeira senha. Tudo numa transação só: ou o acesso nasce E o pedido
    sai da fila, ou nada acontece.
    """
    from app.apps.erp.core.auth import pedidos_de_portal as fila
    from app.apps.erp.core.auth.permissoes import pode_com_banco
    from app.apps.erp.db.models.cadastros import Colaborador
    d = request.get_json(silent=True) or {}
    acao = (d.get("acao") or "").strip().upper()
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            conn = s.connection()
            pedidos = {p["id"]: p for p in fila.listar(conn, status="TODOS",
                                                       limite=1000)}
            p = pedidos.get(pedido_id)
            if p is None:
                raise ErroNaoEncontrado("Pedido não encontrado.")
            if p["status"] != fila.PENDENTE:
                return jsonify({"ok": False, "erro":
                                "Este pedido já foi decidido."}), 400

            if acao == "RECUSAR":
                fila.decidir(conn, pedido_id, status=fila.RECUSADO,
                             decidido_por=atual.id if atual else None,
                             motivo=d.get("motivo") or "")
                s.commit()
                return jsonify({"ok": True, "status": fila.RECUSADO})
            if acao != "LIBERAR":
                return jsonify({"ok": False, "erro":
                                "Diga o que fazer: LIBERAR ou RECUSAR."}), 400
            if not p["colaborador_id"]:
                return jsonify({"ok": False, "erro":
                                "Este pedido não está ligado a um cadastro de "
                                "colaborador."}), 400
            c = s.get(Colaborador, p["colaborador_id"])
            if c is None:
                raise ErroNaoEncontrado("O cadastro desta pessoa não existe mais.")
            # ⚠️ O PERFIL NÃO VEM DO NAVEGADOR quando quem decide não gere
            # operadores (18/11/2026, ao liberar a fila para o DP). Sem esta
            # trava, o DP mandaria "perfil: ADMIN" na mão e criaria um
            # administrador do sistema por uma fila de acesso de obra. Quem
            # tem `gerir_usuarios` continua podendo escolher — essa pessoa já
            # cria operador com qualquer perfil pela tela de operadores.
            administra = pode_com_banco(s, atual, "gerir_usuarios") if atual else False
            r = _liberar_acesso(
                s, c, autor=atual, email=d.get("email") or p["email"],
                # ⚑ O PADRÃO É ADMINISTRATIVO DE OBRA, e é escolha explícita:
                # é o perfil de quem abre pedido pela obra e NÃO vê salário
                # nem documento de ninguém (`ver_remuneracao`, `ver_arquivo`).
                # CONSULTA, o padrão da ficha, não deixaria ela pedir nada —
                # o acesso nasceria inútil e ela voltaria a pedir.
                perfil=(d.get("perfil") if administra else None)
                       or "ADMINISTRATIVO_OBRA",
                perfil_id=d.get("perfil_id") if administra else None,
                # As OBRAS também só vêm do navegador para quem gere
                # operadores: para o DP fica a obra do cadastro, de sempre.
                obra_ids=(d.get("obra_ids") if administra else None),
                todas_as_obras=bool(d.get("todas_as_obras")) if administra
                               else False,
                obra_id=p["obra_id"], convidar=True)
            if not r["ok"]:
                return jsonify(r), 400
            fila.decidir(conn, pedido_id, status=fila.LIBERADO,
                         decidido_por=atual.id if atual else None,
                         usuario_id=r["usuario_id"])
            s.commit()
            return jsonify({"ok": True, "status": fila.LIBERADO, **r})
    except ErroNaoEncontrado as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao decidir o pedido de acesso %s", pedido_id)
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ===========================================================================
# O PRIMEIRO ACESSO — convite por e-mail, a pessoa grava a própria senha
#
# Desenho dele (09/11/2026): *"eu vou cadastrar o e-mail da pessoa, configurar
# o perfil, e disparar um e-mail para ela gravar a senha dela."*
# ===========================================================================
def _link_do_convite() -> str:
    """O endereço público da tela de definir senha, com domínio e tudo.

    `_external=True` monta a partir do que o navegador pediu — então funciona
    igual no Render e na máquina de quem está testando, sem configuração nova.
    O `.rstrip` tira o token de exemplo: quem acrescenta o token é o convite.
    """
    return url_for("erp.pagina_primeiro_acesso", token="x",
                   _external=True)[: -len("x")]


@bp.route("/erp/api/usuarios/<int:usuario_id>/convite", methods=["POST"])
@login_obrigatorio
@permissao("gerir_usuarios")
def api_usuario_convite(usuario_id: int):
    """Manda (ou reenvia) o convite de primeiro acesso.

    Reenviar INVALIDA o anterior — dois links abertos é a situação em que a
    pessoa usa o velho, dá erro, e alguém jura que o sistema está quebrado.
    """
    from app.apps.erp.core.auth import convites as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            d = request.get_json(silent=True) or {}
            tipo = (d.get("tipo") or "PRIMEIRO_ACESSO").upper()
            r = svc.enviar(s, usuario_id, link_base=_link_do_convite(),
                           tipo=tipo, autor=atual)
            s.commit()
        return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except Exception as e:
        logger.exception("ERP: falha ao enviar o convite de acesso")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/usuarios/<int:usuario_id>/convite/situacao")
@login_obrigatorio
@permissao("gerir_usuarios")
def api_usuario_convite_situacao(usuario_id: int):
    """Há convite aberto para esta pessoa? Venceu? Ela já definiu a senha?

    É o que a ficha mostra para quem pergunta "mandei ou não mandei?" — e o
    que evita reenviar por via das dúvidas, invalidando um link que a pessoa
    tinha acabado de receber.
    """
    from app.apps.erp.core.auth import convites as svc
    try:
        with get_session() as s:
            return jsonify({"ok": True, "convite": svc.situacao(s, usuario_id)})
    except Exception as e:
        logger.exception("ERP: falha ao ler a situação do convite")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ===========================================================================
# PRIMEIRO ACESSO PELO PRÓPRIO PORTAL (18/11/2026)
#
# Ele, ao tentar orientar a equipe e esbarrar no absurdo: *"então tá errado
# esse acesso ser liberado pelo aplicativo de ponto. Qual o sentido que tem de
# uma pessoa que vai lançar um diário ter que ir lá no aplicativo do ponto?
# (…) O cara tem que ir pro portal e criar o acesso dele por lá."*
#
# ⚑ AS DUAS ROTAS SÃO PÚBLICAS, e têm de ser: quem não tem acesso não pode
# entrar para pedir acesso. O que as segura é o teto por IP e o código no
# telefone DO CADASTRO — não a sessão.
#
# ⚑ E NENHUMA DELAS CRIA ACESSO. A segunda só põe o pedido na fila de sempre,
# com a identidade marcada como confirmada. Quem cria é quem aprova, pelo
# botão que já existe — decisão dele no mesmo dia: *"ainda assim precisa ser
# validado por alguém no sistema. (…) aí aumenta o nível de segurança."*
# ===========================================================================
def _dentro_do_teto(tipo: str, por_hora: int) -> bool:
    """Teto por IP para as rotas públicas do primeiro acesso.

    ⚑ REUSA O CONTADOR DO PONTO, que já faz exatamente isto e está em
    produção. Escrever um segundo contador aqui seria duas regras para a mesma
    coisa, e a segunda envelheceria sozinha.

    ⚠️ O TETO POR IP É FOLGADO DE PROPÓSITO: atrás de um proxy, a obra inteira
    sai pelo mesmo endereço, e um teto apertado trancaria todo mundo porque um
    colega tentou. A trava fina é por PESSOA, no banco (três códigos por hora)
    e nas cinco tentativas de cada código.
    """
    try:
        from app.apps.ponto import auth as auth_ponto
        return auth_ponto.dentro_do_limite(
            tipo, request.remote_addr or "sem-ip", por_hora)
    except Exception:                                       # noqa: BLE001
        # O contador é defesa extra, não a trava. Se ele falhar, o código no
        # telefone do cadastro continua sendo o que segura a porta.
        logger.warning("ERP: contador de tentativas indisponível")
        return True


@bp.route("/erp/primeiro-acesso")
@permissao_publica("tela de primeiro acesso — quem ainda não tem conta não "
                   "tem como entrar para pedir uma")
def pagina_primeiro_acesso_portal():
    return render_template("erp_primeiro_acesso_portal.html")


@bp.route("/erp/api/primeiro-acesso/codigo", methods=["POST"])
@permissao_publica("pedir o código por WhatsApp; a resposta é a mesma exista "
                   "o CPF ou não")
def api_primeiro_acesso_codigo():
    from app.apps.erp.core.auth import primeiro_acesso_portal as svc
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            if not _dentro_do_teto("codigo-portal", 12):
                return jsonify({"ok": False, "erro":
                                "Muitos pedidos deste lugar. Tente mais "
                                "tarde."}), 429
            r = svc.pedir_codigo(s, d.get("cpf"), d.get("telefone"),
                                 ip=request.remote_addr or "")
            s.commit()
            return jsonify(r), (200 if r.get("ok") else 400)
    except Exception as e:
        logger.exception("ERP: falha ao pedir o código de primeiro acesso")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/primeiro-acesso/concluir", methods=["POST"])
@permissao_publica("conferir o código e abrir o pedido de acesso na fila")
def api_primeiro_acesso_concluir():
    from app.apps.erp.core.auth import primeiro_acesso_portal as svc
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            if not _dentro_do_teto("concluir-portal", 20):
                return jsonify({"ok": False, "erro":
                                "Muitas tentativas deste lugar. Tente mais "
                                "tarde."}), 429
            r = svc.concluir(s, d.get("cpf"), d.get("codigo"), d.get("email"),
                             ip=request.remote_addr or "")
            s.commit()
            return jsonify(r)
    except svc.CodigoRecusado as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao concluir o primeiro acesso")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/esqueci-a-senha", methods=["GET", "POST"])
@permissao_publica("pedir o link de nova senha — quem esqueceu a senha não "
                   "tem como entrar para pedir")
def pagina_esqueci_a_senha():
    """A pessoa pede o link e redefine sozinha.

    Ele, em 10/11/2026: *"esqueci minha senha, é interessante, né? É
    necessário. Não deixar a autonomia."*

    ⚑ A RESPOSTA É SEMPRE A MESMA, exista a conta ou não. Dizer "não há conta
    com esse e-mail" transformaria esta tela numa máquina de descobrir quem
    trabalha na empresa — basta ir testando endereços. O que de fato
    aconteceu fica no log, não na tela.
    """
    from app.apps.erp.core.auth import convites as svc
    if request.method == "GET":
        return render_template("erp_esqueci_senha.html", pronto=False)
    email = (request.form.get("email") or "").strip()
    try:
        with get_session() as s:
            svc.pedir_nova_senha(s, email, link_base=_link_do_convite())
            s.commit()
    except Exception:                                   # noqa: BLE001
        # Nem a falha muda a resposta: um erro aqui que aparecesse na tela
        # também contaria se a conta existe.
        logger.exception("ERP: falha ao atender pedido de nova senha")
    return render_template("erp_esqueci_senha.html", pronto=True)


@bp.route("/erp/primeiro-acesso/<token>", methods=["GET", "POST"])
@permissao_publica("definir a própria senha pelo convite — quem abre ainda "
                   "não tem senha, então não há como exigir login")
def pagina_primeiro_acesso(token: str):
    """A pessoa escolhe a senha dela. **Ninguém mais a conhece.**

    ⚑ PÚBLICA DE PROPÓSITO, e é o único jeito: quem abre este link ainda não
    tem senha para entrar. O que protege é o TOKEN — 32 bytes sorteados,
    guardado só como resumo, de uso único e com prazo (`core/auth/convites`).

    O erro de link inválido é o mesmo para link inexistente, usado e vencido:
    dizer qual dos três ajudaria quem está tentando links no escuro.
    """
    from app.apps.erp.core.auth import convites as svc
    if request.method == "GET":
        try:
            with get_session() as s:
                dados = svc.conferir(s, token)
            return render_template("erp_primeiro_acesso.html", erro=None,
                                   token=token, pessoa=dados, pronto=False)
        except ErroValidacao as e:
            return render_template("erp_primeiro_acesso.html", erro=str(e),
                                   token=token, pessoa=None, pronto=False)
    senha = request.form.get("senha") or ""
    repetida = request.form.get("repetida") or ""
    try:
        if senha != repetida:
            raise ErroValidacao("As duas senhas digitadas não são iguais.")
        with get_session() as s:
            dados = svc.usar(s, token, senha)
            s.commit()
        logger.info("ERP: senha definida pelo convite para %s", dados["email"])
        return render_template("erp_primeiro_acesso.html", erro=None,
                               token=token, pessoa=dados, pronto=True)
    except ErroValidacao as e:
        pessoa = None
        try:
            with get_session() as s:
                pessoa = svc.conferir(s, token)
        except ErroValidacao:
            pessoa = None
        return render_template("erp_primeiro_acesso.html", erro=str(e),
                               token=token, pessoa=pessoa, pronto=False)


# ===========================================================================
# A CONTA DE E-MAIL DO SISTEMA — separada da do DP e da de compras
# ===========================================================================
@bp.route("/erp/api/diarias", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_pessoal", POST="configurar")
def api_diarias():
    """Os acréscimos da diária — feriado, sábado, domingo e quem não recebe.

    Ele, em 10/11/2026: *"esses números ainda valem atualmente, mas isso é
    algo que deva ficar dentro de uma configuração, o acréscimo da diária."*

    ⚑ LER é de quem vê o Pessoal; MUDAR é de quem configura. A diferença
    importa: quem confere uma folha precisa saber com que acréscimo a conta
    foi feita, e isso não é o mesmo que poder mudá-lo.
    """
    from app.apps.erp.core import diarias as svc
    try:
        with get_session() as s:
            if request.method == "GET":
                return jsonify({"ok": True, "config": svc.configuracao(s),
                                "padrao": {"feriado": 20.0, "sabado": 10.0,
                                           "domingo": 20.0,
                                           "sem_diaria": list(
                                               svc.FUNCOES_SEM_DIARIA_PADRAO)}})
            r = svc.configurar(s, request.get_json(silent=True) or {},
                               _usuario_logado(s))
            s.commit()
            return jsonify({"ok": True, "config": r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha nos acréscimos da diária")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ===========================================================================
# A CHAVE DE SEGREDOS — ver, diagnosticar e gerar uma nova
#
# Nasceu de um relato dele em 17/11/2026: *"fui gravar o email no cadastro da
# empresa e apareceu: A chave em ERP_CHAVE_SEGREDOS não tem o formato
# esperado."*
#
# Duas coisas estavam erradas, e nenhuma era a chave:
#
#  1. A TELA NÃO AVISAVA ANTES. Ela só perguntava "a variável existe?". Com uma
#     chave presente mas torta, ele preenchia a conta inteira, digitava a senha
#     e só então levava o recado.
#  2. O RECADO MANDAVA RODAR UM COMANDO DE PROGRAMADOR
#     (`Fernet.generate_key()`). Quem opera este sistema não roda comando
#     nenhum — então a mensagem, na prática, dizia "chame alguém".
#
# ⚠️ O ERP GERA A CHAVE, MAS NÃO A GUARDA. Ela tem de ser copiada à mão para a
# Environment do Render. Gravá-la no banco desfaria a razão de ela existir:
# quem tivesse uma cópia do banco teria as senhas junto.
# ===========================================================================
@bp.route("/erp/api/chave-de-segredos")
@login_obrigatorio
@permissao("configurar")
def api_chave_de_segredos():
    """Em que pé está a chave — e o que a troca dela custaria.

    ⚠️ NUNCA devolve o valor da chave. Só se ela serve, o que há de errado, e
    quantos segredos já foram guardados com ela (porque trocar a chave
    invalida todos eles, e quem decide trocar precisa saber disso ANTES).
    """
    from sqlalchemy import text as sa_text

    from app.apps.erp.core.comum import segredos
    try:
        estado = dict(segredos.situacao())
        with get_session() as s:
            guardados = 0
            for tabela, coluna in (("empresas", "smtp_senha_cifrada"),
                                   ("empresas", "emissao_token_cifrado"),
                                   ("parametros", None)):
                try:
                    with s.begin_nested():
                        if coluna is None:
                            continue
                        n = s.execute(sa_text(
                            f"SELECT count(*) FROM {tabela} "
                            f" WHERE {coluna} IS NOT NULL")).scalar() or 0
                        guardados += int(n)
                except Exception:        # coluna que ainda não existe no banco
                    pass
        estado["segredos_guardados"] = guardados
        return jsonify({"ok": True, **estado})
    except Exception as e:
        logger.exception("ERP: falha ao olhar a chave de segredos")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/chave-de-segredos/gerar", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_chave_de_segredos_gerar():
    """Sorteia uma chave válida para quem configura copiar para o Render.

    ⚑ NÃO GRAVA NADA, nem no banco nem em arquivo, e NÃO ENTRA NO LOG. A
    resposta existe uma vez, na tela de quem pediu, e acabou.
    """
    from app.apps.erp.core.comum import segredos
    try:
        logger.info("ERP: chave de segredos gerada para configuração "
                    "(o valor não é registrado)")
        return jsonify({"ok": True, "chave": segredos.gerar_chave(),
                        "variavel": segredos.VARIAVEL})
    except Exception as e:
        logger.exception("ERP: falha ao gerar a chave de segredos")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/email-sistema", methods=["GET", "POST"])
@login_obrigatorio
@permissao("configurar")
def api_email_sistema():
    """A conta por onde saem as mensagens DO SISTEMA. A senha nunca sai."""
    from app.apps.erp.core.comum import email_sistema as svc
    try:
        with get_session() as s:
            if request.method == "GET":
                return jsonify({"ok": True, "config": svc.configuracao(s),
                                "falta": svc.o_que_falta(s)})
            r = svc.configurar(s, request.get_json(silent=True) or {},
                               _usuario_logado(s))
            s.commit()
            return jsonify({"ok": True, "config": r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha na conta de e-mail do sistema")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/email-sistema/testar", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_email_sistema_testar():
    """Manda uma mensagem de teste — o que transforma "acho que configurei"
    em "está configurado"."""
    from app.apps.erp.core.comum import email_sistema as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            para = ((request.get_json(silent=True) or {}).get("para")
                    or (atual.email if atual else "")).strip()
            if not para:
                return jsonify({"ok": False,
                                "erro": "Para qual endereço mando o teste?"}), 400
            registro = svc.testar(s, para, atual)
            s.commit()
            return jsonify({"ok": registro.situacao == "ENVIADO",
                            "erro": registro.erro, "para": para})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha no teste do e-mail do sistema")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ===========================================================================
# O E-MAIL DO DP (18/11/2026)
#
# Ele: *"O que eu vou encaminhar a partir do DP é uma coisa, é um e-mail. O que
# eu vou encaminhar a partir do suprimento é outro e-mail. (…) o DP, por
# enquanto, eu quero um e-mail geral. (…) Eu tenho que configurar isso no
# próprio DP."*
#
# ⚑ ROTA PRÓPRIA, E NÃO UM `area` NO CORPO DA ROTA DO SISTEMA, e a razão é
# regra do CLAUDE.md: a ação declarada tem de decidir sozinha quem entra. A
# conta do SISTEMA é do administrador (`configurar`); a do DP é de quem opera o
# DP (`pedir_contabilidade`). Uma rota só, com duas alçadas por dentro, faria a
# declaração mentir — e a homologação com banco de verdade acusa isso.
# ===========================================================================
@bp.route("/erp/api/dp/email", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_pessoal", POST="pedir_contabilidade")
def api_email_do_dp():
    """A conta por onde o DP manda (e por onde a resposta volta). Senha nunca sai."""
    from app.apps.erp.core.comum import email_sistema as svc
    try:
        with get_session() as s:
            if request.method == "GET":
                return jsonify({"ok": True, "config": svc.configuracao(s, "DP"),
                                "falta": svc.o_que_falta(s, "DP")})
            r = svc.configurar(s, request.get_json(silent=True) or {},
                               _usuario_logado(s), area="DP")
            s.commit()
            return jsonify({"ok": True, "config": r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha na conta de e-mail do DP")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/dp/email/testar", methods=["POST"])
@login_obrigatorio
@permissao("pedir_contabilidade")
def api_email_do_dp_testar():
    from app.apps.erp.core.comum import email_sistema as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            para = ((request.get_json(silent=True) or {}).get("para")
                    or (atual.email if atual else "")).strip()
            if not para:
                return jsonify({"ok": False,
                                "erro": "Para qual endereço mando o teste?"}), 400
            registro = svc.testar(s, para, atual, area="DP")
            s.commit()
            return jsonify({"ok": registro.situacao == "ENVIADO",
                            "erro": registro.erro, "para": para})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha no teste do e-mail do DP")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/usuarios/<int:usuario_id>/permissoes", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="gerir_usuarios", POST="gerir_usuarios")
def api_permissoes_do_usuario(usuario_id: int):
    """As marcações de permissão de UMA pessoa, sobre o que o PERFIL dela já dá.

    GET devolve, para cada ação: o que a base dá, o que está marcado à mão e o
    resultado. POST grava as marcações — só as que diferem da base viram linha,
    para o cadastro não guardar repetição do que a base já responde.

    ⚠️ A BASE é o perfil cadastrado (migração 065) quando a pessoa tem um, e o
    cargo antigo quando não tem. Comparar com o cargo depois que o perfil
    passou a mandar criaria exceção onde não há diferença nenhuma — e, pior,
    salvar a tela tiraria em silêncio uma ação que o perfil concede.
    """
    from sqlalchemy import select
    from app.apps.erp.core.auth.permissoes import exigir
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.db.models.cadastros import Usuario, UsuarioPermissao
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            exigir(atual, "gerir_usuarios")
            u = s.get(Usuario, usuario_id)
            if u is None:
                return jsonify({"ok": False, "erro": "Operador não encontrado."}), 404

            marcadas = {r.acao: r.concedida for r in s.scalars(
                select(UsuarioPermissao).where(
                    UsuarioPermissao.usuario_id == usuario_id)).all()
                if r.usuario_id == usuario_id}

            base = acoes_do_perfil_no_banco(s, usuario_id)

            def _da_base(acao: str) -> bool:
                return acao in base if base is not None else u.perfil in PERMISSOES[acao]

            if request.method == "GET":
                acoes = []
                for acao in sorted(PERMISSOES):
                    do_cargo = _da_base(acao)
                    marcada = marcadas.get(acao)
                    acoes.append({
                        "acao": acao,
                        "rotulo": ACAO_ROTULOS.get(acao, acao),
                        "do_cargo": do_cargo,
                        "marcada": marcada,
                        "efetiva": decidir(u.perfil, acao, marcadas, base),
                        "travada": u.perfil is PerfilUsuario.ADMIN and acao in PROTEGIDAS_DO_ADMIN,
                    })
                return jsonify({"ok": True, "usuario": {"id": u.id, "nome": u.nome,
                                                        "perfil": u.perfil.value,
                                                        "perfil_rotulo": ROTULOS.get(u.perfil, u.perfil.value),
                                                        "tem_perfil_cadastrado": base is not None},
                                "acoes": acoes})

            pedido = (request.get_json(silent=True) or {}).get("permissoes") or {}
            desconhecidas = [a for a in pedido if a not in PERMISSOES]
            if desconhecidas:
                return jsonify({"ok": False,
                                "erro": f"Ação desconhecida: {', '.join(sorted(desconhecidas))}."}), 400

            for linha in s.scalars(select(UsuarioPermissao).where(
                    UsuarioPermissao.usuario_id == usuario_id)).all():
                if linha.usuario_id == usuario_id:
                    s.delete(linha)
            s.flush()

            gravadas = {}
            for acao, valor in pedido.items():
                quer = bool(valor)
                if quer == _da_base(acao):
                    continue          # igual à base: não vira exceção
                s.add(UsuarioPermissao(usuario_id=usuario_id, acao=acao,
                                       concedida=quer, definida_por=atual.id))
                gravadas[acao] = quer

            registrar_evento(s, "usuario", usuario_id, "PERMISSOES_AJUSTADAS",
                             {"permissoes": gravadas}, atual.id)
            s.commit()
            logger.info("ERP: permissões de %s ajustadas por %s: %s",
                        usuario_id, atual.id, gravadas)
        return jsonify({"ok": True, "permissoes": gravadas})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha ao ajustar permissões do operador")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/projetos", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_erp", POST="configurar")
def api_projetos():
    """Os PROJETOS — o conjunto de obras que se olha junto (migração 066).

    GET é aberto a quem entra no ERP porque o projeto é filtro de tela: sem a
    lista, nem a caixa de seleção do relatório monta. Criar e editar exige
    configurar, como todo cadastro estrutural.
    """
    from app.apps.erp.core.cadastros import projetos as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if request.method == "GET":
                return jsonify({"ok": True, "projetos": svc.listar(
                    s, incluir_arquivados=request.args.get("todos") == "1")})
            d = request.get_json(silent=True) or {}
            projeto = svc.criar(s, d, atual)
            s.commit()
            logger.info("ERP: projeto %s criado por %s", projeto["codigo"], atual.id)
        return jsonify({"ok": True, "projeto": projeto})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha no cadastro de projetos")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/projetos/<int:projeto_id>", methods=["GET", "POST"])
@login_obrigatorio
@permissao("configurar")
def api_projeto(projeto_id: int):
    """Um projeto: ler, editar, ou arquivar (`{"arquivar": true}`).

    ⚠️ Ler UM projeto exige `configurar`, e não `ver_erp` como a listagem: o
    detalhe traz o NOME das obras que estão dentro dele, e quem é preso a uma
    obra não tem por que saber o nome das outras. A listagem devolve só código,
    nome e quantidade — o que a tela precisa para montar o filtro.
    """
    from app.apps.erp.core.cadastros import projetos as svc
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if request.method == "GET":
                return jsonify({"ok": True, "projeto": svc.obter(s, projeto_id)})
            d = request.get_json(silent=True) or {}
            if d.get("arquivar"):
                projeto = svc.arquivar(s, projeto_id, atual)
            else:
                projeto = svc.editar(s, projeto_id, d, atual)
            s.commit()
        return jsonify({"ok": True, "projeto": projeto})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha ao editar projeto")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/perfis", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="gerir_usuarios", POST="gerir_usuarios")
def api_perfis():
    """O CADASTRO de perfis de acesso (migração 065).

    GET devolve os perfis e o catálogo de seções — as telas do sistema
    agrupadas por área, com o que "só olhar" e "olhar e mexer" liberam em cada
    uma. POST cria um perfil novo, que nasce sem abrir porta nenhuma.
    """
    from app.apps.erp.core.auth import perfis as svc_perfis
    from app.apps.erp.core.auth import secoes as cat
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if request.method == "GET":
                return jsonify({
                    "ok": True,
                    "perfis": svc_perfis.listar(
                        s, incluir_arquivados=request.args.get("todos") == "1"),
                    "catalogo": cat.para_a_tela(),
                    "niveis": [{"chave": n, "rotulo": cat.NIVEL_ROTULOS[n]}
                               for n in cat.NIVEIS],
                })
            d = request.get_json(silent=True) or {}
            perfil = svc_perfis.criar(s, d, atual)
            s.commit()
            logger.info("ERP: perfil %s criado por %s", perfil["nome"], atual.id)
        return jsonify({"ok": True, "perfil": perfil})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha no cadastro de perfis")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/perfis/<int:perfil_id>", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="gerir_usuarios", POST="gerir_usuarios")
def api_perfil(perfil_id: int):
    """Um perfil: ler, editar as seções, ou arquivar (`{"arquivar": true}`)."""
    from app.apps.erp.core.auth import perfis as svc_perfis
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            if request.method == "GET":
                return jsonify({"ok": True, "perfil": svc_perfis.obter(s, perfil_id),
                                "acoes": sorted(svc_perfis.acoes_do_perfil(s, perfil_id))})
            d = request.get_json(silent=True) or {}
            if d.get("arquivar"):
                perfil = svc_perfis.arquivar(s, perfil_id, atual)
            else:
                perfil = svc_perfis.editar(s, perfil_id, d, atual)
            s.commit()
            logger.info("ERP: perfil %s alterado por %s", perfil_id, atual.id)
        return jsonify({"ok": True, "perfil": perfil})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha ao editar perfil")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/permissoes")
@login_obrigatorio
@permissao("ver_erp")
def api_permissoes():
    from app.apps.erp.core.auth.permissoes import contexto_permissoes
    with get_session() as s:
        return jsonify({"ok": True, "contexto": contexto_permissoes(s, _usuario_logado(s))})


@bp.route("/erp/api/titulos/<int:titulo_id>/historico")
@login_obrigatorio
@permissao("ver_erp")
def api_historico(titulo_id: int):
    """Trilha completa do título: tudo que mudou, quando e por quem — mais os
    avisos enviados ao solicitante."""
    from sqlalchemy import select
    from app.apps.erp.core.notificacoes import historico as historico_avisos
    from app.apps.erp.db.models.financeiro import Evento
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_titulo_no_escopo
            exigir_titulo_no_escopo(s, _usuario_logado(s), titulo_id)
            eventos = s.execute(select(Evento, Usuario.nome)
                                .join(Usuario, Usuario.id == Evento.usuario_id, isouter=True)
                                .where(Evento.entidade_tipo == "titulo",
                                       Evento.entidade_id == titulo_id)
                                .order_by(Evento.criado_em.desc())).all()
            linhas = [{
                "quando": e.criado_em.strftime("%d/%m/%Y %H:%M:%S"),
                "acao": e.acao, "por": nome or "sistema",
                "detalhe": e.detalhe,
            } for e, nome in eventos]
            return jsonify({"ok": True, "eventos": linhas,
                            "avisos": historico_avisos(s, titulo_id)})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao montar histórico")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/pagamentos/<int:pagamento_id>/avisar", methods=["POST"])
@login_obrigatorio
@permissao("pagar")
def api_reenviar_aviso(pagamento_id: int):
    """Reenvio manual do aviso (quando a pessoa apagou a mensagem, por ex.)."""
    from app.apps.erp.core.notificacoes import avisar_baixa
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            rel = avisar_baixa(s, pagamento_id, forcar=bool(d.get("forcar")))
            s.commit()
        return jsonify({"ok": True, "aviso": rel})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao reenviar aviso")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# Aval (dupla confirmação)
# ---------------------------------------------------------------------------
@bp.route("/erp/api/avais/pendentes")
@login_obrigatorio
@permissao("avalizar")
def api_avais_pendentes():
    from app.apps.erp.core.titulos.aval import pendentes
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            return jsonify({"ok": True, "titulos": pendentes(s, usuario),
                            "perfil": usuario.perfil.value, "quem": usuario.nome})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao listar avais pendentes")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/avais/<int:titulo_id>", methods=["POST"])
@login_obrigatorio
@permissao("avalizar")
def api_avalizar(titulo_id: int):
    """Assinatura da segunda pessoa: confirma ou recusa o título."""
    from app.apps.erp.core.titulos.aval import registrar
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            rel = registrar(s, titulo_id, usuario,
                            decisao=(d.get("decisao") or "CONFIRMADO"),
                            motivo=d.get("motivo", ""),
                            ip=(request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
                                or request.remote_addr or ""),
                            dispositivo=request.headers.get("User-Agent", ""))
            s.commit()
        return jsonify({"ok": True, "aval": rel})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao registrar aval")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/historico/<entidade>/<int:entidade_id>")
@login_obrigatorio
@permissao("ver_erp")
def api_historico_geral(entidade: str, entidade_id: int):
    """Histórico de QUALQUER cadastro: obra, fornecedor, categoria, título,
    movimentação, lote — tudo que mudou, quando e por quem."""
    from sqlalchemy import select
    from app.apps.erp.db.models.financeiro import Evento
    permitidas = ("titulo", "obra", "fornecedor", "categoria", "movimentacao",
                  "lote", "conciliacao", "categoria_depara", "pagamento", "extrato")
    if entidade not in permitidas:
        return jsonify({"ok": False, "erro": f"Entidade inválida: {entidade}"}), 400
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_entidade_no_escopo
            exigir_entidade_no_escopo(s, _usuario_logado(s), entidade, entidade_id)
            linhas = s.execute(
                select(Evento, Usuario.nome)
                .join(Usuario, Usuario.id == Evento.usuario_id, isouter=True)
                .where(Evento.entidade_tipo == entidade, Evento.entidade_id == entidade_id)
                .order_by(Evento.criado_em.desc()).limit(300)).all()
            return jsonify({"ok": True, "eventos": [{
                "quando": e.criado_em.strftime("%d/%m/%Y %H:%M:%S"),
                "acao": e.acao, "por": nome or "sistema", "detalhe": e.detalhe,
            } for e, nome in linhas]})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha no histórico de %s", entidade)
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/auditoria")
@login_obrigatorio
@permissao("configurar")
def api_auditoria():
    """Consulta ampla da trilha, para auditoria: por período, usuário, ação."""
    from sqlalchemy import select
    from app.apps.erp.core.auth.permissoes import exigir
    from app.apps.erp.db.models.financeiro import Evento
    try:
        with get_session() as s:
            exigir(_usuario_logado(s), "ver_relatorios")
            stmt = (select(Evento, Usuario.nome)
                    .join(Usuario, Usuario.id == Evento.usuario_id, isouter=True)
                    .order_by(Evento.criado_em.desc()).limit(500))
            if request.args.get("entidade"):
                stmt = stmt.where(Evento.entidade_tipo == request.args["entidade"])
            if request.args.get("acao"):
                stmt = stmt.where(Evento.acao.ilike(f"%{request.args['acao']}%"))
            if request.args.get("usuario_id", type=int):
                stmt = stmt.where(Evento.usuario_id == request.args.get("usuario_id", type=int))
            if request.args.get("de"):
                stmt = stmt.where(Evento.criado_em >= f"{request.args['de']} 00:00:00")
            if request.args.get("ate"):
                stmt = stmt.where(Evento.criado_em <= f"{request.args['ate']} 23:59:59")
            linhas = s.execute(stmt).all()
            return jsonify({"ok": True, "eventos": [{
                "quando": e.criado_em.strftime("%d/%m/%Y %H:%M:%S"),
                "entidade": e.entidade_tipo, "entidade_id": e.entidade_id,
                "acao": e.acao, "por": nome or "sistema", "detalhe": e.detalhe,
            } for e, nome in linhas]})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha na auditoria")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/interessados/<int:titulo_id>", methods=["GET", "POST", "DELETE"])
@login_obrigatorio
@permissao(GET="ver_erp", POST="lancar", DELETE="lancar")
def api_interessados(titulo_id: int):
    """Quem mais acompanha o título e recebe os avisos."""
    from sqlalchemy import select
    from app.apps.erp.core.auth.permissoes import exigir_titulo_no_escopo
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.db.models.financeiro import TituloInteressado
    try:
        with get_session() as s:
            atual = _usuario_logado(s)
            # Sem esta trava, dava para se incluir como interessado num título
            # fora do escopo e passar a receber o aviso de pagamento COM o
            # comprovante — contornando por fora todo o resto do controle.
            exigir_titulo_no_escopo(s, atual, titulo_id)
            if request.method == "GET":
                from app.apps.erp.core.notificacoes import destinatarios
                from app.apps.erp.db.models.financeiro import Titulo as _T
                t = s.get(_T, titulo_id)
                if t is None:
                    return jsonify({"ok": False, "erro": "Título não encontrado."}), 404
                extras = s.scalars(select(TituloInteressado).where(
                    TituloInteressado.titulo_id == titulo_id)).all()
                pessoas = destinatarios(s, t)
                return jsonify({"ok": True,
                    "interessados": [{"usuario_id": i.usuario_id,
                                      "nome": (s.get(Usuario, i.usuario_id).nome
                                               if s.get(Usuario, i.usuario_id) else "—"),
                                      "motivo": i.motivo} for i in extras],
                    "recebem_aviso": [{"id": u.id, "nome": u.nome,
                                       "tem_contato": bool(u.telefone or u.cpf)}
                                      for u in pessoas]})
            d = request.get_json(silent=True) or {}
            if request.method == "DELETE":
                alvo = s.scalars(select(TituloInteressado).where(
                    TituloInteressado.titulo_id == titulo_id,
                    TituloInteressado.usuario_id == int(d.get("usuario_id")))).first()
                if alvo is not None:
                    s.delete(alvo)
                    registrar_evento(s, "titulo", titulo_id, "INTERESSADO_REMOVIDO",
                                     {"usuario_id": d.get("usuario_id")}, atual.id)
                    s.commit()
                return jsonify({"ok": True})
            adicionados = []
            for uid in (d.get("usuarios") or []):
                pessoa = s.get(Usuario, int(uid))
                if pessoa is None or not pessoa.ativo:
                    continue
                ja = s.scalars(select(TituloInteressado).where(
                    TituloInteressado.titulo_id == titulo_id,
                    TituloInteressado.usuario_id == pessoa.id)).first()
                if ja is not None:
                    continue
                s.add(TituloInteressado(titulo_id=titulo_id, usuario_id=pessoa.id,
                                        motivo=(d.get("motivo") or "").strip() or None,
                                        adicionado_por=atual.id))
                adicionados.append(pessoa.nome)
            if adicionados:
                registrar_evento(s, "titulo", titulo_id, "INTERESSADOS_ADICIONADOS",
                                 {"pessoas": adicionados}, atual.id)
            s.commit()
            return jsonify({"ok": True, "adicionados": adicionados})
    except ErroNaoEncontrado:
        raise            # vira 404 no errorhandler, nao 500
    except Exception as e:
        logger.exception("ERP: falha nos interessados")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/obras/<int:obra_id>/interessados", methods=["GET", "POST"])
@login_obrigatorio
@permissao("configurar")
def api_obra_interessados(obra_id: int):
    """Interessados fixos da obra: entram em todo título dela."""
    from sqlalchemy import select
    from app.apps.erp.core.auth.permissoes import exigir
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.db.models.financeiro import ObraInteressado
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
            exigir_obra_no_escopo(s, _usuario_logado(s), obra_id)
            atual = _usuario_logado(s)
            if request.method == "GET":
                linhas = s.scalars(select(ObraInteressado).where(
                    ObraInteressado.obra_id == obra_id)).all()
                return jsonify({"ok": True, "interessados": [
                    {"usuario_id": i.usuario_id,
                     "nome": (s.get(Usuario, i.usuario_id).nome
                              if s.get(Usuario, i.usuario_id) else "—")} for i in linhas]})
            exigir(atual, "configurar")
            d = request.get_json(silent=True) or {}
            for i in s.scalars(select(ObraInteressado).where(
                    ObraInteressado.obra_id == obra_id)).all():
                s.delete(i)
            s.flush()
            nomes = []
            for uid in (d.get("usuarios") or []):
                pessoa = s.get(Usuario, int(uid))
                if pessoa is not None and pessoa.ativo:
                    s.add(ObraInteressado(obra_id=obra_id, usuario_id=pessoa.id))
                    nomes.append(pessoa.nome)
            registrar_evento(s, "obra", obra_id, "INTERESSADOS_DEFINIDOS",
                             {"pessoas": nomes}, atual.id)
            s.commit()
            return jsonify({"ok": True, "interessados": nomes})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha nos interessados da obra")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/operadores/contato")
@login_obrigatorio
@permissao("lancar")
def api_operadores_contato():
    """Lista enxuta para escolher interessados, dizendo quem consegue receber."""
    from sqlalchemy import select
    try:
        with get_session() as s:
            usuarios = s.scalars(select(Usuario).where(Usuario.ativo.is_(True))
                                 .order_by(Usuario.nome)).all()
            return jsonify({"ok": True, "operadores": [
                {"id": u.id, "nome": u.nome, "perfil": u.perfil.value,
                 "tem_contato": bool(u.telefone or u.cpf)} for u in usuarios]})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# Gestão de obras
# ---------------------------------------------------------------------------
# A lista das fases mora em `core/cadastros/obras.py` — é vocabulário do
# negócio, e tela, agenda e assistente têm de dizer a mesma coisa.
from app.apps.erp.core.cadastros.obras import FASES_OBRA  # noqa: E402


@bp.route("/erp/api/obras")
@login_obrigatorio
@permissao("ver_erp")
def api_listar_obras():
    """Painel de obras: situação, contrato, medições e o que foi gasto."""
    from sqlalchemy import func, select
    from app.apps.erp.core.auth.permissoes import (
        obras_de_registro_sem_autor, obras_do_usuario,
    )
    from app.apps.erp.db.models.cadastros import Obra, ObraAditivo, UsuarioObra
    from app.apps.erp.db.models.financeiro import EspecieTitulo, Rateio, Titulo
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            permitidas = obras_do_usuario(s, usuario)
            stmt = select(Obra).order_by(Obra.codigo)
            if permitidas is not None:
                stmt = stmt.where(Obra.id.in_(permitidas or [0]))
            obras = s.scalars(stmt).all()

            # DUAS PERGUNTAS DIFERENTES, E ERA UMA SÓ ATÉ 11/09/2026.
            #
            # "Quais obras esta pessoa pode ESCOLHER?" e "de quais ela pode
            # ver o DINHEIRO?" não são a mesma coisa. Esta rota alimenta cinco
            # telas — Obras, Arquivo, Agenda, Contratos e Notas emitidas —, e
            # em quatro delas ela é a lista de onde se escolhe a obra. Fechar
            # a lista inteira deixaria o lançador sem conseguir arquivar um
            # documento numa obra.
            #
            # Mas obra é registro SEM AUTOR: quem enxerga "só o que eu lancei"
            # não tinha recorte nenhum aqui, e via valor de contrato, gasto,
            # recebido e margem de TODAS as obras da empresa. É a mesma brecha
            # das Locações, achada no mesmo dia.
            #
            # A separação que fica: a IDENTIFICAÇÃO da obra continua aberta a
            # quem já podia escolher entre elas; os NÚMEROS vêm em branco para
            # quem não alcança aquela obra. Em branco, e não zero — zero seria
            # o sistema afirmando que a obra não gastou nada.
            com_numeros = obras_de_registro_sem_autor(s, usuario)
            ids = [o.id for o in obras] or [0]

            aditivos: dict[int, float] = {}
            for obra_id, total in s.execute(
                    select(ObraAditivo.obra_id, func.sum(ObraAditivo.valor))
                    .where(ObraAditivo.obra_id.in_(ids))
                    .group_by(ObraAditivo.obra_id)):
                aditivos[obra_id] = float(total or 0)

            gastos: dict[int, float] = {}
            recebidos: dict[int, float] = {}
            for obra_id, especie, total in s.execute(
                    select(Rateio.obra_id, Titulo.especie, func.sum(Rateio.valor))
                    .join(Titulo, Titulo.id == Rateio.titulo_id)
                    .where(Rateio.obra_id.in_(ids),
                           Titulo.status.not_in(["CANCELADO", "ESTORNADO"]))
                    .group_by(Rateio.obra_id, Titulo.especie)):
                destino = recebidos if especie == EspecieTitulo.RECEBER else gastos
                destino[obra_id] = float(total or 0)

            # QUEM RESPONDE POR CADA OBRA. Pedido do dono em 17/09/2026:
            # *"é interessante dar destaque a se tem alguém responsável pela
            # obra ou não, porque enquanto não tem, ninguém pode fazer nada"*.
            # Uma consulta só para todas as obras da tela.
            com_responsavel = {
                oid for (oid,) in s.execute(
                    select(UsuarioObra.obra_id).where(
                        UsuarioObra.obra_id.in_(ids),
                        UsuarioObra.responsavel.is_(True)).distinct())}

            fases = dict(FASES_OBRA)
            hoje = date.today()
            linhas = []
            for o in obras:
                ve_numeros = com_numeros is None or o.id in com_numeros
                contrato = float(o.valor_contrato or 0)
                vigente = contrato + aditivos.get(o.id, 0.0)
                gasto = gastos.get(o.id, 0.0)
                linhas.append({
                    "id": o.id, "codigo": o.codigo, "nome": o.nome,
                    "objeto": (o.objeto or "")[:140], "cliente": o.cliente,
                    "municipio": o.municipio, "uf": o.uf, "contrato": o.contrato,
                    "fase": o.fase, "fase_rotulo": fases.get(o.fase, o.fase),
                    "status": o.status,
                    "numeros": ve_numeros,
                    "valor_contrato": contrato if ve_numeros else None,
                    "aditivos": aditivos.get(o.id, 0.0) if ve_numeros else None,
                    "valor_vigente": vigente if ve_numeros else None,
                    "gasto": gasto if ve_numeros else None,
                    "recebido": (recebidos.get(o.id, 0.0) if ve_numeros else None),
                    "margem": (round(recebidos.get(o.id, 0.0) - gasto, 2)
                               if ve_numeros else None),
                    "vigencia_fim": o.vigencia_fim.isoformat() if o.vigencia_fim else None,
                    "vence_em_dias": ((o.vigencia_fim - hoje).days
                                      if o.vigencia_fim else None),
                    "data_base": (o.data_base_orcamento.isoformat()
                                  if o.data_base_orcamento else None),
                    "reajuste_em_dias": ((o.data_base_orcamento.replace(
                        year=o.data_base_orcamento.year + 1) - hoje).days
                        if o.data_base_orcamento else None),
                    "seguro_vigencia_fim": (o.seguro_vigencia_fim.isoformat()
                                            if o.seguro_vigencia_fim else None),
                    "cno": o.cno, "art_rrt": o.art_rrt,
                    "tem_responsavel": o.id in com_responsavel,
                    # A EMPRESA DA OBRA volta na listagem (23/09/2026). O dono:
                    # *"atualizei o cadastro da obra com a empresa, mas a
                    # crítica de sem empresa não saiu"*. E não saía mesmo: o
                    # alerta "Sem empresa" do painel lê este campo da LISTA, e
                    # a lista nunca o mandava. Gravava certo, a ficha mostrava
                    # certo, e a tag vermelha ficava para sempre. Mesmo defeito
                    # de família do código da obra e da conta de pagamento:
                    # campo gravado e não devolvido.
                    "empresa_id": o.empresa_id,
                    "conta_bancaria_id": o.conta_bancaria_id,
                    # O projeto que agrupa a obra (migração 066): vira filtro
                    # na tela e coluna no relatório somado por projeto.
                    "projeto_id": getattr(o, "projeto_id", None),
                    "latitude": float(o.latitude) if o.latitude else None,
                    "longitude": float(o.longitude) if o.longitude else None,
                    "aliquota_iss": float(o.aliquota_iss_pct or 0) or None,
                })
            from app.apps.erp.core.cadastros import projetos as svc_projetos
            try:
                lista_projetos = svc_projetos.listar(s)
            except Exception:
                lista_projetos = []      # migração 066 ainda não aplicada
            return jsonify({"ok": True, "obras": linhas,
                            "projetos": lista_projetos,
                            "fases": [{"chave": k, "rotulo": v} for k, v in FASES_OBRA]})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao listar obras")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/obras/<int:obra_id>/fase", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_mudar_fase(obra_id: int):
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.db.models.cadastros import Obra, ObraFase
    d = request.get_json(silent=True) or {}
    fase = (d.get("fase") or "").upper()
    if fase not in dict(FASES_OBRA):
        return jsonify({"ok": False, "erro": f"Fase inválida: {fase}"}), 400
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
            exigir_obra_no_escopo(s, _usuario_logado(s), obra_id)
            usuario = _usuario_logado(s)
            obra = s.get(Obra, obra_id)
            if obra is None:
                return jsonify({"ok": False, "erro": "Obra não encontrada."}), 404
            anterior = obra.fase
            obra.fase = fase
            obra.fase_desde = date.today()
            if fase in ("CONCLUIDA", "CONCLUIDA_COM_DIVIDA") and not obra.data_conclusao:
                obra.data_conclusao = date.today()
            if fase == "RECEBIMENTO_PROVISORIO":
                obra.data_recebimento_provisorio = date.today()
            if fase == "RECEBIMENTO_DEFINITIVO":
                obra.data_recebimento_definitivo = date.today()
            s.add(ObraFase(obra_id=obra.id, fase=fase,
                           observacao=(d.get("observacao") or "").strip() or None,
                           usuario_id=usuario.id))
            registrar_evento(s, "obra", obra.id, "FASE_ALTERADA",
                             {"de": anterior, "para": fase,
                              "observacao": d.get("observacao")}, usuario.id)
            s.commit()
        return jsonify({"ok": True})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao mudar fase")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/obras/<int:obra_id>/fases")
@login_obrigatorio
@permissao("ver_erp")
def api_historico_fases(obra_id: int):
    from sqlalchemy import select
    from app.apps.erp.db.models.cadastros import ObraFase
    fases = dict(FASES_OBRA)
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
            exigir_obra_no_escopo(s, _usuario_logado(s), obra_id)
            linhas = s.execute(
                select(ObraFase, Usuario.nome)
                .join(Usuario, Usuario.id == ObraFase.usuario_id, isouter=True)
                .where(ObraFase.obra_id == obra_id)
                .order_by(ObraFase.criado_em.desc())).all()
            return jsonify({"ok": True, "fases": [{
                "fase": f.fase, "rotulo": fases.get(f.fase, f.fase),
                "observacao": f.observacao, "por": nome or "—",
                "quando": f.criado_em.strftime("%d/%m/%Y %H:%M")} for f, nome in linhas]})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/obras/<int:obra_id>/titulos")
@login_obrigatorio
@permissao("ver_erp")
def api_titulos_da_obra(obra_id: int):
    """Tudo que passou pela obra: o que se gastou e o que se recebeu."""
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload
    from app.apps.erp.db.models.financeiro import EspecieTitulo, Rateio, Titulo
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
            exigir_obra_no_escopo(s, _usuario_logado(s), obra_id)
            linhas = s.execute(
                select(Titulo, Rateio.valor)
                .join(Rateio, Rateio.titulo_id == Titulo.id)
                .where(Rateio.obra_id == obra_id,
                       Titulo.status.not_in(["CANCELADO", "ESTORNADO"]))
                .options(selectinload(Titulo.fornecedor), selectinload(Titulo.categoria),
                         selectinload(Titulo.parcelas))
                .order_by(Titulo.competencia.desc(), Titulo.id.desc()).limit(400)).all()
            saida = []
            for t, valor in linhas:
                venc = min((p.vencimento for p in t.parcelas), default=None)
                saida.append({
                    "id": t.id, "numero_sp": t.numero_sp,
                    "especie": (t.especie.value if hasattr(t.especie, "value")
                                else str(t.especie)),
                    "credor": t.fornecedor.razao_social, "descricao": t.descricao,
                    "categoria": f"{t.categoria.codigo} · {t.categoria.descricao}",
                    "grupo": t.categoria.grupo_nome or "",
                    "valor": float(valor), "competencia": t.competencia.strftime("%m/%Y"),
                    "vencimento": venc.isoformat() if venc else None,
                    "status": t.status.value,
                    "medicao": t.numero_medicao,
                })
            return jsonify({"ok": True, "titulos": saida})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao listar títulos da obra")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# Prestação de contas: fundo fixo e fatura de cartão
# ---------------------------------------------------------------------------
@bp.route("/erp/api/prestacao/comprovante", methods=["POST"])
@login_obrigatorio
@permissao("lancar")
def api_prestacao_comprovante():
    """Lê um comprovante e devolve a linha, guardando o arquivo no banco."""
    from app.apps.erp.core.documentos.armazenamento import salvar
    from app.apps.erp.core.titulos.prestacao import ler_bloco_de_comprovantes
    arquivo = request.files.get("arquivo")
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Envie o comprovante."}), 400
    try:
        conteudo = arquivo.read()
        nome = arquivo.filename or "comprovante"
        # PDF com várias páginas = vários comprovantes, um por página
        linhas = ler_bloco_de_comprovantes(conteudo, nome)
        with get_session() as s:
            usuario = _usuario_logado(s)
            for linha in linhas:
                pagina = linha.pop("_conteudo_pagina", None)
                nome_pagina = linha.pop("_nome_pagina", nome)
                anexo = salvar(s, pagina if pagina else conteudo, nome_pagina,
                               entidade_tipo="prestacao_rascunho", entidade_id=usuario.id,
                               categoria="COMPROVANTE", usuario=usuario)
                linha["anexo_id"] = anexo.id
            s.commit()
        return jsonify({"ok": True, "linhas": linhas,
                        "paginas": len(linhas),
                        "linha": linhas[0] if linhas else None})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao ler comprovante da prestação")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/prestacao/fatura", methods=["POST"])
@login_obrigatorio
@permissao("lancar")
def api_prestacao_fatura():
    """Lê a fatura do cartão e devolve todas as compras como linhas."""
    from app.apps.erp.core.documentos.armazenamento import salvar
    from app.apps.erp.core.titulos.prestacao import ler_fatura_cartao
    arquivo = request.files.get("arquivo")
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Envie o PDF da fatura."}), 400
    try:
        conteudo = arquivo.read()
        rel = ler_fatura_cartao(conteudo, arquivo.filename or "fatura.pdf")
        with get_session() as s:
            usuario = _usuario_logado(s)
            anexo = salvar(s, conteudo, arquivo.filename or "fatura.pdf",
                           entidade_tipo="prestacao_rascunho", entidade_id=usuario.id,
                           categoria="OUTRO", descricao="Fatura de cartão", usuario=usuario)
            rel["anexo_fatura_id"] = anexo.id
            s.commit()
        return jsonify({"ok": True, "fatura": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao ler fatura")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/prestacao/criticar", methods=["POST"])
@login_obrigatorio
@permissao("lancar")
def api_prestacao_criticar():
    from app.apps.erp.core.titulos.prestacao import criticar
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            from datetime import date as _d
            return jsonify({"ok": True, "critica": criticar(
                s, d.get("itens") or [], solicitante_id=usuario.id,
                modalidade=(d.get("modalidade") or "FUNDO_FIXO").upper(),
                total_declarado=d.get("total_declarado"),
                periodo_inicio=_d.fromisoformat(d["periodo_inicio"]) if d.get("periodo_inicio") else None,
                periodo_fim=_d.fromisoformat(d["periodo_fim"]) if d.get("periodo_fim") else None)})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha na crítica da prestação")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/prestacao", methods=["POST"])
@login_obrigatorio
@permissao("lancar")
def api_criar_prestacao():
    from app.apps.erp.core.documentos.armazenamento import Anexo as _A
    from app.apps.erp.core.titulos.prestacao import criar_prestacao
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            titulo = criar_prestacao(s, d, usuario)
            # vincula os comprovantes ao título criado
            for item in (d.get("itens") or []):
                if item.get("anexo_id"):
                    anexo = s.get(_A, int(item["anexo_id"]))
                    if anexo is not None and anexo.entidade_tipo == "prestacao_rascunho":
                        anexo.entidade_tipo = "titulo"
                        anexo.entidade_id = titulo.id
            if d.get("anexo_fatura_id"):
                anexo = s.get(_A, int(d["anexo_fatura_id"]))
                if anexo is not None:
                    anexo.entidade_tipo = "titulo"
                    anexo.entidade_id = titulo.id
            s.commit()
            return jsonify({"ok": True, "titulo": {
                "id": titulo.id, "numero_sp": titulo.numero_sp,
                "status": titulo.status.value, "total": float(titulo.valor_liquido)}})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao criar prestação")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/prestacao/<int:titulo_id>")
@login_obrigatorio
@permissao("ver_erp")
def api_prestacao_detalhe(titulo_id: int):
    from app.apps.erp.core.titulos.prestacao import detalhar
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_titulo_no_escopo
            exigir_titulo_no_escopo(s, _usuario_logado(s), titulo_id)
            return jsonify({"ok": True, "prestacao": detalhar(s, titulo_id)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404


@bp.route("/erp/api/prestacao/<int:titulo_id>/conferir", methods=["POST"])
@login_obrigatorio
@permissao("aprovar")
def api_conferir(titulo_id: int):
    from app.apps.erp.core.titulos.prestacao import confirmar_analise
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_titulo_no_escopo
            exigir_titulo_no_escopo(s, _usuario_logado(s), titulo_id)
            usuario = _usuario_logado(s)
            rel = confirmar_analise(s, titulo_id, usuario,
                                    item_id=d.get("item_id"),
                                    observacao=d.get("observacao", ""))
            s.commit()
        return jsonify({"ok": True, "resultado": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/prestacao/historico")
@login_obrigatorio
@permissao("ver_erp")
def api_prestacao_historico():
    from app.apps.erp.core.titulos.prestacao import historico_do_solicitante
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            alvo = request.args.get("usuario_id", type=int) or usuario.id
            return jsonify({"ok": True, "historico": historico_do_solicitante(
                s, alvo, (request.args.get("modalidade") or "FUNDO_FIXO").upper())})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/prestacoes/pendentes")
@login_obrigatorio
@permissao("aprovar")
def api_prestacoes_pendentes():
    """Prestações com apontamento aguardando análise — a fila do financeiro e
    do diretor, complementar à do aval."""
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload
    from app.apps.erp.core.auth.permissoes import aplicar_escopo
    from app.apps.erp.db.models.financeiro import StatusTitulo, Titulo, TituloItem
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            stmt = (select(Titulo).where(
                        Titulo.modalidade.in_(["FUNDO_FIXO", "CARTAO"]),
                        Titulo.status.not_in([StatusTitulo.PAGO, StatusTitulo.CANCELADO,
                                              StatusTitulo.ESTORNADO]))
                    .options(selectinload(Titulo.fornecedor))
                    .order_by(Titulo.id.desc()).limit(200))
            stmt = aplicar_escopo(stmt, s, usuario)
            saida = []
            for t in s.scalars(stmt).all():
                itens = s.scalars(select(TituloItem).where(
                    TituloItem.titulo_id == t.id)).all()
                bloqueios = criticas = pendentes_conf = 0
                for i in itens:
                    lista = i.criticas or []
                    bloqueios += sum(1 for x in lista if x.get("gravidade") == "BLOQUEIA")
                    criticas += sum(1 for x in lista if x.get("gravidade") == "CRITICA")
                    if [x for x in lista if x.get("gravidade") in ("BLOQUEIA", "CRITICA")] \
                            and not i.conferido_em:
                        pendentes_conf += 1
                solicitante = s.get(Usuario, t.solicitante_id)
                saida.append({
                    "id": t.id, "numero_sp": t.numero_sp, "modalidade": t.modalidade,
                    "tipo": t.fundo_fixo_tipo, "status": t.status.value,
                    "solicitante": solicitante.nome if solicitante else "—",
                    "total": float(t.valor_liquido), "itens": len(itens),
                    "bloqueios": bloqueios, "criticas": criticas,
                    "nao_conferidos": pendentes_conf,
                    "analises": len(t.alertas_confirmados or []),
                    "competencia": t.competencia.strftime("%m/%Y"),
                })
            return jsonify({"ok": True, "prestacoes": saida})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao listar prestações pendentes")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/movimentacoes/neutras")
@login_obrigatorio
@permissao("conciliar")
def api_neutras():
    """Pontas soltas: entrou e não foi devolvido, saiu e não foi ressarcido."""
    from app.apps.erp.core.titulos.receber import neutras_sem_par
    try:
        with get_session() as s:
            return jsonify({"ok": True, "pendentes": neutras_sem_par(s)})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao listar neutras")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/movimentacoes/vincular", methods=["POST"])
@login_obrigatorio
@permissao("conciliar")
def api_vincular_neutras():
    """Liga as duas pontas que se anulam."""
    from app.apps.erp.core.titulos.receber import vincular_par
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            rel = vincular_par(s, int(d["movimentacao_id"]), int(d["par_id"]),
                               usuario, motivo=d.get("motivo", ""))
            s.commit()
        return jsonify({"ok": True, "resultado": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao vincular par neutro")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/conciliacao/par-neutro", methods=["POST"])
@login_obrigatorio
@permissao("conciliar")
def api_par_neutro():
    """Marca duas linhas do extrato (a entrada e a saída) como par que se anula."""
    from app.apps.erp.core.titulos.receber import resolver_par_no_extrato
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            rel = resolver_par_no_extrato(
                s, int(d["extrato_entrada_id"]), int(d["extrato_saida_id"]),
                motivo=d.get("motivo", ""), contraparte=d.get("contraparte", ""),
                tipo=(d.get("tipo") or "RECEBIMENTO_INDEVIDO"), usuario=usuario)
            s.commit()
        return jsonify({"ok": True, "resultado": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao resolver par neutro")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/titulos/<int:titulo_id>/parcelas", methods=["POST"])
@login_obrigatorio
@permissao("reclassificar")
def api_editar_parcelas(titulo_id: int):
    """Ajusta vencimento e boleto das parcelas em aberto — sem desfazer nada."""
    from datetime import date as _date
    from app.apps.erp.core.auth.permissoes import exigir
    from app.apps.erp.core.cadastros.validadores import somente_digitos
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.db.models.financeiro import Parcela, StatusParcela, Titulo as _T
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            exigir(usuario, "reclassificar")     # financeiro, diretor ou admin
            t = s.get(_T, titulo_id)
            if t is None:
                return jsonify({"ok": False, "erro": "Título não encontrado."}), 404
            motivo = (d.get("motivo") or "").strip()
            if len(motivo) < 5:
                return jsonify({"ok": False, "erro": "Informe o motivo da alteração."}), 400
            mudancas = []
            for alteracao in (d.get("parcelas") or []):
                p = s.get(Parcela, int(alteracao.get("parcela_id", 0)))
                if p is None or p.titulo_id != t.id:
                    continue
                if p.status == StatusParcela.PAGA:
                    return jsonify({"ok": False,
                                    "erro": f"Parcela {p.numero} já está paga — "
                                            f"desfaça a baixa antes de alterar."}), 400
                if alteracao.get("vencimento"):
                    novo = _date.fromisoformat(alteracao["vencimento"])
                    if novo != p.vencimento:
                        mudancas.append(f"parcela {p.numero}: vencimento "
                                        f"{p.vencimento:%d/%m/%Y} → {novo:%d/%m/%Y}")
                        p.vencimento = novo
                if "linha_digitavel" in alteracao:
                    linha = somente_digitos(alteracao["linha_digitavel"] or "")
                    if linha != (p.linha_digitavel or ""):
                        mudancas.append(f"parcela {p.numero}: boleto atualizado")
                        p.linha_digitavel = linha or None
            if not mudancas:
                return jsonify({"ok": False, "erro": "Nada foi alterado."}), 400
            registrar_evento(s, "titulo", t.id, "PARCELAS_ALTERADAS",
                             {"mudancas": mudancas, "motivo": motivo}, usuario.id)
            s.commit()
        return jsonify({"ok": True, "mudancas": mudancas})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao alterar parcelas")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# Contratos de empreita
# ---------------------------------------------------------------------------
@bp.route("/erp/api/empreitas", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_erp", POST="lancar")
def api_empreitas():
    from app.apps.erp.core.titulos.empreita import criar_contrato, listar
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if request.method == "GET":
                return jsonify({"ok": True, "contratos": listar(s, usuario)})
            contrato = criar_contrato(s, request.get_json(silent=True) or {}, usuario)
            s.commit()
            return jsonify({"ok": True, "contrato": {
                "id": contrato.id, "numero": contrato.numero,
                "valor": float(contrato.valor_total), "status": contrato.status}})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha em empreitas")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/empreitas/<int:contrato_id>")
@login_obrigatorio
@permissao("ver_erp")
def api_empreita_detalhe(contrato_id: int):
    from app.apps.erp.core.documentos.armazenamento import listar as listar_anexos
    from app.apps.erp.core.titulos.empreita import detalhar
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_empreita_no_escopo
            exigir_empreita_no_escopo(s, _usuario_logado(s), contrato_id)
            dados = detalhar(s, contrato_id)
            for m in dados["medicoes"]:
                m["anexos"] = listar_anexos(s, "medicao", m["id"])
            dados["anexos"] = listar_anexos(s, "contrato_servico", contrato_id)
            return jsonify({"ok": True, "contrato": dados})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404


@bp.route("/erp/api/empreitas/<int:contrato_id>/aprovar", methods=["POST"])
@login_obrigatorio
@permissao("aprovar")
def api_aprovar_empreita(contrato_id: int):
    from app.apps.erp.core.titulos.empreita import aprovar_contrato
    try:
        with get_session() as s:
            c = aprovar_contrato(s, contrato_id, _usuario_logado(s))
            s.commit()
        return jsonify({"ok": True, "status": c.status})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/empreitas/garantias")
@login_obrigatorio
@permissao("ver_erp")
def api_empreita_garantias():
    """De quem a BWS ainda está com garantia na mão, e há quanto tempo."""
    from app.apps.erp.core.titulos.empreita import garantias_a_liberar, listar_alcadas
    with get_session() as s:
        return jsonify({"ok": True,
                        "garantias": garantias_a_liberar(s),
                        "alcadas": listar_alcadas(s)})


@bp.route("/erp/api/empreitas/<int:contrato_id>/garantia", methods=["POST"])
@login_obrigatorio
@permissao("aprovar")
def api_empreita_liberar_garantia(contrato_id: int):
    """Devolve a garantia retida — vira TÍTULO A PAGAR, não acerto de planilha."""
    from app.apps.erp.core.titulos.empreita import liberar_garantia
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            r = liberar_garantia(
                s, contrato_id, _usuario_logado(s),
                motivo=(d.get("motivo") or ""),
                vencimento=d.get("vencimento"),
                fornecedor_conta_id=(int(d["fornecedor_conta_id"])
                                     if d.get("fornecedor_conta_id") else None),
                forma_pagamento=(d.get("forma_pagamento") or "PIX"))
            s.commit()
        return jsonify({"ok": True, "resultado": r})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/empreitas/<int:contrato_id>/aditivo", methods=["POST"])
@login_obrigatorio
@permissao("aprovar")
def api_aditivar_empreita(contrato_id: int):
    from app.apps.erp.core.titulos.empreita import aditivar
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            rel = aditivar(s, contrato_id, d.get("valor"), d.get("motivo", ""),
                           _usuario_logado(s), quantidade=d.get("quantidade"))
            s.commit()
        return jsonify({"ok": True, "resultado": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/empreitas/<int:contrato_id>/medicoes", methods=["POST"])
@login_obrigatorio
@permissao("lancar")
def api_medir(contrato_id: int):
    """Registra a medição (ainda não é pagamento) e devolve as críticas."""
    from app.apps.erp.core.titulos.empreita import criticar_medicao, registrar_medicao
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_empreita_no_escopo
            exigir_empreita_no_escopo(s, _usuario_logado(s), contrato_id)
            usuario = _usuario_logado(s)
            if d.get("apenas_criticar"):
                return jsonify({"ok": True,
                                "criticas": criticar_medicao(s, contrato_id, d)})
            criticas = criticar_medicao(s, contrato_id, d)
            m = registrar_medicao(s, contrato_id, d, usuario)
            s.commit()
            return jsonify({"ok": True, "medicao": {
                "id": m.id, "numero": m.numero, "valor": float(m.valor_medido),
                "abatido": float(m.valor_adiantamento_abatido),
                "liquido": float(m.valor_liquido)},
                "criticas": [x for x in criticas if x["gravidade"] != "BLOQUEIA"]})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao medir")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/medicoes/<int:medicao_id>/autorizar", methods=["POST"])
@login_obrigatorio
@permissao("aprovar")
def api_autorizar_medicao(medicao_id: int):
    from app.apps.erp.core.titulos.empreita import autorizar_medicao
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            rel = autorizar_medicao(s, medicao_id, _usuario_logado(s), d)
            s.commit()
        return jsonify({"ok": True, "resultado": rel})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao autorizar medição")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/periodo", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_erp", POST="configurar")
def api_periodo():
    """Fechamento e destrave do período — só diretor e admin."""
    from app.apps.erp.core.titulos.empreita import (
        definir_bloqueio, destravar, periodo_bloqueado_ate,
    )
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if request.method == "GET":
                limite = periodo_bloqueado_ate(s)
                return jsonify({"ok": True,
                                "bloqueado_ate": limite.isoformat() if limite else None})
            d = request.get_json(silent=True) or {}
            if d.get("destravar"):
                rel = destravar(s, d.get("ate"), d.get("motivo", ""), usuario,
                                horas=int(d.get("horas") or 24))
            else:
                rel = definir_bloqueio(s, d.get("ate"), usuario)
            s.commit()
        return jsonify({"ok": True, "resultado": rel})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/meu-cadastro")
@login_obrigatorio
@permissao("ver_erp")
def pagina_meu_cadastro():
    return render_template("erp_meu_cadastro.html", **_contexto("prestacao"))


@bp.route("/erp/api/minha-senha", methods=["POST"])
@login_obrigatorio
@permissao("ver_erp")
def api_minha_senha():
    """A pessoa troca a PRÓPRIA senha.

    Dono, 17/09/2026: *"eu sou um usuário, e se eu quiser alterar a minha senha
    de usuário, onde é que eu consigo fazer isso? Eu não encontrei"*. Não dava:
    só um ADMIN trocava senha, pelo cadastro de operadores — o que obriga a
    pessoa a contar a senha nova para alguém, ou a ficar com a que recebeu.

    Exige a senha ATUAL. Sem isso, um computador deixado destravado vira conta
    tomada: quem passasse pela mesa trocaria a senha e fecharia o dono para
    fora. A troca fica na trilha de auditoria — sem a senha, obviamente.
    """
    from app.apps.erp.core.auth.service import gerar_hash, verificar_senha
    from app.apps.erp.core.comum.auditoria import registrar_evento
    d = request.get_json(silent=True) or {}
    atual = (d.get("senha_atual") or "").strip()
    nova = (d.get("senha_nova") or "").strip()
    repetida = (d.get("senha_repetida") or "").strip()
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if not verificar_senha(atual, usuario.senha_hash or ""):
                return jsonify({"ok": False,
                                "erro": "A senha atual não confere."}), 400
            if nova != repetida:
                return jsonify({"ok": False,
                                "erro": "A nova senha e a repetição estão diferentes."}), 400
            if nova == atual:
                return jsonify({"ok": False,
                                "erro": "A nova senha é igual à atual."}), 400
            try:
                usuario.senha_hash = gerar_hash(nova)
            except ValueError as e:
                return jsonify({"ok": False, "erro": str(e)}), 400
            registrar_evento(s, "usuario", usuario.id, "SENHA_TROCADA_PELO_PROPRIO",
                             {"origem": "tela Minha conta"}, usuario.id)
            s.commit()
        return jsonify({"ok": True})
    except Exception as e:
        logger.exception("ERP: falha ao trocar a própria senha")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/meu-cadastro", methods=["GET", "POST"])
@login_obrigatorio
@permissao("ver_erp")
def api_meu_cadastro():
    """Dados de recebimento de quem está logado.

    No reembolso de fundo fixo o favorecido é a própria pessoa — faz sentido
    ela mesma manter a chave Pix, sem depender do financeiro cadastrar.
    """
    from sqlalchemy import select
    from app.apps.erp.core.cadastros import fornecedores as svc_forn
    from app.apps.erp.core.cadastros.validadores import cpf_valido, somente_digitos
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.db.models.cadastros import (
        FormaPagamento, Fornecedor, FornecedorConta, StatusConta,
    )
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            doc = somente_digitos(usuario.cpf or "")
            pessoa = (s.scalars(select(Fornecedor).where(Fornecedor.cnpj_cpf == doc)).first()
                      if doc else None)

            if request.method == "GET":
                conta = None
                if pessoa is not None:
                    conta = next((c for c in pessoa.contas
                                  if c.status == StatusConta.HOMOLOGADA), None)
                return jsonify({"ok": True, "cadastro": {
                    "nome": usuario.nome, "cpf": usuario.cpf, "telefone": usuario.telefone,
                    "fornecedor_id": pessoa.id if pessoa else None,
                    "conta_descricao": (f"{conta.pix_tipo}: {conta.pix_chave}"
                                        if conta and conta.pix_chave else None),
                    "pix_chave": conta.pix_chave if conta else None,
                    "pix_tipo": conta.pix_tipo if conta else None,
                }})

            d = request.get_json(silent=True) or {}
            if not doc or not cpf_valido(doc):
                return jsonify({"ok": False,
                                "erro": "Seu CPF não está cadastrado ou é inválido. "
                                        "Peça ao administrador para corrigir."}), 400
            chave = (d.get("pix_chave") or "").strip()
            if not chave:
                return jsonify({"ok": False, "erro": "Informe a chave Pix."}), 400
            if pessoa is None:
                pessoa = svc_forn.criar(s, {"tipo_pessoa": "PF", "cnpj_cpf": doc,
                                            "razao_social": usuario.nome}, usuario)
                s.flush()
            for c_ in pessoa.contas:
                if c_.forma == FormaPagamento.PIX:
                    c_.status = StatusConta.INATIVA
            s.add(FornecedorConta(
                fornecedor_id=pessoa.id, forma=FormaPagamento.PIX,
                pix_tipo=(d.get("pix_tipo") or "CPF").upper(), pix_chave=chave,
                titular_nome=usuario.nome, titular_doc=doc,
                status=StatusConta.PENDENTE))
            s.flush()
            registrar_evento(s, "fornecedor", pessoa.id, "CONTA_INFORMADA_PELO_PROPRIO", {
                "usuario": usuario.email, "chave": chave,
                "observacao": "aguarda homologação do financeiro"}, usuario.id)
            s.commit()
        return jsonify({"ok": True, "aviso": "Chave registrada — o financeiro homologa "
                                             "antes do primeiro pagamento."})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha no meu cadastro")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# Locações
# ---------------------------------------------------------------------------
@bp.route("/erp/api/locacoes", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_erp", POST="lancar")
def api_locacoes():
    from app.apps.erp.core.locacoes import criar, listar, painel_por_obra
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if request.method == "GET":
                return jsonify({"ok": True, "contratos": listar(s, usuario),
                                "por_obra": painel_por_obra(s, usuario)})
            c = criar(s, request.get_json(silent=True) or {}, usuario)
            s.commit()
            return jsonify({"ok": True, "contrato": {"id": c.id, "numero": c.numero}})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha em locações")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/locacoes/<int:contrato_id>")
@login_obrigatorio
@permissao("ver_erp")
def api_locacao_detalhe(contrato_id: int):
    from app.apps.erp.core.locacoes import detalhar
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_locacao_no_escopo
            exigir_locacao_no_escopo(s, _usuario_logado(s), contrato_id)
            return jsonify({"ok": True, "contrato": detalhar(s, contrato_id)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404


@bp.route("/erp/api/locacoes/<int:contrato_id>/<acao>", methods=["POST"])
@login_obrigatorio
@permissao("lancar")
def api_locacao_acao(contrato_id: int, acao: str):
    from app.apps.erp.core.locacoes import devolver, gerar_previsao, remanejar
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_locacao_no_escopo
            exigir_locacao_no_escopo(s, _usuario_logado(s), contrato_id)
            usuario = _usuario_logado(s)
            if acao == "devolver":
                rel = devolver(s, contrato_id, d, usuario)
            elif acao == "remanejar":
                rel = remanejar(s, contrato_id, d, usuario)
            elif acao == "previsao":
                rel = {"parcelas": gerar_previsao(s, contrato_id,
                                                  meses=int(d.get("meses") or 6),
                                                  usuario=usuario)}
            else:
                return jsonify({"ok": False, "erro": f"Ação inválida: {acao}"}), 400
            s.commit()
        return jsonify({"ok": True, "resultado": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha na ação de locação")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# A conferência mensal dos equipamentos locados
#
# O alerta sozinho não resolvia: o ERP já dizia "10 meses locado, o aluguel já
# paga a compra". O que faltava era ALGUÉM SER OBRIGADO A RESPONDER, e é isso
# que estas rotas fazem — com nome, todo mês.
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# O AGENTE que vai atrás de quem tem pendência
#
# Duas portas, e elas são MUITO diferentes:
#
#   POST /erp/api/agente/rodar  — sem login, protegida por segredo. É a que a
#       rotina diária chama de fora. Não tem sessão nem usuário: é máquina
#       falando com máquina, no mesmo padrão dos outros módulos do monorepo
#       (`<MODULO>_SECRET` no corpo do pedido).
#   GET  /erp/api/agente/historico — com login e permissão, é a tela. Responde
#       "o Ruan foi cobrado?".
#
# A rota de rodar aceita `simular`: monta tudo e não manda nada. É como se
# confere o que o agente FARIA hoje antes de deixá-lo solto — e foi assim que
# ele foi verificado antes de ir para produção.
# ---------------------------------------------------------------------------
@bp.route("/erp/suprimentos/compras")
@login_obrigatorio
@permissao("ver_suprimentos")
def pagina_compras():
    return render_template("erp_suprimentos_compras.html",
                           **_contexto("sup_compras"))


# ---------------------------------------------------------------------------
# O PAINEL DO COORDENADOR E A PROJEÇÃO DO FINANCEIRO (20/11/2026)
#
# As duas telas só LEEM, e leem o cruzamento de coisas que outras telas
# gravam: o recebimento da obra, a nota vinculada ao pedido, o título que
# nasceu da nota. Por isso `ver_suprimentos` basta — a mesma ação que já abre
# a tela de Compras, e que o perfil do financeiro já tem.
# ---------------------------------------------------------------------------
@bp.route("/erp/suprimentos/painel")
@login_obrigatorio
@permissao("ver_suprimentos")
def pagina_suprimentos_painel():
    return render_template("erp_suprimentos_painel.html",
                           **_contexto("sup_painel"))


@bp.route("/erp/api/suprimentos/painel")
@login_obrigatorio
@permissao("ver_suprimentos")
def api_suprimentos_painel():
    """O planejamento do coordenador: cada item pedido pela obra que ainda não
    chegou por inteiro, na ordem em que deve ser olhado — mais o resumo dos
    pedidos de compra (entrega, nota, pagamento) para os quadros do topo."""
    from app.apps.erp.core.suprimentos import acompanhamento as svc
    with get_session() as s:
        usuario = _usuario_logado(s)
        compras = svc.painel(s, usuario)
        return jsonify({"ok": True, **svc.planejamento(s, usuario),
                        "compras": {"resumo": compras["resumo"],
                                    "destaques": compras["destaques"]}})


@bp.route("/erp/suprimentos/relatorios")
@login_obrigatorio
@permissao("ver_suprimentos")
def pagina_suprimentos_relatorios():
    return render_template("erp_suprimentos_relatorios.html",
                           **_contexto("sup_relatorios"))


@bp.route("/erp/api/suprimentos/relatorios")
@login_obrigatorio
@permissao("ver_suprimentos")
def api_suprimentos_relatorios():
    """Os relatórios de gestão das compras, no recorte de obra de quem pede."""
    from datetime import date as _date
    from app.apps.erp.core.suprimentos import relatorios as svc

    def _data(nome):
        try:
            return _date.fromisoformat((request.args.get(nome) or "").strip())
        except ValueError:
            return None

    def _numero(nome):
        v = (request.args.get(nome) or "").strip()
        return int(v) if v.isdigit() else None

    with get_session() as s:
        return jsonify({"ok": True, **svc.gerar(
            s, request.args.get("tipo") or "ECONOMIA", _usuario_logado(s),
            de=_data("de"), ate=_data("ate"), obra_id=_numero("obra_id"),
            fornecedor_id=_numero("fornecedor_id"))})


# ---------------------------------------------------------------------------
# MATERIAL COMPRADO SEM PEDIDO DE COMPRA — ciência e autorização (21/11/2026)
# ---------------------------------------------------------------------------
@bp.route("/erp/api/suprimentos/fora-do-fluxo", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_suprimentos", POST="configurar")
def api_fora_do_fluxo():
    """GET: o que está esperando autorização e se a regra está ligada.
    POST: liga ou desliga a regra (só quem configura o sistema)."""
    from app.apps.erp.core.suprimentos import fora_do_fluxo as svc
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if request.method == "POST":
                svc.ligar(s, usuario, (request.get_json(silent=True) or {}).get("modo") or "")
                s.commit()
            modo, desde = svc.estado(s)
            return jsonify({"ok": True, "modo": modo,
                            "ligado_desde": desde.isoformat() if desde else None,
                            "pendentes": svc.listar(s, usuario) if desde else [],
                            "pode_autorizar": pode(usuario, "autorizar_compra_fora_do_fluxo"),
                            "pode_regularizar": pode(usuario, "receber_material"),
                            "pode_configurar": pode(usuario, "configurar"),
                            "eu": usuario.id})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/suprimentos/fora-do-fluxo/<int:titulo_id>/autorizar", methods=["POST"])
@login_obrigatorio
@permissao("autorizar_compra_fora_do_fluxo")
def api_autorizar_fora_do_fluxo(titulo_id: int):
    """Dá ciência da compra feita sem pedido e libera o lançamento."""
    from app.apps.erp.core.suprimentos import fora_do_fluxo as svc
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_titulo_no_escopo
            usuario = _usuario_logado(s)
            exigir_titulo_no_escopo(s, usuario, titulo_id)
            svc.autorizar(s, titulo_id, usuario,
                          (request.get_json(silent=True) or {}).get("observacao") or "")
            s.commit()
            return jsonify({"ok": True})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise


@bp.route("/erp/api/suprimentos/fora-do-fluxo/<int:titulo_id>/regularizar", methods=["POST"])
@login_obrigatorio
@permissao("receber_material")
def api_regularizar_fora_do_fluxo(titulo_id: int):
    """A solicitação tardia: registra o que foi comprado por fora, e disso
    nascem a solicitação, o pedido e o recebimento ligados ao lançamento."""
    from app.apps.erp.core.suprimentos import fora_do_fluxo as svc
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_titulo_no_escopo
            usuario = _usuario_logado(s)
            exigir_titulo_no_escopo(s, usuario, titulo_id)
            saida = svc.regularizar(s, titulo_id, request.get_json(silent=True) or {}, usuario)
            s.commit()
            return jsonify({"ok": True, **saida})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao regularizar compra sem pedido")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/suprimentos/acompanhar")
@login_obrigatorio
@permissao("ver_suprimentos")
def pagina_suprimentos_acompanhar():
    return render_template("erp_suprimentos_acompanhar.html",
                           **_contexto("sup_acompanhar"))


@bp.route("/erp/api/suprimentos/acompanhamento")
@login_obrigatorio
@permissao("ver_suprimentos")
def api_suprimentos_acompanhamento():
    """Cada pedido de pé, cruzado: entrega × nota × pagamento."""
    from app.apps.erp.core.suprimentos import acompanhamento as svc
    with get_session() as s:
        return jsonify({"ok": True, **svc.painel(
            s, _usuario_logado(s),
            incluir_fechados=request.args.get("fechados") == "1",
            incluir_sem_efeito=request.args.get("todos") == "1")})


@bp.route("/erp/suprimentos/projecao")
@login_obrigatorio
@permissao("ver_suprimentos")
def pagina_suprimentos_projecao():
    return render_template("erp_suprimentos_projecao.html",
                           **_contexto("sup_projecao"))


@bp.route("/erp/api/suprimentos/projecao")
@login_obrigatorio
@permissao("ver_suprimentos")
def api_suprimentos_projecao():
    """O que as compras ainda vão custar e quando, antes de virar título."""
    from app.apps.erp.core.suprimentos import projecao as svc
    with get_session() as s:
        return jsonify({"ok": True, **svc.projetar(s, _usuario_logado(s))})


@bp.route("/erp/api/suprimentos/entrega/locais")
@login_obrigatorio
@permissao("comprar")
def api_locais_de_entrega():
    """O endereço de cada obra e a sede que pode recebê-la — para o pedido
    direto (sem cotação) oferecer "onde desce" como o fechamento do mapa."""
    from app.apps.erp.core.suprimentos import pedido as svc
    ids = [int(x) for x in (request.args.get("obras") or "").split(",") if x.isdigit()]
    with get_session() as s:
        return jsonify({"ok": True, "obras": svc.locais_possiveis(s, ids)})


@bp.route("/erp/api/suprimentos/cotacoes/<int:cotacao_id>/fechamento")
@login_obrigatorio
@permissao("comprar")
def api_preparar_fechamento(cotacao_id: int):
    """O que a tela precisa para fechar um pedido a partir do mapa: o que cada
    fornecedor cotou, o que ainda está livre e onde o material pode descer."""
    from app.apps.erp.core.suprimentos import pedido as svc
    with get_session() as s:
        return jsonify({"ok": True, **svc.preparar_fechamento(s, cotacao_id)})


# ---------------------------------------------------------------------------
# A ASSINATURA DE QUEM ASSINA DOCUMENTO PELO SISTEMA (21/11/2026)
# Cada um cadastra a SUA — por isso `ver_erp`, a ação mais baixa, e nenhum
# número de pessoa na rota: a assinatura é sempre a de quem está logado.
# ---------------------------------------------------------------------------
@bp.route("/erp/assinatura")
@login_obrigatorio
@permissao("ver_erp")
def pagina_assinatura():
    return render_template("erp_assinatura.html")


@bp.route("/erp/api/minha-assinatura/imagem")
@login_obrigatorio
@permissao("ver_erp")
def imagem_da_minha_assinatura():
    from app.apps.erp.core.comum import assinatura
    with get_session() as s:
        conteudo = assinatura.obter(s, _usuario_logado(s).id)
    if not conteudo:
        return "", 404
    resposta = make_response(conteudo)
    resposta.headers["Content-Type"] = "image/jpeg"
    resposta.headers["Cache-Control"] = "no-store"
    return resposta


@bp.route("/erp/api/minha-assinatura", methods=["POST", "DELETE"])
@login_obrigatorio
@permissao("ver_erp")
def api_minha_assinatura():
    from app.apps.erp.core.comum import assinatura
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if request.method == "DELETE":
                assinatura.remover(s, usuario)
            else:
                assinatura.salvar(s, usuario,
                                  (request.get_json(silent=True) or {}).get("imagem"))
            s.commit()
            return jsonify({"ok": True})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/suprimentos/cotacoes/<int:cotacao_id>/solicitacao.pdf")
@login_obrigatorio
@permissao("ver_pedidos_compra")
def pdf_da_solicitacao_de_cotacao(cotacao_id: int):
    """A solicitação de cotação em PDF — o anexo que o fornecedor recebe."""
    from app.apps.erp.core.suprimentos import cotacao_pdf, envio
    from app.apps.erp.db.models.cadastros import Empresa
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            pedida = (request.args.get("empresa") or "").strip()
            empresa = (s.get(Empresa, int(pedida)) if pedida.isdigit()
                       else envio._empresa_do_disparo(s, cotacao_id, None))
            if empresa is None:
                return "Não há empresa definida para esta cotação.", 400
            conteudo = cotacao_pdf.gerar(
                s, cotacao_id, empresa, usuario=usuario,
                prazo=(request.args.get("prazo") or "").strip() or None,
                observacao=request.args.get("observacao") or "")
    except ErroValidacao as e:
        return str(e), 400
    resposta = make_response(conteudo)
    resposta.headers["Content-Type"] = "application/pdf"
    resposta.headers["Content-Disposition"] = (
        f'inline; filename="Cotacao-{cotacao_id}.pdf"')
    return resposta


@bp.route("/erp/suprimentos/cotacoes/<int:cotacao_id>/pdf")
@login_obrigatorio
@permissao("ver_pedidos_compra")
def pdf_do_mapa(cotacao_id: int):
    """O mapa de cotação em PDF, no formato da planilha: o mapa inteiro com as
    condições de cada fornecedor e os resumos. É o que vai para a gestão."""
    from app.apps.erp.core.cadastros import empresas as svc_empresas
    from app.apps.erp.core.suprimentos import mapa_pdf
    with get_session() as s:
        usuario = _usuario_logado(s)
        padrao = svc_empresas.padrao(s)
        conteudo = mapa_pdf.gerar(
            s, cotacao_id, quem=usuario.nome or "",
            empresa_nome=(getattr(padrao, "nome_fantasia", None)
                          or getattr(padrao, "razao_social", "") or ""))
    resposta = make_response(conteudo)
    resposta.headers["Content-Type"] = "application/pdf"
    resposta.headers["Content-Disposition"] = (
        f'inline; filename="Mapa-de-cotacao-{cotacao_id}.pdf"')
    return resposta


@bp.route("/erp/suprimentos/pedidos/<int:pedido_id>/pdf")
@login_obrigatorio
@permissao("ver_pedidos_compra")
def pdf_do_pedido(pedido_id: int):
    """O pedido de compra em PDF, exatamente como vai anexo ao e-mail do
    fornecedor. Serve para conferir antes de mandar — e para baixar."""
    from app.apps.erp.core.suprimentos import envio, pedido_pdf
    from app.apps.erp.db.models.cadastros import Empresa
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            possiveis = envio.empresa_do_pedido(s, pedido_id)
            pedida = (request.args.get("empresa") or "").strip()
            empresa_id = int(pedida) if pedida.isdigit() else possiveis["sugerida"]
            empresa = s.get(Empresa, empresa_id) if empresa_id else None
            if empresa is None:
                return ("Não há empresa definida para este pedido. Ligue a obra "
                        "a uma empresa em Configurações › Empresas."), 400
            conteudo = pedido_pdf.gerar(s, pedido_id, empresa, usuario)
            numero = pedido_pdf.dados(s, pedido_id, empresa)["numero"]
    except ErroValidacao as e:
        return str(e), 400
    resposta = make_response(conteudo)
    resposta.headers["Content-Type"] = "application/pdf"
    resposta.headers["Content-Disposition"] = f'inline; filename="Pedido-{numero}.pdf"'
    return resposta


@bp.route("/erp/api/suprimentos/pedidos/<int:pedido_id>/para-lancar")
@login_obrigatorio
@permissao("lancar")
def api_pedido_para_lancar(pedido_id: int):
    """O lançamento financeiro já preenchido com o que o pedido sabe: credor,
    valor do que chegou, rateio por obra e as parcelas da previsão."""
    from app.apps.erp.core.suprimentos import lancamento as svc
    try:
        with get_session() as s:
            from app.apps.erp.core.suprimentos.recebimento import (
                exigir_pedido_no_escopo)
            usuario = _usuario_logado(s)
            exigir_pedido_no_escopo(s, usuario, pedido_id)
            rec = (request.args.get("recebimento") or "").strip()
            return jsonify({"ok": True, "pedido": svc.preparar(
                s, usuario, pedido_id,
                recebimento_id=int(rec) if rec.isdigit() else None)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/suprimentos/pedidos/<int:pedido_id>/entrega", methods=["POST"])
@login_obrigatorio
@permissao("comprar")
def api_redefinir_entrega(pedido_id: int):
    """Troca onde o material desce (obra, sede ou outro endereço) — enquanto
    nada chegou."""
    from app.apps.erp.core.suprimentos import pedido as svc
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            svc.redefinir_entrega(s, pedido_id, d.get("locais_entrega") or [],
                                  _usuario_logado(s))
            s.commit()
            return jsonify({"ok": True})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP/suprimentos: falha ao redefinir a entrega")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/suprimentos/compras")
@login_obrigatorio
@permissao("ver_suprimentos")
def api_compras():
    """O que cada obra comprou, aberto até o insumo — e o contrário.

    O escopo da pessoa entra na consulta, não na tela: quem enxerga só as
    obras dele não descobre o gasto das outras por um relatório.
    """
    from datetime import date as _date
    from app.apps.erp.core.suprimentos import compras as svc

    def _numero(nome):
        valor = (request.args.get(nome) or "").strip()
        return int(valor) if valor.isdigit() else None

    def _data(nome):
        valor = (request.args.get(nome) or "").strip()
        try:
            return _date.fromisoformat(valor) if valor else None
        except ValueError:
            return None

    filtros = dict(
        obra_id=_numero("obra_id"), insumo_id=_numero("insumo_id"),
        categoria_id=_numero("categoria_id"), fornecedor_id=_numero("fornecedor_id"),
        de=_data("de"), ate=_data("ate"),
        busca=(request.args.get("busca") or "").strip(),
        incluir_pendentes=request.args.get("pendentes") == "1",
    )
    with get_session() as s:
        usuario = _usuario_logado(s)
        dados = svc.arvore(s, agrupar=request.args.get("agrupar", "obra"),
                           usuario=usuario, **filtros)
        dados["linhas"] = svc.linhas(s, usuario=usuario, **filtros)
        return jsonify({"ok": True, **dados})


@bp.route("/erp/api/agente/rodar", methods=["POST"])
@permissao_publica("chamada por rotina externa; a guarda é o ERP_AGENTE_SECRET "
                   "no corpo do pedido, como nos demais módulos do monorepo")
def api_agente_rodar():
    """Roda a varredura do agente. Sem segredo configurado, RECUSA."""
    import os
    from app.apps.erp.core import agente as svc

    esperado = os.getenv("ERP_AGENTE_SECRET", "").strip()
    dados = request.get_json(silent=True) or {}
    if not esperado:
        return jsonify({"ok": False, "erro": "ERP_AGENTE_SECRET não configurado "
                                             "no ambiente. O agente não roda sem ele."}), 503
    if dados.get("secret") != esperado:
        logger.warning("ERP/agente: chamada recusada, segredo inválido")
        return jsonify({"ok": False, "erro": "Segredo inválido."}), 403

    simular = bool(dados.get("simular"))
    with get_session() as s:
        r = svc.varrer(s, simular=simular)
        # OS RELATÓRIOS AGENDADOS PEGAM CARONA NESTE RELÓGIO, de propósito: um
        # segundo relógio seria uma segunda coisa para quebrar, e outra para
        # lembrar de configurar. Falha aqui não derruba a cobrança do agente,
        # que é o que essa rotina veio fazer.
        from app.apps.erp.core.perguntas import agendadas as svc_agendadas
        try:
            r["agendados"] = svc_agendadas.rodar_do_dia(s, simular=simular)
        except Exception as e:
            logger.exception("ERP/agente: relatórios agendados falharam")
            r["agendados"] = {"erro": str(e)}
        if not simular:
            s.commit()
    return jsonify({"ok": True, **r})


@bp.route("/erp/api/agente/historico")
@login_obrigatorio
@permissao("ver_erp")
def api_agente_historico():
    """O que o agente já falou, com quem e quando."""
    from app.apps.erp.core import agente as svc
    with get_session() as s:
        return jsonify({"ok": True,
                        "mensagens": svc.historico(s, assunto=request.args.get("assunto"))})


@bp.route("/erp/api/locacoes/conferencias")
@login_obrigatorio
@permissao("ver_erp")
def api_conferencias_pendentes():
    """O que espera resposta. `todas=1` é a visão de quem cobra."""
    from app.apps.erp.core import locacoes_conferencia as svc
    with get_session() as s:
        atual = _usuario_logado(s)
        todas = request.args.get("todas") == "1"
        return jsonify({"ok": True,
                        "pendentes": svc.pendentes(s, usuario=atual, todas=todas)})


@bp.route("/erp/api/locacoes/conferencias/abrir", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_abrir_conferencias():
    """Abre a conferência do mês para todo contrato ativo. É seguro repetir —
    a restrição do banco garante uma por contrato por mês."""
    from app.apps.erp.core import locacoes_conferencia as svc
    try:
        with get_session() as s:
            r = svc.abrir_do_mes(s, usuario=_usuario_logado(s))
            s.commit()
            return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/locacoes/conferencias/<int:conferencia_id>")
@login_obrigatorio
@permissao("ver_erp")
def api_conferencia(conferencia_id: int):
    from app.apps.erp.core import locacoes_conferencia as svc
    with get_session() as s:
        from app.apps.erp.core.auth.permissoes import exigir_locacao_no_escopo
        conf = svc.obter(s, conferencia_id)
        # a conferência é de um contrato: quem não enxerga o contrato não
        # enxerga a conferência dele
        exigir_locacao_no_escopo(s, _usuario_logado(s), conf["contrato_id"])
        return jsonify({"ok": True, "conferencia": conf})


@bp.route("/erp/api/locacoes/conferencias/<int:conferencia_id>", methods=["POST"])
@login_obrigatorio
@permissao("ver_erp")
def api_responder_conferencia(conferencia_id: int):
    """A obra presta contas. Devolver e remanejar aqui FAZEM acontecer."""
    from app.apps.erp.core import locacoes_conferencia as svc
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_locacao_no_escopo
            conf = svc.obter(s, conferencia_id)
            exigir_locacao_no_escopo(s, _usuario_logado(s), conf["contrato_id"])
            r = svc.responder(s, conferencia_id, d, _usuario_logado(s))
            s.commit()
            return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP/locação: falha ao responder a conferência")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/locacoes/parcelas/<int:parcela_id>/lancar", methods=["POST"])
@login_obrigatorio
@permissao("lancar")
def api_lancar_locacao(parcela_id: int):
    from app.apps.erp.core.locacoes import lancar_parcela
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_parcela_locacao_no_escopo
            exigir_parcela_locacao_no_escopo(s, _usuario_logado(s), parcela_id)
            rel = lancar_parcela(s, parcela_id, request.get_json(silent=True) or {},
                                 _usuario_logado(s))
            s.commit()
        return jsonify({"ok": True, "resultado": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/insumos", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_erp", POST="configurar")
def api_insumos():
    from sqlalchemy import select
    from app.apps.erp.db.models.cadastros import Insumo, InsumoCategoria
    try:
        with get_session() as s:
            if request.method == "GET":
                stmt = select(Insumo).where(Insumo.ativo.is_(True))
                if request.args.get("locavel"):
                    stmt = stmt.where(Insumo.locavel.is_(True))
                itens = s.scalars(stmt.order_by(Insumo.descricao)).all()
                return jsonify({"ok": True, "insumos": [{
                    "id": i.id, "codigo": i.codigo, "descricao": i.descricao,
                    "unidade": i.unidade, "locavel": i.locavel,
                    "valor_compra": float(i.valor_referencia_compra or 0) or None,
                    "valor_locacao": float(i.valor_referencia_locacao or 0) or None,
                } for i in itens]})
            d = request.get_json(silent=True) or {}
            from decimal import Decimal as _D
            i = Insumo(codigo=(d.get("codigo") or "").strip().upper(),
                       descricao=(d.get("descricao") or "").strip(),
                       unidade=(d.get("unidade") or "").strip() or None,
                       locavel=bool(d.get("locavel")),
                       categoria_id=d.get("categoria_id") or None,
                       valor_referencia_compra=(_D(str(d["valor_compra"]).replace(",", "."))
                                                if d.get("valor_compra") else None),
                       valor_referencia_locacao=(_D(str(d["valor_locacao"]).replace(",", "."))
                                                 if d.get("valor_locacao") else None))
            if not i.codigo or not i.descricao:
                return jsonify({"ok": False, "erro": "Informe código e descrição."}), 400
            s.add(i)
            s.commit()
            return jsonify({"ok": True, "insumo_id": i.id})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha em insumos")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/locacoes/ler-contrato", methods=["POST"])
@login_obrigatorio
@permissao("lancar")
def api_ler_contrato_locacao():
    """Lê o contrato da locadora e devolve o rascunho do cadastro."""
    from app.apps.erp.core.documentos.armazenamento import salvar
    from app.apps.erp.core.locacoes import ler_contrato
    arquivo = request.files.get("arquivo")
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Envie o contrato em PDF ou foto."}), 400
    # FORA do try: a recusa por teto tem resposta própria (402), e um
    # `except Exception` a transformaria em 'falha do sistema'.
    _exigir_saldo_de_ia()
    try:
        conteudo = arquivo.read()
        with get_session() as s:
            usuario = _usuario_logado(s)
            rascunho = ler_contrato(s, conteudo, arquivo.filename or "contrato.pdf")
            anexo = salvar(s, conteudo, arquivo.filename or "contrato.pdf",
                           entidade_tipo="locacao_rascunho", entidade_id=usuario.id,
                           categoria="CONTRATO", descricao="Contrato de locação",
                           usuario=usuario)
            rascunho["anexo_id"] = anexo.id
            s.commit()
        return jsonify({"ok": True, "rascunho": rascunho})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao ler contrato de locação")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/mapa")
@login_obrigatorio
@permissao("ver_relatorios")
def api_mapa():
    """Obras, equipamentos locados e volume financeiro por lugar."""
    from app.apps.erp.core.locacoes import mapa
    try:
        with get_session() as s:
            return jsonify({"ok": True, **mapa(s)})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha no mapa")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/ia/consumo")
@login_obrigatorio
@permissao("configurar")
def api_ia_consumo():
    """Painel de consumo de IA — restrito a quem administra."""
    from app.apps.erp.core.auth.permissoes import exigir
    from app.apps.erp.core.comum.ia_custo import painel
    try:
        with get_session() as s:
            exigir(_usuario_logado(s), "configurar")
            return jsonify({"ok": True, "consumo": painel(
                s, dias=request.args.get("dias", type=int) or 90)})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha no painel de IA")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/ia/teto", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_ia_teto():
    """Teto mensal de gasto com IA (US$). Só avisa; não bloqueia nada."""
    from app.apps.erp.core.auth.permissoes import exigir
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.core.comum.ia_custo import definir_teto_mensal, situacao_teto
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            exigir(usuario, "configurar")
            teto = definir_teto_mensal(s, d.get("teto"), usuario.id)
            registrar_evento(s, "parametro", 0, "IA_TETO_ALTERADO",
                             {"teto_usd": (str(teto) if teto is not None else None)},
                             usuario.id)
            s.commit()
            return jsonify({"ok": True, "situacao": situacao_teto(s)})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ValueError as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao definir teto de IA")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# Pessoal
# ---------------------------------------------------------------------------
@bp.route("/erp/api/colaboradores", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_pessoal", POST="editar_colaboradores")
def api_colaboradores():
    from app.apps.erp.core.pessoal import listar_colaboradores, salvar_colaborador
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if request.method == "GET":
                return jsonify({"ok": True, "colaboradores": listar_colaboradores(
                    s, request.args.get("obra_id", type=int),
                    ativos=request.args.get("todos") != "1",
                    usuario=usuario)})
            c = salvar_colaborador(s, request.get_json(silent=True) or {}, usuario)
            s.commit()
            return jsonify({"ok": True, "colaborador_id": c.id})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha em colaboradores")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/convencoes", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_pessoal", POST="editar_colaboradores")
def api_convencoes():
    """As convenções coletivas, e a criação de uma nova de dentro do cadastro.

    ⚑ 17/11/2026, pedido dele com todas as letras: *"eu não quero um botão
    mais cargo. Cargo é para estar. Quando a gente for cadastrar, aqui eu ter
    a relação de cargos e daqui DENTRO do cadastro eu poder criar um novo
    cargo, criar uma nova convenção. Eu não posso criar obra, porque obra se
    cria no outro canto, mas isso aqui é área exclusiva do DP."*

    É a regra "um cadastro, uma porta" do `CLAUDE.md` aplicada à lista: a
    pessoa não sai da ficha para cadastrar o que falta na caixinha.
    """
    from sqlalchemy import func, select

    from app.apps.erp.db.models.cadastros import Convencao
    try:
        with get_session() as s:
            if request.method == "POST":
                d = request.get_json(silent=True) or {}
                nome = (d.get("nome") or "").strip()
                if not nome:
                    return jsonify({"ok": False,
                                    "erro": "Informe o nome da convenção."}), 400
                # Mesmo nome é a mesma convenção: duas linhas para o mesmo
                # acordo fariam duas pessoas da mesma obra terem pisos
                # diferentes sem ninguém ver.
                ja = s.scalars(select(Convencao).where(
                    func.upper(func.btrim(Convencao.nome)) == nome.upper())).first()
                if ja is not None:
                    ja.ativo = True
                    s.commit()
                    return jsonify({"ok": True, "convencao_id": ja.id,
                                    "ja_existia": True})
                nova = Convencao(nome=nome,
                                 uf=(d.get("uf") or "").strip().upper() or None)
                s.add(nova)
                s.commit()
                return jsonify({"ok": True, "convencao_id": nova.id})
            return jsonify({"ok": True, "convencoes": [
                {"id": c.id, "nome": c.nome, "uf": c.uf or ""}
                for c in s.scalars(select(Convencao).where(
                    Convencao.ativo.is_not(False)).order_by(Convencao.nome)).all()]})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/funcoes", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_pessoal", POST="editar_colaboradores")
def api_funcoes():
    from sqlalchemy import func, select
    from decimal import Decimal as _D
    # ⚑ ESTA ROTA FALA COM `cargos` DESDE 16/11/2026 (migração 105). Cargo e
    # função viraram a mesma coisa — *"vamos chamar só de cargo e acabou"* —
    # e o endereço continua `/erp/api/funcoes` só para não quebrar nada que já
    # o chame. Ela devolve `funcoes` E `cargos` com o mesmo conteúdo, para a
    # tela poder ser trocada sem pressa.
    from app.apps.erp.db.models.cadastros import Cargo
    try:
        with get_session() as s:
            if request.method == "POST":
                d = request.get_json(silent=True) or {}
                nome = (d.get("nome") or "").strip()
                if not nome:
                    return jsonify({"ok": False, "erro": "Informe o nome do cargo."}), 400
                # Mesmo nome é o mesmo cargo: duas listas paralelas foi
                # exatamente o problema que a migração 105 acabou de resolver.
                ja = s.scalars(select(Cargo).where(
                    func.upper(func.btrim(Cargo.nome)) == nome.strip().upper())).first()
                diaria = (_D(str(d["valor_diaria"]).replace(",", "."))
                          if d.get("valor_diaria") else None)
                if ja is not None:
                    if diaria is not None:
                        ja.valor_diaria = diaria
                    ja.ativo = True
                    s.commit()
                    return jsonify({"ok": True, "funcao_id": ja.id,
                                    "cargo_id": ja.id, "ja_existia": True})
                f = Cargo(nome=nome, valor_diaria=diaria)
                s.add(f)
                s.commit()
                return jsonify({"ok": True, "funcao_id": f.id, "cargo_id": f.id})
            lista = [
                {"id": f.id, "nome": f.nome,
                 "valor_diaria": float(f.valor_diaria) if f.valor_diaria else None}
                for f in s.scalars(select(Cargo).where(Cargo.ativo.is_(True))
                                   .order_by(Cargo.nome)).all()]
            return jsonify({"ok": True, "funcoes": lista, "cargos": lista})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/dc", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_pessoal", POST="lancar_dc")
def api_dc():
    from app.apps.erp.core.pessoal import VERBAS, criar_despesa, listar
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if request.method == "GET":
                from app.apps.erp.core.pessoal import categorias_de_pessoal
                return jsonify({"ok": True, "despesas": listar(s, usuario),
                                "categorias": categorias_de_pessoal(s),
                                "verbas": [{"chave": k, "rotulo": v[0],
                                            "exige_qtd": v[2]}
                                           for k, v in VERBAS.items()]})
            d = criar_despesa(s, request.get_json(silent=True) or {}, usuario)
            s.commit()
            return jsonify({"ok": True, "despesa": {
                "id": d.id, "numero": d.numero, "status": d.status,
                "total": float(d.valor_total)}})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha em DC")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/dc/criticar", methods=["POST"])
@login_obrigatorio
@permissao("lancar_dc")
def api_dc_criticar():
    from app.apps.erp.core.pessoal import criticar
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            return jsonify({"ok": True, "critica": criticar(
                s, d.get("itens") or [], obra_id=d.get("obra_id"),
                despesa_id=d.get("despesa_id"))})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao criticar DC")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/dc/<int:despesa_id>")
@login_obrigatorio
@permissao("ver_pessoal")
def api_dc_detalhe(despesa_id: int):
    from app.apps.erp.core.pessoal import detalhar
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_despesa_no_escopo
            exigir_despesa_no_escopo(s, _usuario_logado(s), despesa_id)
            return jsonify({"ok": True, "despesa": detalhar(s, despesa_id)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404


@bp.route("/erp/api/dc/<int:despesa_id>/<acao>", methods=["POST"])
@login_obrigatorio
@permissao("ver_pessoal")
def api_dc_acao(despesa_id: int, acao: str):
    from app.apps.erp.core.pessoal import aprovar, devolver, gerar_titulo, planilha_pagamento
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_despesa_no_escopo
            exigir_despesa_no_escopo(s, _usuario_logado(s), despesa_id)
            usuario = _usuario_logado(s)
            if acao == "aprovar":
                rel = aprovar(s, despesa_id, usuario, d.get("observacao", ""))
            elif acao == "devolver":
                rel = devolver(s, despesa_id, d.get("motivo", ""), usuario)
            elif acao == "gerar-titulo":
                rel = gerar_titulo(s, despesa_id, d, usuario)
            elif acao == "planilha":
                rel = planilha_pagamento(s, despesa_id)
            else:
                return jsonify({"ok": False, "erro": f"Ação inválida: {acao}"}), 400
            s.commit()
        return jsonify({"ok": True, "resultado": rel})
    except ErroPermissao as e:
        return jsonify({"ok": False, "erro": str(e)}), 403
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha na ação da DC")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# O CADASTRO COMPLETO DA PESSOA (migrações 085 a 087, 05/11/2026)
#
# O cadastro saiu do Pipefy e veio para cá. As rotas abaixo são a porta, e todas
# exigem `editar_colaboradores` para escrever — hoje: ADMIN, diretor financeiro
# e DP. É a decisão do dono: *"todos os pedidos vão para o DP, e quem tiver
# perfil do DP que vai aprovar."*
#
# ⚠️ DADO PESSOAL. Endereço, RG, PIS, filiação e dependentes só saem para quem
# tem `editar_colaboradores`. Quem tem apenas `ver_pessoal` continua vendo o que
# já via: nome, obra, função e situação.
# ---------------------------------------------------------------------------
@bp.route("/erp/api/colaboradores/opcoes")
@login_obrigatorio
@permissao("ver_pessoal")
def api_colaboradores_opcoes():
    """As listas das caixinhas (vínculo, contrato, escolaridade…) e os cargos.

    ⚑ AS OBRAS ENTRARAM EM 15/11/2026, e isto era DEFEITO EM PRODUÇÃO: a tela
    de Solicitações ao DP monta a caixinha de obra com `o.obras` desta rota —
    que nunca existiu aqui. Resultado: a caixinha vinha com uma opção só ("
    escolha a obra…"), e como a obra é obrigatória, **não havia como abrir um
    pedido de cadastro de colaborador** desde que ela passou a ser exigida
    (10/11/2026). O pedido mais importante da fila estava impossível, e a tela
    não dizia por quê — a lista simplesmente estava vazia.

    ⚑ RESPEITA O RECORTE POR OBRA: `obras_do_usuario` devolve `None` para quem
    alcança todas, e a lista das permitidas para quem é preso a obra. Mandar
    todas as obras para quem só enxerga uma seria vazar o nome das outras numa
    caixinha — e deixar abrir pedido onde ele não deveria.
    """
    from sqlalchemy import select

    from app.apps.erp.core.auth.permissoes import obras_do_usuario
    from app.apps.erp.core.cadastros import cargos as svc_cargos
    from app.apps.erp.core.cadastros import pessoas as svc_pessoas
    from app.apps.erp.db.models.cadastros import Obra
    with get_session() as s:
        permitidas = obras_do_usuario(s, _usuario_logado(s))
        obras = [o for o in s.scalars(select(Obra).order_by(Obra.codigo)).all()
                 if getattr(o, "status", None) != "ENCERRADA"
                 and (permitidas is None or o.id in set(permitidas))]
        # SEM O PISO: esta rota é aberta a quem tem `ver_pessoal` (inclui
        # supervisor de obra e parceiro), e eles precisam da lista de cargos
        # para a caixinha — não da tabela salarial da empresa.
        return jsonify({"ok": True, **svc_pessoas.opcoes(s),
                        **svc_cargos.listar(s, com_salario=False),
                        "obras": [{"id": o.id, "codigo": o.codigo,
                                   "nome": o.nome} for o in obras]})


@bp.route("/erp/api/colaboradores/conferir-cpf")
@login_obrigatorio
@permissao("conferir_cpf_colaborador")
def api_colaboradores_conferir_cpf():
    """"Esta pessoa já existe?" — respondido ENQUANTO se digita.

    Pedido do dono: *"o RP também tem que alertar (…) se a pessoa está começando
    a digitar o cadastro, esse cadastro aqui já existe"*. No Pipefy o aviso
    chegava depois de 51 campos preenchidos, e o card era apagado.
    """
    from app.apps.erp.core.cadastros import pessoas as svc
    with get_session() as s:
        return jsonify({"ok": True, **svc.conferir_cpf(s, request.args.get("cpf", ""))})


@bp.route("/erp/api/colaboradores/<int:colaborador_id>/ficha")
@login_obrigatorio
@permissao("editar_colaboradores")
def api_colaborador_ficha(colaborador_id: int):
    from app.apps.erp.core.cadastros import pessoas as svc
    try:
        with get_session() as s:
            return jsonify({"ok": True, "colaborador": svc.ficha(s, colaborador_id)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404


@bp.route("/erp/api/colaboradores/completo", methods=["POST"])
@login_obrigatorio
@permissao("editar_colaboradores")
def api_colaborador_salvar_completo():
    from app.apps.erp.core.cadastros import pessoas as svc
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            c = svc.salvar(s, request.get_json(silent=True) or {}, usuario)
            lido = svc.ficha(s, c.id)
            s.commit()
        return jsonify({"ok": True, "colaborador": lido})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao salvar o cadastro do colaborador")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/colaboradores/<int:colaborador_id>/dependentes", methods=["POST"])
@login_obrigatorio
@permissao("editar_colaboradores")
def api_colaborador_dependente(colaborador_id: int):
    from app.apps.erp.core.cadastros import pessoas as svc
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            svc.salvar_dependente(s, colaborador_id,
                                  request.get_json(silent=True) or {}, usuario)
            lido = svc.ficha(s, colaborador_id)
            s.commit()
        return jsonify({"ok": True, "colaborador": lido})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/colaboradores/<int:colaborador_id>/dependentes/<int:dependente_id>",
          methods=["DELETE"])
@login_obrigatorio
@permissao("editar_colaboradores")
def api_colaborador_dependente_excluir(colaborador_id: int, dependente_id: int):
    from app.apps.erp.core.cadastros import pessoas as svc
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            svc.excluir_dependente(s, colaborador_id, dependente_id, usuario)
            lido = svc.ficha(s, colaborador_id)
            s.commit()
        return jsonify({"ok": True, "colaborador": lido})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/colaboradores/<int:colaborador_id>/<acao>", methods=["POST"])
@login_obrigatorio
@permissao("editar_colaboradores")
def api_colaborador_vinculo(colaborador_id: int, acao: str):
    """Desligar, readmitir, trocar o vínculo e registrar alteração.

    Quatro ações numa rota porque são a MESMA decisão de autorização e mexem na
    mesma história. O `acao` é conferido contra a lista — endereço inventado
    responde 404, não 500.
    """
    from app.apps.erp.core.cadastros import pessoas as svc
    acoes = {"desligar": svc.desligar, "readmitir": svc.readmitir,
             "trocar-vinculo": svc.trocar_vinculo,
             "alteracao": svc.registrar_alteracao}
    fn = acoes.get(acao)
    if fn is None:
        raise ErroNaoEncontrado(f"Ação desconhecida: {acao}")
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            fn(s, colaborador_id, request.get_json(silent=True) or {}, usuario)
            lido = svc.ficha(s, colaborador_id)
            s.commit()
        return jsonify({"ok": True, "colaborador": lido})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha na ação %s do colaborador", acao)
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# CARGOS E CONVENÇÕES — e o piso com histórico
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# A FILA DE SOLICITAÇÕES DO DP (migração 088, 05/11/2026)
#
# O segundo pipe do Pipefy, dentro do ERP. Duas ações: `solicitar_dp` abre e
# acompanha (quem está na obra); `atender_dp` atende e recusa (o DP) — decisão
# do dono: *"todos os pedidos vão para o DP, e quem tiver perfil do DP que vai
# aprovar."*
# ---------------------------------------------------------------------------
@bp.route("/erp/dp/solicitacoes")
@login_obrigatorio
@permissao("solicitar_dp")
def pagina_dp_solicitacoes():
    return render_template("erp_dp_solicitacoes.html",
                           **_contexto("dp_solicitacoes"))


@bp.route("/erp/api/dp/solicitacoes", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="solicitar_dp", POST="solicitar_dp")
def api_dp_solicitacoes():
    """A fila, e abrir pedido novo.

    Quem é preso a obra vê o que é da obra dele mais o que ele mesmo abriu — o
    pedido de cadastro não tem obra ligada até ser atendido, e sem a segunda parte
    quem pediu não acompanharia o próprio pedido.
    """
    from app.apps.erp.core import pessoal_solicitacoes as svc
    from app.apps.erp.core.auth.permissoes import obras_do_usuario
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if request.method == "GET":
                # ⚑ O que o ponto lançou e precisa da contabilidade entra na
                # fila sozinho (20/11/2026) — `core/ponte_do_ponto.py`.
                from app.apps.erp.core import ponte_do_ponto
                if ponte_do_ponto.trazer(s).get("abertas"):
                    s.commit()
                return jsonify({"ok": True, **svc.listar(
                    s, situacao=request.args.get("situacao", ""),
                    tipo=request.args.get("tipo", ""),
                    colaborador_id=request.args.get("colaborador_id", type=int),
                    obra_ids=obras_do_usuario(s, usuario),
                    usuario_id=usuario.id if usuario else None)})
            p = svc.abrir(s, request.get_json(silent=True) or {}, usuario)
            s.commit()
            return jsonify({"ok": True, "solicitacao": p}), 201
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP/DP: falha na fila de solicitações")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/dp/solicitacoes/<int:solicitacao_id>")
@login_obrigatorio
@permissao("solicitar_dp")
def api_dp_solicitacao(solicitacao_id: int):
    from app.apps.erp.core import pessoal_solicitacoes as svc
    from app.apps.erp.core.auth.permissoes import exigir_solicitacao_dp_no_escopo
    try:
        with get_session() as s:
            exigir_solicitacao_dp_no_escopo(s, _usuario_logado(s), solicitacao_id)
            return jsonify({"ok": True, "solicitacao": svc.ler(s, solicitacao_id)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404


@bp.route("/erp/api/dp/solicitacoes/<int:solicitacao_id>/sugestoes")
@login_obrigatorio
@permissao("atender_dp")
def api_dp_solicitacao_sugestoes(solicitacao_id: int):
    """O que a janela de atendimento já traz preenchido (23/11/2026): no
    desligamento de carteira, o último dia e a saída pela forma e pela data do
    aviso, e as faltas lidas do ponto desde o dia seguinte ao corte."""
    from app.apps.erp.core import dp_analise, pessoal_solicitacoes as svc
    from app.apps.erp.core.auth.permissoes import exigir_solicitacao_dp_no_escopo
    try:
        with get_session() as s:
            exigir_solicitacao_dp_no_escopo(s, _usuario_logado(s), solicitacao_id)
            p = svc.ler(s, solicitacao_id)
            sug = (dp_analise.sugestoes_do_desligamento(s, p)
                   if p["tipo"] == "DESLIGAMENTO"
                   else {"valores": {}, "explicacoes": {}})
            return jsonify({"ok": True, **sug})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404


@bp.route("/erp/api/dp/solicitacoes/<int:solicitacao_id>/<acao>", methods=["POST"])
@login_obrigatorio
@permissao("solicitar_dp")
def api_dp_solicitacao_acao(solicitacao_id: int, acao: str):
    """Atender, recusar, cancelar e marcar item do checklist.

    ⚠️ A PERMISSÃO DA ROTA É A MAIS FRACA (`solicitar_dp`), porque CANCELAR é de
    quem abriu. Atender e recusar exigem `atender_dp` por dentro — e isto é a
    exceção que o CLAUDE.md permite: duas ações diferentes na mesma rota, com a
    conferência explícita abaixo. A alternativa seria duas rotas para o mesmo
    objeto, e aí o escopo teria de ser conferido duas vezes.
    """
    from app.apps.erp.core import pessoal_solicitacoes as svc
    from app.apps.erp.core.auth.permissoes import exigir_solicitacao_dp_no_escopo, pode
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            exigir_solicitacao_dp_no_escopo(s, usuario, solicitacao_id)
            # ⚑ OS PASSOS DO DESLIGAMENTO (19/11/2026) são do DP, como atender:
            # marcar o passo, confirmar a continuidade e encerrar o processo.
            if acao in ("atender", "recusar", "item", "passo", "continuidade",
                        "encerrar", "assinar", "assinada", "corrigir",
                        "contabilidade", "termo", "termo_validar",
                        "cancelar_andamento", "despachar", "termo_papel",
                        "devolucao_papel") \
                    and not pode(usuario, "atender_dp"):
                return jsonify({"ok": False, "erro":
                                "Só o DP atende solicitação. Você pode abrir e "
                                "acompanhar o que abriu."}), 403
            if acao == "atender":
                p = svc.atender(s, solicitacao_id, d, usuario)
            elif acao == "recusar":
                p = svc.recusar(s, solicitacao_id, d, usuario)
            elif acao == "cancelar":
                p = svc.cancelar(s, solicitacao_id, d, usuario)
            elif acao == "item":
                p = svc.marcar_item(s, solicitacao_id, int(d.get("item_id") or 0),
                                    bool(d.get("feito")), usuario)
            elif acao == "contabilidade":
                # ⚑ 20/11/2026: monta o pedido à contabilidade desta
                # solicitação, já preenchido (admissão, alteração de cargo,
                # dependente, férias, cancelamento de desligamento).
                from app.apps.erp.core import contabil_processo as svc_contab
                atual = svc.ler(s, solicitacao_id)
                r = svc_contab.montar_pedido(s, atual, usuario)
                s.flush()
                p = svc.ler(s, solicitacao_id)
                p["aviso"] = (f"Pedido {r['numero']} montado. Confira o texto e "
                              "envie em Contabilidade.")
            elif acao == "termo":
                # Gera (de novo) o documento para a pessoa assinar.
                from app.apps.erp.core import desligamento_processo as svc_proc
                atual = svc.ler(s, solicitacao_id)
                if atual["tipo"] == "DESCONTO_EM_FOLHA":
                    svc.gerar_autorizacao_de_desconto(s, atual, usuario)
                elif atual["tipo"] in ("FERIAS", "ALTERACAO_FERIAS"):
                    from app.apps.erp.core import ferias as svc_ferias
                    svc_ferias.gerar_aviso(s, atual, usuario)
                else:
                    svc_proc.gerar_termo(s, atual, usuario)
                s.flush()
                p = svc.ler(s, solicitacao_id)
            elif acao == "termo_validar":
                from app.apps.erp.core import termos
                tid = int(d.get("termo_id") or 0)
                if termos.ler(s, tid).get("solicitacao_id") != solicitacao_id:
                    raise ErroNaoEncontrado("Documento não encontrado.")
                termos.validar(s, tid, usuario, aceitar=bool(d.get("aceitar")),
                               motivo=str(d.get("motivo") or ""))
                svc.termo_voltou(s, tid, usuario)
                s.flush()
                p = svc.ler(s, solicitacao_id)
            elif acao in ("termo_papel", "devolucao_papel"):
                # ⚑ O PLANO B (23/11/2026): o papel assinado chegou ao
                # ESCRITÓRIO — a obra sem internet, a pessoa que assinou à mão
                # e mandou pelo malote. O DP anexa aqui e o processo segue
                # igual ao do portal. Quem anexa é o DP, que está olhando o
                # papel: o documento para assinar já entra validado.
                arq = request.files.get("arquivo")
                if arq is None:
                    raise ErroValidacao("Escolha a foto ou o PDF do papel assinado.")
                svc.papel_no_escritorio(
                    s, solicitacao_id, acao, int(request.form.get("id") or 0),
                    arq.read(), arq.filename or "assinado", usuario)
                s.flush()
                p = svc.ler(s, solicitacao_id)
            elif acao == "cancelar_andamento":
                # ⚑ 20/11/2026: cancelar o que o DP já atendeu — com a
                # contabilidade, se ela já tinha recebido.
                from app.apps.erp.core import contabil_processo as svc_contab
                r = svc_contab.cancelar_em_andamento(
                    s, svc.ler(s, solicitacao_id), str(d.get("motivo") or ""), usuario)
                s.flush()
                p = svc.ler(s, solicitacao_id)
                if r.get("cancelamento"):
                    p["aviso"] = (f"Pedido de cancelamento {r['cancelamento']} "
                                  "montado: envie à contabilidade. O pedido fica "
                                  "em andamento até ela confirmar.")
            elif acao == "despachar":
                numero = svc.despachar(s, solicitacao_id, usuario)
                s.flush()
                p = svc.ler(s, solicitacao_id)
                if numero:
                    p["aviso"] = f"Pedido {numero} montado. Confira e envie."
            elif acao == "pedir_mudanca":
                # Quem pediu avisa que quer cancelar ou mudar (não precisa ser DP).
                p = svc.pedir_mudanca(s, solicitacao_id, d, usuario)
            elif acao == "corrigir":
                # ⚑ 19/11/2026: o DP corrige o pedido (e o pré-cadastro) na
                # própria tela do atendimento, olhando o documento.
                p = svc.corrigir(s, solicitacao_id, d, usuario)
            elif acao in ("passo", "continuidade", "encerrar", "assinar",
                          "assinada"):
                from app.apps.erp.core import desligamento_processo as svc_proc
                atual = svc.ler(s, solicitacao_id)
                if acao == "passo":
                    feito = svc_proc.marcar(s, atual, d, usuario)
                elif acao == "assinar":
                    feito = svc_proc.mandar_para_assinar(s, atual, d, usuario)
                elif acao == "assinada":
                    feito = svc_proc.validar_assinada(s, atual, d, usuario)
                elif acao == "continuidade":
                    feito = svc_proc.confirmar_continuidade(s, atual, d, usuario)
                else:
                    svc_proc.encerrar(s, atual, usuario)
                    feito = {}
                s.flush()
                p = svc.ler(s, solicitacao_id)
                if (feito or {}).get("aviso"):
                    p["aviso"] = feito["aviso"]
            else:
                raise ErroNaoEncontrado(f"Ação desconhecida: {acao}")
            s.commit()
        return jsonify({"ok": True, "solicitacao": p})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP/DP: falha na ação %s da solicitação", acao)
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/cargos")
@login_obrigatorio
@permissao("editar_colaboradores")
def api_cargos():
    """A tabela de cargos COM o piso — por isso não é `ver_pessoal`.

    Piso salarial é a tabela de remuneração da empresa inteira. Quem lança
    despesa de colaborador (supervisor, administrativo de obra, parceiro) não
    precisa dela, e `ver_pessoal` alcança todos eles.
    """
    from app.apps.erp.core.cadastros import cargos as svc
    with get_session() as s:
        return jsonify({"ok": True,
                        **svc.listar(s, so_ativos=request.args.get("todos") != "1")})


@bp.route("/erp/api/cargos/piso", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_cargos_piso():
    """Troca o piso de um cargo numa convenção, abrindo linha no histórico.

    É `configurar` e não `editar_colaboradores`: mexer no piso muda a
    conferência da folha de TODA a gente daquele cargo, não de uma pessoa.
    """
    from app.apps.erp.core.cadastros import cargos as svc
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            cc = svc.definir_piso(
                s, int(d.get("cargo_convencao_id") or 0), salario=d.get("salario"),
                vigencia=_data_ou_nada(d.get("vigencia")),
                valor_diaria=d.get("valor_diaria"), motivo=d.get("motivo") or "",
                usuario=usuario)
            historico = svc.historico_do_piso(s, cc.id)
            s.commit()
        return jsonify({"ok": True, "historico": historico})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/cargos/<int:cargo_convencao_id>/historico")
@login_obrigatorio
@permissao("editar_colaboradores")
def api_cargos_historico(cargo_convencao_id: int):
    from app.apps.erp.core.cadastros import cargos as svc
    with get_session() as s:
        return jsonify({"ok": True,
                        "historico": svc.historico_do_piso(s, cargo_convencao_id)})


@bp.route("/erp/api/cargos/novo", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_cargos_novo():
    """Cadastra o cargo numa convenção — criando os dois se não existirem.

    A tela pede o NOME, não o id: quem está cadastrando o primeiro pedreiro não
    deveria ter de cadastrar o cargo, depois a convenção, depois cruzar os dois.
    A normalização cuida de não criar cargo repetido por caixa ou acento.
    """
    from app.apps.erp.core.cadastros import cargos as svc
    from app.apps.erp.core.comum.auditoria import registrar_evento
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            cargo = svc.achar_ou_criar_cargo(s, d.get("cargo"), cbo=d.get("cbo"))
            conv = svc.achar_ou_criar_convencao(s, d.get("convencao"))
            cc = svc.cruzamento(s, cargo.id, conv.id, criar=True)
            cc.categoria = svc.normalizar_categoria(d.get("categoria"))
            if str(d.get("salario") or "").strip():
                svc.definir_piso(s, cc.id, salario=d["salario"],
                                 vigencia=_data_ou_nada(d.get("vigencia")),
                                 valor_diaria=d.get("valor_diaria"),
                                 motivo=d.get("motivo") or "cadastro pela tela",
                                 usuario=usuario)
            registrar_evento(s, "cargo_convencao", cc.id, "CRIADO",
                             {"cargo": cargo.nome, "convencao": conv.nome},
                             usuario.id)
            s.commit()
        return jsonify({"ok": True, "cargo_convencao_id": cc.id})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao cadastrar cargo na convenção")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/cargos/modelo.xlsx")
@login_obrigatorio
@permissao("configurar")
def api_cargos_modelo():
    """A planilha para importar, já com o que está cadastrado (23/11/2026)."""
    from flask import Response
    from app.apps.erp.core.cadastros import cargos as svc
    with get_session() as s:
        conteudo = svc.planilha_modelo(s)
    return Response(conteudo, mimetype="application/vnd.openxmlformats-"
                    "officedocument.spreadsheetml.sheet",
                    headers={"Content-Disposition":
                             'attachment; filename="cargos_e_convencoes.xlsx"'})


@bp.route("/erp/api/convencoes/resumo")
@login_obrigatorio
@permissao("editar_colaboradores")
def api_convencoes_resumo():
    """Cada convenção com quantos cargos e quantas pessoas tem (23/11/2026)."""
    from app.apps.erp.core.cadastros import cargos as svc
    with get_session() as s:
        return jsonify({"ok": True, "convencoes": svc.convencoes_com_gente(
            s, so_ativas=request.args.get("todos") != "1")})


@bp.route("/erp/api/convencoes/<int:convencao_id>/pessoas")
@login_obrigatorio
@permissao("editar_colaboradores")
def api_convencao_pessoas(convencao_id: int):
    from app.apps.erp.core.cadastros import cargos as svc
    with get_session() as s:
        return jsonify({"ok": True, "pessoas": svc.pessoas_da_convencao(s, convencao_id)})


@bp.route("/erp/api/convencoes/<int:convencao_id>/excluir", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_convencao_excluir(convencao_id: int):
    """Exclui a convenção sem ninguém; desativa a que só tem histórico."""
    from app.apps.erp.core.cadastros import cargos as svc
    try:
        with get_session() as s:
            r = svc.excluir_convencao(s, convencao_id, _usuario_logado(s))
            s.commit()
        return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/cargos/importar", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_cargos_importar():
    """A planilha de cargos do Pipefy, separando cargo de convenção.

    `gravar=0` simula e devolve o relatório sem tocar no banco — é o botão
    "conferir" da tela. Cadastro base roda uma vez e todo mundo vê depois.
    """
    from app.apps.erp.core.importadores import cargos_planilha
    arquivo = request.files.get("arquivo")
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Envie a planilha."}), 400
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            r = cargos_planilha.importar(
                s, arquivo.read(), gravar=request.form.get("gravar") == "1",
                vigencia=_data_ou_nada(request.form.get("vigencia")), usuario=usuario)
            s.commit()
        return jsonify({"ok": True, "relatorio": r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao importar a planilha de cargos")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# O PORTAL DA OBRA — autenticado pela sessão do APLICATIVO DO PONTO
#
# Não há login novo e não há permissão do ERP concedida aqui. Quem não passou
# pelo CPF+PIN do aplicativo do ponto não tem a chave na sessão, e o portal
# responde "entre pelo aplicativo". Detalhe em `core/portal.py`.
#
# `permissao_publica` com o motivo escrito é como o guard do ERP exige declarar
# rota que não usa o login do ERP — e a conferência de quem é acontece dentro,
# na primeira linha de cada função.
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# O DIÁRIO DE OBRA (migração 092)
#
# As MESMAS rotas servem a tela cheia do ERP e a enxuta do portal: o diário é
# um só, e duplicar a regra por porta seria criar duas verdades sobre o mesmo
# dia. Quem muda é a tela, não o que ela chama.
# ---------------------------------------------------------------------------
@bp.route("/erp/obras/diario")
@login_obrigatorio
@permissao("ver_diario")
def pagina_diario():
    return render_template("erp_diario.html", **_contexto("diario"))


def _dia_do_pedido(bruto: str | None):
    from datetime import date as _date
    if not bruto:
        return _date.today()
    try:
        return _date.fromisoformat(str(bruto)[:10])
    except ValueError:
        raise ErroValidacao("Data inválida.")


@bp.route("/erp/api/diario/<int:obra_id>")
@login_obrigatorio
@permissao("ver_diario")
def api_diario_dia(obra_id: int):
    """O dia, já preenchido com tudo o que o sistema sabe."""
    from app.apps.erp.core import diario as svc
    from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
    try:
        with get_session() as s:
            exigir_obra_no_escopo(s, _usuario_logado(s), obra_id)
            return jsonify({"ok": True,
                            **svc.abrir_dia(s, obra_id,
                                            _dia_do_pedido(request.args.get("data")))})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise                # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao abrir o diário da obra %s", obra_id)
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/diario/<int:obra_id>", methods=["POST"])
@login_obrigatorio
@permissao("lancar_diario")
def api_diario_salvar(obra_id: int):
    from app.apps.erp.core import diario as svc
    from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            exigir_obra_no_escopo(s, usuario, obra_id)
            dados = request.get_json(silent=True) or {}
            dia = _dia_do_pedido(dados.get("data"))
            r = svc.salvar(s, obra_id, dia, dados, usuario,
                           motivo=dados.get("motivo") or "")
            s.commit()
            return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha ao gravar o diário da obra %s", obra_id)
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/diario/<int:obra_id>/fechar", methods=["POST"])
@login_obrigatorio
@permissao("lancar_diario")
def api_diario_fechar(obra_id: int):
    from app.apps.erp.core import diario as svc
    from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            exigir_obra_no_escopo(s, usuario, obra_id)
            dados = request.get_json(silent=True) or {}
            r = svc.fechar(s, obra_id, _dia_do_pedido(dados.get("data")), usuario)
            s.commit()
            return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha ao fechar o diário da obra %s", obra_id)
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/diario/<int:obra_id>/fiscalizacao", methods=["POST"])
@login_obrigatorio
@permissao("ver_diario")
def api_diario_fiscalizacao(obra_id: int):
    """A observação da FISCALIZAÇÃO.

    ⚑ A ação exigida é `ver_diario`, NÃO `lancar_diario`, e isso é de
    propósito: quem escreve aqui é a fiscalização do contratante, que entra
    pelo perfil PARCEIRO e não lança diário nenhum. Exigir `lancar_diario`
    trancaria justamente quem este campo existe para ouvir.

    Quem escreve o quê é decidido em `core/diario.py`: esta função só pode
    tocar no campo dela, e `salvar` não pode tocar nele de jeito nenhum.
    """
    from app.apps.erp.core import diario as svc
    from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            exigir_obra_no_escopo(s, usuario, obra_id)
            dados = request.get_json(silent=True) or {}
            r = svc.observar_como_fiscalizacao(
                s, obra_id, _dia_do_pedido(dados.get("data")),
                dados.get("observacao") or "", usuario)
            s.commit()
            return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha na observação da fiscalização")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/diario/<int:obra_id>/extras", methods=["POST"])
@login_obrigatorio
@permissao("lancar_diario")
def api_diario_extra(obra_id: int):
    """O terceirizado que trabalhou e não bate ponto aqui."""
    from app.apps.erp.core import diario as svc
    from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            exigir_obra_no_escopo(s, usuario, obra_id)
            dados = request.get_json(silent=True) or {}
            dia = _dia_do_pedido(dados.get("data"))
            if dados.get("remover"):
                svc.remover_extra(s, obra_id, dia, int(dados["remover"]))
                s.commit()
                return jsonify({"ok": True, "removido": True})
            r = svc.acrescentar_extra(s, obra_id, dia, dados, usuario)
            s.commit()
            return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha no efetivo extra do diário")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/diario/<int:obra_id>/equipamentos", methods=["POST"])
@login_obrigatorio
@permissao("lancar_diario")
def api_diario_equipamentos(obra_id: int):
    """A lista de equipamento em obra: acrescentar, apagar, ou repetir a do
    último dia que tinha (retroescavadeira não vai embora à noite)."""
    from app.apps.erp.core import diario as svc
    from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            exigir_obra_no_escopo(s, usuario, obra_id)
            dados = request.get_json(silent=True) or {}
            dia = _dia_do_pedido(dados.get("data"))
            if dados.get("remover"):
                svc.remover_equipamento(s, obra_id, dia, int(dados["remover"]))
                s.commit()
                return jsonify({"ok": True, "removido": True})
            if dados.get("repetir_ontem"):
                r = svc.repetir_equipamentos_de_ontem(s, obra_id, dia)
                s.commit()
                return jsonify({"ok": True, **r})
            r = svc.acrescentar_equipamento(s, obra_id, dia, dados)
            s.commit()
            return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha nos equipamentos do diário")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/diario/<int:obra_id>/sugerir-texto", methods=["POST"])
@login_obrigatorio
@permissao("lancar_diario")
def api_diario_sugerir_texto(obra_id: int):
    """A IA lê o que foi escrito e devolve uma versão melhor — para a pessoa
    editar e gravar, nunca para gravar sozinha.

    ⚑ A OBSERVAÇÃO DA FISCALIZAÇÃO NÃO PASSA POR AQUI, e a recusa mora na
    regra (`diario_texto.CAMPOS`, lista fechada), não nesta rota: assim ela
    vale para qualquer caminho que venha a chamar a sugestão.
    """
    from app.apps.erp.core import diario_texto as svc_texto
    from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            # O escopo é conferido mesmo não gravando nada: o texto é da obra,
            # e quem não alcança a obra não lê o relato dela nem para melhorar.
            exigir_obra_no_escopo(s, usuario, obra_id)
            quem = usuario.id if usuario else None
    except ErroNaoEncontrado:
        raise
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    dados = request.get_json(silent=True) or {}
    # ⚑ FORA do `try`, pelo mesmo motivo: a recusa por teto é 402, e o
    # `except Exception` abaixo a viraria "falha do sistema".
    _exigir_saldo_de_ia()
    try:
        r = svc_texto.sugerir(dados.get("campo"), dados.get("texto"),
                              usuario_id=quem)
        return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha na sugestão de texto do diário")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/diario/importar", methods=["POST"])
@login_obrigatorio
@permissao("importar")
def api_diario_importar():
    """Carga dos diários já escritos, de uma planilha .xlsx.

    ⚑ `simular=1` é o PADRÃO: sem mandar `gravar=1` de propósito, nada é
    escrito. Importação de mil linhas que dá errado no meio é trabalho de dias
    para desfazer, e a conferência custa um clique.

    `mapa` é {código da planilha: id da obra}, para o dono amarrar à mão o
    código que não casou sozinho. A planilha NÃO cria obra.
    """
    from app.apps.erp.core import diario_importacao as imp
    arquivo = request.files.get("arquivo")
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Escolha o arquivo .xlsx."}), 400
    nome = (arquivo.filename or "").lower()
    if not nome.endswith(".xlsx"):
        return jsonify({"ok": False, "erro":
                        "O arquivo precisa ser .xlsx. Se for .xls antigo, "
                        "abra no Excel e salve como .xlsx."}), 400
    gravar = request.form.get("gravar") in ("1", "true", "sim")
    mapa = {}
    bruto = request.form.get("mapa") or ""
    if bruto.strip():
        import json as _json
        try:
            mapa = {str(k): int(v) for k, v in _json.loads(bruto).items() if v}
        except Exception:                                      # noqa: BLE001
            return jsonify({"ok": False,
                            "erro": "Não entendi o apontamento das obras."}), 400
    try:
        conteudo = arquivo.read()
        with get_session() as s:
            usuario = _usuario_logado(s)
            rel = imp.importar(s, conteudo,
                               usuario.id if usuario else None,
                               simular=not gravar, mapa_obras=mapa)
            if gravar:
                s.commit()
        return jsonify({"ok": True, "resultado": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha na importação de diários")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/obras/cdiarios", methods=["POST"])
@login_obrigatorio
@permissao("importar")
def api_obras_cdiarios():
    """Completa o cadastro das obras com a C. Diários (aba Centro de Custo).

    ⚑ `simular` é o PADRÃO, como em toda importação da casa: sem `gravar=1` de
    propósito, nada é escrito. E o que já está preenchido no cadastro só é
    trocado com `regravar=1` — o que alguém digitou aqui vale mais que a
    planilha, e quando os dois divergem o relatório mostra os dois lados.
    """
    from app.apps.erp.core.importadores import obras_cdiarios as imp
    arquivo = request.files.get("arquivo")
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Escolha o arquivo .xlsx."}), 400
    if not (arquivo.filename or "").lower().endswith(".xlsx"):
        return jsonify({"ok": False, "erro":
                        "O arquivo precisa ser .xlsx. No Google Planilhas: "
                        "Arquivo › Fazer download › Microsoft Excel."}), 400
    gravar = request.form.get("gravar") in ("1", "true", "sim")
    regravar = request.form.get("regravar") in ("1", "true", "sim")
    try:
        conteudo = arquivo.read()
        with get_session() as s:
            usuario = _usuario_logado(s)
            rel = imp.importar(s, conteudo, usuario.id if usuario else None,
                               simular=not gravar, regravar=regravar)
            if gravar:
                s.commit()
        return jsonify({"ok": True, "resultado": rel})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao completar obras pela C. Di\u00e1rios")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/diario/quadro")
@login_obrigatorio
@permissao("ver_diario")
def api_diario_quadro():
    """O mês inteiro de TODAS as obras: linhas de obra, colunas de dia.

    Pedido dele em 15/11/2026: *"aqui era para mostrar por mês, todas as obras
    (…) cada coluna é um dia (…) você bate o olho e já visualiza exatamente
    todos os dias que estão faltando (…) clicar e já ir direto para o dia."*

    ⚑ RESPEITA O ESCOPO: quem é preso a obras vê só as dele. Sem isto, o
    quadro seria a porta dos fundos para a lista inteira de obras da empresa.
    """
    from datetime import date as _date

    from app.apps.erp.core import diario as svc
    from app.apps.erp.core.auth.permissoes import obras_do_usuario
    try:
        hoje = _date.today()
        ano = request.args.get("ano", type=int) or hoje.year
        mes = request.args.get("mes", type=int) or hoje.month
        with get_session() as s:
            minhas = obras_do_usuario(s, _usuario_logado(s))
            # ⚑ ?todas=1 traz também as obras em fase de conclusão. A tela
            # inicial NÃO manda isso: ele, em 19/11/2026, *"a tela inicial
            # exibe só as obras ativas. As obras concluídas ficam ocultas. Aí
            # eu altero o filtro e visualizo todas, se eu quiser."*
            return jsonify({"ok": True, **svc.quadro_do_mes(
                s, ano, mes, obra_ids=minhas,
                incluir_encerradas=request.args.get("todas") == "1")})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha no quadro do mês do diário")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/diario/<int:obra_id>/fotos", methods=["POST"])
@login_obrigatorio
@permissao("lancar_diario")
def api_diario_fotos(obra_id: int):
    """Anexa uma foto ao dia — ou troca a legenda, ou tira a foto.

    Pedido dele em 14/11/2026: *"poder colocar serviços executados e fotos, e
    integrar isso no diário impresso. (…) Lembrando que fotos sempre no Google
    Drive."* E vão: a foto passa pelo armazenamento de sempre, que comprime,
    manda para o Drive e só cai para o banco se o Drive não responder.

    Três verbos numa rota só porque a tela trata as três coisas no mesmo
    bloco; o que muda é o corpo. Upload vem como `multipart`, os outros dois
    como JSON.
    """
    from app.apps.erp.core import diario_fotos as svc
    from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            exigir_obra_no_escopo(s, usuario, obra_id)
            dia = _dia_do_pedido(request.args.get("data"))
            arquivo = request.files.get("foto")
            if arquivo is not None:
                r = svc.anexar(s, obra_id, dia, arquivo.read(),
                               arquivo.filename or "foto.jpg",
                               legenda=request.form.get("legenda", ""),
                               usuario=usuario)
                s.commit()
                return jsonify({"ok": True, **r})
            d = request.get_json(silent=True) or {}
            if d.get("remover"):
                svc.remover(s, obra_id, dia, int(d["remover"]), usuario)
            elif d.get("legendar"):
                svc.legendar(s, obra_id, dia, int(d["legendar"]),
                             d.get("legenda") or "", usuario)
            else:
                return jsonify({"ok": False,
                                "erro": "Não entendi o que fazer com a foto."}), 400
            s.commit()
        return jsonify({"ok": True})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha na foto do diário")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/diario/<int:obra_id>/repetir", methods=["POST"])
@login_obrigatorio
@permissao("lancar_diario")
def api_diario_repetir(obra_id: int):
    """Copia um dia escrito para outros dias que ainda não têm diário.

    Pedido dele em 14/11/2026: *"esse dia aqui foi igual aos 3 próximos. Ou
    então esse sábado foi igual a outros 4 sábados."*

    ⚑ NUNCA SOBRESCREVE dia já escrito, não escreve o futuro e não escreve
    fora do prazo da obra. Os pulados voltam ditos, com o motivo de cada um —
    "copiei 3 de 5" sem dizer quais é pior que não copiar.
    """
    from app.apps.erp.core import diario as svc
    from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            exigir_obra_no_escopo(s, usuario, obra_id)
            origem = _dia_do_pedido(d.get("de"))
            # Dois caminhos: os atalhos ("os 3 próximos", "os 4 sábados") e a
            # lista marcada no calendário. O atalho só SUGERE — quem confirma
            # manda a lista, e é ela que vale.
            if d.get("sugerir"):
                return jsonify({"ok": True, "dias": svc.dias_para_repetir(
                    s, obra_id, origem, str(d.get("modo") or "seguidos"),
                    int(d.get("quantos") or 1))})
            destinos = []
            for bruto in (d.get("dias") or []):
                destinos.append(_dia_do_pedido(bruto))
            r = svc.repetir_dia(s, obra_id, origem, destinos, usuario,
                                com_equipamentos=bool(d.get("com_equipamentos")))
            s.commit()
        return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha ao repetir o dia do diário")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/diario/<int:obra_id>/calendario")
@login_obrigatorio
@permissao("ver_diario")
def api_diario_calendario(obra_id: int):
    """O mês inteiro: o que tem diário, o que falta, o que não era para ter."""
    from datetime import date as _date

    from app.apps.erp.core import diario as svc
    from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
    try:
        hoje = _date.today()
        ano = request.args.get("ano", type=int) or hoje.year
        mes = request.args.get("mes", type=int) or hoje.month
        if not 1 <= mes <= 12:
            return jsonify({"ok": False, "erro": "Mês inválido."}), 400
        with get_session() as s:
            exigir_obra_no_escopo(s, _usuario_logado(s), obra_id)
            return jsonify({"ok": True, **svc.calendario(s, obra_id, ano, mes)})
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha no calendário do diário")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/obras/diario/papel/<int:obra_id>")
@login_obrigatorio
@permissao("ver_diario")
def pagina_diario_papel(obra_id: int):
    """O RDO para imprimir — página própria, feita para a folha A4.

    Sem o `erp_base.html` de propósito: menu de 18 telas e barra azul não
    existem num documento que vai ser assinado pela fiscalização. Quem gera o
    PDF é o navegador (Ctrl+P › Salvar como PDF), e é por isso que o modelo é
    uma folha de estilo e não milímetro de biblioteca — cada órgão exige o
    seu, e trocar de modelo tem de ser trocar de CSS.
    """
    from app.apps.erp.core import diario as svc
    from app.apps.erp.core.auth.permissoes import exigir_obra_no_escopo
    with get_session() as s:
        exigir_obra_no_escopo(s, _usuario_logado(s), obra_id)
        de = _dia_do_pedido(request.args.get("de"))
        ate = _dia_do_pedido(request.args.get("ate") or request.args.get("de"))
        # `modelo` é ESPIADA, não configuração: serve para ver os três
        # formatos lado a lado. O que vale sai do cadastro da obra, porque o
        # modelo é cláusula do contrato.
        r = svc.relatorio(s, obra_id, de, ate,
                          so_escritos=request.args.get("todos") != "1",
                          modelo=request.args.get("modelo"))
    return render_template("erp_diario_papel.html", r=r)


@bp.route("/erp/api/diario/procurar")
@login_obrigatorio
@permissao("ver_diario")
def api_diario_procurar():
    """Acha o diário pelo que foi ESCRITO nele.

    Ele, em 10/11/2026: *"será que dá para a gente pesquisar? Eu quero
    localizar um diário através de alguma informação que eu relatei."*

    É a pergunta certa a fazer de um diário: ninguém lembra a DATA em que a
    laje do bloco B foi concretada — lembra que foi concretada.

    O escopo vale aqui: quem é preso a obra só acha diário das obras dele.
    """
    from app.apps.erp.core import diario as svc
    from app.apps.erp.core.auth.permissoes import obras_do_usuario
    try:
        with get_session() as s:
            permitidas = obras_do_usuario(s, _usuario_logado(s))
            obra = request.args.get("obra_id")
            return jsonify({"ok": True, **svc.procurar(
                s, request.args.get("q") or "",
                obra_id=int(obra) if obra else None,
                obra_ids=(list(permitidas) if permitidas is not None else None),
                de=(_dia_do_pedido(request.args.get("de"))
                    if request.args.get("de") else None),
                ate=(_dia_do_pedido(request.args.get("ate"))
                     if request.args.get("ate") else None))})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao procurar no diário")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/diario/pendencias")
@login_obrigatorio
@permissao("ver_diario")
def api_diario_pendencias():
    """Por qual obra começar — a visão de quem olha todas."""
    from app.apps.erp.core import diario as svc
    from app.apps.erp.core.auth.permissoes import obras_do_usuario
    try:
        with get_session() as s:
            permitidas = obras_do_usuario(s, _usuario_logado(s))
            return jsonify({"ok": True, "obras": svc.pendencias_por_obra(
                s, obra_ids=list(permitidas) if permitidas is not None else None,
                incluir_encerradas=request.args.get("todas") == "1")})
    except Exception as e:
        logger.exception("ERP: falha nas pendências de diário")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/portal")
@login_obrigatorio
@permissao("ver_erp")
def pagina_portal():
    """A TELA ENXUTA — pedir e acompanhar, no celular, dentro do ERP.

    MUDOU DE DONO EM 07/11/2026, não de endereço. Ela era PÚBLICA e lia a
    sessão do aplicativo do ponto; agora exige o login DO ERP e declara ação,
    como qualquer outra rota. O endereço continua `/erp/portal` — cheguei a
    trocá-lo para `/erp/pedir` e o dono preferiu o antigo ("fica ruim, melhor
    /erp/portal"), o que é melhor mesmo: endereço que a obra já pode ter
    salvo no celular não se muda sem motivo, e o motivo aqui era a PORTA, não
    o nome dela.

    A ação é `ver_erp` — a mais baixa que existe — de propósito: QUALQUER um
    que entre no sistema pode abrir esta tela. O que ele vê dentro dela é que
    muda: quem não tem `solicitar_dp` (o PARCEIRO, por exemplo) não ganha o
    formulário, só a lista do que já existe. A tela não decide isso sozinha —
    pergunta a `core/auth/permissoes.py`, como todo o resto.

    A página continua SEM o `erp_base.html`: o menu de 18 telas num celular,
    para quem só quer fazer um pedido, é o oposto de "ambiente mais enxuto".
    Quem quiser o ERP inteiro vai nele — está a um endereço de distância.
    """
    # ⚑ O link "ERP completo" só existe para quem tem mais que o portal
    # (17/11/2026). Quem só pede e acompanha não precisa de um botão que leva
    # a um menu de 18 telas — leva a se perder.
    with get_session() as _s:
        quem = _usuario_logado(_s)
        from app.apps.erp.core.auth.permissoes import pode as _pode
        mais = any(_pode(quem, a) for a in
                   ("ver_titulos", "ver_pessoal", "ver_obras", "ver_diario",
                    "ver_suprimentos", "configurar"))
    return render_template("erp_portal.html", pode_mais_que_o_portal=mais)


@bp.route("/erp/portal/api/inicio")
@login_obrigatorio
@permissao("ver_erp")
def api_portal_inicio():
    from app.apps.erp.core import portal as svc_portal
    try:
        with get_session() as s:
            # O USUÁRIO VEM DAQUI, JÁ COM AS EXCEÇÕES DE PERMISSÃO DENTRO.
            # Recarregá-lo pelo ORM lá na regra traria um usuário sem elas, e
            # quem teve permissão concedida ou tirada no cadastro passaria a
            # ser julgado só pelo cargo — em silêncio.
            usuario = _usuario_logado(s)
            eu = svc_portal.quem_esta(s, usuario)
            return jsonify({
                "ok": True,
                "nome": eu["nome"],
                "pode_pedir": svc_portal.pode_pedir(s, eu),
                "administrativo": svc_portal.pode_pedir_por_outros(s, eu),
                "obras": svc_portal.obras_que_alcanca(s, eu),
                "areas": svc_portal.areas(s, eu),
                "tipos": svc_portal.tipos(s),
                "pessoas": svc_portal.pessoas_ao_alcance(s, usuario),
                "obras_lista": svc_portal.obras_para_escolher(s, usuario),
                # A obra onde a pessoa bate ponto, já escolhida na caixinha
                # (17/11/2026). Sugestão: a lista continua aberta.
                "obra_sugerida": svc_portal.obra_sugerida(s, usuario),
                "listas": svc_portal.listas_fechadas(s),
                "pedidos": svc_portal.meus_pedidos(s, usuario),
                "ferias": svc_portal.ferias_da_equipe(s, usuario),
                # Os documentos para a pessoa assinar (20/11/2026, noite).
                "termos": svc_portal.termos_para_assinar(s, usuario),
                # Experiência terminando (20/11/2026): o aviso para a obra.
                "experiencias": svc_portal.experiencias_da_equipe(s, usuario),
                "devolucoes": svc_portal.minhas_devolucoes(s, usuario)})
    except svc_portal.SemAcesso as e:
        return jsonify({"ok": False, "erro": str(e)}), 401


@bp.route("/erp/portal/api/pessoas")
@permissao_publica("o portal tem autenticação própria, do ponto — ver "
                   "core/portal.quem_esta")
def api_portal_pessoas():
    """Procura a pessoa por NOME ou por CPF, dentro do alcance de quem pede.

    ⚑ NO SERVIDOR, e não filtrando a lista que já desceu: a caixinha trazia
    até 500 pessoas, e pôr o CPF de todas no navegador para poder filtrar ali
    seria expor o cadastro a quem abrir o código da página. Aqui só aparece
    quem já se sabia procurar, e o CPF volta com os três últimos dígitos.
    """
    from app.apps.erp.core import portal as svc_portal
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            svc_portal.quem_esta(s, usuario)            # 401 se não entrou
            achados = svc_portal.procurar_pessoas(
                s, usuario, request.args.get("q", ""),
                # A admissão procura também quem JÁ SAIU — é a readmissão.
                desligados=(request.args.get("desligados") == "1"
                            and request.args.get("tipo", "") == "CADASTRO"),
                # O tipo de pedido pode valer só para um vínculo — a admissão
                # de prestador, por exemplo. A tela diz qual; o servidor
                # confere contra a regra, para a tela não poder alargar.
                so_vinculo=svc_portal.vinculo_do_tipo(
                    request.args.get("tipo", "")))
        return jsonify({"ok": True, "pessoas": achados})
    except svc_portal.SemAcesso as e:
        return jsonify({"ok": False, "erro": str(e)}), 401
    except Exception as e:
        logger.exception("ERP/portal: falha ao procurar pessoa")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/portal/api/conferir-cpf")
@permissao_publica("o portal tem autenticação própria, do ponto — ver "
                   "core/portal.quem_esta")
def api_portal_conferir_cpf():
    """"Esta pessoa já existe?" — respondido ENQUANTO o CPF é digitado.

    ⚑ FALTAVA NO PORTAL, e ele perguntou por isso em 15/11/2026, olhando o
    formulário de cadastro: *"o nome está pesquisável, o CNPJ, o CPF vai ser
    criticado se a pessoa já existir?"*

    A resposta era NÃO — e isso é o pior caso: a obra preenchia o cadastro
    inteiro, anexava os três documentos, e só ao enviar descobria que a pessoa
    já existia. Na tela do ERP o aviso já chegava no quarto dígito desde
    06/11/2026; no portal, que é por onde a obra trabalha, não chegava nunca.

    ⚑ O QUE ELA DEVOLVE É O MÍNIMO: existe, o nome, e se está ativa ou
    desligada. Nada de salário, endereço ou documento — quem abre pedido não
    precisa disso, e varrer CPFs não mapearia o cadastro.
    """
    from app.apps.erp.core import portal as svc_portal
    from app.apps.erp.core.cadastros import pessoas as svc_pessoas
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            svc_portal.quem_esta(s, usuario)            # 401 se não entrou
            achado = svc_pessoas.conferir_cpf(s, request.args.get("cpf", ""))
            # ⚑ NA READMISSÃO, O QUE O CADASTRO JÁ TEM (19/11/2026) — só os
            # NOMES dos campos preenchidos, nunca os valores: quem digitou o
            # CPF está com a pessoa ao lado, mas o RG e o endereço antigos não
            # descem para o celular. A tela deixa de exigir o que já existe.
            tem: list[str] = []
            if achado["situacao"] == "desligado" and achado.get("colaborador_id"):
                from app.apps.erp.core import pessoal_solicitacoes as svc_sp
                tem = sorted(svc_sp.o_que_o_cadastro_ja_tem(
                    s, "CADASTRO", int(achado["colaborador_id"])))
        # O recado completo do cadastro traz "abra a ficha dela" — coisa que
        # quem está na obra não pode fazer. A frase daqui é a do portal.
        return jsonify({"ok": True, "situacao": achado["situacao"],
                        "nome": achado.get("nome", ""),
                        "aviso": achado.get("aviso", ""), "tem": tem})
    except svc_portal.SemAcesso as e:
        return jsonify({"ok": False, "erro": str(e)}), 401
    except Exception as e:
        logger.exception("ERP/portal: falha ao conferir o CPF")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/portal/api/ler-documento", methods=["POST"])
@permissao_publica("o portal tem autenticação própria, do ponto — ver "
                   "core/portal.quem_esta")
def api_portal_ler_documento():
    """Lê o papel que acabou de ser fotografado e devolve o que entendeu.

    ⚑ AVISA, NÃO BARRA. A leitura preenche campo vazio e aponta o que parece
    errado ("você pôs a CTPS no lugar do comprovante de endereço"), mas quem
    decide é a pessoa: a IA erra, e um aviso que impede o envio vira uma
    parede justamente quando ela erra. O único bloqueio objetivo, esse no
    servidor, é não ter anexado nada.

    ⚑ E A CONFERÊNCIA ACONTECE NA HORA, que é o ponto todo: com a pessoa ainda
    ao lado do colaborador e os papéis na mão. Hoje o DP descobre o problema
    três dias depois, quando a obra já dispersou.
    """
    from app.apps.erp.core import portal as svc_portal
    from app.apps.erp.core import documentos_de_pessoa as svc_doc
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            svc_portal.quem_esta(s, usuario)            # 401 se não entrou
    except svc_portal.SemAcesso as e:
        return jsonify({"ok": False, "erro": str(e)}), 401
    arquivo = request.files.get("arquivo")
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Nenhum arquivo."}), 400
    # ⚑ O TETO DE IA DO MÊS, e FORA do `try`: a recusa por teto tem resposta
    # própria (402), e dentro do `try` o `except Exception` a transformaria em
    # "falha do sistema". Foi uma trava do repositório que pegou esta
    # omissão — a rota gastava IA sem olhar o teto.
    _exigir_saldo_de_ia()
    try:
        r = svc_doc.ler(arquivo.read(), arquivo.filename or "",
                        str(request.form.get("tipo") or ""))
        return jsonify({"ok": True, **r})
    except Exception as e:
        logger.exception("ERP/portal: falha ao ler o documento")
        # ⚑ 200 de propósito: a tela trata "não consegui ler" como ANEXADO,
        # e não como falha do anexo. O papel já subiu.
        return jsonify({"ok": False, "erro": recado_de_falha(e)})


@bp.route("/erp/portal/api/ler-documentos", methods=["POST"])
@permissao_publica("o portal tem autenticação própria, do ponto — ver "
                   "core/portal.quem_esta")
def api_portal_ler_documentos():
    """Lê VÁRIOS papéis numa leitura só e devolve o que cada um é e os campos.

    ⚑ 19/11/2026 — ele: *"eu quero jogar o RG, jogar a CNH, o comprovante de
    endereço, o CPF, e o sistema fazer a leitura de todas essas informações."*
    Uma chamada de IA por lote, não por arquivo (`documentos_de_pessoa`).
    """
    from app.apps.erp.core import portal as svc_portal
    from app.apps.erp.core import documentos_de_pessoa as svc_doc
    try:
        with get_session() as s:
            svc_portal.quem_esta(s, _usuario_logado(s))     # 401 se não entrou
    except svc_portal.SemAcesso as e:
        return jsonify({"ok": False, "erro": str(e)}), 401
    arquivos = [(f.read(), f.filename or "documento")
                for f in request.files.getlist("arquivos")][:svc_doc.TETO_DE_ARQUIVOS]
    if not arquivos:
        return jsonify({"ok": False, "erro": "Nenhum arquivo."}), 400
    _exigir_saldo_de_ia()
    try:
        from app.apps.erp.core.comum.ia_custo import contexto
        with contexto(operacao="leitura_documentos_admissao"):
            r = svc_doc.ler_varios(arquivos)
        return jsonify({"ok": True, **r})
    except Exception as e:
        logger.exception("ERP/portal: falha ao ler o lote de documentos")
        # 200: a tela trata como "anexado, não consegui ler" e pede digitação.
        return jsonify({"ok": False, "erro": recado_de_falha(e)})


@bp.route("/erp/portal/api/pedidos", methods=["POST"])
@login_obrigatorio
@permissao("solicitar_dp")
def api_portal_abrir():
    """Abre o pedido. Com papéis, vem como formulário (`pedido` em JSON e os
    arquivos em `papeis`, com o tipo de cada um em `tipos`, na mesma ordem) —
    e o pedido e os papéis são gravados na MESMA transação: ou os dois, ou
    nenhum. Pedido de admissão sem os papéis é o que não pode existir."""
    import json as _json
    from app.apps.erp.core import pessoal_solicitacoes as svc_sp
    from app.apps.erp.core import portal as svc_portal
    try:
        if request.files:
            dados = _json.loads(request.form.get("pedido") or "{}")
            tipos_dos_papeis = request.form.getlist("tipos")
            arquivos = [{"conteudo": f.read(), "nome": f.filename or "documento",
                         "tipo": (tipos_dos_papeis[i] if i < len(tipos_dos_papeis)
                                  else "")}
                        for i, f in enumerate(request.files.getlist("papeis"))]
        else:
            dados = request.get_json(silent=True) or {}
            arquivos = []
        with get_session() as s:
            r = svc_portal.abrir(s, dados, _usuario_logado(s))
            if arquivos:
                from sqlalchemy import text as _text
                cid = s.execute(_text("SELECT colaborador_id FROM dp_solicitacoes "
                                      " WHERE id = :i"), {"i": r["id"]}).scalar()
                r["papeis_guardados"] = svc_sp.guardar_papeis(
                    s, r["id"], cid, arquivos)
            s.commit()
            return jsonify({"ok": True, **r})
    except svc_portal.SemAcesso as e:
        return jsonify({"ok": False, "erro": str(e)}), 401
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha ao abrir pedido pelo portal")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/portal/api/devolucoes/<int:devolucao_id>", methods=["POST"])
@login_obrigatorio
@permissao("solicitar_dp")
def api_portal_devolver(devolucao_id: int):
    """A obra devolve o documento assinado. É o "tem que ser via sistema"."""
    from app.apps.erp.core import portal as svc_portal
    arquivo = request.files.get("arquivo")
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Escolha o arquivo."}), 400
    try:
        with get_session() as s:
            r = svc_portal.devolver(s, devolucao_id, arquivo.read(),
                                    _usuario_logado(s),
                                    arquivo.filename or "documento")
            s.commit()
            return jsonify({"ok": True, **r})
    except svc_portal.SemAcesso as e:
        return jsonify({"ok": False, "erro": str(e)}), 401
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha ao devolver documento pelo portal")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# O DOCUMENTO QUE O COLABORADOR ASSINA (20/11/2026, noite) — `core/termos.py`.
# Quem conduz é quem alcança a obra (o administrativo); a pessoa assina com o
# PIN dela. Fora do alcance responde "não encontrado".
# ---------------------------------------------------------------------------
@bp.route("/erp/portal/api/termos/<int:termo_id>", methods=["GET", "POST"])
@login_obrigatorio
@permissao("solicitar_dp")
def api_portal_termo(termo_id: int):
    from app.apps.erp.core import pessoal_solicitacoes as svc_sp
    from app.apps.erp.core import portal as svc_portal
    from app.apps.erp.core import termos
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            eu = svc_portal.quem_esta(s, usuario)
            obras = svc_portal.obras_que_alcanca(s, eu)
            if request.method == "GET":
                return jsonify({"ok": True,
                                "termo": termos.para_assinar(s, termo_id, obras)})
            conduz = {"colaborador_id": eu.get("colaborador_id"), "nome": eu["nome"]}
            if request.files.get("arquivo") is not None:
                arq = request.files["arquivo"]
                r = termos.enviar_papel(s, termo_id, obras, conteudo=arq.read(),
                                        nome_arquivo=arq.filename or "assinado",
                                        conduzido_por=conduz)
            else:
                d = request.get_json(silent=True) or {}
                if d.get("acao") == "recusa":
                    r = termos.registrar_recusa(
                        s, termo_id, obras, motivo=str(d.get("motivo") or ""),
                        testemunhas=d.get("testemunhas") or [], conduzido_por=conduz)
                else:
                    r = termos.assinar(
                        s, termo_id, obras, pin=str(d.get("pin") or ""),
                        assinatura=d.get("assinatura"), conduzido_por=conduz,
                        latitude=d.get("latitude"), longitude=d.get("longitude"),
                        ip=request.headers.get("X-Forwarded-For", request.remote_addr or ""))
            svc_sp.termo_voltou(s, termo_id, None)
            s.commit()
            return jsonify({"ok": True, **r})
    except svc_portal.SemAcesso as e:
        return jsonify({"ok": False, "erro": str(e)}), 401
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha no documento para assinar")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ---------------------------------------------------------------------------
# O LADO DO DP: as devoluções e a caixa de entrada
# ---------------------------------------------------------------------------
@bp.route("/erp/api/dp/devolucoes", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_pessoal", POST="pedir_contabilidade")
def api_dp_devolucoes():
    from app.apps.erp.core import portal as svc_portal
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if request.method == "GET":
                return jsonify({"ok": True, "devolucoes": svc_portal.devolucoes_abertas(
                    s, atrasadas=request.args.get("atrasadas") == "1")})
            r = svc_portal.abrir_devolucao(
                s, request.get_json(silent=True) or {}, usuario)
            s.commit()
            return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha nas devoluções da obra")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/dp/devolucoes/<int:devolucao_id>/cobrar", methods=["POST"])
@login_obrigatorio
@permissao("pedir_contabilidade")
def api_dp_devolucao_cobrar(devolucao_id: int):
    from app.apps.erp.core import portal as svc_portal
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            r = svc_portal.cobrar_devolucao(s, devolucao_id, usuario)
            s.commit()
            return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha ao cobrar a devolução")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ⚑ QUEM CONFIGURA A CAIXA É O DP (18/11/2026), não só o administrador. Ele:
# *"como a gente vai monitorar a caixa de mensagens, eu tenho que associar a
# caixa própria. Eu tenho que configurar isso no próprio DP."*
#
# Ela JÁ ERA a caixa do DP na prática (é ela que o agente lê para casar resposta
# com pedido da contabilidade) — o que faltava era quem a configura ser quem a
# usa. `configurar` é ADMIN; isso deixava o DP dependendo dele para uma caixa
# que é do DP.
@bp.route("/erp/api/dp/caixa", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_pessoal", POST="pedir_contabilidade")
def api_dp_caixa():
    """A caixa de entrada que o agente lê. A SENHA nunca sai na resposta."""
    from app.apps.erp.core.comum import email_entrada
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if request.method == "GET":
                from sqlalchemy import text as _text
                lidas = s.execute(_text("""
                    SELECT message_id, de, assunto, situacao, motivo, anexos,
                           arquivados, processado_em, pedido_id
                      FROM email_entrada_mensagens
                     ORDER BY processado_em DESC LIMIT 100""")).mappings().all()
                return jsonify({
                    "ok": True,
                    "config": email_entrada.configuracao(s),
                    "mensagens": [{**{k: (str(v) if k == "processado_em" else v)
                                      for k, v in dict(l).items()}}
                                  for l in lidas]})
            r = email_entrada.configurar(
                s, request.get_json(silent=True) or {}, usuario)
            s.commit()
            return jsonify({"ok": True, "config": r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha na caixa de entrada")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/dp/caixa/<acao>", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_dp_caixa_acao(acao: str):
    """testar · ler (uma rodada agora) · conferir (uma rodada SEM gravar)."""
    from app.apps.erp.core.comum import email_entrada
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if acao == "testar":
                r = email_entrada.testar(s)
            elif acao in ("ler", "conferir"):
                r = email_entrada.rodada(s, usuario, gravar=(acao == "ler"))
            else:
                return jsonify({"ok": False, "erro": "Ação desconhecida."}), 400
            s.commit()
            return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except email_entrada.ErroDeLeitura as e:
        return jsonify({"ok": False, "erro": str(e)}), 502
    except Exception as e:
        logger.exception("ERP: falha ao ler a caixa de entrada")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/dp/contabilidade")
@login_obrigatorio
@permissao("ver_pessoal")
def pagina_dp_contabilidade():
    return render_template("erp_dp_contabilidade.html",
                           **_contexto("dp_contabilidade"))


# ---------------------------------------------------------------------------
# A PROGRAMAÇÃO DE FÉRIAS (migração 116, 20/11/2026) — `core/ferias.py`.
# Quem abre pedido ao DP vê as férias das obras que alcança (o administrativo
# de obra, que cuida do ponto); o DP vê todas.
# ---------------------------------------------------------------------------
@bp.route("/erp/dp/ferias")
@login_obrigatorio
@permissao("solicitar_dp")
def pagina_dp_ferias():
    return render_template("erp_dp_ferias.html", **_contexto("dp_ferias"))


@bp.route("/erp/api/dp/ferias")
@login_obrigatorio
@permissao("solicitar_dp")
def api_dp_ferias():
    from app.apps.erp.core import ferias as svc_ferias
    from app.apps.erp.core.auth.permissoes import obras_do_usuario
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            obras = obras_do_usuario(s, usuario)
            passadas = request.args.get("passadas") == "1"
            hoje = date.today()
            lista = svc_ferias.listar(
                s, obras=obras, com_canceladas=passadas,
                desde=hoje - timedelta(days=365 if passadas else 0))
            return jsonify({"ok": True, "ferias": lista,
                            "avisar_com_dias": svc_ferias.AVISAR_COM_DIAS,
                            "tabela": svc_ferias.tem_tabela(s)})
    except Exception as e:
        logger.exception("ERP/DP: falha na programação de férias")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/dp/ferias/periodos")
@login_obrigatorio
@permissao("solicitar_dp")
def api_dp_ferias_periodos():
    """Os períodos de férias de cada pessoa de carteira — para não vencerem."""
    from app.apps.erp.core import ferias as svc_ferias
    from app.apps.erp.core.auth.permissoes import obras_do_usuario
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if not svc_ferias._tem_controle(s):
                return jsonify({"ok": True, "periodos": [], "tabela": False})
            return jsonify({"ok": True, "tabela": True,
                            "periodos": svc_ferias.situacao_dos_periodos(
                                s, obras=obras_do_usuario(s, usuario))})
    except Exception as e:
        logger.exception("ERP/DP: falha nos períodos de férias")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/dp/ferias/controle/<int:colaborador_id>", methods=["POST"])
@login_obrigatorio
@permissao("atender_dp")
def api_dp_ferias_controle(colaborador_id: int):
    """O DP informa até quando as férias da pessoa estão quitadas."""
    from app.apps.erp.core import ferias as svc_ferias
    from app.apps.erp.core.auth.permissoes import exigir_colaborador_no_escopo
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            exigir_colaborador_no_escopo(s, usuario, colaborador_id)
            svc_ferias.informar_quitado(s, colaborador_id, d.get("quitado_ate"), usuario)
            s.commit()
            return jsonify({"ok": True})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise


@bp.route("/erp/dp/regras")
@login_obrigatorio
@permissao("ver_pessoal")
def pagina_dp_regras():
    return render_template("erp_dp_regras.html", **_contexto("dp_regras"))


@bp.route("/erp/api/dp/regras")
@login_obrigatorio
@permissao("ver_pessoal")
def api_dp_regras():
    """AS CONDICIONAIS MEDIDAS NOS CADASTROS — só leitura, não muda nada.

    ⚠️ CUSTO, porque vale dizer antes de alguém apertar: é UMA consulta que
    lê todos os cadastros não cancelados (hoje ~2.600 linhas, 20 colunas) e
    cruza 2 campos-chave × 18 campos na memória. Não vai ao banco por pessoa
    nem por campo. O resultado NÃO é guardado: a resposta muda com o cadastro,
    e cache aqui faria ele decidir por número velho.
    """
    from app.apps.erp.core.cadastros import regras_pelos_dados as svc
    try:
        with get_session() as s:
            minimo = request.args.get("minimo", type=int)
            return jsonify({"ok": True, **svc.comparar_com_o_formulario(
                s, svc.medir(s, minimo=minimo) if minimo else None)})
    except Exception as e:
        logger.exception("ERP/DP: falha ao medir as regras do cadastro")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/dp/pedidos", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_pessoal", POST="pedir_contabilidade")
def api_dp_pedidos():
    """A fila de pedidos à contabilidade, e a abertura de um pedido novo.

    Quem é preso a obra VÊ o pedido que nasceu da requisição da obra dele — e é
    isso que acaba com o "e aí, saiu?" por WhatsApp. Pedir é do DP.
    """
    from app.apps.erp.core import contabilidade as svc_ct
    from app.apps.erp.core.auth.permissoes import obras_do_usuario
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if request.method == "GET":
                permitidas = obras_do_usuario(s, usuario)
                alvo = request.args.get("colaborador_id", type=int)
                templates = svc_ct.templates_para_tela()
                # ⚑ COM UMA PESSOA NA MÃO, CADA MODELO JÁ DIZ O QUE FALTA NELA
                # (17/11/2026). Sem isto a tela só descobre a falta DEPOIS de
                # a pessoa escolher o modelo e o servidor recusar — que é
                # exatamente o que o Pipefy fazia, e o que a aba Demandas
                # existe para não repetir. Vem na MESMA resposta: a tela já
                # pedia esta lista, e perguntar por modelo seriam sete idas.
                if alvo:
                    from app.apps.erp.db.models.cadastros import Colaborador
                    c = s.get(Colaborador, alvo)
                    if c is not None:
                        for t in templates:
                            t["falta"] = svc_ct.o_que_falta_para_pedir(s, t["chave"], c)
                            t["avisos"] = svc_ct.avisos_do_pedido(s, t["chave"], c)
                return jsonify({
                    "ok": True,
                    "pedidos": svc_ct.listar(
                        s, situacao=request.args.get("situacao", ""),
                        atrasados=request.args.get("atrasados") == "1",
                        colaborador_id=alvo,
                        obra_ids=list(permitidas) if permitidas is not None else None),
                    "templates": templates,
                    "emails": svc_ct.endereco_padrao(s)})
            r = svc_ct.abrir(s, request.get_json(silent=True) or {}, usuario)
            s.commit()
            return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha nos pedidos à contabilidade")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/dp/contabilidade/emails", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_pessoal", POST="configurar")
def api_dp_contabilidade_emails():
    """Para onde o ERP manda os pedidos à contabilidade.

    Fica em tabela de parâmetro, não em variável de ambiente: é dado que o
    dono troca sozinho pela tela, e trocá-lo não pode exigir publicação.
    """
    from app.apps.erp.core import contabilidade as svc_ct
    try:
        with get_session() as s:
            if request.method == "GET":
                return jsonify({"ok": True, "emails": svc_ct.endereco_padrao(s)})
            usuario = _usuario_logado(s)
            dados = request.get_json(silent=True) or {}
            emails = svc_ct.definir_enderecos(s, dados.get("emails") or "",
                                              usuario)
            s.commit()
            return jsonify({"ok": True, "emails": emails})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao gravar o e-mail da contabilidade")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/dp/pedidos/<int:pedido_id>")
@login_obrigatorio
@permissao("ver_pessoal")
def api_dp_pedido(pedido_id: int):
    from app.apps.erp.core import contabilidade as svc_ct
    from app.apps.erp.core.auth.permissoes import exigir_pedido_contabilidade_no_escopo
    with get_session() as s:
        usuario = _usuario_logado(s)
        exigir_pedido_contabilidade_no_escopo(s, usuario, pedido_id)
        return jsonify({"ok": True, "pedido": svc_ct.ler(s, pedido_id)})


@bp.route("/erp/api/dp/pedidos/<int:pedido_id>/<acao>", methods=["POST"])
@login_obrigatorio
@permissao("pedir_contabilidade")
def api_dp_pedido_acao(pedido_id: int, acao: str):
    """enviar · cobrar · receber · concluir · aceitar-divergencia · cancelar."""
    from app.apps.erp.core import contabilidade as svc_ct
    from app.apps.erp.core.auth.permissoes import exigir_pedido_contabilidade_no_escopo
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            exigir_pedido_contabilidade_no_escopo(s, usuario, pedido_id)
            if acao == "enviar":
                # ⚑ 23/11/2026: a tela de atendimento manda o texto como ficou
                # (o DP leu e talvez corrigiu) junto com o "enviar".
                if d.get("assunto") is not None or d.get("corpo") is not None:
                    p_ = svc_ct.ler(s, pedido_id)
                    svc_ct.editar_rascunho(
                        s, pedido_id, assunto=d.get("assunto", p_["assunto"]),
                        corpo=d.get("corpo", p_["corpo"]), usuario=usuario)
                r = svc_ct.enviar(s, pedido_id, usuario, para=d.get("para"))
            elif acao == "reenviar":
                r = svc_ct.reenviar(s, pedido_id, usuario)
            elif acao == "editar":
                r = svc_ct.editar_rascunho(s, pedido_id, assunto=d.get("assunto"),
                                           corpo=d.get("corpo"), usuario=usuario)
            elif acao == "anexar":
                arq = request.files.get("arquivo")
                if arq is None:
                    raise ErroValidacao("Escolha o arquivo.")
                r = svc_ct.anexar_ao_rascunho(s, pedido_id, conteudo=arq.read(),
                                              nome_arquivo=arq.filename or "anexo",
                                              usuario=usuario)
            elif acao == "tirar-anexo":
                r = svc_ct.tirar_anexo(s, pedido_id, int(d.get("documento_id") or 0),
                                       usuario)
            elif acao == "cobrar":
                r = svc_ct.cobrar(s, pedido_id, usuario)
            elif acao == "receber":
                r = svc_ct.receber(s, pedido_id, usuario,
                                   documento_id=d.get("documento_id"),
                                   lido=d.get("lido") or {})
            elif acao == "concluir":
                r = svc_ct.concluir(s, pedido_id, usuario,
                                    motivo=str(d.get("motivo") or ""))
            elif acao == "aceitar-divergencia":
                r = svc_ct.aceitar_divergencia(s, pedido_id, usuario,
                                               motivo=str(d.get("motivo") or ""))
            elif acao == "cancelar":
                r = svc_ct.cancelar(s, pedido_id, usuario,
                                    motivo=str(d.get("motivo") or ""))
            else:
                return jsonify({"ok": False, "erro": "Ação desconhecida."}), 400
            s.commit()
            return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha na ação do pedido à contabilidade")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ===========================================================================
# OS CHECKLISTS DO DP (18/11/2026)
#
# Ele: *"vi que você importou checklist do Pipefy, só que eles estão todos
# desatualizados. Precisa de uma tela pra configurar eles de forma fácil."*
#
# ⚑ LER É DE QUEM VÊ PESSOAL; ESCREVER É DE QUEM ATENDE. Mudar o checklist muda
# a rotina de trabalho do DP inteiro — é mais que marcar um item, e por isso não
# cabe em `ver_pessoal`.
# ===========================================================================
@bp.route("/erp/api/dp/checklists", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_pessoal", POST="atender_dp")
def api_dp_checklists():
    """Os passos de cada tipo de solicitação, e a gravação deles."""
    from app.apps.erp.core import pessoal_solicitacoes as svc
    try:
        with get_session() as s:
            if request.method == "GET":
                return jsonify({"ok": True,
                                "checklists": svc.checklists_para_a_tela(s)})
            d = request.get_json(silent=True) or {}
            r = svc.gravar_checklist(
                s, str(d.get("tipo") or "").strip().upper(),
                list(d.get("itens") or []),
                vinculo=d.get("vinculo"), usuario=_usuario_logado(s))
            s.commit()
            return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha nos checklists do DP")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/dp/agente", methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_pessoal", POST="pedir_contabilidade")
def api_dp_agente():
    """O estado das sete rotinas, e ligar/desligar cada uma.

    Todas nascem DESLIGADAS, e é pedido dele: *"a gente não vai começar com ele
    ativo."* O modo SUGERE é o do meio — o agente escreve o que faria e alguém
    aprova com um clique.
    """
    from app.apps.erp.core import agente_dp as svc_ag
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if request.method == "GET":
                return jsonify({"ok": True,
                                "rotinas": svc_ag.configuracao(s),
                                "sugestoes": svc_ag.sugestoes(s),
                                "modos": list(svc_ag.MODOS)})
            d = request.get_json(silent=True) or {}
            r = svc_ag.definir(s, str(d.get("rotina") or ""),
                               modo_novo=str(d.get("modo") or ""),
                               params=d.get("parametros") or {},
                               usuario=usuario)
            s.commit()
            return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha na configuração do agente de DP")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/dp/agente/rodar", methods=["POST"])
@login_obrigatorio
@permissao("pedir_contabilidade")
def api_dp_agente_rodar():
    """Roda agora o que está ligado. Nada ligado, nada feito — e diz isso."""
    from app.apps.erp.core import agente_dp as svc_ag
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            r = svc_ag.rodar(s, usuario)
            s.commit()
            return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao rodar o agente de DP")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/dp/agente/sugestoes/<int:sugestao_id>/<acao>", methods=["POST"])
@login_obrigatorio
@permissao("pedir_contabilidade")
def api_dp_agente_sugestao(sugestao_id: int, acao: str):
    """aplicar · recusar — é o clique que o modo SUGERE existe para pedir."""
    from app.apps.erp.core import agente_dp as svc_ag
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if acao == "aplicar":
                r = svc_ag.aplicar_sugestao(s, sugestao_id, usuario)
            elif acao == "recusar":
                r = svc_ag.recusar_sugestao(s, sugestao_id, usuario,
                                            motivo=str(d.get("motivo") or ""))
            else:
                return jsonify({"ok": False, "erro": "Ação desconhecida."}), 400
            s.commit()
            return jsonify({"ok": True, **r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise
    except Exception as e:
        logger.exception("ERP: falha na sugestão do agente")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/dp/triagem/<int:solicitacao_id>")
@login_obrigatorio
@permissao("atender_dp")
def api_dp_triagem(solicitacao_id: int):
    """O que a triagem aponta nesta solicitação. Só leitura, roda desligada."""
    from app.apps.erp.core import agente_dp as svc_ag
    from app.apps.erp.core.auth.permissoes import exigir_solicitacao_dp_no_escopo
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            exigir_solicitacao_dp_no_escopo(s, usuario, solicitacao_id)
            return jsonify({"ok": True, **svc_ag.triagem(s, solicitacao_id)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400


@bp.route("/erp/api/colaboradores/<int:colaborador_id>/ponto",
          methods=["GET", "POST"])
@login_obrigatorio
@permissao(GET="ver_pessoal", POST="editar_colaboradores")
def api_colaborador_marcacoes_do_ponto(colaborador_id: int):
    """As marcações do PONTO desta pessoa, dentro da ficha do ERP.

    Um cadastro só (06/11/2026, dele: *"tem cadastro da pessoa, tem cadastro no
    ponto (…) a gente tem que falar a mesma linguagem nos dois lados, afinal é
    a mesma empresa, é o mesmo ERP"*). A leitura e a gravação chamam as funções
    do próprio ponto — a regra é dele.
    """
    from app.apps.erp.core import portal as svc_portal
    from app.apps.erp.core.auth.permissoes import exigir_colaborador_no_escopo
    try:
        with get_session() as s:
            # `ver_pessoal` alcança supervisor de obra e administrativo de obra,
            # que são presos a obra — então o número da pessoa tem de passar
            # pelo escopo. Fora dele responde "não encontrado", nunca "sem
            # permissão": varrer os números mapearia a equipe inteira.
            exigir_colaborador_no_escopo(s, _usuario_logado(s), colaborador_id)
            if request.method == "GET":
                return jsonify({"ok": True, "ponto": svc_portal.marcacoes_do_ponto(
                    s, colaborador_id)})
            r = svc_portal.definir_marcacoes_do_ponto(
                s, colaborador_id, request.get_json(silent=True) or {})
            s.commit()
            return jsonify({"ok": True, "ponto": r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha nas marcações do ponto")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/colaboradores/fila")
@login_obrigatorio
@permissao("ver_pessoal")
def api_colaboradores_fila():
    """Quem está parado em cada etapa do cadastro, e há quantos dias.

    É o quadro do Pipefy virado lista. O recorte por obra é o mesmo da listagem
    de colaboradores — quem responde por uma obra vê a fila dela.
    """
    from app.apps.erp.core.auth.permissoes import obras_do_usuario
    from app.apps.erp.core.cadastros import pessoas as svc_pessoas
    with get_session() as s:
        usuario = _usuario_logado(s)
        permitidas = obras_do_usuario(s, usuario)
        return jsonify({
            "ok": True,
            "fila": svc_pessoas.fila_do_cadastro(
                s, etapa=request.args.get("etapa", ""),
                obra_ids=list(permitidas) if permitidas is not None else None),
            "etapas": [{"valor": e, "rotulo": svc_pessoas.ETAPA_ROTULOS[e],
                        "o_que_fazer": svc_pessoas.ETAPA_O_QUE_FAZER.get(e, ""),
                        "aberta": e in svc_pessoas.ETAPAS_ABERTAS}
                       for e in svc_pessoas.ETAPAS]})


@bp.route("/erp/api/colaboradores/<int:colaborador_id>/etapa", methods=["POST"])
@login_obrigatorio
@permissao("editar_colaboradores")
def api_colaborador_etapa(colaborador_id: int):
    """Move o cadastro de etapa, ou conclui a conferência do DP.

    `acao=conferido` deixa o SISTEMA decidir para onde vai, pelo vínculo — é
    regra, não escolha, e deixar escolher à mão foi o que no Pipefy fez
    prestador cair na fila da contabilidade e esperar um e-mail que nunca saía.
    """
    from app.apps.erp.core.cadastros import pessoas as svc_pessoas
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            if d.get("acao") == "conferido":
                r = svc_pessoas.concluir_conferencia(s, colaborador_id, usuario)
                s.commit()
                return jsonify({"ok": True, **r})
            c = svc_pessoas.mover_etapa(
                s, colaborador_id, d.get("etapa", ""), usuario,
                motivo=str(d.get("motivo") or ""))
            s.commit()
            return jsonify({"ok": True, "etapa": c.etapa,
                            "rotulo": svc_pessoas.ETAPA_ROTULOS.get(c.etapa, c.etapa)})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao mover a etapa do cadastro")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


# ===========================================================================
# AFASTAMENTO E RETORNO (18/11/2026)
#
# Pergunta dele: *"Colaborador estava afastado e vai retornar ao trabalho. O que
# deve ser exigido dele e o que o sistema deve pedir pra travar o simples
# retorno?"* — e, sobre quem faz: *"o DP vai fazer isso depois que alimentar com
# as informações pertinentes para o retorno."*
#
# ⚑ AS DUAS ROTAS DE ESCRITA SÃO A ÚNICA PORTA de entrar e sair de "Afastado".
# Mover a etapa pelo quadro recusa, com a frase que manda para cá — a trava
# está em `pessoas.mover_etapa`, não na tela, porque o arrasto contornaria.
# ===========================================================================
@bp.route("/erp/api/colaboradores/<int:colaborador_id>/afastamento")
@login_obrigatorio
@permissao("editar_colaboradores")
def api_afastamento(colaborador_id: int):
    """O afastamento em curso, o histórico, e o que a volta vai exigir."""
    from app.apps.erp.core.auth.permissoes import exigir_colaborador_no_escopo
    from app.apps.erp.core.cadastros import afastamentos as svc_afast
    try:
        with get_session() as s:
            exigir_colaborador_no_escopo(s, _usuario_logado(s), colaborador_id)
            return jsonify({
                "ok": True,
                "tipos": svc_afast.opcoes(),
                "aberto": svc_afast.aberto(s, colaborador_id),
                "historico": svc_afast.historico(s, colaborador_id),
                "estabilidade": svc_afast.estabilidade(s, colaborador_id),
                "retorno": svc_afast.o_que_falta_para_voltar(s, colaborador_id)})
    except ErroNaoEncontrado as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except Exception as e:
        logger.exception("ERP: falha ao ler o afastamento de %s", colaborador_id)
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/colaboradores/<int:colaborador_id>/afastamento",
          methods=["POST"])
@login_obrigatorio
@permissao("editar_colaboradores")
def api_afastar(colaborador_id: int):
    """Afasta: registra o tipo e as datas, e leva a etapa para “Afastado”."""
    from app.apps.erp.core.auth.permissoes import exigir_colaborador_no_escopo
    from app.apps.erp.core.cadastros import afastamentos as svc_afast
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            exigir_colaborador_no_escopo(s, usuario, colaborador_id)
            r = svc_afast.abrir(s, colaborador_id, d, usuario)
            # ⚑ 20/11/2026: a contabilidade fica sabendo pela solicitação.
            from app.apps.erp.core import pessoal_solicitacoes as svc_sp
            numero = svc_sp.comunicar_pela_ficha(s, colaborador_id, "AFASTAMENTO",
                                                 d, usuario)
            s.commit()
            return jsonify({"ok": True, **r, "solicitacao": numero})
    except ErroNaoEncontrado as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao afastar %s", colaborador_id)
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/colaboradores/<int:colaborador_id>/afastamento/retorno",
          methods=["POST"])
@login_obrigatorio
@permissao("editar_colaboradores")
def api_retorno_de_afastamento(colaborador_id: int):
    """Registra a volta: confere o exame, guarda a estabilidade, devolve à ativa."""
    from app.apps.erp.core.auth.permissoes import exigir_colaborador_no_escopo
    from app.apps.erp.core.cadastros import afastamentos as svc_afast
    d = request.get_json(silent=True) or {}
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            exigir_colaborador_no_escopo(s, usuario, colaborador_id)
            r = svc_afast.retornar(s, colaborador_id, d, usuario)
            from app.apps.erp.core import pessoal_solicitacoes as svc_sp
            numero = svc_sp.comunicar_pela_ficha(s, colaborador_id,
                                                 "RETORNO_AFASTAMENTO", d, usuario)
            s.commit()
            if isinstance(r, dict):
                r = {**r, "solicitacao": numero}
            return jsonify(r)
    except ErroNaoEncontrado as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao registrar o retorno de %s", colaborador_id)
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/conferencia-folha")
@login_obrigatorio
@permissao("editar_colaboradores")
def api_conferencia_folha():
    """O ERP e a planilha pagariam a mesma coisa? — ver `conferencia_folha`.

    ⚑ 17/11/2026. Ele: *"precisamos fazer de uma forma infalível essa
    desativação da planilha e a leitura do Análise de SPs pela base do ERP. A
    gravidade disso é pagar funcionários de forma equivocada."*

    Exige `editar_colaboradores` porque o relatório carrega salário, auxílios e
    valor de diária de todo mundo.
    """
    from datetime import date as _date

    from app.apps.erp.core import conferencia_folha
    ate = None
    bruto = (request.args.get("ate") or "").strip()
    if bruto:
        try:
            ate = _date.fromisoformat(bruto)
        except ValueError:
            return jsonify({"ok": False,
                            "erro": "Data inválida — use AAAA-MM-DD."}), 400
    with get_session() as s:
        return jsonify(conferencia_folha.comparar(s, ate=ate))


@bp.route("/erp/api/colaboradores/criados-recentemente")
@login_obrigatorio
@permissao("editar_colaboradores")
def api_colaboradores_criados_recentemente():
    """Quem nasceu no cadastro nas últimas horas — para conferir e desfazer.

    ⚑ 17/11/2026. Ele, depois de uma importação: *"tem mais gente no ERP do
    que eu coloquei para importar. Só quero os que eu coloquei para importar."*
    """
    from app.apps.erp.core.cadastros import pessoas as svc_pessoas
    horas = request.args.get("horas", type=int) or 48
    with get_session() as s:
        return jsonify({"ok": True, "horas": horas,
                        "pessoas": svc_pessoas.criados_recentemente(
                            s, horas=min(max(horas, 1), 720))})


@bp.route("/erp/api/colaboradores/remover", methods=["POST"])
@login_obrigatorio
@permissao("editar_colaboradores")
def api_colaboradores_remover():
    """Apaga cadastros criados por engano — um por um, dizendo o que deu.

    Recusa quem tem movimento, e a recusa vem do BANCO (ver
    `pessoas.remover_cadastro`). Quem não pôde ser apagada volta na lista de
    recusadas com o motivo, em vez de o lote inteiro falhar por causa de uma.
    """
    from app.apps.erp.core.cadastros import pessoas as svc_pessoas
    d = request.get_json(silent=True) or {}
    ids = [int(x) for x in (d.get("ids") or []) if str(x).strip().isdigit()]
    if not ids:
        return jsonify({"ok": False, "erro": "Nenhuma pessoa escolhida."}), 400
    apagadas, desligadas, recusadas = [], [], []
    with get_session() as s:
        usuario = _usuario_logado(s)
        for cid in ids:
            try:
                r = svc_pessoas.remover_cadastro(s, cid, usuario,
                                                 levar_junto=True)
                # Tem batida de ponto ⇒ desligada, não apagada (regra dele em
                # 17/11/2026; batida é registro legal do REP-P).
                (desligadas if r.get("desligada") else apagadas).append(r)
            except ErroValidacao as e:
                recusadas.append({"id": cid, "motivo": str(e)})
        s.commit()
    return jsonify({"ok": True, "apagadas": apagadas,
                    "desligadas": desligadas, "recusadas": recusadas})


@bp.route("/erp/api/colaboradores/para-folha")
@login_obrigatorio
@permissao("editar_colaboradores")
def api_colaboradores_para_folha():
    """O cadastro na forma que a FOLHA lê — o ERP como fonte.

    Decisão do dono (05/11/2026): o Análise de SPs passa a LER daqui em vez de
    ler a planilha, e nada é escrito em planilha nenhuma. Esta rota é para a
    TELA — conferir e exportar durante o período em paralelo. Quem monta a
    folha dentro do monorepo chama `pessoas.para_folha()` direto: mesmo
    processo, mesmo banco, nenhum segredo novo e nenhuma volta de rede.

    Exige `editar_colaboradores` porque carrega salário, PIS e chave Pix.
    """
    from app.apps.erp.core.cadastros import pessoas as svc_pessoas
    with get_session() as s:
        return jsonify({"ok": True, "colaboradores": svc_pessoas.para_folha(
            s, so_ativos=request.args.get("todos") != "1",
            obra_id=(int(request.args["obra_id"])
                     if request.args.get("obra_id") else None))})


@bp.route("/erp/api/colaboradores/importar", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_colaboradores_importar():
    """As pessoas que estão no Pipefy / na planilha do DP, para dentro do ERP.

    `gravar=0` simula e devolve o relatório sem tocar no banco — é o botão
    "conferir" da tela. `so_ativos=0` traz também os desligados, que por decisão
    do dono (04/11/2026) ficam de fora por padrão.
    """
    from app.apps.erp.core.importadores import pessoas_planilha
    arquivo = request.files.get("arquivo")
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Envie a planilha."}), 400
    gravar = request.form.get("gravar") == "1"
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            r = pessoas_planilha.importar(
                s, arquivo.read(), gravar=gravar,
                so_ativos=request.form.get("so_ativos", "1") != "0",
                usuario=usuario)
            # Na simulação nada é escrito, e o rollback é a garantia disso.
            s.commit() if gravar else s.rollback()
        return jsonify({"ok": True, "relatorio": r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao importar a planilha de pessoas")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/colaboradores/importar-fichas", methods=["POST"])
@login_obrigatorio
@permissao("configurar")
def api_colaboradores_importar_fichas():
    """As fichas de registro do Fortes (PDF) para dentro do ERP.

    ⚑ LÊ A CAMADA DE TEXTO, não a imagem: a ficha é PDF gerado pelo Fortes, e
    o número está escrito lá, exato. Sem IA, sem custo e sem erro de leitura —
    o que importa num campo como o PIS, que tem onze dígitos e só acusa erro
    quando o INSS reclama. Ficha digitalizada (só imagem) é devolvida como
    ilegível, com o número da página, em vez de adivinhada.

    `gravar=0` simula e devolve o relatório sem tocar no banco.
    `criar_novos=1` deixa a ficha CRIAR quem o ERP não tem — nasce desligado,
    porque a ficha não traz cargo (decisão dele: a nomenclatura é nossa), nem
    telefone, nem chave Pix, que são obrigatórios no cadastro de hoje.
    """
    from app.apps.erp.core.importadores import fichas_fortes
    arquivo = request.files.get("arquivo")
    if arquivo is None:
        return jsonify({"ok": False, "erro": "Envie o PDF das fichas."}), 400
    gravar = request.form.get("gravar") == "1"
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            r = fichas_fortes.importar(
                s, arquivo.read(), gravar=gravar,
                criar_novos=request.form.get("criar_novos") == "1",
                usuario=usuario)
            # Na simulação nada é escrito, e o rollback é a garantia disso.
            s.commit() if gravar else s.rollback()
        return jsonify({"ok": True, "relatorio": r})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        logger.exception("ERP: falha ao importar as fichas do Fortes")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/colaboradores/<int:colaborador_id>/historico")
@login_obrigatorio
@permissao("ver_pessoal")
def api_historico_colaborador(colaborador_id: int):
    """Tudo que já se pagou a esta pessoa: DC, títulos diretos e rateados.

    ⚠️ QUEM NÃO VÊ REMUNERAÇÃO NÃO VÊ VALOR AQUI (17/11/2026). Esta rota pede
    `ver_pessoal`, que o administrativo de obra tem — e ela devolve tudo o que
    já se pagou à pessoa. Sem o corte, a visão restrita que ele pediu teria um
    buraco do tamanho do histórico financeiro inteiro.

    O painel continua abrindo: a pessoa, a obra, o cargo, a situação e as
    datas. O que sai são os valores e as verbas.
    """
    from app.apps.erp.core.auth.permissoes import pode
    from app.apps.erp.core.pessoal import historico
    try:
        with get_session() as s:
            from app.apps.erp.core.auth.permissoes import exigir_colaborador_no_escopo
            quem = _usuario_logado(s)
            exigir_colaborador_no_escopo(s, quem, colaborador_id)
            h = historico(
                s, colaborador_id, meses=request.args.get("meses", type=int) or 24)
            if not pode(quem, "ver_remuneracao"):
                h = {**h, "lancamentos": [], "total": None,
                     "ultimos_12_meses": None, "por_verba": [],
                     "sem_remuneracao": True}
            return jsonify({"ok": True, "historico": h})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 404
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha no histórico do colaborador")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/lotes/<int:lote_id>/adicionar-sps", methods=["POST"])
@login_obrigatorio
@permissao("pagar")
def api_lote_adicionar_sps(lote_id: int):
    """Cola-se a lista de SPs e as parcelas em aberto entram no lote."""
    from app.apps.erp.core.pagamentos.lotes import (
        adicionar_parcelas, extrair_ids_sp, parcelas_por_sp,
    )
    d = request.get_json(silent=True) or {}
    numeros = extrair_ids_sp(d.get("texto") or "")
    if not numeros:
        return jsonify({"ok": False,
                        "erro": "Nenhum número de SP reconhecido no texto."}), 400
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            achado = parcelas_por_sp(s, numeros)
            ids = [p["parcela_id"] for p in achado.get("parcelas", [])
                   if p.get("status") != "PAGA"]
            rel = adicionar_parcelas(s, lote_id, ids, usuario) if ids else {"adicionadas": 0}
            s.commit()
        return jsonify({"ok": True, "resultado": {
            "adicionadas": rel.get("adicionadas", len(ids)),
            "ja_estavam": rel.get("ja_estavam", []),
            "nao_encontradas": achado.get("nao_encontradas", [])}})
    except ErroValidacao as e:
        return jsonify({"ok": False, "erro": str(e)}), 400
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao adicionar SPs ao lote")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/api/lotes/<int:lote_id>", methods=["DELETE"])
@login_obrigatorio
@permissao("pagar")
def api_excluir_lote(lote_id: int):
    """Apaga o agrupamento. As SPs continuam intactas — o lote não é um estado
    do título, é só uma forma de olhar para um conjunto delas."""
    from sqlalchemy import select
    from app.apps.erp.core.comum.auditoria import registrar_evento
    from app.apps.erp.db.models.financeiro import Lote, LoteParcela
    try:
        with get_session() as s:
            usuario = _usuario_logado(s)
            lote = s.get(Lote, lote_id)
            if lote is None:
                return jsonify({"ok": False, "erro": "Lote não encontrado."}), 404
            nome = lote.nome
            quantas = len(s.scalars(select(LoteParcela).where(
                LoteParcela.lote_id == lote_id)).all())
            for lp in s.scalars(select(LoteParcela).where(
                    LoteParcela.lote_id == lote_id)).all():
                s.delete(lp)
            s.delete(lote)
            registrar_evento(s, "lote", lote_id, "EXCLUIDO",
                             {"nome": nome, "parcelas": quantas}, usuario.id)
            s.commit()
        return jsonify({"ok": True, "nome": nome})
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        logger.exception("ERP: falha ao excluir lote")
        return jsonify({"ok": False, "erro": recado_de_falha(e)}), 500


@bp.route("/erp/health")
@permissao_publica("health check do Render, sem dado de negocio")
def health():
    """Health check do módulo — não exige login."""
    try:
        from sqlalchemy import text
        with get_session() as s:
            s.execute(text("SELECT 1"))
        pendentes = _migracoes_pendentes()
        return jsonify({"ok": not pendentes, "modulo": "erp", "banco": "conectado",
                        "migracoes_pendentes": pendentes,
                        "aviso": ("aplique as atualizações em Configurações"
                                  if pendentes else "estrutura em dia")}), 200
    except ErroNaoEncontrado:
        raise        # recusa de escopo vira 404, nunca 500
    except Exception as e:
        return jsonify({"ok": False, "modulo": "erp", "erro": str(e)}), 503


# ---------------------------------------------------------------------------
# PONTO ELETRÔNICO (03/10/2026) — a gestão do ponto entra no ERP como mais um
# módulo do menu. As rotas moram em `app/apps/ponto/gestao.py` e se penduram
# neste mesmo blueprint, para herdar login, guarda de permissão e recorte por
# obra. IMPORTAÇÃO PROTEGIDA, no fim do arquivo de propósito: se o ponto falhar
# ao carregar, o ERP sobe inteiro, só sem o menu do ponto.
# ---------------------------------------------------------------------------
try:
    from app.apps.ponto import gestao as _gestao_do_ponto  # noqa: F401,E402
except Exception:  # noqa: BLE001
    logger.exception("ERP: a gestão do ponto não carregou; o ERP segue sem ela")


def _ponto_ao_lado_de_pessoal() -> None:
    """O PONTO VOLTA A TER O PRÓPRIO LUGAR NO MENU, LOGO DEPOIS DE PESSOAL (13/11/2026).

    Histórico: em 06/11/2026 as abas do ponto foram para DENTRO de Pessoal (o dono: *"a gente tem lá pessoal e tem
    ponto (…) já seria o momento de integrar isso aí"*). Em 13/11/2026 ele voltou atrás no MENU, mantendo a integração
    onde ela importa — a ficha da pessoa:

    > *"Quando a gente juntou ponto dentro de pessoal, a quantidade de telas ficou muito grande, muito longa (…) ficou
    > um pouco bagunçado, era melhor dividir a parte de ponto e a parte de pessoal."*

    E a integração que ele queria de verdade era outra: clicar na pessoa e ver TUDO dela (cadastro, ponto, aparelhos,
    pagamentos) numa janela só — feita na janela da pessoa do ponto, que a tela de Colaboradores agora abre
    (ver `erp_colaboradores.html`, `ficha`).

    O QUE ESTA FUNÇÃO FAZ, e só isto: põe o módulo "ponto" logo depois de "pessoal" e ordena as abas de Pessoal pelo
    dia a dia. As abas do ponto (e a ordem delas, decidida com o dono em 13/11) são do módulo do ponto
    (`app/apps/ponto/gestao.py`, `ABAS`). Nenhuma rota e nenhuma permissão mudam. Se o ponto não carregar, nada aqui
    quebra: o ERP sobe sem o menu dele.
    """
    pessoal = next((m for m in MODULOS if m["chave"] == "pessoal"), None)
    ponto = next((m for m in MODULOS if m["chave"] == "ponto"), None)
    if pessoal is not None:
        ORDEM = ("colaboradores", "dp_solicitacoes", "dp_contabilidade", "dc")
        pessoal["abas"] = ([a for chave in ORDEM for a in pessoal["abas"] if a[0] == chave]
                           + [a for a in pessoal["abas"] if a[0] not in ORDEM])
    if pessoal is None or ponto is None:
        return
    MODULOS.remove(ponto)
    MODULOS.insert(MODULOS.index(pessoal) + 1, ponto)


try:
    _ponto_ao_lado_de_pessoal()
except Exception:  # noqa: BLE001
    logger.exception("ERP: não consegui pôr o Ponto ao lado de Pessoal no menu; a ordem fica a de origem")

# MENSAGERIA (05/10/2026) — a tela "Mensagens" (por onde cada aviso sai, teto
# do WhatsApp, registro) entra do mesmo jeito: rotas em
# `app/apps/mensageria/gestao.py`, penduradas neste blueprint, importação
# protegida. Se falhar, o ERP sobe sem o menu Mensagens.
try:
    from app.apps.mensageria import gestao as _gestao_de_mensagens  # noqa: F401,E402
except Exception:  # noqa: BLE001
    logger.exception("ERP: a tela de mensagens não carregou; o ERP segue sem ela")
