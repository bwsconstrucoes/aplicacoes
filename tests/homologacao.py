"""O que os quatro arquivos de homologação têm em comum.

O roteiro de homologação (`HOMOLOGACAO_PERFIS.md`) pede que o dono navegue
com um usuário de cada perfil e confira o que abre e o que é recusado. Isto
aqui faz a parte mecânica disso a cada envio, contra um Postgres real:

  - para CADA perfil e CADA tela ou consulta sem parâmetro na URL, a resposta
    é "abre" (não 403, não 500) quando o perfil tem a ação, e 403 quando não;
  - para CADA ação de escrita sem parâmetro, o perfil que não a tem recebe 403
    antes de qualquer outra coisa.

A lista de telas e ações NÃO é escrita à mão: vem do registro de permissões
do próprio código. Tela nova entra sozinha; tela que mude de ação também.

O que sobra para o olho humano: o visual, a leitura de documentos por IA e os
fluxos que exigem dados reais (avalizar, pagar). O roteiro continua valendo
para isso — mas encolheu.
"""
from __future__ import annotations

import pytest
from flask import Flask

from app.apps.erp import routes
from app.apps.erp.core.auth.permissoes import PERMISSOES, ROTULOS
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.db.models.cadastros import PerfilUsuario as P, Usuario

from conftest import como

__all__ = [
    "INVENTARIO", "LEITURAS", "ESCRITAS", "TELAS", "PERFIS_DO_ROTEIRO",
    "TODOS_OS_PERFIS", "PERMISSOES", "ROTULOS", "P", "Usuario", "como",
    "identificacao", "operadores", "ACOES_POR_PERFIL",
    "SENHA_DE_HOMOLOGACAO",
]

# Endpoints deixados de fora, com o motivo escrito:
FORA = {
    "erp.api_aplicar_migracoes",   # abre conexão própria, fora da transação do teste
    "erp.pagina_login", "erp.sair", "erp.health",   # públicos, não têm perfil
}

PERFIS_DO_ROTEIRO = (P.ADMINISTRATIVO_OBRA, P.SUPERVISOR_OBRA, P.FINANCEIRO,
                     P.DEPARTAMENTO_PESSOAL)
TODOS_OS_PERFIS = tuple(P)


def _inventario():
    """(endpoint, caminho, método, ação) para toda rota SEM parâmetro na URL."""
    a = Flask(__name__)
    a.secret_key = "inventario"
    a.register_blueprint(routes.bp)
    saida = []
    for regra in a.url_map.iter_rules():
        if not regra.endpoint.startswith("erp.") or regra.arguments:
            continue
        if regra.endpoint in FORA or regra.endpoint in routes._ISENTOS:
            continue
        mapa = routes._REGISTRO_PERMISSOES.get(regra.endpoint) or {}
        for metodo in ("GET", "POST"):
            if metodo in regra.methods and mapa.get(metodo):
                saida.append((regra.endpoint, str(regra), metodo, mapa[metodo]))
    return sorted(saida)


INVENTARIO = _inventario()
LEITURAS = [x for x in INVENTARIO if x[2] == "GET"]
ESCRITAS = [x for x in INVENTARIO if x[2] == "POST"]


def _id(caso):
    endpoint, caminho, metodo, acao = caso
    return f"{metodo} {caminho} [{acao}]"


TELAS = [c for c in LEITURAS if "/api/" not in c[1]]


def identificacao(caso):
    """O nome que o pytest mostra para cada caso."""
    endpoint, caminho, metodo, acao = caso
    return f"{metodo} {caminho} [{acao}]"


# ⚑ A SENHA É EMBARALHADA UMA VEZ SÓ, e não uma vez por teste (17/11/2026).
#
# Ele, em 16/11/2026: *"eu havia comentado da demora dos testes (…) precisa
# otimizar eles."* São 1.659 casos de homologação, e cada um gastava 63 ms
# embaralhando a MESMA senha para produzir o MESMO texto — 104 segundos de
# trabalho jogado fora.
#
# Embaralhar senha é caro DE PROPÓSITO: é o que torna inviável adivinhar a
# senha de alguém por tentativa. Em produção esse custo é a proteção. Aqui é
# desperdício puro: nenhum destes testes faz login por senha — todos entram
# pela sessão (`como`), e o campo só existe porque a coluna não aceita vazio.
SENHA_DE_HOMOLOGACAO = gerar_hash("senha-de-homologacao-1")


def _acoes_equivalentes(perfil: P) -> set[str]:
    """O que o perfil cadastrado equivalente ao cargo concede.

    ⚑ A MONTAGEM DO PERFIL MORA NO `conftest.py` (27/11/2026), num lugar só:
    desde que o acesso passou a vir apenas do perfil cadastrado, TODO teste
    com banco precisa disso, e não só a homologação.
    """
    from app.apps.erp.core.auth import secoes as cat
    from conftest import _secoes_equivalentes_ao_cargo

    return cat.acoes_do_perfil(_secoes_equivalentes_ao_cargo(perfil))


# O que cada operador de homologação REALMENTE pode, já pelo perfil cadastrado.
# É contra isto que os casos conferem — e não contra a tabela de cargos, que
# desde 27/11/2026 não concede nada a quem tem perfil.
ACOES_POR_PERFIL = {perfil: _acoes_equivalentes(perfil) for perfil in TODOS_OS_PERFIS}


@pytest.fixture
def operadores(sessao_real) -> dict[P, Usuario]:
    """Um usuário de cada perfil, no banco de verdade.

    O PERFIL CADASTRADO vem do `conftest` (`perfis_equivalentes_aos_cargos`):
    quem é gravado sem perfil recebe o equivalente ao cargo. Sem isso, desde
    27/11/2026 nenhum destes operadores abriria nada — o acesso passou a vir
    só do perfil.
    """
    pessoas = {}
    for perfil in TODOS_OS_PERFIS:
        u = Usuario(nome=f"Homologação {ROTULOS.get(perfil, perfil.value)}",
                    email=f"homolog.{perfil.value.lower()}@teste.bws.local",
                    senha_hash=SENHA_DE_HOMOLOGACAO, perfil=perfil)
        sessao_real.add(u)
        pessoas[perfil] = u
    sessao_real.flush()
    sessao_real.commit()
    return pessoas
