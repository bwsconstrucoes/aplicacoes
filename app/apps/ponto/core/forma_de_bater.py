# -*- coding: utf-8 -*-
"""
A FORMA DE BATER — o padrão vale para todos, e só as exceções se cadastram.

Decisão do dono, 05/10/2026: *"Em relação ao padrão de batida, é só o aparelho
da obra que bate. Vamos cadastrar apenas as exceções. Banco de horas, mesma
coisa: a princípio ninguém tem, vamos cadastrar as exceções."*

  PADRÃO      a pessoa bate no aparelho da obra (tablet ou celular da obra,
              perfil COMPARTILHADO), com QR Code ou CPF. O celular dela serve
              para ver o mês, mandar atestado, pedir ajuste e mostrar o QR —
              não para bater.
  EXCEÇÃO     "também bate no próprio celular" (`colaborador_config.
              bate_no_celular`): marcada em Pessoas, ou sozinha quando alguém
              aprova o celular da pessoa como INDIVIDUAL na tela de aparelhos.
              Desmarcar a exceção faz o celular parar de bater na hora, sem
              precisar bloquear o aparelho.
  OUTRA PESSOA BATE POR ELA: é o aparelho de LISTA (o celular do encarregado
              que bate pela equipe dele) — exceção do APARELHO, não da pessoa,
              e se cadastra na aprovação do aparelho.

Banco de horas: o regime padrão já é "sem banco" (`banco.py`); a exceção se
cadastra em Pessoas › Banco de horas, com o acordo anexado.

Antes da migração 004, a coluna não existe e vale o comportamento de antes
(celular aprovado bate) — o código sobe para o Render antes do botão.
"""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db

logger = logging.getLogger("ponto.forma_de_bater")

RECUSA_CELULAR = ("o seu ponto é batido no aparelho da obra. Pelo celular você vê o seu mês, "
                  "manda atestado, pede ajuste e mostra o seu QR Code")


def em_vigor(conn: Connection) -> bool:
    """A regra "só o aparelho da obra" já pode valer (migração 004 aplicada)?"""
    return db.tem_coluna(conn, "colaborador_config", "bate_no_celular")


def pode_no_celular(pessoa: dict, regra_em_vigor: bool) -> bool:
    """PURA."""
    return (not regra_em_vigor) or bool(pessoa.get("bate_no_celular"))


def recusa_no_celular(pessoa: dict, regra_em_vigor: bool) -> Optional[str]:
    """PURA. None se a pessoa pode bater no próprio celular; senão a frase."""
    return None if pode_no_celular(pessoa, regra_em_vigor) else RECUSA_CELULAR


def definir(conn: Connection, colaborador_id: int, bate_no_celular: bool, por: str) -> None:
    from ..erros import ErroDeValidacao
    if not em_vigor(conn):
        raise ErroDeValidacao("aplique as atualizações do ponto (migração 004) antes de cadastrar exceções")
    db.executar(conn, """
        INSERT INTO ponto.colaborador_config (colaborador_id, bate_no_celular) VALUES (:c, :b)
        ON CONFLICT (colaborador_id) DO UPDATE SET bate_no_celular = :b, atualizado_em = now()
    """, c=colaborador_id, b=bool(bate_no_celular))
    logger.info("Ponto: pessoa %s %s no próprio celular (por %s)", colaborador_id,
                "passa a bater" if bate_no_celular else "deixa de bater", por)


def excecoes(conn: Connection, obras: Optional[list[int]] = None) -> dict:
    """Quem foge do padrão — num lugar só, para conferir de olho: celular
    próprio, banco de horas, e os aparelhos de LISTA (alguém bate por outros)."""
    from . import banco, cadastros
    pessoas = cadastros.listar_colaboradores(conn, so_ativos=True)
    if obras is not None:
        alcance = set(obras)
        pessoas = [p for p in pessoas if p.get("obra_id") in alcance
                   or any(o["id"] in alcance for o in p.get("obras_adicionais", []))]
    celular = [{"id": p["id"], "nome": p["nome"], "obra": p.get("obra_codigo")}
               for p in pessoas if p.get("bate_no_celular")]
    com_banco = [{"id": p["id"], "nome": p["nome"], "obra": p.get("obra_codigo"),
                  "regime": banco.ROTULOS.get(p.get("regime_banco"), p.get("regime_banco"))}
                 for p in pessoas if p.get("regime_banco") not in (None, "SEM_BANCO")]
    listas = db.todos(conn, """
        SELECT d.id, d.descricao, d.status,
               (SELECT count(*) FROM ponto.dispositivo_autorizados a WHERE a.dispositivo_id = d.id) AS pessoas
          FROM ponto.dispositivos d WHERE d.perfil = 'LISTA' AND d.status <> 'BLOQUEADO' ORDER BY d.id""")
    from . import papeis
    # Os papéis no aplicativo (08/10/2026): quem faz pedido pelo próprio celular
    # e quem é administrativo de obra — também são exceções, e ficam à vista.
    pedem = [{"id": p["id"], "nome": p["nome"], "obra": p.get("obra_codigo")}
             for p in pessoas if p.get("pede_no_celular")]
    administrativos = [{"id": p["id"], "nome": p["nome"], "obra": p.get("obra_codigo")}
                       for p in pessoas if p.get("administrativo_obra")]
    return {"em_vigor": em_vigor(conn), "celular": celular, "banco": com_banco,
            "aparelhos_de_lista": [{**l, "pessoas": int(l["pessoas"])} for l in listas],
            "papeis_disponivel": papeis.disponivel(conn), "pedem_no_celular": pedem,
            "administrativos": administrativos}
