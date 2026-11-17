# -*- coding: utf-8 -*-
"""
A FILA DE PEDIDOS DE ACESSO AO PORTAL — de quem sabe o e-mail para quem tem a alçada.

Desenho do dono, 17/11/2026: *"que tal a partir do ponto eu requerer a
liberação do acesso ao portal? Ela já tem o cadastro no ponto, e lá pelo ponto
ela vai informar o e-mail dela."*

POR QUE ISTO EXISTE. O acesso ao ERP só nasce com e-mail, e o e-mail quase
nunca está no cadastro: ninguém pede e-mail para contratar gente de obra. O DP
não tem como adivinhar, então o acesso simplesmente não nascia. A pessoa sabe
o próprio e-mail e já entra todo dia no ponto com CPF e PIN — autenticação de
verdade, com bloqueio por tentativa. Então o pedido nasce lá, e a DECISÃO
continua aqui.

⚑ ISTO NÃO É UMA PORTA NOVA DE LIBERAR ACESSO. A porta é uma só e já existia:
`/erp/api/colaboradores/<id>/acesso` ("Liberar acesso e convidar", na ficha da
pessoa). O que este módulo acrescenta é a FILA que alimenta essa porta — e
liberar um pedido chama exatamente a mesma rota, não um caminho paralelo. Era
o erro que o dono cobrou em 17/11 ("três botões para importar pessoas"): o
pedido novo vira coisa a ACRESCENTAR em vez de alimentar o que já existe.

⚑ ESTE MÓDULO NÃO DECIDE NADA SOBRE PERMISSÃO. Ele guarda e lista pedidos.
Quem pode liberar, e com qual alçada, continua em `permissoes.py` e na rota.

⚑ FUNCIONA ANTES DO BOTÃO DA MIGRAÇÃO. O código chega ao Render antes de
alguém apertar "Aplicar atualizações do banco", e nesse intervalo o ponto NÃO
pode quebrar por causa de uma tabela que ainda não existe: `tabela_existe`
responde, e quem chama mostra "ainda não disponível" em vez de erro.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Optional

from sqlalchemy import text

logger = logging.getLogger("erp.pedidos_de_portal")

PENDENTE = "PENDENTE"
LIBERADO = "LIBERADO"
RECUSADO = "RECUSADO"

_EMAIL = re.compile(r"^[^@\s]+@[^@\s.]+\.[^@\s]+$")


def email_valido(email: Any) -> bool:
    """Um e-mail que dá para mandar convite. Sem invenção: só o que o convite
    precisa — tem arroba, tem ponto depois dela, e não tem espaço."""
    return bool(_EMAIL.match(str(email or "").strip().lower()))


# ⚑ A RESPOSTA FICA GUARDADA, e o motivo é conta de custo (regra do CLAUDE.md:
# contar quantas idas ao banco custa UMA operação e quantas custa o lote).
#
# `tabela_existe` é chamada na ABERTURA DO APLICATIVO do ponto — a rota mais
# chamada do serviço, uma vez por pessoa por vez que ela abre o celular. Sem
# memória, seria uma consulta ao `information_schema` em cada uma delas, para
# perguntar algo que só muda quando alguém aperta o botão da migração.
#
# O "sim" vale para sempre (tabela não some). O "não" é perguntado de novo a
# cada minuto, para o recurso ligar sozinho logo depois do botão — mesmo
# desenho de `app/apps/ponto/db.py::tem_coluna`, pelo mesmo motivo.
_existe_sim = False
_existe_nao_em = 0.0


def tabela_existe(conn) -> bool:
    global _existe_sim, _existe_nao_em
    import time
    if _existe_sim:
        return True
    if time.time() - _existe_nao_em < 60:
        return False
    linha = conn.execute(text(
        "SELECT 1 AS x FROM information_schema.tables "
        " WHERE table_schema = 'public' AND table_name = 'portal_pedidos_de_acesso'"
    )).first()
    if linha is not None:
        _existe_sim = True
        return True
    _existe_nao_em = time.time()
    return False


def esquecer() -> None:
    """Esquece o que se sabia da tabela. Para o teste, e para quem aplicar a
    migração sem esperar o minuto."""
    global _existe_sim, _existe_nao_em
    _existe_sim, _existe_nao_em = False, 0.0


def registrar(conn, *, colaborador_id: Optional[int], cpf: str, nome: str,
              email: str, telefone: str = "", obra_id: Optional[int] = None,
              origem: str = "PONTO") -> dict[str, Any]:
    """Guarda o pedido. Devolve `{'ok': True, 'id': n}` ou o motivo da recusa.

    NÃO levanta exceção por coisa esperada (e-mail torto, pedido repetido,
    pessoa que já tem acesso): quem chama é um aplicativo de celular, e erro
    500 ali vira "o sistema não funciona".
    """
    email = str(email or "").strip().lower()
    if not email_valido(email):
        return {"ok": False, "erro": "Confira o e-mail: ele precisa ter @ e o "
                                     "domínio depois (exemplo: nome@gmail.com)."}
    if not tabela_existe(conn):
        return {"ok": False, "erro": "O pedido de acesso ainda não está "
                                     "disponível. Avise o escritório."}

    # JÁ TEM ACESSO? Então não é pedido, é "esqueci a senha" — e para isso já
    # existe tela própria. Abrir pedido aqui faria o DP liberar o que está
    # liberado e a pessoa continuar sem conseguir entrar.
    if colaborador_id:
        ja = conn.execute(text(
            "SELECT u.id, u.ativo, u.email FROM colaboradores c "
            "  JOIN usuarios u ON u.id = c.usuario_id WHERE c.id = :c"),
            {"c": int(colaborador_id)}).mappings().first()
        if ja is not None and ja["ativo"]:
            return {"ok": False, "ja_tem": True, "email": ja["email"],
                    "erro": "Você já tem acesso ao sistema, com o e-mail "
                            f"{ja['email']}. Se esqueceu a senha, use "
                            "\"Esqueci a senha\" na tela de entrada."}
        pendente = conn.execute(text(
            "SELECT id, email, criado_em FROM portal_pedidos_de_acesso "
            " WHERE colaborador_id = :c AND status = 'PENDENTE'"),
            {"c": int(colaborador_id)}).mappings().first()
        if pendente is not None:
            # TROCAR O E-MAIL DO PEDIDO QUE AINDA ESPERA é o caso real de quem
            # digitou errado. Recusar mandaria a pessoa esperar por um convite
            # que vai para um endereço que não existe.
            if pendente["email"] != email:
                conn.execute(text(
                    "UPDATE portal_pedidos_de_acesso SET email = :e "
                    " WHERE id = :id"), {"e": email, "id": pendente["id"]})
                return {"ok": True, "id": int(pendente["id"]), "corrigido": True}
            return {"ok": True, "id": int(pendente["id"]), "repetido": True}

    linha = conn.execute(text("""
        INSERT INTO portal_pedidos_de_acesso
               (colaborador_id, cpf, nome, email, telefone, obra_id, origem)
        VALUES (:c, :cpf, :nome, :email, :tel, :obra, :origem)
        RETURNING id"""), {
            "c": int(colaborador_id) if colaborador_id else None,
            "cpf": re.sub(r"\D", "", str(cpf or ""))[:11],
            "nome": (nome or "")[:200] or None,
            "email": email, "tel": (telefone or "")[:30] or None,
            "obra": int(obra_id) if obra_id else None,
            "origem": (origem or "PONTO").upper()[:20],
        }).mappings().first()
    logger.info("Portal: pedido de acesso %s de %s (%s)", linha["id"], nome, email)
    return {"ok": True, "id": int(linha["id"])}


def meu(conn, colaborador_id: int) -> Optional[dict[str, Any]]:
    """O último pedido desta pessoa, para o celular dizer em que pé está."""
    if not tabela_existe(conn):
        return None
    linha = conn.execute(text("""
        SELECT id, email, status, motivo, criado_em, decidido_em
          FROM portal_pedidos_de_acesso
         WHERE colaborador_id = :c ORDER BY criado_em DESC LIMIT 1"""),
        {"c": int(colaborador_id)}).mappings().first()
    if linha is None:
        return None
    return {"id": int(linha["id"]), "email": linha["email"],
            "status": linha["status"], "motivo": linha["motivo"] or "",
            "criado_em": linha["criado_em"].isoformat() if linha["criado_em"] else None,
            "decidido_em": linha["decidido_em"].isoformat() if linha["decidido_em"] else None}


def listar(conn, *, status: str = PENDENTE, limite: int = 200) -> list[dict[str, Any]]:
    """A fila para o DP. Inclui a obra e o que o sistema já sabe da pessoa."""
    if not tabela_existe(conn):
        return []
    linhas = conn.execute(text("""
        SELECT p.id, p.colaborador_id, p.cpf, p.nome, p.email, p.telefone,
               p.status, p.motivo, p.origem, p.criado_em, p.decidido_em,
               o.codigo AS obra_codigo, o.nome AS obra_nome, p.obra_id,
               c.nome AS nome_no_cadastro, c.situacao, c.etapa,
               c.usuario_id, u.email AS email_do_acesso, u.ativo AS acesso_ativo
          FROM portal_pedidos_de_acesso p
          LEFT JOIN obras o ON o.id = p.obra_id
          LEFT JOIN colaboradores c ON c.id = p.colaborador_id
          LEFT JOIN usuarios u ON u.id = c.usuario_id
         WHERE (:todos OR p.status = :status)
         ORDER BY p.criado_em DESC LIMIT :limite"""),
        {"status": status, "todos": (status or "").upper() == "TODOS",
         "limite": int(limite)}).mappings().all()
    from app.apps.erp.core.comum.rotulos import rotulo_da_obra
    saida = []
    for l in linhas:
        saida.append({
            "id": int(l["id"]),
            "colaborador_id": int(l["colaborador_id"]) if l["colaborador_id"] else None,
            "cpf": l["cpf"], "email": l["email"], "telefone": l["telefone"] or "",
            "nome": l["nome_no_cadastro"] or l["nome"] or "",
            "obra_id": int(l["obra_id"]) if l["obra_id"] else None,
            "obra": rotulo_da_obra(l["obra_codigo"], l["obra_nome"]) if l["obra_codigo"] else "",
            "situacao": l["situacao"] or "", "etapa": l["etapa"] or "",
            "status": l["status"], "motivo": l["motivo"] or "",
            "origem": l["origem"],
            "ja_tem_acesso": bool(l["usuario_id"] and l["acesso_ativo"]),
            "email_do_acesso": l["email_do_acesso"] or "",
            "criado_em": l["criado_em"].isoformat() if l["criado_em"] else None,
        })
    return saida


def quantos_pendentes(conn) -> int:
    if not tabela_existe(conn):
        return 0
    linha = conn.execute(text(
        "SELECT count(*) AS n FROM portal_pedidos_de_acesso "
        " WHERE status = 'PENDENTE'")).mappings().first()
    return int(linha["n"] or 0)


def decidir(conn, pedido_id: int, *, status: str, decidido_por: Optional[int] = None,
            usuario_id: Optional[int] = None, motivo: str = "") -> bool:
    """Fecha o pedido. Só sai de PENDENTE — decisão não se desfaz por clique
    repetido (duas pessoas do DP abrindo a mesma fila ao mesmo tempo)."""
    if not tabela_existe(conn):
        return False
    n = conn.execute(text("""
        UPDATE portal_pedidos_de_acesso
           SET status = :st, motivo = :motivo, usuario_id = COALESCE(:u, usuario_id),
               decidido_por = :por, decidido_em = now()
         WHERE id = :id AND status = 'PENDENTE'"""),
        {"st": status, "motivo": (motivo or "")[:500] or None,
         "u": int(usuario_id) if usuario_id else None,
         "por": int(decidido_por) if decidido_por else None,
         "id": int(pedido_id)}).rowcount
    return bool(n)
