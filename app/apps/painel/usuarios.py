# -*- coding: utf-8 -*-
"""
Pessoas com acesso ao painel, presas a obras e a telas.

Pedido do dono em 21/09/2026: dar acesso a um parceiro de obra que entre com
senha propria e veja SO as obras dele, e so as telas liberadas.

TRES REGRAS QUE NAO SE DISCUTEM, e todas falham FECHADO:

1. **Sem obra marcada, nao ve nada.** Lista vazia nao quer dizer "todas" —
   quer dizer nenhuma. O contrario seria um cadastro pela metade virando
   acesso total.
2. **Sem tela marcada, nao entra em tela nenhuma.** Idem.
3. **Pessoa cadastrada aqui nunca escreve no OMIE**, nunca abre Configuracoes e
   nunca usa o Explorador. Isso e do administrador, que entra pela senha mestre
   (PAINEL_SENHA) e continua vendo tudo.

A senha e guardada EMBARALHADA (PBKDF2 com sal), pelo `werkzeug.security` que o
Flask ja traz. Nem o dono le a senha de alguem depois — so troca. Este banco tem
o financeiro inteiro da empresa; senha legivel aqui seria vazamento esperando
acontecer.
"""
from __future__ import annotations

import logging
import unicodedata

from werkzeug.security import check_password_hash, generate_password_hash

from .db import conexao, consultar

logger = logging.getLogger("painel.usuarios")

# As telas que podem ser liberadas, com o nome que a pessoa le.
#
# Configuracoes e Explorador NAO estao aqui de proposito: os dois escrevem (um
# aplica migracao e dispara carga, o outro altera e exclui titulo no OMIE), e
# quem e preso a obra nao escreve. Se um dia precisar, e decisao consciente —
# nao um descuido de quem marcou uma caixinha a mais.
TELAS = {
    "visao": "Visão Geral",
    "dre": "DRE",
    "analitico": "Despesas Analítico",
    "receita": "Receita de Obra",
    "fluxo": "Fluxo de Caixa",
    "obras": "Resultado por Obra",
    "execucao": "Comprometido × Executado",
    "caixa": "Necessidade de Caixa",
    "prestacao": "Prestação de Contas",
    "extrato": "Extrato de Conta Corrente",
    "calendario": "Calendário",
}

# O que o dono pediu para liberar de saida. O resto fica pronto para quando ele
# quiser, que foi o pedido dele: "se de repente entender que seja necessario
# liberar outra tela, ja estaria configurado".
TELAS_SUGERIDAS = ("dre", "analitico")


def _normalizar(texto) -> str:
    """Login sem depender de maiuscula nem de como o acento foi digitado."""
    return unicodedata.normalize("NFC", str(texto or "")).strip().lower()


def listar() -> list[dict]:
    """Todas as pessoas cadastradas, com as obras, contas e telas de cada uma."""
    pessoas = [{"id": i, "usuario": u, "nome": n, "ativo": bool(a),
                "ultimo_acesso": ult, "obras": [], "telas": [], "contas": [],
                "projetos": []}
               for i, u, n, a, ult in consultar(
        "SELECT id, usuario, nome, ativo, ultimo_acesso FROM usuarios"
        " ORDER BY lower(usuario)")]
    por_id = {p["id"]: p for p in pessoas}
    for uid, dep in consultar("SELECT usuario_id, departamento FROM usuario_obras"
                              " ORDER BY departamento"):
        if uid in por_id:
            por_id[uid]["obras"].append(dep)
    for uid, tela in consultar("SELECT usuario_id, tela FROM usuario_telas"):
        if uid in por_id:
            por_id[uid]["telas"].append(tela)
    for uid, conta in consultar("SELECT usuario_id, conta FROM usuario_contas"
                                " ORDER BY conta"):
        if uid in por_id:
            por_id[uid]["contas"].append(conta)
    for uid, projeto in consultar("SELECT usuario_id, projeto FROM usuario_projetos"
                                  " ORDER BY projeto"):
        if uid in por_id:
            por_id[uid]["projetos"].append(projeto)
    return pessoas


def buscar(usuario: str) -> dict | None:
    """A pessoa, pelo login. Devolve None se nao existe ou esta desativada."""
    login = _normalizar(usuario)
    if not login:
        return None
    linhas = consultar(
        "SELECT id, usuario, nome, senha_hash FROM usuarios"
        " WHERE lower(usuario) = ? AND ativo", (login,))
    if not linhas:
        return None
    uid, login_real, nome, senha_hash = linhas[0]
    obras_marcadas = [d for (d,) in consultar(
        "SELECT departamento FROM usuario_obras WHERE usuario_id = ?"
        " ORDER BY departamento", (uid,))]
    projetos = [p for (p,) in consultar(
        "SELECT projeto FROM usuario_projetos WHERE usuario_id = ?"
        " ORDER BY projeto", (uid,))]
    # "obras" e o que VALE: as marcadas uma a uma MAIS todas as obras dos
    # projetos liberados, hoje. E o que o filtro de toda tela le — por isso
    # a obra nova de um projeto entra sozinha no acesso de quem tem o projeto.
    obras = obras_marcadas
    if projetos:
        from . import consultas
        obras = sorted(set(obras_marcadas) | set(consultas.obras_dos_projetos(projetos)))
    return {"id": uid, "usuario": login_real, "nome": nome,
            "senha_hash": senha_hash,
            "obras": obras, "obras_marcadas": obras_marcadas,
            "projetos": projetos,
            "telas": [t for (t,) in consultar(
                "SELECT tela FROM usuario_telas WHERE usuario_id = ?", (uid,))],
            "contas": [c for (c,) in consultar(
                "SELECT conta FROM usuario_contas WHERE usuario_id = ?"
                " ORDER BY conta", (uid,))]}


def senha_confere(pessoa: dict, digitada: str) -> bool:
    """Confere a senha embaralhada. Pessoa inexistente nunca confere."""
    if not pessoa or not pessoa.get("senha_hash"):
        return False
    try:
        return check_password_hash(pessoa["senha_hash"], str(digitada or ""))
    except Exception:  # noqa: BLE001 — hash corrompido é senha que não confere
        logger.exception("Painel: hash de senha inválido para %s",
                         pessoa.get("usuario"))
        return False


def _e_a_senha_do_dono(senha) -> bool:
    """A senha mestre não pode virar senha de ninguém.

    Desde 22/09/2026 quem decide o caminho da entrada é a SENHA, e não o campo
    de usuário estar vazio — foi o conserto de um jeito de trancar o dono para
    fora do painel. A consequência é esta: se a senha de uma pessoa presa a uma
    obra fosse igual à do dono, ela entraria como administrador. Fecha-se aqui,
    no cadastro, que é onde custa nada."""
    from .auth import senha_confere as mestre_confere
    return bool(str(senha or "")) and mestre_confere(senha)


def criar(usuario: str, senha: str, nome: str = "", obras=(), telas=(),
          contas=(), projetos=()) -> dict:
    """Cadastra a pessoa. Devolve {'ok': True, 'id': n} ou o erro em português."""
    login = _normalizar(usuario)
    if not login:
        return {"ok": False, "erro": "Escolha um nome de usuário."}
    if len(str(senha or "")) < 6:
        return {"ok": False, "erro": "A senha precisa de pelo menos 6 letras."}
    if _e_a_senha_do_dono(senha):
        return {"ok": False, "erro": "Essa é a senha do dono do painel. "
                                     "Escolha outra para esta pessoa."}
    if consultar("SELECT 1 FROM usuarios WHERE lower(usuario) = ?", (login,)):
        return {"ok": False, "erro": f"Já existe um usuário “{login}”."}
    with conexao() as conn:
        cur = conn.execute(
            "INSERT INTO usuarios (usuario, nome, senha_hash) VALUES (?,?,?)"
            " RETURNING id",
            (login, str(nome or "").strip(), generate_password_hash(senha)))
        uid = cur.fetchone()[0]
        cur.close()
        _gravar_escopo(conn, uid, obras, telas, contas, projetos)
        conn.commit()
    logger.info("Painel: usuário %s criado com %d obra(s), %d projeto(s) e %d tela(s).",
                login, len(set(obras)), len(set(projetos)), len(set(telas)))
    return {"ok": True, "id": uid}


def _gravar_escopo(conn, uid: int, obras, telas, contas=(), projetos=()) -> None:
    conn.execute("DELETE FROM usuario_obras WHERE usuario_id = ?", (uid,))
    conn.execute("DELETE FROM usuario_telas WHERE usuario_id = ?", (uid,))
    conn.execute("DELETE FROM usuario_contas WHERE usuario_id = ?", (uid,))
    conn.execute("DELETE FROM usuario_projetos WHERE usuario_id = ?", (uid,))
    for projeto in sorted({str(p).strip() for p in (projetos or []) if str(p).strip()}):
        conn.execute("INSERT INTO usuario_projetos (usuario_id, projeto)"
                     " VALUES (?,?)", (uid, projeto))
    for conta in sorted({str(c).strip() for c in (contas or []) if str(c).strip()}):
        conn.execute("INSERT INTO usuario_contas (usuario_id, conta)"
                     " VALUES (?,?)", (uid, conta))
    for dep in sorted({str(o).strip() for o in (obras or []) if str(o).strip()}):
        conn.execute("INSERT INTO usuario_obras (usuario_id, departamento)"
                     " VALUES (?,?)", (uid, dep))
    for tela in sorted({str(t).strip() for t in (telas or [])
                        if str(t).strip() in TELAS}):
        conn.execute("INSERT INTO usuario_telas (usuario_id, tela)"
                     " VALUES (?,?)", (uid, tela))


def atualizar(uid: int, *, nome=None, senha=None, ativo=None,
              obras=None, telas=None, contas=None, projetos=None) -> dict:
    """Muda o que foi pedido e só isso. Senha vazia não apaga a que existe."""
    if not consultar("SELECT 1 FROM usuarios WHERE id = ?", (int(uid),)):
        return {"ok": False, "erro": "Usuário não encontrado."}
    if senha is not None and str(senha).strip() and len(str(senha)) < 6:
        return {"ok": False, "erro": "A senha precisa de pelo menos 6 letras."}
    if senha is not None and _e_a_senha_do_dono(senha):
        return {"ok": False, "erro": "Essa é a senha do dono do painel. "
                                     "Escolha outra para esta pessoa."}
    with conexao() as conn:
        if nome is not None:
            conn.execute("UPDATE usuarios SET nome = ? WHERE id = ?",
                         (str(nome).strip(), int(uid)))
        if ativo is not None:
            conn.execute("UPDATE usuarios SET ativo = ? WHERE id = ?",
                         (bool(ativo), int(uid)))
        if senha is not None and str(senha).strip():
            conn.execute("UPDATE usuarios SET senha_hash = ? WHERE id = ?",
                         (generate_password_hash(senha), int(uid)))
            logger.info("Painel: senha do usuário %s trocada.", uid)
        if (obras is not None or telas is not None or contas is not None
                or projetos is not None):
            atual = buscar_por_id(int(uid)) or {}
            # as MARCADAS, nunca as efetivas: regravar as efetivas congelaria
            # as obras do projeto como se tivessem sido marcadas uma a uma
            _gravar_escopo(conn, int(uid),
                           atual.get("obras_marcadas", []) if obras is None else obras,
                           atual["telas"] if telas is None else telas,
                           atual.get("contas", []) if contas is None else contas,
                           atual.get("projetos", []) if projetos is None else projetos)
        conn.commit()
    return {"ok": True}


def buscar_por_id(uid: int) -> dict | None:
    linhas = consultar("SELECT usuario FROM usuarios WHERE id = ?", (int(uid),))
    return buscar(linhas[0][0]) if linhas else None


def apagar(uid: int) -> dict:
    """Tira o acesso de vez. As obras e telas vão junto (ON DELETE CASCADE)."""
    with conexao() as conn:
        conn.execute("DELETE FROM usuarios WHERE id = ?", (int(uid),))
        conn.commit()
    logger.info("Painel: usuário %s apagado.", uid)
    return {"ok": True}


def marcar_acesso(uid: int) -> None:
    """Quando entrou pela última vez — para o dono saber quem usa de verdade."""
    try:
        with conexao() as conn:
            conn.execute("UPDATE usuarios SET ultimo_acesso = now()"
                         " WHERE id = ?", (int(uid),))
            conn.commit()
    except Exception:  # noqa: BLE001 — anotar o acesso nunca impede de entrar
        logger.exception("Painel: não consegui marcar o acesso de %s", uid)
