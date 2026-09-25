# -*- coding: utf-8 -*-
"""
Quem entra na Análise de SPs: cadastro próprio, com senha e telas.

Pedido do dono em 25/09/2026, com todas as letras: *"os usuários eu adiciono,
eles estão com senha única. Eu quero fazer similar ao painel. Vou poder
cadastrar o operador, definir a senha, definir as telas que ele tem acesso. Aí
vai ter um usuário master, e os outros a gente define as permissões."*

DOIS JEITOS DE ENTRAR, e eles são bem diferentes:

  - **a senha do Render** (`ANALISESPS_SENHA_OPERADOR` / `..._CONSULTA`) é o
    MESTRE. Vê todas as telas, configura, aplica migração, mexe no
    certificado e cadastra as pessoas. É o dono;
  - **usuário e senha próprios** (esta tabela, migração 023) alcançam SÓ as
    telas marcadas, e alteram dado só se estiverem marcados como operador.

⚠️ POR QUE A SENHA DO RENDER CONTINUA VALENDO. Não é preguiça: é o que impede o
dono de se trancar para fora. Se a migração não tiver rodado, se ele apagar o
próprio cadastro sem querer, se o banco estiver fora do ar — a senha do Render
ainda entra. Um cadastro que pode trancar o único administrador não é segurança,
é armadilha.

TRÊS REGRAS QUE FALHAM FECHADO:

1. **Sem tela marcada, a pessoa não entra.** Lista vazia quer dizer NENHUMA,
   nunca "todas". Cadastro esquecido pela metade não vira acesso total.
2. **Quem é cadastrado aqui nunca abre Configurações**, nem aplica migração,
   nem encosta no certificado digital, nem lança aporte no OMIE, nem cadastra
   outra pessoa. Isso é do mestre (a lista está em `auth.SO_DO_MESTRE`).
3. **O padrão de `pode_operar` é FALSE** — vê e exporta, não altera. Subir o
   poder de alguém é uma marcação consciente, não o que acontece por descuido.

A SENHA FICA EMBARALHADA (PBKDF2 com sal, pelo `werkzeug.security` que o Flask
já traz). Nem o dono lê a senha de alguém depois — só troca.

O NOME NÃO É ENFEITE. Ele é a chave do lote, dos filtros e das colunas de cada
pessoa (`auth.chave_pessoa`). Cadastrar Thiago com o nome que ele já escolhia na
entrada faz o lote dele continuar sendo o mesmo; cadastrar com nome diferente
dá um lote vazio, e ele não vai entender por quê. A tela de cadastro avisa isso
e oferece a lista de nomes que já existe.
"""
from __future__ import annotations

import logging
import unicodedata

logger = logging.getLogger("analisesps.usuarios")

# Piso da senha. Não é política de segurança da empresa — é o mínimo para uma
# senha não ser adivinhada na terceira tentativa.
MINIMO_DA_SENHA = 6

# Teto do login e do nome, para o campo não virar porta de texto gigante.
MAX_LOGIN = 40

# O recado de quando a tabela ainda não existe — o botão não foi apertado.
FALTA_MIGRAR = ("O cadastro de acesso precisa da atualização do banco. Vá em "
                "Configurações e aperte “Aplicar atualizações do banco”.")


def _pronto() -> bool:
    """A migração 023 já rodou? Enquanto não, só a senha do Render entra.

    O código sobe para o Render ANTES de alguém apertar "Aplicar atualizações
    do banco" — então esta pergunta não é paranoia: é o estado normal do
    sistema por alguns minutos, a cada publicação."""
    from .db import consultar_um
    try:
        consultar_um("SELECT 1 FROM analisesps.usuarios LIMIT 1")
        return True
    except Exception:  # noqa: BLE001 — tabela ainda não existe é estado normal
        return False


def normalizar_login(bruto) -> str:
    """O login como ele é guardado e comparado: sem espaço e sem maiúscula.

    Normaliza o acento (NFC) pelo mesmo motivo da senha no painel: "ç" pode vir
    como um caractere ou como "c" mais a cedilha, e os dois parecem iguais na
    tela sem serem iguais em bytes. Sem isto, o mesmo login digitado no celular
    e no computador podia não bater."""
    texto = unicodedata.normalize("NFC", str(bruto or "")).strip().lower()
    return texto[:MAX_LOGIN]


def telas_liberaveis() -> list[tuple[str, str]]:
    """As telas que se pode marcar para alguém, na ordem em que aparecem.

    Sai da MESMA lista que desenha a navegação (`web.TELAS`) — se as duas
    fossem escritas à mão, uma tela nova apareceria no menu e não no cadastro,
    e ninguém descobriria até alguém reclamar de um 404.

    Configurações fica FORA de propósito: é de onde se aplica migração, se
    troca o certificado e se cadastra gente. Isso é do mestre."""
    from .web import TELAS
    from .auth import SO_DO_MESTRE_POR_TELA
    return [(chave, rotulo) for chave, rotulo, _rota in TELAS
            if chave not in SO_DO_MESTRE_POR_TELA]


def chaves_liberaveis() -> set[str]:
    return {chave for chave, _ in telas_liberaveis()}


# ---------------------------------------------------------------------------
# Ler
# ---------------------------------------------------------------------------
def listar() -> list[dict]:
    """Todo mundo cadastrado, com as telas de cada um. Sem a tabela, vazio."""
    if not _pronto():
        return []
    from .db import consultar
    pessoas = [{"id": i, "usuario": u, "nome": n, "ativo": bool(a),
                "pode_operar": bool(op), "ultimo_acesso": ult, "telas": []}
               for i, u, n, a, op, ult in consultar(
        "SELECT id, usuario, nome, ativo, pode_operar, ultimo_acesso"
        "  FROM analisesps.usuarios ORDER BY lower(usuario)")]
    por_id = {p["id"]: p for p in pessoas}
    for uid, tela in consultar("SELECT usuario_id, tela"
                               "  FROM analisesps.usuario_telas ORDER BY tela"):
        if uid in por_id:
            por_id[uid]["telas"].append(tela)
    return pessoas


def buscar(login: str) -> dict | None:
    """A pessoa, pelo login. None quando não existe ou está desativada."""
    if not _pronto():
        return None
    chave = normalizar_login(login)
    if not chave:
        return None
    from .db import consultar
    linhas = consultar(
        "SELECT id, usuario, nome, senha_hash, pode_operar"
        "  FROM analisesps.usuarios WHERE lower(usuario) = ? AND ativo",
        (chave,))
    if not linhas:
        return None
    uid, login_real, nome, senha_hash, pode_operar = linhas[0]
    return {
        "id": uid, "usuario": login_real, "nome": nome,
        "senha_hash": senha_hash, "pode_operar": bool(pode_operar),
        "telas": [t for (t,) in consultar(
            "SELECT tela FROM analisesps.usuario_telas WHERE usuario_id = ?",
            (uid,))],
    }


def buscar_por_id(uid) -> dict | None:
    if not _pronto():
        return None
    from .db import consultar
    linhas = consultar("SELECT usuario FROM analisesps.usuarios WHERE id = ?",
                       (int(uid),))
    return buscar(linhas[0][0]) if linhas else None


def senha_confere(pessoa: dict | None, digitada: str) -> bool:
    """Confere a senha embaralhada. Pessoa inexistente nunca confere."""
    from werkzeug.security import check_password_hash
    if not pessoa or not pessoa.get("senha_hash"):
        return False
    try:
        return check_password_hash(pessoa["senha_hash"], str(digitada or ""))
    except Exception:  # noqa: BLE001 — hash corrompido é senha que não confere
        logger.exception("Análise de SPs: hash inválido no usuário %s",
                         pessoa.get("usuario"))
        return False


# ---------------------------------------------------------------------------
# Escrever
# ---------------------------------------------------------------------------
def _e_senha_do_mestre(senha) -> bool:
    """A senha do Render não pode virar senha de ninguém.

    Quem entra pela senha do Render é o mestre — então uma pessoa cadastrada
    com a mesma senha entraria como mestre, e as telas marcadas para ela não
    valeriam nada. Fecha-se aqui, no cadastro, onde não custa."""
    from . import auth
    return bool(str(senha or "")) and auth.identificar(senha) is not None


def _conferir_senha(senha) -> str:
    """Devolve o erro em português, ou "" quando a senha serve."""
    if len(str(senha or "")) < MINIMO_DA_SENHA:
        return f"A senha precisa de pelo menos {MINIMO_DA_SENHA} letras."
    if _e_senha_do_mestre(senha):
        return ("Essa é uma das senhas gerais do sistema, a que abre tudo. "
                "Escolha outra para esta pessoa.")
    return ""


def criar(login: str, senha: str, nome: str = "", telas=(),
          pode_operar: bool = False) -> dict:
    """Cadastra. Devolve {'ok': True, 'id': n} ou o erro em português."""
    if not _pronto():
        return {"ok": False, "erro": FALTA_MIGRAR}
    from .auth import limpar_nome
    from .db import conexao, consultar
    from werkzeug.security import generate_password_hash

    chave = normalizar_login(login)
    if not chave:
        return {"ok": False, "erro": "Escolha um nome de usuário para a pessoa."}
    erro = _conferir_senha(senha)
    if erro:
        return {"ok": False, "erro": erro}
    if consultar("SELECT 1 FROM analisesps.usuarios WHERE lower(usuario) = ?",
                 (chave,)):
        return {"ok": False, "erro": f"Já existe um usuário “{chave}”."}

    with conexao() as conn:
        cur = conn.execute(
            "INSERT INTO analisesps.usuarios (usuario, nome, senha_hash, pode_operar)"
            " VALUES (?,?,?,?) RETURNING id",
            (chave, limpar_nome(nome), generate_password_hash(senha),
             bool(pode_operar)))
        uid = int(cur.fetchone()[0])
        cur.close()
        _gravar_telas(conn, uid, telas)
        conn.commit()
    logger.info("Análise de SPs: usuário %s criado (%s) com %d tela(s).",
                chave, "operador" if pode_operar else "consulta",
                len(set(telas or [])))
    return {"ok": True, "id": uid}


def _gravar_telas(conn, uid: int, telas) -> None:
    """Regrava as telas da pessoa. Tela desconhecida é descartada em silêncio —
    um pedido montado à mão não inventa permissão que não existe."""
    permitidas = chaves_liberaveis()
    conn.execute("DELETE FROM analisesps.usuario_telas WHERE usuario_id = ?",
                 (int(uid),))
    for tela in sorted({str(t).strip() for t in (telas or [])
                        if str(t).strip() in permitidas}):
        conn.execute("INSERT INTO analisesps.usuario_telas (usuario_id, tela)"
                     " VALUES (?,?)", (int(uid), tela))


def atualizar(uid, *, nome=None, senha=None, ativo=None, telas=None,
              pode_operar=None) -> dict:
    """Muda o que foi pedido e só isso. Senha em branco mantém a que existe."""
    if not _pronto():
        return {"ok": False, "erro": FALTA_MIGRAR}
    from .auth import limpar_nome
    from .db import conexao, consultar
    from werkzeug.security import generate_password_hash

    if not consultar("SELECT 1 FROM analisesps.usuarios WHERE id = ?", (int(uid),)):
        return {"ok": False, "erro": "Usuário não encontrado."}
    trocar_senha = senha is not None and str(senha).strip() != ""
    if trocar_senha:
        erro = _conferir_senha(senha)
        if erro:
            return {"ok": False, "erro": erro}

    with conexao() as conn:
        if nome is not None:
            conn.execute("UPDATE analisesps.usuarios SET nome = ? WHERE id = ?",
                         (limpar_nome(nome), int(uid)))
        if ativo is not None:
            conn.execute("UPDATE analisesps.usuarios SET ativo = ? WHERE id = ?",
                         (bool(ativo), int(uid)))
        if pode_operar is not None:
            conn.execute("UPDATE analisesps.usuarios SET pode_operar = ?"
                         " WHERE id = ?", (bool(pode_operar), int(uid)))
        if trocar_senha:
            conn.execute("UPDATE analisesps.usuarios SET senha_hash = ?"
                         " WHERE id = ?",
                         (generate_password_hash(senha), int(uid)))
            logger.info("Análise de SPs: senha do usuário %s trocada.", uid)
        if telas is not None:
            _gravar_telas(conn, int(uid), telas)
        conn.commit()
    return {"ok": True}


def apagar(uid) -> dict:
    """Tira o acesso de vez. As telas vão junto (ON DELETE CASCADE)."""
    if not _pronto():
        return {"ok": False, "erro": FALTA_MIGRAR}
    from .db import conexao
    with conexao() as conn:
        conn.execute("DELETE FROM analisesps.usuarios WHERE id = ?", (int(uid),))
        conn.commit()
    logger.info("Análise de SPs: usuário %s apagado.", uid)
    return {"ok": True}


def marcar_acesso(uid) -> None:
    """Quando entrou pela última vez — para o dono saber quem usa de verdade.

    Nunca impede de entrar: anotar o acesso é conforto, não autenticação."""
    if not _pronto():
        return
    from .db import conexao
    try:
        with conexao() as conn:
            conn.execute("UPDATE analisesps.usuarios SET ultimo_acesso = now()"
                         " WHERE id = ?", (int(uid),))
            conn.commit()
    except Exception:  # noqa: BLE001
        logger.exception("Análise de SPs: não consegui marcar o acesso de %s", uid)

