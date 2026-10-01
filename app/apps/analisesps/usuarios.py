# -*- coding: utf-8 -*-
"""
Quem entra na Análise de SPs: cadastro próprio, com senha e telas.

Pedido do dono em 25/09/2026, com todas as letras: *"os usuários eu adiciono,
eles estão com senha única. Eu quero fazer similar ao painel. Vou poder
cadastrar o operador, definir a senha, definir as telas que ele tem acesso. Aí
vai ter um usuário master, e os outros a gente define as permissões."*

TODO MUNDO ENTRA COM USUÁRIO E SENHA PRÓPRIOS — desde 25/09/2026, pedido do
dono: *"elimine do login o login via Nomes na lista da entrada. Vamos ficar
somente com os cadastrados."* A lista de nomes ao lado da senha acabou.

O MESTRE É UMA MARCAÇÃO NA PESSOA (migração 024), como "pode alterar". Quem é
mestre vê **todas** as telas, abre Configurações, aplica migração, mexe no
certificado, lança aporte no OMIE e cadastra gente — as telas marcadas para ele
não importam, ele alcança todas.

⚠️ A SENHA DO RENDER (`ANALISESPS_SENHA_OPERADOR` / `..._CONSULTA`) CONTINUA
EXISTINDO, mas só como **porta de emergência**: entra-se por ela deixando o
campo de usuário em branco. Não é mais o caminho do dia a dia.

Por que ela não foi eliminada, embora o pedido tenha sido "só os cadastrados":
sem ela, perder o último cadastro de mestre tranca todo mundo para fora **sem
volta** — não há e-mail de recuperação, não há outro administrador, não há
nada. A porta fica, está dita na tela de entrada, e tirá-la é uma decisão que
o dono pode tomar sabendo o preço.

TRÊS REGRAS QUE FALHAM FECHADO:

1. **Sem tela marcada, a pessoa não entra.** Lista vazia quer dizer NENHUMA,
   nunca "todas". Cadastro esquecido pela metade não vira acesso total.
2. **Quem NÃO é mestre nunca abre Configurações**, nem aplica migração, nem
   encosta no certificado digital, nem lança aporte no OMIE, nem cadastra
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

FALTA_MIGRAR_MESTRE = ("A marcação de mestre precisa da atualização do banco "
                       "(a 024). Aperte “Aplicar atualizações do banco” aqui "
                       "mesmo, nesta tela, e tente de novo.")

ERRO_ULTIMO_MESTRE = ("Este é o único mestre do sistema. Se ele sair, ninguém "
                      "mais cadastra gente nem abre as Configurações. Marque "
                      "outra pessoa como mestre primeiro.")


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
    # ⚠️ A COLUNA `mestre` É DA MIGRAÇÃO 024, e o código sobe para o Render
    # ANTES de alguém apertar o botão. Perguntar se ela existe é o que impede
    # a tela de Configurações de estourar nesse intervalo — e Configurações é
    # justamente a tela de onde se aperta o botão.
    from .db import tem_coluna
    tem_mestre = tem_coluna("usuarios", "mestre")
    campo = "mestre" if tem_mestre else "FALSE"
    pessoas = [{"id": i, "usuario": u, "nome": n, "ativo": bool(a),
                "pode_operar": bool(op), "mestre": bool(m),
                "ultimo_acesso": ult, "telas": []}
               for i, u, n, a, op, m, ult in consultar(
        f"SELECT id, usuario, nome, ativo, pode_operar, {campo}, ultimo_acesso"
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
    from .db import consultar, tem_coluna
    campo = "mestre" if tem_coluna("usuarios", "mestre") else "FALSE"
    linhas = consultar(
        f"SELECT id, usuario, nome, senha_hash, pode_operar, {campo}"
        "  FROM analisesps.usuarios WHERE lower(usuario) = ? AND ativo",
        (chave,))
    if not linhas:
        return None
    uid, login_real, nome, senha_hash, pode_operar, mestre = linhas[0]
    return {
        "id": uid, "usuario": login_real, "nome": nome,
        "senha_hash": senha_hash,
        # ⚠️ MESTRE MANDA SOBRE TUDO. Ele altera e alcança todas as telas,
        # esteja marcado como operador ou não — deixar essas duas coisas
        # discordarem seria um jeito de o dono cadastrar a si mesmo e não
        # conseguir salvar nada.
        "mestre": bool(mestre),
        "pode_operar": bool(pode_operar) or bool(mestre),
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
          pode_operar: bool = False, mestre: bool = False) -> dict:
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

    from .db import tem_coluna
    tem_mestre = tem_coluna("usuarios", "mestre")
    if mestre and not tem_mestre:
        return {"ok": False, "erro": FALTA_MIGRAR_MESTRE}

    with conexao() as conn:
        colunas = "usuario, nome, senha_hash, pode_operar"
        valores = [chave, limpar_nome(nome), generate_password_hash(senha),
                   bool(pode_operar) or bool(mestre)]
        if tem_mestre:
            colunas += ", mestre"
            valores.append(bool(mestre))
        cur = conn.execute(
            f"INSERT INTO analisesps.usuarios ({colunas})"
            f" VALUES ({','.join('?' * len(valores))}) RETURNING id",
            tuple(valores))
        uid = int(cur.fetchone()[0])
        cur.close()
        _gravar_telas(conn, uid, telas)
        conn.commit()
    logger.info("Análise de SPs: usuário %s criado (%s) com %d tela(s).",
                chave,
                "MESTRE" if mestre else ("operador" if pode_operar else "consulta"),
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
              pode_operar=None, mestre=None) -> dict:
    """Muda o que foi pedido e só isso. Senha em branco mantém a que existe."""
    if not _pronto():
        return {"ok": False, "erro": FALTA_MIGRAR}
    from .auth import limpar_nome
    from .db import conexao, consultar, tem_coluna
    from werkzeug.security import generate_password_hash

    if not consultar("SELECT 1 FROM analisesps.usuarios WHERE id = ?", (int(uid),)):
        return {"ok": False, "erro": "Usuário não encontrado."}

    tem_mestre = tem_coluna("usuarios", "mestre")
    if mestre and not tem_mestre:
        return {"ok": False, "erro": FALTA_MIGRAR_MESTRE}
    # ⚠️ NÃO DEIXAR O ÚLTIMO MESTRE SE DESFAZER. Tirar a marcação do único
    # mestre, ou desativá-lo, deixa o sistema sem ninguém que possa cadastrar,
    # configurar ou aplicar migração — e o conserto passaria pela porta de
    # emergência, que é justamente o que não se quer usar no dia a dia.
    if tem_mestre and (mestre is False or ativo is False):
        if _e_o_ultimo_mestre(int(uid)):
            return {"ok": False, "erro": ERRO_ULTIMO_MESTRE}
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
        if mestre is not None and tem_mestre:
            conn.execute("UPDATE analisesps.usuarios SET mestre = ?"
                         " WHERE id = ?", (bool(mestre), int(uid)))
        if pode_operar is not None:
            # mestre altera sempre: as duas marcações não podem discordar
            conn.execute("UPDATE analisesps.usuarios SET pode_operar = ?"
                         " WHERE id = ?",
                         (bool(pode_operar) or bool(mestre), int(uid)))
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
    from .db import conexao, tem_coluna
    if tem_coluna("usuarios", "mestre") and _e_o_ultimo_mestre(int(uid)):
        return {"ok": False, "erro": ERRO_ULTIMO_MESTRE}
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



def _e_o_ultimo_mestre(uid: int) -> bool:
    """Esta pessoa é o único mestre ATIVO que existe?

    Pergunta feita antes de apagar, desativar ou desmarcar — as três coisas
    que deixariam o sistema sem administrador."""
    from .db import consultar_um
    linha = consultar_um(
        "SELECT (SELECT count(*) FROM analisesps.usuarios WHERE mestre AND ativo), "
        "       (SELECT count(*) FROM analisesps.usuarios "
        "         WHERE id = ? AND mestre AND ativo)", (int(uid),))
    if not linha:
        return False
    return int(linha[0] or 0) <= 1 and int(linha[1] or 0) == 1


def ha_mestre() -> bool:
    """Existe alguém que administra? Enquanto não houver, a tela de entrada
    explica como entrar pela porta de emergência — senão o dono aplicaria a
    migração e ficaria olhando uma tela de login sem saber o que fazer."""
    if not _pronto():
        return False
    from .db import consultar_um, tem_coluna
    if not tem_coluna("usuarios", "mestre"):
        return False
    linha = consultar_um("SELECT count(*) FROM analisesps.usuarios"
                         " WHERE mestre AND ativo")
    return bool(linha and int(linha[0] or 0) > 0)
