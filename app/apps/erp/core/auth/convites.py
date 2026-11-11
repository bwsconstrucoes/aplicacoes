# -*- coding: utf-8 -*-
"""
O PRIMEIRO ACESSO — a pessoa grava a PRÓPRIA senha.

Desenho dele, em 09/11/2026, com estas palavras:

> *"Facilitar o acesso da pessoa. E facilitar o primeiro acesso. Como é que a
> gente poderia fazer o primeiro acesso? Minha sugestão: eu vou cadastrar o
> e-mail da pessoa, configurar o perfil — obras, etc. — e disparar um e-mail
> para ela gravar a senha dela."*

**E ISSO É MELHOR QUE O DE HOJE POR SEGURANÇA, não só por comodidade.** Hoje
alguém escolhe uma senha pela pessoa e manda por WhatsApp: a senha passa por um
terceiro, fica escrita numa conversa que ninguém apaga, e quase nunca é trocada
depois. Com o convite, **ninguém além da pessoa conhece a senha dela** — nem
quem cadastrou, nem quem tem acesso ao banco.

---

⚑ **O TOKEN NÃO É GUARDADO. Só o resumo dele (SHA-256).**

É a mesma razão de a senha ser hash: um backup que vaza, uma consulta mal
feita, um relatório exportado — nenhum deles entrega a conta de ninguém, porque
o que está no banco **não serve para abrir nada**. Guardar o token em claro
seria guardar uma chave mestra temporária de cada pessoa da empresa.

⚑ **USO ÚNICO E COM PRAZO.**

`usado_em` fecha o link assim que ele é usado: link de definir senha que
continua valendo é link que, reencaminhado por engano, troca a senha de
alguém. O prazo existe pelo mesmo motivo — convite esquecido numa caixa de
e-mail por seis meses é uma porta aberta que ninguém está olhando.

⚑ **CONVITE NOVO MATA O ANTERIOR.**

Reenviar porque "não chegou" tem de deixar UM link válido. Dois links abertos
é a situação em que a pessoa usa o velho, dá erro, e alguém jura que o sistema
está quebrado.

⚑ **CONVITE NÃO RESSUSCITA ACESSO BLOQUEADO.**

Quem foi desligado tem o acesso desligado (ver `cadastros/pessoas.desligar`).
Se o convite religasse, bastaria a pessoa ter um link velho no e-mail para
voltar a entrar depois de sair da empresa. Aqui ele é recusado com o motivo.
"""
from __future__ import annotations

import hashlib
import logging
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroValidacao, registrar_evento,
)
from app.apps.erp.db.models.cadastros import Usuario

logger = logging.getLogger(__name__)

TIPOS = ("PRIMEIRO_ACESSO", "NOVA_SENHA")
# Sete dias. Curto o bastante para um link esquecido não ficar valendo para
# sempre; longo o bastante para quem entrou de férias na semana do cadastro.
DIAS_DE_VALIDADE = 7
# O tamanho do segredo. 32 bytes em base64 dão 43 caracteres — não se adivinha,
# e ainda cabe numa linha de e-mail sem quebrar.
TAMANHO = 32
TAMANHO_MINIMO_DA_SENHA = 8


def _resumo(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def criar(s: Session, usuario_id: int, *, tipo: str = "PRIMEIRO_ACESSO",
          autor: Optional[Usuario] = None) -> tuple[int, str]:
    """Abre um convite e devolve (id, token EM CLARO).

    O token em claro existe **uma vez só**, nesta devolução: ele vai para o
    e-mail e some. Nem esta função o guarda.
    """
    if tipo not in TIPOS:
        raise ErroValidacao(f"Tipo de convite desconhecido: {tipo}.")
    pessoa = s.get(Usuario, usuario_id)
    if pessoa is None:
        raise ErroNaoEncontrado("Operador não encontrado.")
    if not (pessoa.email or "").strip():
        raise ErroValidacao(
            "Este operador não tem e-mail cadastrado — sem e-mail não há para "
            "onde mandar o convite.")

    # O ANTERIOR MORRE AQUI, e não quando o novo for usado.
    s.execute(text("""
        UPDATE usuario_convites SET usado_em = now()
         WHERE usuario_id = :u AND usado_em IS NULL"""), {"u": usuario_id})

    token = secrets.token_urlsafe(TAMANHO)
    expira = datetime.now(timezone.utc) + timedelta(days=DIAS_DE_VALIDADE)
    novo = s.execute(text("""
        INSERT INTO usuario_convites
            (usuario_id, tipo, token_hash, expira_em, criado_por, enviado_para)
        VALUES (:u, :t, :h, :e, :a, :p) RETURNING id"""),
        {"u": usuario_id, "t": tipo, "h": _resumo(token), "e": expira,
         "a": autor.id if autor else None,
         "p": (pessoa.email or "").strip().lower()}).scalar()
    registrar_evento(s, "usuario", usuario_id, "CONVITE_CRIADO",
                     {"tipo": tipo, "expira_em": str(expira)[:10]},
                     autor.id if autor else None)
    s.flush()
    return novo, token


def enviar(s: Session, usuario_id: int, *, link_base: str,
           tipo: str = "PRIMEIRO_ACESSO",
           autor: Optional[Usuario] = None) -> dict[str, Any]:
    """Cria o convite e manda por e-mail, pela CONTA DO SISTEMA.

    **Nunca levanta por falha de envio.** Devolve o que aconteceu, com o
    motivo escrito. Cadastro perdido porque o servidor de e-mail estava fora
    do ar seria muito pior que convite que não saiu — e o convite se reenvia
    por um botão.
    """
    from app.apps.erp.core.comum import email as svc_email
    from app.apps.erp.core.comum import email_sistema

    pessoa = s.get(Usuario, usuario_id)
    conta = email_sistema.conta(s)
    if conta is None:
        return {"enviado": False, "erro": email_sistema.o_que_falta(s),
                "link": ""}

    convite_id, token = criar(s, usuario_id, tipo=tipo, autor=autor)
    link = f"{link_base.rstrip('/')}/{token}"
    primeiro = tipo == "PRIMEIRO_ACESSO"
    assunto = ("Seu acesso ao ERP da BWS" if primeiro
               else "Nova senha do seu acesso ao ERP da BWS")
    corpo = "\n".join([
        f"Olá, {pessoa.nome}.",
        "",
        ("Seu acesso ao ERP da BWS Construções foi criado."
         if primeiro else
         "Foi pedida uma nova senha para o seu acesso ao ERP da BWS."),
        "Para escolher a sua senha, abra o endereço abaixo:",
        "", link, "",
        f"O link vale por {DIAS_DE_VALIDADE} dias e só funciona UMA vez.",
        "",
        f"Seu usuário de entrada é o e-mail: {pessoa.email}",
        "",
        "Ninguém do sistema sabe a sua senha — nem quem cadastrou você. "
        "Se você não esperava esta mensagem, avise o administrativo e não "
        "abra o link.",
        "",
        "Mensagem automática do ERP da BWS Construções.",
    ])
    registro = svc_email.enviar(
        s, empresa=conta, para=[pessoa.email], assunto=assunto, corpo=corpo,
        entidade_tipo="usuario_convite", entidade_id=convite_id, usuario=autor,
        destinatario_tipo="usuario", destinatario_id=usuario_id)
    deu = registro.situacao == "ENVIADO"
    s.execute(text("UPDATE usuario_convites SET envio_id = :e WHERE id = :i"),
              {"e": registro.id, "i": convite_id})
    if not deu:
        logger.warning("ERP/convite: não saiu para %s — %s",
                       pessoa.email, registro.erro)
    s.flush()
    # O LINK VOLTA PARA A TELA, e isso é de propósito: quando o e-mail não sai
    # (conta errada, servidor fora), quem cadastrou tem como entregar o acesso
    # de outro jeito em vez de ficar sem saída. É a mesma informação que o
    # e-mail levaria, e ela não fica guardada em lugar nenhum.
    return {"enviado": deu, "erro": registro.erro, "link": link,
            "para": pessoa.email}


# ---------------------------------------------------------------------------
# "ESQUECI MINHA SENHA" — a pessoa se resolve sozinha (10/11/2026)
#
# Ele: *"esqueci minha senha, é interessante, né? É necessário. Não deixar a
# autonomia."* Sem isto, toda senha esquecida é um telefonema para o
# administrativo, e quem está em obra espera a boa vontade de alguém.
#
# ⚑ TRÊS CUIDADOS, E CADA UM FECHA UM BURACO CONHECIDO:
#
# 1. **A RESPOSTA É SEMPRE A MESMA**, exista a conta ou não. Dizer "não há
#    conta com esse e-mail" transforma esta tela numa máquina de descobrir
#    quem trabalha na empresa — basta ir testando endereços.
# 2. **UM PEDIDO A CADA DEZ MINUTOS por pessoa.** Sem isso, qualquer um que
#    saiba o e-mail de alguém enche a caixa dela de convites — e, pior, cada
#    pedido novo invalida o link anterior, então bastaria repetir para
#    impedir a pessoa de entrar.
# 3. **CONTA BLOQUEADA NÃO RECEBE LINK.** Quem foi desligado não reabre o
#    próprio acesso pedindo senha nova.
# ---------------------------------------------------------------------------
MINUTOS_ENTRE_PEDIDOS = 10


def pedir_nova_senha(s: Session, email: str, *, link_base: str) -> dict[str, Any]:
    """A pessoa pediu o link pela tela de entrada. Devolve o que ACONTECEU.

    Quem chama **não mostra** este resultado para a pessoa: a tela responde
    sempre a mesma frase. O retorno existe para o log e para o teste.
    """
    endereco = (email or "").strip().lower()
    if "@" not in endereco:
        return {"enviado": False, "motivo": "endereço sem arroba"}
    pessoa = s.execute(text(
        "SELECT id, ativo FROM usuarios WHERE lower(email) = :e"),
        {"e": endereco}).mappings().first()
    if pessoa is None:
        logger.info("ERP/convite: pedido de senha para e-mail que não existe")
        return {"enviado": False, "motivo": "não existe conta"}
    if not pessoa["ativo"]:
        logger.warning("ERP/convite: pedido de senha de conta BLOQUEADA (%s)",
                       endereco)
        return {"enviado": False, "motivo": "conta bloqueada"}
    recente = s.execute(text("""
        SELECT 1 FROM usuario_convites
         WHERE usuario_id = :u AND usado_em IS NULL
           AND criado_em > now() - make_interval(mins => :m)"""),
        {"u": pessoa["id"], "m": MINUTOS_ENTRE_PEDIDOS}).first()
    if recente is not None:
        logger.info("ERP/convite: pedido repetido dentro de %s minutos",
                    MINUTOS_ENTRE_PEDIDOS)
        return {"enviado": False, "motivo": "pedido repetido"}
    r = enviar(s, pessoa["id"], link_base=link_base, tipo="NOVA_SENHA")
    return {"enviado": r["enviado"], "motivo": r.get("erro") or ""}


def conferir(s: Session, token: str) -> dict[str, Any]:
    """O convite existe, está no prazo e não foi usado? Devolve de quem é.

    Erros em português e SEM dizer demais: "este link não vale mais" serve
    tanto para inexistente quanto para usado. Dizer qual dos dois ajudaria
    quem está testando links no escuro.
    """
    linha = s.execute(text("""
        SELECT c.id, c.usuario_id, c.tipo, c.expira_em, c.usado_em,
               u.nome, u.email, u.ativo
          FROM usuario_convites c JOIN usuarios u ON u.id = c.usuario_id
         WHERE c.token_hash = :h"""),
        {"h": _resumo(token or "")}).mappings().first()
    if linha is None or linha["usado_em"] is not None:
        raise ErroValidacao(
            "Este link não vale mais. Peça ao administrativo para enviar "
            "outro convite.")
    if linha["expira_em"] < datetime.now(timezone.utc):
        raise ErroValidacao(
            f"Este link venceu (ele vale {DIAS_DE_VALIDADE} dias). Peça ao "
            f"administrativo para enviar outro convite.")
    if not linha["ativo"]:
        raise ErroValidacao(
            "Este acesso está bloqueado. Fale com o administrativo.")
    return {"convite_id": linha["id"], "usuario_id": linha["usuario_id"],
            "tipo": linha["tipo"], "nome": linha["nome"],
            "email": linha["email"]}


def usar(s: Session, token: str, senha: str) -> dict[str, Any]:
    """Grava a senha que a PESSOA escolheu e fecha o convite."""
    from app.apps.erp.core.auth.service import gerar_hash

    achado = conferir(s, token)
    senha = (senha or "").strip()
    if len(senha) < TAMANHO_MINIMO_DA_SENHA:
        raise ErroValidacao(
            f"A senha precisa ter pelo menos {TAMANHO_MINIMO_DA_SENHA} "
            f"caracteres.")
    pessoa = s.get(Usuario, achado["usuario_id"])
    pessoa.senha_hash = gerar_hash(senha)
    s.execute(text("UPDATE usuario_convites SET usado_em = now() WHERE id = :i"),
              {"i": achado["convite_id"]})
    # O AUTOR DO EVENTO É A PRÓPRIA PESSOA: foi ela quem definiu a senha, e a
    # trilha tem de dizer isso — não quem mandou o convite dias atrás.
    registrar_evento(s, "usuario", pessoa.id, "SENHA_DEFINIDA",
                     {"por": "a própria pessoa, pelo convite"}, pessoa.id)
    s.flush()
    return {"nome": pessoa.nome, "email": pessoa.email}


def situacao(s: Session, usuario_id: int) -> dict[str, Any]:
    """O que a tela do cadastro mostra: há convite aberto? quando venceu?"""
    linha = s.execute(text("""
        SELECT tipo, criado_em, expira_em, usado_em, enviado_para
          FROM usuario_convites WHERE usuario_id = :u
         ORDER BY criado_em DESC, id DESC LIMIT 1"""),
        {"u": usuario_id}).mappings().first()
    if linha is None:
        return {"tem": False}
    agora = datetime.now(timezone.utc)
    return {
        "tem": True, "tipo": linha["tipo"],
        "enviado_para": linha["enviado_para"],
        "criado_em": str(linha["criado_em"])[:16],
        # `expira_DIA`, e não `expira_em`: o que sai daqui já é SÓ A DATA, e o
        # nome tem de dizer isso. A trava das telas confere o nome do campo
        # contra o tipo da coluna, e com razão — foi assim que apareceu, numa
        # tela, "08 17:50:45.327771+00:00/11/2026".
        "expira_dia": str(linha["expira_em"])[:10],
        "usado": linha["usado_em"] is not None,
        "usado_em": str(linha["usado_em"])[:16] if linha["usado_em"] else None,
        "vencido": linha["usado_em"] is None and linha["expira_em"] < agora,
    }
