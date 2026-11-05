# -*- coding: utf-8 -*-
"""
LER A CAIXA DE ENTRADA — o ERP recebendo e-mail, não só mandando.

O dono, em 05/11/2026, sobre o ponto cego que eu havia registrado:

> *"Isso é fundamental. Que agente é esse que depende de uma intervenção
> humana? Aí não é agente. Todo e-mail que chegar, vou ter que jogar os anexos?
> Não tem sentido nenhum. Então o ERP precisa estar fazendo a leitura das
> caixas de entrada, baixando anexo de forma completamente autônoma para saber
> o que fazer."*

Ele está certo. Este arquivo é o conserto.

**NENHUMA BIBLIOTECA NOVA ENTRA POR CAUSA DISTO:** `imaplib` e `email` são da
biblioteca padrão do Python. Era o único custo real que eu tinha a apontar, e
ele não existe.

**A CREDENCIAL NÃO É ENVVAR**, e isso é regra do repositório: a senha da caixa
fica **cifrada no banco** (`core/comum/segredos.py`, a mesma mecânica da senha
de SMTP das empresas) e é configurada na tela. Assim ligar a leitura não pede
deploy, e a senha não aparece em log, em tela nem no histórico do Git.

**COMO A RESPOSTA É CASADA COM O PEDIDO**, em três tentativas, da mais confiável
para a menos:

  1. **`In-Reply-To` / `References`** contra o `Message-ID` que o ERP gerou ao
     enviar. É o casamento certo: não depende de ninguém manter nada intacto.
  2. **A marca `[CT-00001]` no assunto**, para quando o cliente de e-mail do
     outro lado não devolve o cabeçalho (acontece).
  3. **O CPF no assunto ou no corpo**, quando há UM pedido aberto para aquela
     pessoa. Com mais de um, não escolhe: deixa SEM_DONO, porque anexar a
     rescisão no pedido de férias é pior que não anexar.

**O QUE ELE NUNCA FAZ:** apagar mensagem, responder sozinho, e arquivar anexo em
pedido que ele não casou com certeza. Mensagem sem dono vira linha na tela, com
o motivo escrito — "não achei de quem é" é informação; silêncio não é.
"""
from __future__ import annotations

import email
import imaplib
import logging
import re
from email.header import decode_header, make_header
from email.message import Message
from email.utils import parsedate_to_datetime
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.comum.segredos import cifrar, decifrar
from app.apps.erp.db.models.cadastros import Parametro, Usuario

logger = logging.getLogger(__name__)

# As chaves na tabela `parametros`, que é onde o ERP guarda configuração de tela.
CHAVES = {
    "servidor": "email_entrada_servidor",
    "porta": "email_entrada_porta",
    "usuario": "email_entrada_usuario",
    "senha": "email_entrada_senha",          # cifrada
    "pasta": "email_entrada_pasta",
    "ligado": "email_entrada_ligado",
}
# Tamanho máximo de anexo que entra. Acima disso, a mensagem fica registrada com
# o motivo e ninguém fica sem saber: anexo de 80 MB numa instância que divide
# 2 GB com treze módulos derruba o monorepo inteiro, não só a leitura.
TETO_ANEXO = 25 * 1024 * 1024
# Quantas mensagens por rodada. Caixa com mil mensagens não lidas (a primeira
# vez que isso ligar) não pode virar uma rodada de uma hora.
TETO_MENSAGENS = 40
MARCA_PEDIDO = re.compile(r"\[(CT-\d{4,6})\]")
MARCA_DEVOLUCAO = re.compile(r"\[(DV-\d{4,6})\]")
SO_DIGITOS = re.compile(r"\D")
CPF_NO_TEXTO = re.compile(r"\b(\d{3}\.?\d{3}\.?\d{3}-?\d{2})\b")

TIPOS_QUE_ENTRAM = (
    "application/pdf", "image/jpeg", "image/png", "image/heic",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
)


# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
def _ler(s: Session, chave: str, padrao: str = "") -> str:
    linha = s.get(Parametro, CHAVES[chave])
    return (linha.valor if linha is not None else padrao) or padrao


def _gravar(s: Session, chave: str, valor: str,
            usuario: Optional[Usuario] = None) -> None:
    nome = CHAVES[chave]
    linha = s.get(Parametro, nome)
    if linha is None:
        linha = Parametro(chave=nome, valor=valor)
        s.add(linha)
    else:
        linha.valor = valor
    if hasattr(linha, "alterado_por"):
        linha.alterado_por = usuario.id if usuario else None


def configuracao(s: Session) -> dict[str, Any]:
    """O que está configurado. A SENHA NUNCA SAI — só se ela existe."""
    senha = _ler(s, "senha")
    return {
        "servidor": _ler(s, "servidor"),
        "porta": int(_ler(s, "porta", "993") or 993),
        "usuario": _ler(s, "usuario"),
        "pasta": _ler(s, "pasta", "INBOX") or "INBOX",
        "tem_senha": bool(senha),
        "ligado": _ler(s, "ligado") == "1",
        # "usável" é diferente de "ligado": sem servidor e sem senha, ligar não
        # adianta — e a tela precisa dizer isso em vez de deixar apertar um
        # botão que não faz nada.
        "usavel": bool(_ler(s, "servidor") and _ler(s, "usuario") and senha),
    }


def configurar(s: Session, dados: dict[str, Any],
               usuario: Optional[Usuario] = None) -> dict[str, Any]:
    for campo in ("servidor", "usuario", "pasta"):
        if campo in dados:
            _gravar(s, campo, str(dados[campo] or "").strip(), usuario)
    if "porta" in dados:
        _gravar(s, "porta", str(int(dados["porta"] or 993)), usuario)
    aviso = ""
    if dados.get("senha"):
        # CIFRADA, NUNCA EM CLARO. Mesma mecânica da senha de SMTP. E a falta da
        # chave do cofre NÃO derruba o resto: o servidor e o usuário são salvos,
        # e o recado diz o que falta — o contrário faria a pessoa perder o que
        # digitou por causa de uma variável de ambiente.
        try:
            _gravar(s, "senha", cifrar(str(dados["senha"])), usuario)
        except ErroValidacao as e:
            aviso = str(e)
    if "ligado" in dados:
        _gravar(s, "ligado", "1" if dados["ligado"] else "0", usuario)
    s.flush()
    return {**configuracao(s), "aviso": aviso}


# ---------------------------------------------------------------------------
# A conexão
# ---------------------------------------------------------------------------
class ErroDeLeitura(Exception):
    """Falha ao falar com o servidor, já em português."""


def _conectar(s: Session):
    c = configuracao(s)
    if not c["usavel"]:
        raise ErroValidacao(
            "Falta configurar a caixa de entrada: servidor, usuário e senha. "
            "Enquanto isso, o agente não tem como ler o retorno da "
            "contabilidade.")
    senha = decifrar(_ler(s, "senha"))
    try:
        caixa = imaplib.IMAP4_SSL(c["servidor"], c["porta"], timeout=30)
        caixa.login(c["usuario"], senha or "")
        caixa.select(c["pasta"], readonly=False)
        return caixa
    except imaplib.IMAP4.error as e:
        raise ErroDeLeitura(
            f"O servidor recusou a conexão ou o login: {e}. Confira o "
            "endereço, o usuário e a senha — e, se a conta for Google, use "
            "uma senha de aplicativo.") from e
    except OSError as e:
        raise ErroDeLeitura(
            f"Não consegui falar com {c['servidor']}:{c['porta']} — {e}") from e


def testar(s: Session) -> dict[str, Any]:
    """Conecta, conta as não lidas e sai. É o que transforma "acho que
    configurei" em "está configurado"."""
    caixa = _conectar(s)
    try:
        ok, dados = caixa.search(None, "UNSEEN")
        quantas = len((dados[0] or b"").split()) if ok == "OK" else 0
        return {"conectou": True, "nao_lidas": quantas}
    finally:
        _fechar(caixa)


def _fechar(caixa) -> None:
    try:
        caixa.close()
        caixa.logout()
    except Exception:                                   # noqa: BLE001
        pass


# ---------------------------------------------------------------------------
# Ler a mensagem
# ---------------------------------------------------------------------------
def _texto_do_cabecalho(valor: Any) -> str:
    if not valor:
        return ""
    try:
        return str(make_header(decode_header(str(valor))))
    except Exception:                                   # noqa: BLE001
        return str(valor)


def _corpo_em_texto(msg: Message) -> str:
    """Só o texto, para procurar CPF. HTML entra sem as marcações."""
    partes = []
    for p in msg.walk() if msg.is_multipart() else [msg]:
        if p.get_content_maintype() != "text":
            continue
        try:
            bruto = p.get_payload(decode=True) or b""
            txt = bruto.decode(p.get_content_charset() or "utf-8", "replace")
        except Exception:                               # noqa: BLE001
            continue
        if p.get_content_subtype() == "html":
            txt = re.sub(r"<[^>]+>", " ", txt)
        partes.append(txt)
    return "\n".join(partes)[:20000]


def _anexos(msg: Message) -> list[dict[str, Any]]:
    saida = []
    for p in msg.walk():
        if p.get_content_maintype() == "multipart":
            continue
        nome = _texto_do_cabecalho(p.get_filename())
        disposicao = (p.get("Content-Disposition") or "").lower()
        if not nome and "attachment" not in disposicao:
            continue
        try:
            conteudo = p.get_payload(decode=True) or b""
        except Exception:                               # noqa: BLE001
            continue
        if not conteudo:
            continue
        saida.append({
            "nome": nome or "anexo",
            "mime": p.get_content_type(),
            "conteudo": conteudo,
            "grande": len(conteudo) > TETO_ANEXO,
        })
    return saida


# ---------------------------------------------------------------------------
# CASAR A MENSAGEM COM O QUE A ORIGINOU — três tentativas, nesta ordem
# ---------------------------------------------------------------------------
def casar(s: Session, *, in_reply_to: str, references: str, assunto: str,
          corpo: str) -> dict[str, Any]:
    """De quem é esta mensagem? Devolve `{pedido_id, devolucao_id, como, motivo}`.

    Quando não dá para saber COM CERTEZA, devolve vazio com o motivo escrito.
    Anexar a rescisão no pedido de férias é pior que não anexar nada.
    """
    ids = [x.strip() for x in
           re.split(r"[\s,]+", f"{in_reply_to} {references}") if x.strip()]
    if ids:
        linha = s.execute(text("""
            SELECT id FROM dp_pedidos
             WHERE message_id = ANY(:ids) ORDER BY id DESC LIMIT 1"""),
            {"ids": ids}).first()
        if linha:
            return {"pedido_id": linha.id, "como": "In-Reply-To"}

    m = MARCA_PEDIDO.search(assunto or "") or MARCA_PEDIDO.search(corpo or "")
    if m:
        linha = s.execute(text("SELECT id FROM dp_pedidos WHERE numero = :n"),
                          {"n": m.group(1)}).first()
        if linha:
            return {"pedido_id": linha.id, "como": "número no assunto"}
        return {"motivo": f"o assunto cita {m.group(1)}, que não existe aqui"}

    d = MARCA_DEVOLUCAO.search(assunto or "")
    if d:
        linha = s.execute(text("SELECT id FROM dp_devolucoes WHERE numero = :n"),
                          {"n": d.group(1)}).first()
        if linha:
            return {"devolucao_id": linha.id, "como": "número da devolução"}

    cpfs = {SO_DIGITOS.sub("", x) for x in
            CPF_NO_TEXTO.findall(f"{assunto}\n{corpo}")}
    cpfs = {c for c in cpfs if len(c) == 11}
    if cpfs:
        linhas = s.execute(text("""
            SELECT p.id, c.nome FROM dp_pedidos p
              JOIN colaboradores c ON c.id = p.colaborador_id
             WHERE c.cpf = ANY(:cpfs)
               AND p.situacao IN ('ENVIADO', 'COBRADO')"""),
            {"cpfs": list(cpfs)}).mappings().all()
        if len(linhas) == 1:
            return {"pedido_id": linhas[0]["id"], "como": "CPF no texto"}
        if len(linhas) > 1:
            return {"motivo": (f"o CPF casa com {len(linhas)} pedidos abertos "
                               "desta pessoa — não escolho: anexar no pedido "
                               "errado é pior que não anexar")}
    return {"motivo": "não achei a que pedido esta mensagem responde"}


# ---------------------------------------------------------------------------
# A RODADA: ler, casar, arquivar, conferir
# ---------------------------------------------------------------------------
def rodada(s: Session, usuario: Optional[Usuario] = None, *,
           gravar: bool = True) -> dict[str, Any]:
    """Lê as não lidas, casa cada uma e entrega o retorno ao pedido.

    `gravar=False` só olha e conta — é o botão "conferir" da tela, para ver o
    que ele FARIA antes de deixá-lo mexer em pedido nenhum.

    Mensagem já processada é pulada pelo Message-ID: processar duas vezes
    significaria dois documentos iguais no Drive e dois eventos "respondido" no
    mesmo pedido.
    """
    from app.apps.erp.core import contabilidade as svc_ct

    conf = configuracao(s)
    if not conf["usavel"]:
        return {"ligado": False, "erro": "a caixa de entrada não está configurada"}

    caixa = _conectar(s)
    rel: dict[str, Any] = {
        "ligado": True, "gravou": gravar, "olhou": 0, "casadas": 0,
        "sem_dono": 0, "sem_anexo": 0, "repetidas": 0, "arquivados": 0,
        "mensagens": [], "erros": [],
    }
    try:
        ok, dados = caixa.search(None, "UNSEEN")
        if ok != "OK":
            return {**rel, "erro": "o servidor não respondeu à busca"}
        numeros = (dados[0] or b"").split()[:TETO_MENSAGENS]
        for num in numeros:
            rel["olhou"] += 1
            try:
                _uma_mensagem(s, caixa, num, rel, usuario, gravar, svc_ct)
            except Exception as e:                      # noqa: BLE001
                logger.exception("ERP/caixa: falha numa mensagem")
                rel["erros"].append(str(e))
    finally:
        _fechar(caixa)
    logger.info("ERP/caixa: %d mensagens, %d casadas, %d sem dono, %d anexos "
                "arquivados (%s)", rel["olhou"], rel["casadas"],
                rel["sem_dono"], rel["arquivados"],
                "gravado" if gravar else "simulação")
    return rel


def _uma_mensagem(s: Session, caixa, num: bytes, rel: dict[str, Any],
                  usuario: Optional[Usuario], gravar: bool, svc_ct) -> None:
    ok, bruto = caixa.fetch(num, "(RFC822)")
    if ok != "OK" or not bruto or not isinstance(bruto[0], tuple):
        rel["erros"].append("não consegui baixar uma das mensagens")
        return
    msg = email.message_from_bytes(bruto[0][1])
    mid = (msg.get("Message-ID") or "").strip()
    assunto = _texto_do_cabecalho(msg.get("Subject"))
    de = _texto_do_cabecalho(msg.get("From"))
    if not mid:
        # Sem Message-ID não há como garantir que não vai ser processada de
        # novo. Gerar um nosso aqui seria pior: na próxima rodada seria outro.
        mid = f"<sem-id-{assunto[:40]}-{de[:40]}>"
    if s.execute(text("SELECT 1 FROM email_entrada_mensagens "
                      "WHERE message_id = :m"), {"m": mid}).first():
        rel["repetidas"] += 1
        return

    corpo = _corpo_em_texto(msg)
    anexos = _anexos(msg)
    achado = casar(s, in_reply_to=msg.get("In-Reply-To") or "",
                   references=msg.get("References") or "",
                   assunto=assunto, corpo=corpo)
    quando = None
    try:
        quando = parsedate_to_datetime(msg.get("Date")) if msg.get("Date") else None
    except Exception:                                   # noqa: BLE001
        quando = None

    situacao, motivo, arquivados = "LIDA", achado.get("motivo"), 0
    if not achado.get("pedido_id") and not achado.get("devolucao_id"):
        situacao = "SEM_DONO"
        rel["sem_dono"] += 1
    elif not [a for a in anexos if not a["grande"]]:
        # Casou, mas não trouxe documento. É resposta de texto ("estamos
        # providenciando") — e isso NÃO é o retorno: o pedido continua
        # esperando, e dizer o contrário fecharia o ciclo cedo.
        situacao = "SEM_ANEXO"
        motivo = "a mensagem casou com o pedido mas não trouxe anexo"
        rel["sem_anexo"] += 1
        if gravar and achado.get("pedido_id"):
            svc_ct._evento(s, achado["pedido_id"], "RESPOSTA_SEM_ANEXO",
                           f"a contabilidade respondeu ({de}) sem anexar o "
                           "documento — o pedido continua esperando",
                           {"assunto": assunto}, usuario, True)
    else:
        situacao = "CASADA"
        rel["casadas"] += 1
        if gravar:
            arquivados = _entregar_anexos(s, achado, anexos, de, assunto,
                                          usuario, svc_ct)
            rel["arquivados"] += arquivados

    if gravar:
        s.execute(text("""
            INSERT INTO email_entrada_mensagens
                (message_id, de, assunto, recebido_em, pedido_id, devolucao_id,
                 situacao, motivo, anexos, arquivados)
            VALUES (:m, :de, :a, :q, :p, :d, :s, :mo, :n, :ar)
            ON CONFLICT (message_id) DO NOTHING"""),
            {"m": mid, "de": de, "a": assunto, "q": quando,
             "p": achado.get("pedido_id"), "d": achado.get("devolucao_id"),
             "s": situacao, "mo": motivo, "n": len(anexos), "ar": arquivados})
        # MARCA COMO LIDA só depois de tudo dar certo. Se a rodada morrer no
        # meio, a mensagem volta na próxima — melhor reprocessar (o Message-ID
        # impede duplicar) que perder.
        try:
            caixa.store(num, "+FLAGS", "\\Seen")
        except Exception:                               # noqa: BLE001
            pass
    rel["mensagens"].append({"de": de, "assunto": assunto,
                             "situacao": situacao, "motivo": motivo,
                             "anexos": len(anexos), "arquivados": arquivados,
                             "como": achado.get("como", "")})


def _entregar_anexos(s: Session, achado: dict[str, Any],
                     anexos: list[dict[str, Any]], de: str, assunto: str,
                     usuario: Optional[Usuario], svc_ct) -> int:
    """Arquiva no Drive, com o TIPO que o pedido espera, e confere o retorno.

    O tipo vem do pedido, não do nome do arquivo: "doc1.pdf" não diz nada, e o
    pedido sabe exatamente o que estava esperando.
    """
    from app.apps.erp.core.arquivo import service as svc_arq

    quantos, primeiro_id = 0, None
    if achado.get("pedido_id"):
        p = svc_ct.ler(s, achado["pedido_id"])
        tipo = p.get("documento_esperado") or "DOC-PESSOA-OUTRO"
        dono = {"colaborador_id": p["colaborador_id"]}
        referencia = f"{p['numero']} — retorno da contabilidade"
    else:
        linha = s.execute(text("""
            SELECT colaborador_id, documento_esperado, numero
              FROM dp_devolucoes WHERE id = :d"""),
            {"d": achado["devolucao_id"]}).mappings().first()
        tipo = (linha or {}).get("documento_esperado") or "DOC-PESSOA-OUTRO"
        dono = {"colaborador_id": (linha or {}).get("colaborador_id")}
        referencia = f"{(linha or {}).get('numero', '')} — devolvido"

    for a in anexos:
        if a["grande"]:
            continue
        if a["mime"] not in TIPOS_QUE_ENTRAM:
            # Assinatura de imagem, ícone de rodapé e logotipo do e-mail vêm
            # como anexo. Arquivar isso enche o Drive de lixo com cara de
            # documento.
            continue
        try:
            d = svc_arq.arquivar(
                s, a["conteudo"], a["nome"], tipo_codigo=tipo, **dono,
                referencia=referencia, origem="EMAIL",
                observacao=f"recebido de {de} — assunto: {assunto}",
                usuario=usuario)
            quantos += 1
            primeiro_id = primeiro_id or d.id
        except ErroValidacao as e:
            logger.warning("ERP/caixa: não arquivei %s — %s", a["nome"], e)

    if achado.get("pedido_id") and primeiro_id:
        # O RETORNO CHEGOU. A conferência roda com o que o documento trouxer —
        # e, por enquanto, isso é vazio: a leitura do CONTEÚDO é a rotina de
        # conferência do agente, e ela é a última do desenho. Sem ela, o pedido
        # fica RESPONDIDO **sem conferência**, e a tela diz isso.
        svc_ct.receber(s, achado["pedido_id"], usuario,
                       documento_id=primeiro_id, lido={}, pelo_agente=True)
    elif achado.get("devolucao_id") and primeiro_id:
        s.execute(text("""
            UPDATE dp_devolucoes
               SET situacao = 'DEVOLVIDA', documento_id = :doc,
                   devolvido_em = now() WHERE id = :d"""),
            {"doc": primeiro_id, "d": achado["devolucao_id"]})
    return quantos
