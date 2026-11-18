# -*- coding: utf-8 -*-
"""
PRIMEIRO ACESSO PELO PRÓPRIO PORTAL — a pessoa prova quem é, e alguém aprova.

Desenho dele em 18/11/2026, ao tentar orientar a equipe e esbarrar no absurdo:

> *"Então tá errado esse acesso ser liberado pelo aplicativo de ponto. Qual o
> sentido que tem de uma pessoa que vai lançar um diário ter que ir lá no
> aplicativo do ponto? Não tem sentido nenhum. (…) O cara tem que ir pro portal
> e criar o acesso dele por lá."*

Ele estava certo, e o defeito era meu: eu pus o pedido no ponto porque lá a
pessoa JÁ estava identificada — resolvi o meu problema, não o dela.

**AS DUAS PERGUNTAS, E ESTE MÓDULO SÓ RESPONDE A PRIMEIRA:**

  · **QUEM é esta pessoa?** → o CPF, o telefone e o código do WhatsApp.
  · **Ela DEVE ter o portal?** → gente, na fila de sempre. Decisão dele no
    mesmo dia: *"ainda assim precisa ser validado por alguém no sistema (…)
    aí aumenta o nível de segurança."*

⚑ O TELEFONE CONFERE, NÃO ESCOLHE O DESTINO. Ele: *"o CPF e telefone já
cadastrado em sistema. Não vai ser para um telefone que não existe no
sistema."* O código vai para o número DO CADASTRO. Se fosse para o digitado,
quem soubesse o CPF de alguém poria o próprio telefone e entraria no lugar
dela — o que a pessoa digita é prova, não endereço.

⚑ A RESPOSTA É SEMPRE A MESMA, exista o CPF ou não, bata o telefone ou não.
Dizer "este CPF não existe" transformaria a tela num conferidor de CPF, e
dizer "o telefone não bate" confirmaria que aquele CPF está na base. O que não
saiu fica registrado em ERP › Mensagens, com o motivo, para o DP ver e
consertar o cadastro — a pessoa liga para o escritório e o escritório já sabe
o que aconteceu.

⚑ A FILA É A MESMA de quem pede pelo ponto (`portal_pedidos_de_acesso`). Não
nasce uma segunda fila nem um segundo botão de liberar: é o erro que ele
cobrou em 17/11 (*"três botões para importar pessoas"*). O que muda é a ORIGEM
e a marca de identidade confirmada.
"""
from __future__ import annotations

import logging
import re
import secrets
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger("erp.primeiro_acesso")

# A MESMA resposta para tudo. Ver o cabeçalho.
RESPOSTA = ("Se o CPF e o telefone conferirem com o nosso cadastro, o código "
            "chega no WhatsApp desse telefone em alguns instantes. Ele vale "
            "15 minutos.")

VALIDADE_MIN = 15
CODIGOS_POR_HORA = 3
TENTATIVAS = 5
FINALIDADE = "erp.codigo_portal"


def _so_digitos(valor: Any) -> str:
    return re.sub(r"\D", "", str(valor or ""))


def _resumo(codigo: str) -> str:
    return sha256(codigo.encode()).hexdigest()


def _agora() -> datetime:
    return datetime.now(timezone.utc)


def _mesmo_telefone(do_cadastro: Any, digitado: Any) -> bool:
    """Os dois telefones são a mesma linha?

    ⚑ COMPARA PELOS ÚLTIMOS 8 DÍGITOS, e é o que faz isto funcionar no mundo
    real: o cadastro tem "(81) 98888-7777", a pessoa digita "81988887777", e
    um tem o nono dígito que o outro não tem. Comparar o texto inteiro
    reprovaria quase todo mundo por causa de parêntese e traço; comparar os 8
    finais acerta a linha e ignora formato, DDI e o nono dígito.

    Oito e não nove porque o nono dígito é exatamente o que costuma faltar num
    dos lados. Oito dígitos ainda identificam a linha dentro do DDD.
    """
    a, b = _so_digitos(do_cadastro), _so_digitos(digitado)
    if len(a) < 8 or len(b) < 8:
        return False
    return a[-8:] == b[-8:]


def _registrar_nao_enviado(motivo: str, *, cpf: str = "", telefone: str = "",
                           status: str = "BARRADO") -> None:
    """Deixa o que NÃO saiu visível em ERP › Mensagens, com o motivo.

    É o que salva a pessoa do silêncio: ela recebe a resposta genérica, liga
    para o escritório, e o escritório já sabe o que aconteceu.
    """
    try:
        from app.apps.ponto.core.envios import registrar_nao_enviado as reg
        reg(FINALIDADE, motivo, telefone=telefone, cpf=cpf,
            texto="código de primeiro acesso ao portal", status=status)
    except Exception:                                       # noqa: BLE001
        logger.warning("Portal: não consegui registrar o não-envio (%s)", motivo)


def _mandar(telefone: str, texto: str) -> None:
    """Manda por WhatsApp, FORA da requisição.

    Em segundo plano porque o envio fala com um serviço de fora: esperar por
    ele deixaria a pessoa olhando uma tela parada, e um serviço lento viraria
    uma tela travada.
    """
    def trabalho():
        try:
            from app.apps.notificador import notificar
            notificar(telefone=telefone, mensagem=texto,
                      canais=("whatsapp",), finalidade=FINALIDADE)
        except Exception as e:                              # noqa: BLE001
            logger.exception("Portal: falha ao mandar o código por WhatsApp")
            _registrar_nao_enviado(f"erro ao chamar o envio: {str(e)[:200]}",
                                   telefone=telefone, status="FALHOU")

    import threading
    threading.Thread(target=trabalho, daemon=True).start()


# ---------------------------------------------------------------------------
# 1. Pedir o código
# ---------------------------------------------------------------------------
def pedir_codigo(s: Session, cpf: Any, telefone: Any, *,
                 ip: str = "") -> dict[str, Any]:
    """Manda o código para o telefone DO CADASTRO. Resposta sempre igual."""
    cpf_ok = _so_digitos(cpf)
    if len(cpf_ok) != 11:
        # O ÚNICO erro que a tela conta, porque não revela nada: um CPF com
        # dez dígitos está errado para qualquer um, exista ou não na base.
        return {"ok": False, "erro": "Confira o CPF — ele tem 11 números."}

    linha = s.execute(text("""
        SELECT c.id, c.nome, c.telefone, c.situacao, c.obra_id, c.usuario_id,
               u.ativo AS acesso_ativo, u.email AS email_do_acesso
          FROM colaboradores c
          LEFT JOIN usuarios u ON u.id = c.usuario_id
         WHERE regexp_replace(COALESCE(c.cpf, ''), '\\D', '', 'g') = :cpf
         LIMIT 1"""), {"cpf": cpf_ok}).mappings().first()

    if linha is None:
        _registrar_nao_enviado(
            "CPF não encontrado no cadastro (digitado errado, ou a pessoa não "
            "está na base)", cpf=cpf_ok)
        return {"ok": True, "mensagem": RESPOSTA}

    if (linha["situacao"] or "").upper() == "DESLIGADO":
        _registrar_nao_enviado("pessoa desligada — o código não é enviado",
                               cpf=cpf_ok, telefone=linha["telefone"] or "")
        return {"ok": True, "mensagem": RESPOSTA}

    # ⚑ QUEM JÁ TEM ACESSO NÃO FAZ PRIMEIRO ACESSO — é "esqueci a senha", e
    # para isso há tela própria. Mandar código aqui faria a pessoa percorrer
    # o caminho inteiro para descobrir no fim que já tinha conta.
    if linha["usuario_id"] and linha["acesso_ativo"]:
        _registrar_nao_enviado(
            "já tem acesso ao sistema — o caminho dela é “Esqueci minha senha”",
            cpf=cpf_ok, telefone=linha["telefone"] or "")
        return {"ok": True, "mensagem": RESPOSTA}

    if not _mesmo_telefone(linha["telefone"], telefone):
        _registrar_nao_enviado(
            f"o telefone digitado não confere com o do cadastro (está: "
            f"{linha['telefone'] or 'vazio'}) — confira o cadastro de "
            f"{linha['nome']}", cpf=cpf_ok, telefone=linha["telefone"] or "")
        return {"ok": True, "mensagem": RESPOSTA}

    do_cadastro = _so_digitos(linha["telefone"])
    if len(do_cadastro) < 10:
        _registrar_nao_enviado(
            f"sem celular com DDD no cadastro de {linha['nome']} (está: "
            f"{linha['telefone'] or 'vazio'})", cpf=cpf_ok, status="SEM_DESTINO")
        return {"ok": True, "mensagem": RESPOSTA}

    recentes = s.execute(text("""
        SELECT count(*) FROM portal_codigos_de_acesso
         WHERE colaborador_id = :c AND criado_em > now() - interval '1 hour'"""),
        {"c": linha["id"]}).scalar() or 0
    if recentes >= CODIGOS_POR_HORA:
        _registrar_nao_enviado(
            f"já pediu {recentes} códigos na última hora (o teto é "
            f"{CODIGOS_POR_HORA})", cpf=cpf_ok, telefone=do_cadastro)
        return {"ok": True, "mensagem": RESPOSTA}

    codigo = f"{secrets.randbelow(1_000_000):06d}"
    s.execute(text("""
        INSERT INTO portal_codigos_de_acesso
               (colaborador_id, codigo_hash, expira_em, ip)
        VALUES (:c, :h, :e, :ip)"""),
        {"c": linha["id"], "h": _resumo(codigo),
         "e": _agora() + timedelta(minutes=VALIDADE_MIN), "ip": (ip or "")[:64]})
    s.flush()

    destino = do_cadastro if do_cadastro.startswith("55") else "55" + do_cadastro
    _mandar(destino, f"BWS: seu código para criar o acesso ao portal é "
                     f"{codigo}. Ele vale {VALIDADE_MIN} minutos. Não passe "
                     f"este código para ninguém.")
    logger.info("Portal: código de primeiro acesso para %s", linha["nome"])
    return {"ok": True, "mensagem": RESPOSTA}


# ---------------------------------------------------------------------------
# 2. Conferir o código e abrir o pedido
# ---------------------------------------------------------------------------
class CodigoRecusado(Exception):
    """O código não serve. A frase já vem pronta para a tela."""


def _conferir(s: Session, cpf_ok: str, codigo: Any) -> dict[str, Any]:
    """Devolve o colaborador, ou levanta com a frase que a pessoa lê."""
    digitado = _so_digitos(codigo)
    pessoa = s.execute(text("""
        SELECT id, nome, obra_id FROM colaboradores
         WHERE regexp_replace(COALESCE(cpf, ''), '\\D', '', 'g') = :cpf
         LIMIT 1"""), {"cpf": cpf_ok}).mappings().first()
    if pessoa is None or len(digitado) != 6:
        raise CodigoRecusado("Código inválido. Confira os seis números.")

    linha = s.execute(text("""
        SELECT id, codigo_hash, expira_em, usado_em, tentativas
          FROM portal_codigos_de_acesso
         WHERE colaborador_id = :c AND usado_em IS NULL
         ORDER BY id DESC LIMIT 1"""),
        {"c": pessoa["id"]}).mappings().first()
    if linha is None:
        raise CodigoRecusado(
            "Não há código esperando para este CPF. Peça um novo código.")
    if linha["expira_em"] < _agora():
        raise CodigoRecusado(
            f"Este código venceu (ele vale {VALIDADE_MIN} minutos). Peça um "
            f"novo.")
    if linha["tentativas"] >= TENTATIVAS:
        raise CodigoRecusado(
            "Este código foi errado vezes demais. Peça um novo.")
    if linha["codigo_hash"] != _resumo(digitado):
        # A TENTATIVA ERRADA É CONTADA, senão seis dígitos se descobrem na
        # força bruta: um milhão de combinações é pouco para uma máquina.
        s.execute(text("UPDATE portal_codigos_de_acesso "
                       "   SET tentativas = tentativas + 1 WHERE id = :i"),
                  {"i": linha["id"]})
        s.flush()
        faltam = TENTATIVAS - (linha["tentativas"] + 1)
        raise CodigoRecusado(
            "Código errado." + (f" Faltam {faltam} tentativas." if faltam > 0
                                else " Peça um novo código."))
    return {"colaborador_id": pessoa["id"], "nome": pessoa["nome"],
            "obra_id": pessoa["obra_id"], "codigo_id": linha["id"]}


def concluir(s: Session, cpf: Any, codigo: Any, email: Any, *,
             ip: str = "") -> dict[str, Any]:
    """Confere o código e põe o pedido NA FILA DE SEMPRE, já confirmado.

    ⚠️ NÃO CRIA O ACESSO. Quem cria é quem aprova, pelo botão que já existe —
    decisão dele: *"ainda assim precisa ser validado por alguém no sistema."*
    """
    from app.apps.erp.core.auth import pedidos_de_portal as fila

    cpf_ok = _so_digitos(cpf)
    email_ok = str(email or "").strip().lower()
    if not fila.email_valido(email_ok):
        raise CodigoRecusado(
            "Confira o e-mail: ele precisa ter @ e o domínio depois "
            "(exemplo: nome@gmail.com).")

    achado = _conferir(s, cpf_ok, codigo)

    # O código vale UMA VEZ. Marcado antes de abrir o pedido: se algo falhar
    # depois, melhor a pessoa pedir outro código que um código reusável.
    s.execute(text("UPDATE portal_codigos_de_acesso "
                   "   SET usado_em = now() WHERE id = :i"),
              {"i": achado["codigo_id"]})

    r = fila.registrar(
        s.connection(), colaborador_id=achado["colaborador_id"], cpf=cpf_ok,
        nome=achado["nome"], email=email_ok, obra_id=achado["obra_id"],
        origem="PORTAL")
    if not r.get("ok"):
        raise CodigoRecusado(r.get("erro") or "Não consegui abrir o pedido.")

    s.execute(text("""
        UPDATE portal_pedidos_de_acesso
           SET identidade_confirmada_em = now() WHERE id = :id"""),
        {"id": r["id"]})
    s.flush()
    logger.info("Portal: %s concluiu o primeiro acesso (pedido %s)",
                achado["nome"], r["id"])
    return {"ok": True, "nome": achado["nome"], "email": email_ok,
            "mensagem": ("Pronto! Seu pedido foi enviado com a identidade "
                         "confirmada. Assim que o escritório liberar, você "
                         f"recebe um e-mail em {email_ok} para criar a sua "
                         "senha.")}
