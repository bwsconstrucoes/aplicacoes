# -*- coding: utf-8 -*-
"""
Como o colaborador entra no "Meu ponto" do celular: CPF + PIN de 6 dígitos.

O PIN é criado (ou trocado) com um CÓDIGO de 6 dígitos mandado por WhatsApp para
o telefone do cadastro do ERP — o mesmo gateway que já avisa pagamento. Sem
e-mail, sem senha complicada: é gente de obra que vai usar.

PROTEÇÕES, cada uma com o motivo:
  · PIN e código só em hash (PBKDF2 com sal), nunca em claro;
  · 5 PINs errados bloqueiam por 15 minutos — CPF é fácil de descobrir, e PIN
    de 6 dígitos sem bloqueio cai em força bruta;
  · o código vale 15 minutos, morre no 5º erro e só serve uma vez;
  · no máximo 3 códigos por hora por pessoa — o WhatsApp da empresa não pode
    virar canhão de mensagem nas mãos de quem digita CPFs alheios;
  · a resposta ao pedido de código é A MESMA exista o CPF ou não: dizer
    "CPF não cadastrado" ensinaria quem trabalha na empresa.
  · PIN óbvio (123456, 000000, a data de nascimento não temos) é recusado.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import logging
import os
import re
import secrets
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy.engine import Connection

from .. import db, horario
from ..erros import ErroDeValidacao, NaoAutenticado, Recusada
from . import cadastros, recusas

logger = logging.getLogger("ponto.acesso")

# O WhatsApp do código sai FORA da requisição (21/10/2026, incidente: na hora da
# saída o Render inteiro parou). O envio esperava o Z-API até 60 s, 3 vezes, com
# a requisição e a conexão do banco presas; com o WhatsApp falhando, quatro
# pedidos de código tomavam as 4 linhas do serviço — de TODAS as áreas. Agora o
# código é gravado e a resposta sai na hora; o envio vai para esta fila, com no
# máximo 2 ao mesmo tempo, e nunca segura conexão do banco.
_fila_do_whatsapp = ThreadPoolExecutor(max_workers=2, thread_name_prefix="ponto-codigo")


def _mandar_em_segundo_plano(telefone: str, texto: str) -> None:
    def trabalho():
        try:
            from app.apps.notificador import notificar
            notificar(telefone=telefone, mensagem=texto, canais=("whatsapp",), finalidade="ponto.codigo_acesso")
        except Exception as e:  # noqa: BLE001 — falha de envio não derruba nada
            logger.exception("Ponto: não consegui mandar o código por WhatsApp")
            from .envios import registrar_nao_enviado          # a tentativa fica visível na tela Mensagens
            registrar_nao_enviado("ponto.codigo_acesso", f"erro ao chamar o envio: {str(e)[:200]}",
                                  telefone=telefone, texto="código de acesso do Meu ponto", status="FALHOU")
    _fila_do_whatsapp.submit(trabalho)

TENTATIVAS_PIN = 5
BLOQUEIO_MIN = 15
CODIGO_VALIDADE_MIN = 15
CODIGO_TENTATIVAS = 5
CODIGOS_POR_HORA = 3
_ITERACOES = 120_000
RESPOSTA_DO_CODIGO = ("Se o CPF estiver cadastrado com telefone, o código chega por WhatsApp "
                      "em instantes. Ele vale 15 minutos.")


def _hash(segredo: str, sal: bytes | None = None) -> str:
    sal = sal or os.urandom(16)
    h = hashlib.pbkdf2_hmac("sha256", segredo.encode("utf-8"), sal, _ITERACOES)
    return f"{sal.hex()}${h.hex()}"


def _confere(segredo: str, guardado: str | None) -> bool:
    if not guardado or "$" not in guardado:
        return False
    sal_hex, h_hex = guardado.split("$", 1)
    calc = hashlib.pbkdf2_hmac("sha256", segredo.encode("utf-8"), bytes.fromhex(sal_hex), _ITERACOES)
    return hmac.compare_digest(calc.hex(), h_hex)


def validar_pin(pin: str) -> str:
    valor = re.sub(r"\D", "", str(pin or ""))
    if len(valor) != 6:
        raise ErroDeValidacao("o PIN tem 6 números", campo="pin")
    if len(set(valor)) == 1 or valor in ("123456", "654321", "012345", "123123", "112233"):
        raise ErroDeValidacao("PIN fácil demais — escolha outro", campo="pin")
    return valor


def _config(conn: Connection, colaborador_id: int) -> dict:
    db.executar(conn, "INSERT INTO ponto.colaborador_config (colaborador_id) VALUES (:c) "
                      "ON CONFLICT DO NOTHING", c=colaborador_id)
    return db.um(conn, "SELECT * FROM ponto.colaborador_config WHERE colaborador_id = :c",
                 c=colaborador_id)


def pedir_codigo(conn: Connection, cpf, *, ip: str = "", enviar=None) -> dict:
    """Manda o código por WhatsApp. Responde sempre igual (ver o cabeçalho)."""
    try:
        cpf_ok = cadastros.normalizar_cpf(cpf)
    except ErroDeValidacao:
        raise
    pessoa = cadastros.colaborador_por_cpf(conn, cpf_ok)
    telefone = re.sub(r"\D", "", (pessoa or {}).get("telefone") or "")
    # o que não sai fica anotado na tela ERP › Mensagens, com o motivo (31/10/2026) — quem pediu continua
    # recebendo a mesma resposta (não revela se o CPF existe)
    from .envios import BARRADO, SEM_DESTINO, registrar_nao_enviado
    assunto = "código de acesso do Meu ponto" + (f" para {pessoa['nome']}" if pessoa else "")
    if not pessoa or pessoa["situacao"] == "DESLIGADO" or len(telefone) < 10:
        logger.info("Ponto: código pedido para cpf %s sem cadastro/telefone", recusas.mascarar_cpf(cpf_ok))
        if not pessoa:
            registrar_nao_enviado("ponto.codigo_acesso", "CPF não encontrado no cadastro do ponto (digitado errado, "
                                  "ou a pessoa não está na base)", cpf=cpf_ok, texto=assunto, status=BARRADO)
        elif pessoa["situacao"] == "DESLIGADO":
            registrar_nao_enviado("ponto.codigo_acesso", "pessoa desligada — o código não é enviado",
                                  telefone=telefone, cpf=cpf_ok, texto=assunto, status=BARRADO)
        else:
            registrar_nao_enviado("ponto.codigo_acesso", f"sem celular com DDD no cadastro (está: "
                                  f"{pessoa.get('telefone') or 'vazio'})", cpf=cpf_ok, texto=assunto, status=SEM_DESTINO)
        return {"mensagem": RESPOSTA_DO_CODIGO}
    recentes = db.um(conn, "SELECT count(*) AS n FROM ponto.codigos_acesso WHERE colaborador_id = :c "
                           "AND criado_em > now() - interval '1 hour'", c=pessoa["id"])["n"]
    if recentes >= CODIGOS_POR_HORA:
        logger.warning("Ponto: teto de códigos por hora para cpf %s", recusas.mascarar_cpf(cpf_ok))
        registrar_nao_enviado("ponto.codigo_acesso", f"já pediu {recentes} códigos na última hora (o teto é "
                              f"{CODIGOS_POR_HORA}) — o próximo só sai depois", telefone=telefone, cpf=cpf_ok,
                              texto=assunto, status=BARRADO)
        return {"mensagem": RESPOSTA_DO_CODIGO}
    codigo = f"{secrets.randbelow(1_000_000):06d}"
    db.executar(conn, """
        INSERT INTO ponto.codigos_acesso (colaborador_id, codigo_hash, expira_em, ip)
        VALUES (:c, :h, now() + make_interval(mins => :v), :ip)
    """, c=pessoa["id"], h=_hash(codigo), v=CODIGO_VALIDADE_MIN, ip=(ip or "")[:64])
    texto = (f"BWS Ponto: seu código é {codigo}. Ele vale {CODIGO_VALIDADE_MIN} minutos. "
             "Não passe este código para ninguém.")
    destino = telefone if telefone.startswith("55") else "55" + telefone
    if enviar is None:                     # o de verdade: nunca dentro da requisição
        _mandar_em_segundo_plano(destino, texto)
        return {"mensagem": RESPOSTA_DO_CODIGO}
    try:
        enviar(destino, texto)
    except Exception:  # noqa: BLE001 — falha de envio não revela nada a quem pediu
        logger.exception("Ponto: não consegui mandar o código por WhatsApp")
    return {"mensagem": RESPOSTA_DO_CODIGO}


# ---------------------------------------------------------------------------
# POR QUE O CÓDIGO NÃO CHEGOU (16/10/2026, o dono: "a pessoa não recebe o código.
# E não chega nada aqui no aparelhos. Consultei o telefone no cadastro, tá normal.
# E aí eu não sei o que fazer."). Para quem pede, a resposta é sempre a mesma (não
# revela se o CPF existe); para o DP, na ficha da pessoa, o motivo de verdade.
# ---------------------------------------------------------------------------
def _formatar_telefone(digitos: str) -> str:
    d = digitos[2:] if digitos.startswith("55") and len(digitos) >= 12 else digitos
    if len(d) == 11:
        return f"({d[:2]}) {d[2:7]}-{d[7:]}"
    if len(d) == 10:
        return f"({d[:2]}) {d[2:6]}-{d[6:]}"
    return d or "—"


def diagnostico(conn: Connection, colaborador_id: int) -> dict:
    """Para a ficha da pessoa: o telefone que o ponto usa (e de onde vem), o PIN, o
    que impede o código de sair, e os últimos códigos com o resultado do envio."""
    from .. import horario
    pessoa = cadastros.colaborador_por_id(conn, colaborador_id)
    if not pessoa:
        raise ErroDeValidacao("pessoa não encontrada")
    digitos = re.sub(r"\D", "", pessoa.get("telefone") or "")
    erp = db.um(conn, "SELECT telefone FROM public.colaboradores WHERE id = :c", c=colaborador_id) or {}
    do_erp = re.sub(r"\D", "", erp.get("telefone") or "")
    origem = ("cadastro do ERP" if digitos and digitos == do_erp
              else "Registro de Colaboradores (Pipefy)" if digitos else "—")
    impede = []
    if pessoa["situacao"] == "DESLIGADO":
        impede.append("a pessoa está DESLIGADA — o código não é enviado")
    elif not pessoa["ativo_no_ponto"]:
        impede.append("a pessoa está marcada como “não bate ponto” — o código chega, mas ela não consegue entrar")
    if len(digitos) < 10:
        impede.append("sem celular com DDD no cadastro — o código não tem para onde ir")
    elif len(digitos) == 10 or (len(digitos) == 12 and digitos.startswith("55")):
        impede.append("o celular tem 8 dígitos depois do DDD (falta o 9?) — o WhatsApp pode não achar o número")
    recentes = db.um(conn, "SELECT count(*) AS n FROM ponto.codigos_acesso WHERE colaborador_id = :c "
                           "AND criado_em > now() - interval '1 hour'", c=colaborador_id)["n"]
    if recentes >= CODIGOS_POR_HORA:
        impede.append(f"já pediu {recentes} códigos na última hora — o próximo só sai depois")
    cfg = db.um(conn, "SELECT pin_hash, pin_criado_em, pin_bloqueado_ate, pin_falhas FROM ponto.colaborador_config "
                      "WHERE colaborador_id = :c", c=colaborador_id) or {}
    tem_envios = db.um(conn, """SELECT 1 AS x FROM information_schema.tables
                                 WHERE table_schema = 'mensageria' AND table_name = 'envios'""") is not None
    codigos = []
    for c in db.todos(conn, """SELECT id, criado_em, expira_em, usado_em, tentativas FROM ponto.codigos_acesso
                                WHERE colaborador_id = :c ORDER BY id DESC LIMIT 5""", c=colaborador_id):
        envio = None
        if tem_envios:
            envio = db.um(conn, """SELECT status, detalhe, destinatario FROM mensageria.envios
                                    WHERE tipo = 'ponto.codigo_acesso' AND destinatario = :dest
                                      AND criado_em BETWEEN :a - interval '5 seconds' AND :a + interval '2 minutes'
                                    ORDER BY criado_em LIMIT 1""", a=c["criado_em"],
                          dest=(digitos if digitos.startswith("55") else "55" + digitos))
        codigos.append({
            "pedido_em": horario.texto(c["criado_em"]),
            "usado": bool(c["usado_em"]), "vencido": c["expira_em"] < horario.agora(),
            "envio": (None if not tem_envios else
                      {"status": envio["status"], "detalhe": envio["detalhe"],
                       "para": _formatar_telefone(re.sub(r"\D", "", envio["destinatario"] or ""))} if envio
                      else {"status": "SEM_REGISTRO", "detalhe": "nenhum envio registrado na hora — gerado na tela, "
                                                                 "ou o envio não chegou a ser tentado", "para": ""}),
        })
    return {"telefone": _formatar_telefone(digitos), "origem_telefone": origem,
            "telefone_no_erp": _formatar_telefone(do_erp) if do_erp and do_erp != digitos else None,
            "situacao": pessoa["situacao"], "tem_pin": bool(cfg.get("pin_hash")),
            "pin_criado_em": horario.texto(cfg.get("pin_criado_em")),
            # os PINs errados seguidos desde o último acerto (no 5º, bloqueia 15 min e volta a zero)
            "pin_falhas": int(cfg.get("pin_falhas") or 0),
            "pin_bloqueado_ate": horario.texto(cfg.get("pin_bloqueado_ate"))
            if cfg.get("pin_bloqueado_ate") and cfg["pin_bloqueado_ate"] > horario.agora() else None,
            "impede": impede, "codigos": codigos}


def codigo_em_maos(conn: Connection, colaborador_id: int, por: str) -> str:
    """O código de primeiro acesso mostrado ao DP, para entregar EM MÃOS (o WhatsApp
    não chegou). Não precisa de celular; vale o mesmo tempo; fica no registro quem
    gerou. Pessoa desligada não ganha."""
    pessoa = cadastros.colaborador_por_id(conn, colaborador_id)
    if not pessoa:
        raise ErroDeValidacao("pessoa não encontrada")
    if pessoa["situacao"] == "DESLIGADO":
        raise ErroDeValidacao("a pessoa está desligada — não ganha código de acesso")
    codigo = f"{secrets.randbelow(1_000_000):06d}"
    db.executar(conn, """
        INSERT INTO ponto.codigos_acesso (colaborador_id, codigo_hash, expira_em, ip)
        VALUES (:c, :h, now() + make_interval(mins => :v), :ip)
    """, c=colaborador_id, h=_hash(codigo), v=CODIGO_VALIDADE_MIN, ip=("em mãos: " + (por or ""))[:64])
    logger.info("Ponto: código de acesso EM MÃOS para a pessoa %s, gerado por %s", colaborador_id, por)
    return codigo


def criar_pin(conn: Connection, cpf, codigo: str, pin: str) -> dict:
    """Confere o código e grava o PIN. Devolve a pessoa (já entra)."""
    cpf_ok = cadastros.normalizar_cpf(cpf)
    pin_ok = validar_pin(pin)
    pessoa = cadastros.colaborador_por_cpf(conn, cpf_ok)
    if not pessoa:
        raise NaoAutenticado("código inválido ou vencido")
    # Vale QUALQUER código ainda válido, não só o último (16/10/2026: o código
    # entregue em mãos pelo DP e um do WhatsApp pedido depois convivem).
    validos = db.todos(conn, """
        SELECT * FROM ponto.codigos_acesso
         WHERE colaborador_id = :c AND usado_em IS NULL AND expira_em > now()
           AND tentativas < :t ORDER BY id DESC LIMIT 5
    """, c=pessoa["id"], t=CODIGO_TENTATIVAS)
    digitado = re.sub(r"\D", "", str(codigo or ""))
    acertou = next((v for v in validos if _confere(digitado, v["codigo_hash"])), None)
    cod = acertou or (validos[0] if validos else None)
    if not acertou:
        if validos:
            # Em transação PRÓPRIA: a recusa desfaz a transação de quem chamou,
            # e o erro precisa ficar contado — senão o teto de tentativas não existe.
            # Conta em TODOS os válidos: chutar contra vários não rende mais tentativas.
            with db.conexao() as separada:
                db.executar(separada, "UPDATE ponto.codigos_acesso SET tentativas = tentativas + 1 "
                                      "WHERE id = ANY(:ids)", ids=[int(v["id"]) for v in validos])
        raise NaoAutenticado("código inválido ou vencido")
    # Código certo, mas a entrada recusaria (16/10/2026: o PIN era criado, a tela
    # voltava para "entrar" e a pessoa achava que o código não funcionava). Diz o
    # motivo — ela já provou ser dona do celular — e NÃO gasta o código.
    if pessoa["situacao"] == "DESLIGADO":
        raise Recusada("o seu cadastro consta como desligado — fale com o DP")
    if not pessoa["ativo_no_ponto"]:
        raise Recusada("o seu cadastro está marcado como “não bate ponto” — fale com o DP")
    db.executar(conn, "UPDATE ponto.codigos_acesso SET usado_em = now() WHERE id = :id", id=cod["id"])
    _config(conn, pessoa["id"])
    db.executar(conn, """
        UPDATE ponto.colaborador_config SET pin_hash = :h, pin_criado_em = now(), pin_falhas = 0,
               pin_bloqueado_ate = NULL, atualizado_em = now() WHERE colaborador_id = :c
    """, h=_hash(pin_ok), c=pessoa["id"])
    logger.info("Ponto: PIN criado para cpf %s", recusas.mascarar_cpf(cpf_ok))
    return pessoa


def entrar(conn: Connection, cpf, pin: str) -> dict:
    """CPF + PIN. Levanta NaoAutenticado com a MESMA frase para qualquer erro,
    exceto o bloqueio (que precisa ser dito, senão a pessoa tenta para sempre)."""
    cpf_ok = cadastros.normalizar_cpf(cpf)
    pessoa = cadastros.colaborador_por_cpf(conn, cpf_ok)
    generico = NaoAutenticado("CPF ou PIN incorreto")
    if not pessoa:
        raise generico
    cfg = _config(conn, pessoa["id"])
    if cfg.get("pin_bloqueado_ate") and cfg["pin_bloqueado_ate"] > horario.agora():
        minutos = int((cfg["pin_bloqueado_ate"] - horario.agora()).total_seconds() // 60) + 1
        raise NaoAutenticado(f"muitas tentativas erradas; tente de novo em {minutos} min")
    if not cfg.get("pin_hash"):
        raise NaoAutenticado("você ainda não criou o seu PIN — toque em “primeiro acesso”")
    if not _confere(re.sub(r"\D", "", str(pin or "")), cfg["pin_hash"]):
        falhas = int(cfg.get("pin_falhas") or 0) + 1
        bloqueio = (horario.agora() + dt.timedelta(minutes=BLOQUEIO_MIN)) if falhas >= TENTATIVAS_PIN else None
        # Transação PRÓPRIA, pelo mesmo motivo do código: a recusa desfaz a de
        # quem chamou, e sem isto o bloqueio por tentativas nunca chegaria.
        with db.conexao() as separada:
            db.executar(separada, "UPDATE ponto.colaborador_config SET pin_falhas = :f, "
                                  "pin_bloqueado_ate = :b WHERE colaborador_id = :c",
                        f=(0 if bloqueio else falhas), b=bloqueio, c=pessoa["id"])
        logger.warning("Ponto: PIN errado para cpf %s (%d)", recusas.mascarar_cpf(cpf_ok), falhas)
        raise generico
    db.executar(conn, "UPDATE ponto.colaborador_config SET pin_falhas = 0, pin_bloqueado_ate = NULL "
                      "WHERE colaborador_id = :c", c=pessoa["id"])
    # PIN CERTO, cadastro fora do ponto (31/10/2026, o dono: "tá muito comum o pessoal dizer que o pin não funciona
    # mais"). Antes, o desligado, o "não bate ponto" e quem a planilha tira do ponto ouviam "CPF ou PIN incorreto" —
    # com o PIN certo — e achavam que o PIN tinha parado. Quem acertou o PIN provou ser o dono do CPF: ouve o motivo.
    if pessoa["situacao"] == "DESLIGADO":
        raise Recusada("o seu cadastro consta como desligado — o PIN está certo; fale com o DP")
    if not pessoa["ativo_no_ponto"]:
        raise Recusada("o seu cadastro está marcado como “não bate ponto” — o PIN está certo; fale com o DP")
    return pessoa
