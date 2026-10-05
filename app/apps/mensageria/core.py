# -*- coding: utf-8 -*-
"""
A decisão de POR ONDE cada mensagem sai — e o registro do que saiu.

Decisão do dono em 05/10/2026: *"eu preciso ter gestão sobre quais mensagens
vão para o WhatsApp e quais não vão (…) a prioridade de envio é Telegram; por
enquanto, ponto permitido no WhatsApp."* E: o Z-API bloqueia quando o volume
sobe, então o WhatsApp tem teto para a empresa inteira, e quem estourar o teto
avisa os ADMIN.

Como funciona:

  · Todo envio tem um TIPO (`ponto.qr`, `erp.titulo_pago`…). O catálogo abaixo
    é onde o código declara os tipos que existem; a tabela `mensageria.tipos`
    guarda a POLÍTICA escolhida na tela para cada um. Tipo novo no código
    aparece na tela sozinho, já com a política de nascimento (Telegram).
  · POLÍTICA: TELEGRAM (só), TELEGRAM_OU_WHATSAPP (Telegram; WhatsApp só para
    quem não tem Telegram), WHATSAPP (só), DESLIGADO.
  · Chave geral "WhatsApp ligado" (parâmetro), além das políticas: nasce
    DESLIGADA. Ligar é um clique na tela — e é o dono quem clica.
  · TETO de WhatsApp por hora e por dia, contado no registro. Estourou: a
    mensagem não sai (quem chamou decide se tenta depois — a fila do ponto
    tenta) e os ADMIN recebem um aviso por Telegram, no máximo um por hora.
  · REGISTRO (`mensageria.envios`): tudo que o notificador tentou, com canal,
    destinatário, texto, resultado. Guardado por 90 dias (decisão do dono).

Quem manda de fato é o `notificador.py`. Este módulo não fala com o Z-API nem
com o Telegram — só decide e anota.
"""
from __future__ import annotations

import datetime as dt
import logging
import re
import time
from typing import Optional

from . import db

logger = logging.getLogger("mensageria")

# ---------------------------------------------------------------------------
# Políticas
# ---------------------------------------------------------------------------
TELEGRAM = "TELEGRAM"
TELEGRAM_OU_WHATSAPP = "TELEGRAM_OU_WHATSAPP"
WHATSAPP = "WHATSAPP"
DESLIGADO = "DESLIGADO"
POLITICAS = {
    TELEGRAM: "Só Telegram",
    TELEGRAM_OU_WHATSAPP: "Telegram; WhatsApp para quem não tem Telegram",
    WHATSAPP: "Só WhatsApp",
    DESLIGADO: "Desligado",
}

# ---------------------------------------------------------------------------
# O catálogo — chave, nome em português, de que módulo é, política de nascimento
# e uma linha dizendo o que é. A chave é o que o código passa em `finalidade=`.
# ---------------------------------------------------------------------------
CATALOGO: list[dict] = [
    # Ponto — o dono liberou o WhatsApp para o ponto em 05/10/2026
    {"chave": "ponto.qr", "nome": "QR Code do ponto", "modulo": "Ponto", "politica": WHATSAPP,
     "descricao": "O QR Code pessoal para bater o ponto no tablet da obra (troca a cada 7 a 14 dias)."},
    {"chave": "ponto.codigo_acesso", "nome": "Código de acesso do Meu ponto", "modulo": "Ponto",
     "politica": WHATSAPP,
     "descricao": "O código de 6 dígitos para a pessoa criar o PIN e entrar no Meu ponto pelo celular."},
    {"chave": "ponto.mosaico", "nome": "Mosaico de fotos da obra", "modulo": "Ponto", "politica": WHATSAPP,
     "descricao": "O link do mosaico de fotos do dia para o responsável pela obra conferir."},
    {"chave": "ponto.aviso_foto", "nome": "Aviso de batida sem foto", "modulo": "Ponto",
     "politica": WHATSAPP, "descricao": "Aviso ao encarregado quando a batida ficou sem foto."},
    {"chave": "ponto.resumo_dia", "nome": "Resumo diário do ponto", "modulo": "Ponto", "politica": WHATSAPP,
     "descricao": "O resumo do dia (quem bateu, quem faltou) para os telefones configurados no ponto."},
    # ERP
    {"chave": "erp.titulo_pago", "nome": "Título pago, com comprovante", "modulo": "ERP", "politica": TELEGRAM,
     "descricao": "Avisa quem lançou e os interessados quando o título é pago; o comprovante vai junto."},
    {"chave": "erp.encaminhamento", "nome": "Encaminhamento de documento", "modulo": "ERP",
     "politica": TELEGRAM, "descricao": "Um título, nota ou documento encaminhado a uma pessoa pelo ERP."},
    {"chave": "erp.agente_cobranca", "nome": "Agente de cobrança", "modulo": "ERP", "politica": TELEGRAM,
     "descricao": "As mensagens do robô que cobra pendências (resposta de cotação, documento faltando)."},
    {"chave": "erp.pergunta_agendada", "nome": "Resposta de pergunta agendada", "modulo": "ERP",
     "politica": TELEGRAM, "descricao": "A resposta de uma pergunta que a pessoa pediu para receber em horário combinado."},
    {"chave": "erp.teto_ia", "nome": "Aviso de consumo de IA", "modulo": "ERP", "politica": TELEGRAM,
     "descricao": "Aviso aos ADMIN quando o consumo de IA passa de 80% do teto, ou estoura."},
    {"chave": "erp.insumos", "nome": "Avisos de Suprimentos", "modulo": "ERP", "politica": TELEGRAM,
     "descricao": "Avisos do módulo de Suprimentos (insumo, cotação, pedido)."},
    # Mensageria
    {"chave": "mensageria.limite", "nome": "Aviso de teto do WhatsApp", "modulo": "Mensagens",
     "politica": TELEGRAM,
     "descricao": "Aviso aos ADMIN quando o WhatsApp bate no teto por hora ou por dia."},
    {"chave": "mensageria.teste", "nome": "Teste da tela Mensagens", "modulo": "Mensagens",
     "politica": TELEGRAM_OU_WHATSAPP,
     "descricao": "A mensagem de teste mandada pela própria tela (o canal é escolhido na hora)."},
    # Telegram
    {"chave": "telegram.aviso_cadastro", "nome": "Convite para cadastrar o Telegram", "modulo": "Telegram",
     "politica": DESLIGADO,
     "descricao": "Mensagem por WhatsApp a quem deveria ter recebido um aviso no Telegram e ainda não se cadastrou."},
]
_POR_CHAVE = {t["chave"]: t for t in CATALOGO}

# Parâmetros
WHATSAPP_LIGADO = "whatsapp.ligado"
POR_HORA = "whatsapp.por_hora"
POR_DIA = "whatsapp.por_dia"
ULTIMO_AVISO_LIMITE = "whatsapp.ultimo_aviso_limite"
RETENCAO_DIAS = 90
PADRAO_POR_HORA = 40
PADRAO_POR_DIA = 200

FUSO = dt.timezone(dt.timedelta(hours=-3))   # Fortaleza, sem horário de verão


def _agora() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def normalizar_chave(valor) -> str:
    """'Ponto' -> 'ponto'; mantém só letras, números, ponto e sublinhado."""
    return re.sub(r"[^a-z0-9._]+", "_", str(valor or "").strip().lower()).strip("_.")


# ---------------------------------------------------------------------------
# Parâmetros
# ---------------------------------------------------------------------------
def ler_parametro(conn, chave: str, padrao: str = "") -> str:
    linha = db.um(conn, "SELECT valor FROM mensageria.parametros WHERE chave = :c", c=chave)
    return linha["valor"] if linha else padrao


def gravar_parametro(conn, chave: str, valor, por: str = "") -> None:
    db.executar(conn, """
        INSERT INTO mensageria.parametros (chave, valor, atualizado_por) VALUES (:c, :v, :p)
        ON CONFLICT (chave) DO UPDATE SET valor = :v, atualizado_por = :p, atualizado_em = now()
    """, c=chave, v=str(valor), p=(por or "")[:120])


def _inteiro(texto: str, padrao: int) -> int:
    try:
        return max(0, int(str(texto).strip()))
    except (TypeError, ValueError):
        return padrao


def whatsapp_ligado(conn) -> bool:
    return ler_parametro(conn, WHATSAPP_LIGADO, "") == "1"


def tetos(conn) -> tuple[int, int]:
    return (_inteiro(ler_parametro(conn, POR_HORA, ""), PADRAO_POR_HORA),
            _inteiro(ler_parametro(conn, POR_DIA, ""), PADRAO_POR_DIA))


# ---------------------------------------------------------------------------
# Catálogo
# ---------------------------------------------------------------------------
def semear_tipos(conn) -> None:
    """Garante que todo tipo do código tem linha na tabela — sem mexer na
    política de quem já existe (a tela é quem manda nela)."""
    for t in CATALOGO:
        db.executar(conn, """
            INSERT INTO mensageria.tipos (chave, nome, modulo, descricao, politica)
            VALUES (:c, :n, :m, :d, :p)
            ON CONFLICT (chave) DO UPDATE SET nome = :n, modulo = :m, descricao = :d
        """, c=t["chave"], n=t["nome"], m=t["modulo"], d=t["descricao"], p=t["politica"])


def listar_tipos(conn) -> list[dict]:
    semear_tipos(conn)
    return db.todos(conn, """
        SELECT chave, nome, modulo, descricao, politica, atualizado_em, atualizado_por
          FROM mensageria.tipos ORDER BY modulo, nome""")


def politica_do_tipo(conn, chave: str) -> Optional[str]:
    """A política gravada; None se o tipo não existe no catálogo nem na tabela."""
    linha = db.um(conn, "SELECT politica FROM mensageria.tipos WHERE chave = :c", c=chave)
    if linha:
        return linha["politica"]
    t = _POR_CHAVE.get(chave)
    if not t:
        return None
    semear_tipos(conn)
    return t["politica"]


def gravar_politica(conn, chave: str, politica: str, por: str = "") -> None:
    if politica not in POLITICAS:
        raise ValueError("política desconhecida")
    if chave not in _POR_CHAVE and not db.um(conn, "SELECT 1 AS x FROM mensageria.tipos WHERE chave = :c",
                                             c=chave):
        raise ValueError("tipo de mensagem desconhecido")
    db.executar(conn, """UPDATE mensageria.tipos SET politica = :p, atualizado_em = now(),
                                atualizado_por = :q WHERE chave = :c""",
                p=politica, q=(por or "")[:120], c=chave)


# ---------------------------------------------------------------------------
# A decisão
# ---------------------------------------------------------------------------
class Decisao:
    """O que o notificador deve fazer com um envio de um tipo."""
    __slots__ = ("tipo", "politica", "canais", "fallback", "motivo")

    def __init__(self, tipo, politica, canais, fallback, motivo=""):
        self.tipo, self.politica, self.canais = tipo, politica, tuple(canais)
        self.fallback, self.motivo = fallback, motivo


def decidir(conn, tipo: str) -> Decisao:
    """Traduz a política do tipo (e a chave geral do WhatsApp) em canais."""
    politica = politica_do_tipo(conn, tipo)
    if politica is None:
        # Tipo que o código não declarou: sai só pelo Telegram, e fica no
        # registro com a chave que veio, para aparecer e ganhar linha depois.
        return Decisao(tipo, TELEGRAM, ("telegram",), False, "tipo fora do catálogo")
    if politica == DESLIGADO:
        return Decisao(tipo, politica, (), False, "tipo desligado na tela Mensagens")
    wa_ok = whatsapp_ligado(conn)
    if politica == TELEGRAM:
        return Decisao(tipo, politica, ("telegram",), False)
    if politica == WHATSAPP:
        if not wa_ok:
            return Decisao(tipo, politica, (), False, "WhatsApp desligado na tela Mensagens")
        return Decisao(tipo, politica, ("whatsapp",), False)
    # TELEGRAM_OU_WHATSAPP
    if not wa_ok:
        return Decisao(tipo, politica, ("telegram",), False, "WhatsApp desligado na tela Mensagens")
    return Decisao(tipo, politica, ("telegram", "whatsapp"), True)


def whatsapp_permitido(tipo: str) -> bool:
    """O tipo pode sair por WhatsApp AGORA (política + chave geral)? É o que o
    ponto pergunta antes de ligar a fila. Sem mensageria no banco, False."""
    if not db.disponivel():
        return False
    try:
        with db.conexao() as conn:
            return "whatsapp" in decidir(conn, tipo).canais
    except Exception:  # noqa: BLE001
        logger.warning("Mensageria: não consegui decidir para %s", tipo, exc_info=True)
        return False


# ---------------------------------------------------------------------------
# O teto do WhatsApp
# ---------------------------------------------------------------------------
def _inicio_do_dia_local(momento: dt.datetime) -> dt.datetime:
    local = momento.astimezone(FUSO)
    return dt.datetime.combine(local.date(), dt.time(0, 0), tzinfo=FUSO)


def contagem_whatsapp(conn, momento: Optional[dt.datetime] = None) -> dict:
    momento = momento or _agora()
    hora = db.um(conn, """SELECT count(*) AS n FROM mensageria.envios
                           WHERE canal = 'whatsapp' AND status = 'ENVIADO'
                             AND criado_em > :t""", t=momento - dt.timedelta(hours=1))["n"]
    dia = db.um(conn, """SELECT count(*) AS n FROM mensageria.envios
                          WHERE canal = 'whatsapp' AND status = 'ENVIADO'
                            AND criado_em >= :t""", t=_inicio_do_dia_local(momento))["n"]
    return {"hora": int(hora), "dia": int(dia)}


def cabe_no_teto(conn, momento: Optional[dt.datetime] = None) -> tuple[bool, str]:
    por_hora, por_dia = tetos(conn)
    c = contagem_whatsapp(conn, momento)
    if por_hora and c["hora"] >= por_hora:
        return False, f"teto de {por_hora} mensagens de WhatsApp por hora atingido"
    if por_dia and c["dia"] >= por_dia:
        return False, f"teto de {por_dia} mensagens de WhatsApp por dia atingido"
    return True, ""


def _deve_avisar_limite(conn, momento: dt.datetime) -> bool:
    ultimo = ler_parametro(conn, ULTIMO_AVISO_LIMITE, "")
    try:
        if ultimo and momento - dt.datetime.fromisoformat(ultimo) < dt.timedelta(hours=1):
            return False
    except ValueError:
        pass
    gravar_parametro(conn, ULTIMO_AVISO_LIMITE, momento.isoformat(), "o próprio sistema")
    return True


def tipo_que_mais_consome(conn, momento: dt.datetime) -> str:
    linha = db.um(conn, """SELECT tipo, count(*) AS n FROM mensageria.envios
                            WHERE canal = 'whatsapp' AND status = 'ENVIADO' AND criado_em >= :t
                            GROUP BY tipo ORDER BY n DESC LIMIT 1""", t=_inicio_do_dia_local(momento))
    if not linha:
        return ""
    nome = (_POR_CHAVE.get(linha["tipo"]) or {}).get("nome") or linha["tipo"]
    return f"{nome} ({linha['n']} hoje)"


# ---------------------------------------------------------------------------
# O registro
# ---------------------------------------------------------------------------
def registrar(conn, *, tipo: str, canal: str, destinatario: str, cpf: str, texto: str,
              nome_arquivo: str, status: str, detalhe: str, origem: str = "") -> None:
    db.executar(conn, """
        INSERT INTO mensageria.envios (tipo, canal, destinatario, cpf, texto, nome_arquivo,
                                       status, detalhe, origem)
        VALUES (:tipo, :canal, :dest, :cpf, :texto, :arq, :status, :det, :origem)
    """, tipo=(tipo or "")[:80], canal=(canal or "")[:20], dest=(destinatario or "")[:40],
         cpf=re.sub(r"\D", "", cpf or "")[:14], texto=(texto or "")[:4000],
         arq=(nome_arquivo or "")[:200], status=(status or "")[:20], det=(detalhe or "")[:500],
         origem=(origem or "")[:60])


def limpar_antigos(conn) -> int:
    return db.executar(conn, "DELETE FROM mensageria.envios WHERE criado_em < now() - make_interval(days => :d)",
                       d=RETENCAO_DIAS)


_ultima_limpeza = 0.0


def limpar_se_preciso(conn) -> None:
    """Uma faxina por dia, feita por quem passar primeiro (o serviço não tem relógio)."""
    global _ultima_limpeza
    if time.time() - _ultima_limpeza < 24 * 3600:
        return
    _ultima_limpeza = time.time()
    try:
        n = limpar_antigos(conn)
        if n:
            logger.info("Mensageria: %s registros com mais de %s dias apagados", n, RETENCAO_DIAS)
    except Exception:  # noqa: BLE001
        logger.warning("Mensageria: faxina do registro falhou", exc_info=True)


def listar_envios(conn, *, tipo: str = "", canal: str = "", status: str = "", busca: str = "",
                  dias: int = 7, limite: int = 300) -> list[dict]:
    sql = ["SELECT id, criado_em, tipo, canal, destinatario, cpf, texto, nome_arquivo, status, detalhe, origem",
           "FROM mensageria.envios WHERE criado_em > now() - make_interval(days => :dias)"]
    p: dict = {"dias": max(1, min(int(dias or 7), RETENCAO_DIAS)), "lim": max(1, min(int(limite or 300), 2000))}
    if tipo:
        sql.append("AND tipo = :tipo"); p["tipo"] = tipo
    if canal:
        sql.append("AND canal = :canal"); p["canal"] = canal
    if status:
        sql.append("AND status = :status"); p["status"] = status
    if busca:
        sql.append("AND (destinatario ILIKE :b OR cpf ILIKE :b OR texto ILIKE :b)")
        p["b"] = f"%{busca.strip()}%"
    sql.append("ORDER BY id DESC LIMIT :lim")
    return db.todos(conn, "\n".join(sql), **p)


def resumo(conn) -> dict:
    """Números para o topo da tela: hoje por canal e situação, e o teto."""
    momento = _agora()
    linhas = db.todos(conn, """SELECT canal, status, count(*) AS n FROM mensageria.envios
                                WHERE criado_em >= :t GROUP BY canal, status""",
                      t=_inicio_do_dia_local(momento))
    por_hora, por_dia = tetos(conn)
    return {"hoje": linhas, "contagem_whatsapp": contagem_whatsapp(conn, momento),
            "teto_por_hora": por_hora, "teto_por_dia": por_dia,
            "whatsapp_ligado": whatsapp_ligado(conn), "retencao_dias": RETENCAO_DIAS}


def nome_do_tipo(chave: str) -> str:
    return (_POR_CHAVE.get(chave) or {}).get("nome") or chave
