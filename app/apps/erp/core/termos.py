# -*- coding: utf-8 -*-
"""
O DOCUMENTO QUE O COLABORADOR ASSINA (migração 117, 20/11/2026).

⚑ PEDIDO DELE: *"a gente tem que ver uma forma de o administrativo de obra já
coletar o aviso prévio das pessoas, formalizar e coletar a assinatura de forma
legal, parecido com a forma que a gente está coletando a assinatura do ponto
(…) a colaboradora vai assinar e dispara para o DP. O DP verifica e manda para
frente, para a contabilidade. Já com o aviso assinado, datado."*

E a ordem, que é dele e é o que dá lisura: *"eu só não posso liberar de
imediato para a obra assinar, ela pede, e aí o DP só confere e libera para a
obra"*. Por isso o termo NASCE NO ATENDIMENTO do DP — nunca no pedido da obra.

O QUE ESTE MÓDULO FAZ, em três caminhos, porque a obra é o mundo real:

| Forma | Como | O que fecha o passo |
|---|---|---|
| NA_TELA | a pessoa lê no celular de quem conduz, assina com o dedo e digita o PIN DELA (o do ponto) | sozinho: PIN + traço + lacre provam quem e o quê |
| PAPEL | imprimiu, assinou, fotografou | o DP valida olhando a foto |
| RECUSA | a pessoa se recusou a assinar | o DP valida; vão os nomes de duas testemunhas |

⚑ A MESMA BASE JURÍDICA DA FOLHA DE PONTO (`ponto/core/assinaturas.py`): a
assinatura eletrônica vale quando prova QUEM assinou e que o texto NÃO MUDOU
(Lei 14.063/2020; MP 2.200-2/2001, art. 10 §2º). Daí o PIN da própria pessoa,
a hora do servidor, quem conduziu, o local e o LACRE (SHA-256 do texto como
foi mostrado). A confirmar com o advogado — está escrito no histórico.

⚑ CUSTO: abrir e listar não saem do banco. Assinar gera um PDF (na memória) e
guarda no Arquivo — uma gravação, a mesma de qualquer documento anexado.
"""
from __future__ import annotations

import base64
import hashlib
import io
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroValidacao, registrar_evento,
)
from app.apps.erp.db.models.cadastros import Usuario

logger = logging.getLogger(__name__)

MAX_ASSINATURA = 400 * 1024
TERMO_DE_CIENCIA = ("Declaro que li este documento, que recebi uma via e que a "
                    "assinatura abaixo é minha. Confirmo com o meu PIN pessoal.")
FORMA_ROTULOS = {"NA_TELA": "assinado no celular, com o PIN da pessoa",
                 "PAPEL": "assinado no papel (foto enviada)",
                 "RECUSA": "a pessoa se recusou a assinar (com testemunhas)"}

_tabela: dict[str, bool] = {"existe": False}


def tem_tabela(s: Session) -> bool:
    """A migração 117 pode não estar aplicada: sem ela, nada cai — o passo
    diz que falta a atualização do banco."""
    if not _tabela["existe"]:
        _tabela["existe"] = s.execute(text(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = "
            "'public' AND table_name = 'dp_termos'")).first() is not None
    return _tabela["existe"]


_colunas_126: dict[str, bool] = {}


def tem_origem(s: Session) -> bool:
    """A migração 126 (23/11/2026, a central de assinaturas): origem, origem_ids e assinado_pela_pessoa."""
    if not _colunas_126.get("ok"):
        _colunas_126["ok"] = s.execute(text(
            "SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' "
            "AND table_name = 'dp_termos' AND column_name = 'origem_ids'")).first() is not None
    return _colunas_126["ok"]


def lacre(titulo: str, texto_: str, cpf: str) -> str:
    """PURA. O texto como foi mostrado, mais de quem é."""
    bruto = f"{titulo}\n{texto_}\n{cpf}".encode("utf-8")
    return hashlib.sha256(bruto).hexdigest()


def _numero(s: Session) -> str:
    n = s.execute(text("SELECT nextval('dp_termo_numero_seq')")).scalar()
    return f"TA-{int(n):05d}"


# ---------------------------------------------------------------------------
# Abrir, ler, cancelar
# ---------------------------------------------------------------------------
def abrir(s: Session, *, colaborador_id: int, tipo: str, titulo: str,
          texto_: str, solicitacao_id: Optional[int] = None,
          obra_id: Optional[int] = None, dias: int = 3,
          usuario: Optional[Usuario] = None, origem: Optional[str] = None,
          origem_ids: Optional[list[int]] = None) -> dict[str, Any]:
    """`origem`/`origem_ids` (migração 126): o documento que nasce sozinho de outro lançamento — o termo de
    compensação nasce dos pedidos de compensação do ponto. Sem obra, ele vale assim mesmo: a própria pessoa assina
    pelo aplicativo do ponto (sem obra, só não há administrativo para conduzir)."""
    if not tem_tabela(s):
        raise ErroValidacao("Falta aplicar a atualização do banco (117) para "
                            "mandar documentos para assinar.")
    c = s.execute(text("SELECT id, nome, cpf, obra_id FROM colaboradores WHERE id = :i"),
                  {"i": colaborador_id}).mappings().first()
    if c is None:
        raise ErroValidacao("Colaborador não encontrado.")
    # Quem colhe a assinatura é a obra onde a pessoa está batendo ponto (a
    # obra ATUAL, migração 118); sem ela, a do cadastro.
    from app.apps.erp.core import obra_atual
    atual = obra_atual.da_pessoa(s, colaborador_id)
    obra = obra_id or (atual or {}).get("obra_id") or c["obra_id"]
    if not obra and not origem:
        raise ErroValidacao("Sem obra não há quem colha a assinatura — "
                            "informe a obra no cadastro da pessoa.")
    numero = _numero(s)
    linha = s.execute(text("""
        INSERT INTO dp_termos (numero, solicitacao_id, colaborador_id, obra_id,
                               tipo, titulo, texto, lacre, prazo, criado_por)
        VALUES (:n, :sol, :c, :o, :t, :ti, :tx, :l, :p, :u) RETURNING id"""),
        {"n": numero, "sol": solicitacao_id, "c": colaborador_id, "o": obra,
         "t": tipo, "ti": titulo, "tx": texto_,
         "l": lacre(titulo, texto_, c["cpf"] or ""),
         "p": date.today() + timedelta(days=max(1, int(dias or 3))),
         "u": usuario.id if usuario else None}).scalar()
    if origem and tem_origem(s):
        s.execute(text("UPDATE dp_termos SET origem = :o, origem_ids = :ids WHERE id = :i"),
                  {"o": origem, "ids": [int(x) for x in origem_ids or []], "i": linha})
    registrar_evento(s, "colaborador", colaborador_id, "TERMO_PARA_ASSINAR",
                     {"numero": numero, "tipo": tipo, "titulo": titulo},
                     usuario.id if usuario else None)
    return {"id": int(linha), "numero": numero}


def ler(s: Session, termo_id: int) -> dict[str, Any]:
    linha = s.execute(text("""
        SELECT t.*, c.nome AS pessoa, c.cpf, o.codigo AS obra,
               d.anexo_id
          FROM dp_termos t
          JOIN colaboradores c ON c.id = t.colaborador_id
          LEFT JOIN obras o ON o.id = t.obra_id
          LEFT JOIN documentos d ON d.id = t.documento_id
         WHERE t.id = :i"""), {"i": termo_id}).mappings().first()
    if linha is None:
        raise ErroNaoEncontrado("Documento não encontrado.")
    d = dict(linha)
    d.pop("assinatura_png", None)
    return d


def do_pedido(s: Session, solicitacao_id: int,
              tipos: Optional[tuple[str, ...]] = None) -> Optional[dict[str, Any]]:
    """O termo vivo (não cancelado) mais recente desta solicitação."""
    if not tem_tabela(s):
        return None
    linha = s.execute(text("""
        SELECT t.id FROM dp_termos t
         WHERE t.solicitacao_id = :s AND t.situacao <> 'CANCELADO'
           AND (CAST(:tipos AS TEXT[]) IS NULL OR t.tipo = ANY(CAST(:tipos AS TEXT[])))
         ORDER BY t.id DESC LIMIT 1"""),
        {"s": solicitacao_id, "tipos": list(tipos) if tipos else None}).scalar()
    return ler(s, int(linha)) if linha else None


def concluido(t: Optional[dict[str, Any]]) -> bool:
    """O termo já vale? Na tela com PIN, sozinho; papel e recusa, com o DP."""
    if not t:
        return False
    if t["situacao"] == "ASSINADO" and t.get("forma") == "NA_TELA":
        return True
    return t["situacao"] in ("ASSINADO", "RECUSADO") and bool(t.get("validado_em"))


def cancelar(s: Session, termo_id: int, motivo: str,
             usuario: Optional[Usuario]) -> None:
    t = ler(s, termo_id)
    if t["situacao"] != "ABERTO":
        raise ErroValidacao(f"{t['numero']} já está {t['situacao'].lower()}.")
    s.execute(text("UPDATE dp_termos SET situacao = 'CANCELADO', "
                   "motivo_recusa = :m WHERE id = :i"),
              {"m": (motivo or "")[:500], "i": termo_id})


# ---------------------------------------------------------------------------
# O lado da obra (portal)
# ---------------------------------------------------------------------------
def pendentes(s: Session, obras: Optional[list[int]], *,
              colaborador_id: Optional[int] = None,
              pessoas: Optional[list[int]] = None) -> list[dict[str, Any]]:
    """Os documentos que a obra tem de levar para a pessoa assinar. `colaborador_id`: só os de uma pessoa (o
    "Meu ponto", 23/11/2026) — aí a obra não importa. `pessoas`: os da obra OU dessas pessoas (o alcance do
    administrativo no ponto é por pessoa, não só por obra)."""
    if not tem_tabela(s):
        return []
    if colaborador_id is None and obras is not None and not obras and not pessoas:
        return []
    linhas = s.execute(text("""
        SELECT t.id, t.numero, t.titulo, t.prazo, t.tipo, t.colaborador_id, t.obra_id,
               t.motivo_recusa, c.nome AS pessoa, o.codigo AS obra
          FROM dp_termos t
          JOIN colaboradores c ON c.id = t.colaborador_id
          LEFT JOIN obras o ON o.id = t.obra_id
         WHERE t.situacao = 'ABERTO'
           AND (CAST(:pessoa AS BIGINT) IS NULL OR t.colaborador_id = :pessoa)
           AND (CAST(:pessoa AS BIGINT) IS NOT NULL OR :todas OR t.obra_id = ANY(:obras)
                OR t.colaborador_id = ANY(:pessoas))
         ORDER BY t.prazo NULLS LAST, t.id"""),
        {"todas": obras is None, "obras": obras or [], "pessoa": colaborador_id,
         "pessoas": [int(x) for x in pessoas or []]}).mappings().all()
    hoje = date.today()
    return [{"id": l["id"], "numero": l["numero"], "titulo": l["titulo"],
             "pessoa": l["pessoa"], "obra": l["obra"] or "", "tipo": l["tipo"],
             "colaborador_id": int(l["colaborador_id"]),
             "devolvido": str(l["motivo_recusa"] or "").startswith("DEVOLVIDO PELO DP"),
             "prazo": str(l["prazo"]) if l["prazo"] else None,
             "atrasado": bool(l["prazo"] and l["prazo"] < hoje)} for l in linhas]


def da_pessoa(s: Session, colaborador_id: int) -> list[dict[str, Any]]:
    """Todos os documentos de uma pessoa, menos os cancelados (a ficha, 23/11/2026): o que falta e o que foi."""
    if not tem_tabela(s):
        return []
    linhas = s.execute(text("""
        SELECT t.id, t.numero, t.titulo, t.tipo, t.situacao, t.forma, t.prazo, t.assinado_em,
               t.validado_em, t.conduzido_por_nome, t.criado_em, d.anexo_id
          FROM dp_termos t LEFT JOIN documentos d ON d.id = t.documento_id
         WHERE t.colaborador_id = :c AND t.situacao <> 'CANCELADO'
         ORDER BY t.criado_em DESC"""), {"c": colaborador_id}).mappings().all()
    return [_resumo(l) for l in linhas]


def _resumo(l) -> dict[str, Any]:
    situ = l["situacao"]
    vale = concluido(dict(l))
    rotulo = ("para assinar" if situ == "ABERTO"
              else "assinado" if vale and situ == "ASSINADO"
              else "recusa registrada" if vale
              else "esperando o DP validar")
    return {"id": l["id"], "numero": l["numero"], "titulo": l["titulo"], "tipo": l["tipo"],
            "situacao": situ, "forma": l["forma"], "rotulo": rotulo, "vale": vale,
            "prazo": str(l["prazo"]) if l["prazo"] else None,
            "assinado_em": l["assinado_em"].isoformat() if l["assinado_em"] else None,
            "conduzido_por": l["conduzido_por_nome"] or "", "anexo_id": l["anexo_id"]}


def painel_dp(s: Session, *, dias: int = 60) -> dict[str, Any]:
    """O lado do DP (23/11/2026): o que está para assinar, o que espera validação e o que foi nos últimos dias."""
    if not tem_tabela(s):
        return {"disponivel": False, "abertos": [], "validar": [], "feitos": []}
    linhas = s.execute(text("""
        SELECT t.id, t.numero, t.titulo, t.tipo, t.situacao, t.forma, t.prazo, t.assinado_em,
               t.validado_em, t.conduzido_por_nome, t.criado_em, t.colaborador_id, t.motivo_recusa,
               c.nome AS pessoa, o.codigo AS obra, d.anexo_id
          FROM dp_termos t
          JOIN colaboradores c ON c.id = t.colaborador_id
          LEFT JOIN obras o ON o.id = t.obra_id
          LEFT JOIN documentos d ON d.id = t.documento_id
         WHERE t.situacao = 'ABERTO'
            OR (t.situacao IN ('ASSINADO', 'RECUSADO') AND t.validado_em IS NULL AND t.forma <> 'NA_TELA')
            OR (t.situacao <> 'CANCELADO' AND t.assinado_em >= now() - make_interval(days => :d))
         ORDER BY t.prazo NULLS LAST, t.id"""), {"d": int(dias)}).mappings().all()
    hoje = date.today()
    abertos, validar, feitos = [], [], []
    for l in linhas:
        r = {**_resumo(l), "pessoa": l["pessoa"], "obra": l["obra"] or "",
             "colaborador_id": int(l["colaborador_id"]),
             "atrasado": bool(l["situacao"] == "ABERTO" and l["prazo"] and l["prazo"] < hoje),
             "motivo": l["motivo_recusa"] or ""}
        (abertos if l["situacao"] == "ABERTO" else feitos if r["vale"] else validar).append(r)
    return {"disponivel": True, "abertos": abertos, "validar": validar, "feitos": feitos}


def da_origem(s: Session, origem: str, colaboradores: list[int]) -> list[dict[str, Any]]:
    """Os termos (não cancelados) que nasceram de uma origem, para essas pessoas — com os ids que cobrem."""
    if not tem_tabela(s) or not tem_origem(s) or not colaboradores:
        return []
    return [dict(l) for l in s.execute(text("""
        SELECT id, colaborador_id, situacao, origem_ids FROM dp_termos
         WHERE origem = :o AND situacao <> 'CANCELADO' AND colaborador_id = ANY(:cs)"""),
        {"o": origem, "cs": [int(x) for x in colaboradores]}).mappings().all()]


def _no_alcance(s: Session, termo_id: int, obras: Optional[list[int]]) -> dict[str, Any]:
    t = ler(s, termo_id)
    if obras is not None and t["obra_id"] not in set(obras):
        raise ErroNaoEncontrado("Documento não encontrado.")
    return t


def para_assinar(s: Session, termo_id: int, obras: Optional[list[int]], *,
                 colaborador_id: Optional[int] = None) -> dict[str, Any]:
    """`colaborador_id`: a própria pessoa lendo o documento dela (o "Meu ponto") — o de outra, "não encontrado"."""
    if colaborador_id is not None:
        t = ler(s, termo_id)
        if int(t["colaborador_id"]) != int(colaborador_id):
            raise ErroNaoEncontrado("Documento não encontrado.")
    else:
        t = _no_alcance(s, termo_id, obras)
    return {"id": t["id"], "numero": t["numero"], "titulo": t["titulo"],
            "texto": t["texto"], "pessoa": t["pessoa"],
            "cpf_fim": str(t["cpf"] or "")[-4:], "situacao": t["situacao"],
            "termo_de_ciencia": TERMO_DE_CIENCIA}


def _png(assinatura: Any) -> bytes:
    bruto = str(assinatura or "")
    if "," in bruto[:100]:
        bruto = bruto.split(",", 1)[1]
    try:
        dados = base64.b64decode(bruto, validate=True)
    except Exception as e:  # noqa: BLE001
        raise ErroValidacao("A assinatura não chegou — assine de novo no quadro.") from e
    if not dados.startswith(b"\x89PNG") or len(dados) < 200:
        raise ErroValidacao("A assinatura não chegou — assine de novo no quadro.")
    if len(dados) > MAX_ASSINATURA:
        raise ErroValidacao("Assinatura grande demais — limpe e assine de novo.")
    return dados


def _conferir_pin(cpf: str, pin: str) -> None:
    """O PIN DA PRÓPRIA PESSOA, o mesmo do ponto. É o que prova que foi ela —
    não basta quem conduz estar logado."""
    from app.apps.ponto import db as ponto_db
    from app.apps.ponto.core import acesso
    from app.apps.ponto.erros import ErroDeValidacao, NaoAutenticado, Recusada
    try:
        with ponto_db.conexao() as conn:
            acesso.entrar(conn, cpf, pin)
    except (NaoAutenticado, ErroDeValidacao, Recusada) as e:
        # A frase do ponto já é a certa ("CPF ou PIN incorreto", "ainda não
        # criou o seu PIN", o bloqueio por tentativas).
        raise ErroValidacao(f"PIN não confere: {e}") from e
    except Exception as e:  # noqa: BLE001 — erro de sistema não vai para a obra
        logger.exception("ERP/termos: não consegui conferir o PIN no ponto")
        raise ErroValidacao("Não consegui conferir o PIN agora. Tente de novo "
                            "em instantes — ou assine no papel e envie a foto.") from e


def _arquivar(s: Session, t: dict[str, Any], pdf: bytes, observacao: str):
    from app.apps.erp.core.arquivo import service as svc_arq
    return svc_arq.arquivar(
        s, pdf, f"{t['tipo'].lower()}_{t['numero']}.pdf", tipo_codigo=t["tipo"],
        colaborador_id=t["colaborador_id"],
        referencia=f"{t['numero']} — {t['titulo']}", origem="PORTAL",
        observacao=observacao, usuario=None)


def assinar(s: Session, termo_id: int, obras: Optional[list[int]], *,
            pin: str, assinatura: Any, conduzido_por: dict[str, Any],
            latitude: Any = None, longitude: Any = None,
            ip: str = "", pela_pessoa: bool = False) -> dict[str, Any]:
    """A pessoa assina na tela: PIN dela + traço + lacre. Fecha sozinho.

    `pela_pessoa` (23/11/2026, a central de assinaturas): a própria pessoa, logada no aplicativo do ponto com CPF e
    PIN, assina o documento DELA — o pedido do dono: *"o colaborador acessar e conseguir visualizar o que é que tem
    pendente para ele, para assinar"*. Aí quem "conduz" é ela mesma, e o documento tem de ser dela (senão, "não
    encontrado"). Sem `pela_pessoa`, continua valendo a regra de antes: quem conduz não assina o próprio."""
    t = ler(s, termo_id) if pela_pessoa else _no_alcance(s, termo_id, obras)
    if pela_pessoa and conduzido_por.get("colaborador_id") != t["colaborador_id"]:
        raise ErroNaoEncontrado("Documento não encontrado.")
    if t["situacao"] != "ABERTO":
        raise ErroValidacao(f"{t['numero']} já está {t['situacao'].lower()}.")
    if not pela_pessoa and conduzido_por.get("colaborador_id") == t["colaborador_id"]:
        raise ErroValidacao("Quem conduz não assina o próprio documento por aqui "
                            "— outra pessoa conduz o seu.")
    if lacre(t["titulo"], t["texto"], t["cpf"] or "") != t["lacre"]:
        raise ErroValidacao("O texto deste documento mudou depois de aberto — "
                            "peça ao DP para gerar de novo.")
    png = _png(assinatura)
    _conferir_pin(t["cpf"] or "", pin)
    agora = datetime.now(timezone.utc)
    quem_conduz = ("a própria pessoa, no aplicativo do ponto" if pela_pessoa
                   else conduzido_por.get("nome") or "")
    pdf = documento_pdf(t, assinatura=png, quando=agora, forma="NA_TELA",
                        conduzido_por=quem_conduz, latitude=latitude, longitude=longitude)
    doc = _arquivar(s, t, pdf, "assinado pela própria pessoa, no aplicativo do ponto" if pela_pessoa
                    else f"assinado no celular, conduzido por {conduzido_por.get('nome') or '?'}")
    s.execute(text("""
        UPDATE dp_termos
           SET situacao = 'ASSINADO', forma = 'NA_TELA', assinatura_png = :png,
               documento_id = :doc, assinado_em = :em,
               conduzido_por_colaborador = :cc, conduzido_por_nome = :cn,
               latitude = :lat, longitude = :lon, ip = :ip
         WHERE id = :i"""),
        {"png": png, "doc": doc.id, "em": agora,
         "cc": conduzido_por.get("colaborador_id"),
         "cn": str(conduzido_por.get("nome") or "")[:120],
         "lat": _num(latitude), "lon": _num(longitude), "ip": (ip or "")[:60],
         "i": termo_id})
    if pela_pessoa and tem_origem(s):
        s.execute(text("UPDATE dp_termos SET assinado_pela_pessoa = TRUE WHERE id = :i"), {"i": termo_id})
    registrar_evento(s, "colaborador", t["colaborador_id"], "TERMO_ASSINADO",
                     {"numero": t["numero"], "forma": "NA_TELA",
                      "documento_id": doc.id}, None)
    return {"assinado": True, "numero": t["numero"], "documento_id": doc.id}


def enviar_papel(s: Session, termo_id: int, obras: Optional[list[int]], *,
                 conteudo: bytes, nome_arquivo: str,
                 conduzido_por: dict[str, Any]) -> dict[str, Any]:
    """A foto do papel assinado. Vale depois que o DP olhar."""
    from app.apps.erp.core.arquivo import service as svc_arq
    t = _no_alcance(s, termo_id, obras)
    if t["situacao"] != "ABERTO":
        raise ErroValidacao(f"{t['numero']} já está {t['situacao'].lower()}.")
    if not conteudo:
        raise ErroValidacao("Escolha a foto ou o PDF do papel assinado.")
    doc = svc_arq.arquivar(
        s, conteudo, nome_arquivo or "assinado", tipo_codigo=t["tipo"],
        colaborador_id=t["colaborador_id"],
        referencia=f"{t['numero']} — {t['titulo']} (papel)", origem="PORTAL",
        observacao=f"papel assinado enviado por {conduzido_por.get('nome') or '?'}",
        usuario=None)
    s.execute(text("""
        UPDATE dp_termos SET situacao = 'ASSINADO', forma = 'PAPEL',
               documento_id = :doc, assinado_em = now(),
               conduzido_por_colaborador = :cc, conduzido_por_nome = :cn
         WHERE id = :i"""),
        {"doc": doc.id, "cc": conduzido_por.get("colaborador_id"),
         "cn": str(conduzido_por.get("nome") or "")[:120], "i": termo_id})
    return {"assinado": True, "numero": t["numero"], "documento_id": doc.id,
            "falta": "o DP validar"}


def registrar_recusa(s: Session, termo_id: int, obras: Optional[list[int]], *,
                     motivo: str, testemunhas: list[dict[str, Any]],
                     conduzido_por: dict[str, Any]) -> dict[str, Any]:
    """A pessoa se recusou a assinar. A praxe trabalhista: o documento vale com
    a declaração de duas testemunhas de que ela foi comunicada."""
    t = _no_alcance(s, termo_id, obras)
    if t["situacao"] != "ABERTO":
        raise ErroValidacao(f"{t['numero']} já está {t['situacao'].lower()}.")
    validas = [x for x in (testemunhas or [])
               if len(" ".join(str(x.get("nome") or "").split())) >= 5]
    if len(validas) < 2:
        raise ErroValidacao("Informe o nome de duas testemunhas que viram a "
                            "pessoa ser comunicada.")
    if len((motivo or "").strip()) < 5:
        raise ErroValidacao("Conte o que aconteceu — o DP precisa saber.")
    agora = datetime.now(timezone.utc)
    pdf = documento_pdf(t, assinatura=None, quando=agora, forma="RECUSA",
                        conduzido_por=conduzido_por.get("nome") or "",
                        testemunhas=validas, motivo=motivo)
    doc = _arquivar(s, t, pdf, "recusa de assinatura, com testemunhas")
    import json
    s.execute(text("""
        UPDATE dp_termos SET situacao = 'RECUSADO', forma = 'RECUSA',
               documento_id = :doc, assinado_em = :em, motivo_recusa = :m,
               testemunhas = CAST(:tst AS jsonb),
               conduzido_por_colaborador = :cc, conduzido_por_nome = :cn
         WHERE id = :i"""),
        {"doc": doc.id, "em": agora, "m": motivo.strip()[:1000],
         "tst": json.dumps(validas, ensure_ascii=False),
         "cc": conduzido_por.get("colaborador_id"),
         "cn": str(conduzido_por.get("nome") or "")[:120], "i": termo_id})
    return {"recusado": True, "numero": t["numero"], "falta": "o DP validar"}


# ---------------------------------------------------------------------------
# O lado do DP
# ---------------------------------------------------------------------------
def validar(s: Session, termo_id: int, usuario: Optional[Usuario], *,
            aceitar: bool, motivo: str = "") -> dict[str, Any]:
    """O DP olha o papel (ou a recusa) e aceita — ou devolve para a obra."""
    t = ler(s, termo_id)
    if t["situacao"] not in ("ASSINADO", "RECUSADO") or t.get("validado_em"):
        raise ErroValidacao(f"{t['numero']} não espera validação.")
    if aceitar:
        s.execute(text("UPDATE dp_termos SET validado_por = :u, validado_em = now() "
                       "WHERE id = :i"), {"u": usuario.id if usuario else None,
                                          "i": termo_id})
        return {"validado": True}
    if len((motivo or "").strip()) < 5:
        raise ErroValidacao("Diga o que está errado — a obra precisa saber.")
    s.execute(text("""
        UPDATE dp_termos SET situacao = 'ABERTO', forma = NULL, documento_id = NULL,
               assinado_em = NULL, testemunhas = NULL,
               motivo_recusa = :m, prazo = :p
         WHERE id = :i"""),
        {"m": "DEVOLVIDO PELO DP: " + motivo.strip()[:500],
         "p": date.today() + timedelta(days=2), "i": termo_id})
    return {"validado": False}


# ---------------------------------------------------------------------------
# O PDF
# ---------------------------------------------------------------------------
def _num(v: Any) -> Optional[float]:
    try:
        return float(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _latin(t: Any) -> str:
    """A fonte do PDF é latin-1: travessão e aspas curvas viram os simples."""
    trocas = {"—": "-", "–": "-", "“": '"', "”": '"', "‘": "'", "’": "'",
              "…": "...", "•": "-", "º": "o", "ª": "a", "⚠": "!"}
    s_ = str(t or "")
    for a, b in trocas.items():
        s_ = s_.replace(a, b)
    return s_.encode("latin-1", "replace").decode("latin-1")


def _cpf_formatado(cpf: str) -> str:
    c = "".join(ch for ch in str(cpf or "") if ch.isdigit())
    return f"{c[0:3]}.{c[3:6]}.{c[6:9]}-{c[9:11]}" if len(c) == 11 else c


def documento_pdf(t: dict[str, Any], *, assinatura: Optional[bytes],
                  quando: datetime, forma: str, conduzido_por: str,
                  latitude: Any = None, longitude: Any = None,
                  testemunhas: Optional[list[dict[str, Any]]] = None,
                  motivo: str = "") -> bytes:
    """PURA (sem banco). O documento e o comprovante da assinatura."""
    from fpdf import FPDF
    from zoneinfo import ZoneInfo
    pdf = FPDF(orientation="P", format="A4")
    pdf.set_auto_page_break(True, margin=14)
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 13)
    pdf.multi_cell(0, 7, _latin(t["titulo"]), new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)
    pdf.set_font("Helvetica", "", 10)
    pdf.multi_cell(0, 5.2, _latin(t["texto"]), new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)
    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(0, 7, _latin("Comprovante de assinatura"), new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 9)
    local = quando.astimezone(ZoneInfo("America/Fortaleza"))
    if forma == "NA_TELA":
        pdf.multi_cell(0, 4.6, _latin(TERMO_DE_CIENCIA), new_x="LMARGIN", new_y="NEXT")
        y = pdf.get_y() + 2
        if assinatura:
            try:
                pdf.image(io.BytesIO(assinatura), x=pdf.l_margin, y=y, w=70)
            except Exception:  # noqa: BLE001 — imagem ruim não derruba o comprovante
                pass
        pdf.set_y(y + 30)
    else:
        pdf.multi_cell(0, 4.6, _latin(
            "A pessoa foi comunicada do conteúdo deste documento e se RECUSOU a "
            f"assinar. O que aconteceu: {motivo}"), new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)
        for i, x in enumerate(testemunhas or [], 1):
            pdf.multi_cell(0, 4.6, _latin(
                f"Testemunha {i}: {x.get('nome')}"
                + (f" - CPF {_cpf_formatado(x.get('cpf'))}" if x.get("cpf") else "")),
                new_x="LMARGIN", new_y="NEXT")
    linhas = [f"Colaborador: {t.get('pessoa') or ''} - CPF {_cpf_formatado(t.get('cpf'))}",
              f"Em: {local:%d/%m/%Y %H:%M} (horário de Fortaleza)"
              + (", com o PIN pessoal da pessoa" if forma == "NA_TELA" else ""),
              f"Conduzido por: {conduzido_por or '-'}",
              (f"Local: {latitude}, {longitude}"
               if latitude not in (None, "") and longitude not in (None, "")
               else "Local: não informado"),
              f"Documento {t['numero']} - lacre do texto (SHA-256): {t['lacre']}"]
    for l_ in linhas:
        pdf.multi_cell(0, 4.6, _latin(l_), new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "I", 7.5)
    pdf.multi_cell(0, 3.8, _latin(
        "Assinatura eletrônica (Lei 14.063/2020; MP 2.200-2/2001, art. 10 §2º). "
        "O lacre é calculado a partir do texto deste documento: qualquer "
        "mudança nele produz outro lacre."), new_x="LMARGIN", new_y="NEXT")
    return bytes(pdf.output())
