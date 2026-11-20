# -*- coding: utf-8 -*-
"""
OS PEDIDOS QUE PASSAM PELA CONTABILIDADE — só fecham quando ela devolve.

⚑ PEDIDO DELE, 20/11/2026, sobre a alteração de cargo, o dependente e as
férias: *"tem todo o procedimento de encaminhamento para contabilidade,
acompanhamento, retorno, o agente tem que acompanhar. A mesma coisa vale para
o dependente, porque isso impacta em salário-família (…) E a mesma coisa serve
para as férias (…) o administrativo de obra pode pedir as férias. E o DP tem
que fazer a mesma coisa, solicitar a contabilidade, aguardar retorno e etc.
Para então finalizar."*

É o desenho da admissão (`admissao_processo`) estendido: atender grava o que
é do DP e o pedido fica **Em andamento**, com três passos — o DP atendeu, o
pedido à contabilidade saiu, a contabilidade devolveu — e **fecha sozinho**
quando o pedido à contabilidade é concluído (`contabilidade.concluir` chama
`fechar_pelo_pedido`).

Quando se aplica, e por quê (decisões minhas, escritas para ele trocar):

| Pedido | Passa pela contabilidade quando | Modelo |
|---|---|---|
| Alteração de cargo | a pessoa é CTPS (prestador não tem registro a alterar) | ALTERACAO_CARGO |
| Dependente | CTPS e com salário-família ou dependente de IR marcado | DEPENDENTE |
| Férias, alteração e cancelamento de férias | sempre (só existem para CTPS) | FERIAS, ALTERACAO_FERIAS, CANCELAMENTO_FERIAS |
| Cancelar desligamento | sempre, para CTPS (20/11/2026: *"esse cancelamento tem que ser avisado à contabilidade"*) | CANCELAMENTO_DESLIGAMENTO |

O pedido à contabilidade sai MONTADO com o que a solicitação já tem — o DP
confere o texto e envia; não redigita.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.db.models.cadastros import Usuario

logger = logging.getLogger(__name__)

EM_ANDAMENTO = "EM_ANDAMENTO"
TEMPLATE_DO_TIPO = {
    "ALTERACAO_CARGO": "ALTERACAO_CARGO",
    "DEPENDENTE": "DEPENDENTE",
    "FERIAS": "FERIAS",
    "ALTERACAO_FERIAS": "ALTERACAO_FERIAS",
    "CANCELAMENTO_FERIAS": "CANCELAMENTO_FERIAS",
    "CANCELAR_DESLIGAMENTO": "CANCELAMENTO_DESLIGAMENTO",
    # ⚑ 20/11/2026 (noite): de carteira, o que a contabilidade precisa saber
    # vai a ela. Ele: *"Na gestão do carteira assinada, nosso sistema funciona
    # como uma ponte, mas tudo é fechado lá (…) E a gente requer sempre o
    # retorno de tudo. Quem não é CLT, aí tudo se resolve internamente."*
    "DESCONTO_EM_FOLHA": "DESCONTO_FOLHA",
    "AFASTAMENTO": "AFASTAMENTO",
    "RETORNO_AFASTAMENTO": "RETORNO_AFASTAMENTO",
    # A admissão usa o mesmo botão para o registro da carteira.
    "CADASTRO": "ASSINATURA_CTPS",
    "ADMISSAO_PRESTADOR": "ASSINATURA_CTPS",
}


def e_processo(tipo: str) -> bool:
    return tipo in ("ALTERACAO_CARGO", "DEPENDENTE", "FERIAS",
                    "ALTERACAO_FERIAS", "CANCELAMENTO_FERIAS",
                    "CANCELAR_DESLIGAMENTO", "DESCONTO_EM_FOLHA",
                    "AFASTAMENTO", "RETORNO_AFASTAMENTO")


# ⚑ A ATUALIZAÇÃO CADASTRAL (nome, endereço) NÃO VAI À CONTABILIDADE — decisão
# dele, 20/11/2026: *"Troca de nome e endereço não mandar pra contabilidade."*
# Ela se resolve no cadastro, como o telefone e o Pix.


def _vinculo(s: Session, colaborador_id: Optional[int]) -> str:
    if not colaborador_id:
        return ""
    return str(s.execute(text("SELECT vinculo FROM colaboradores WHERE id = :i"),
                         {"i": colaborador_id}).scalar() or "CTPS").upper()


def aplica(s: Session, tipo: str, colaborador_id: Optional[int],
           pedido: dict[str, Any]) -> bool:
    """Este pedido precisa passar pela contabilidade?"""
    vinculo = _vinculo(s, colaborador_id)
    if tipo in ("ALTERACAO_CARGO", "FERIAS", "ALTERACAO_FERIAS",
                "CANCELAMENTO_FERIAS", "CANCELAR_DESLIGAMENTO",
                "DESCONTO_EM_FOLHA", "AFASTAMENTO", "RETORNO_AFASTAMENTO"):
        return vinculo == "CTPS"
    if tipo == "DEPENDENTE":
        marcou = lambda v: str(v or "") == "1"
        varios = pedido.get("dependentes")
        if isinstance(varios, list) and varios:
            algum = any(marcou((d or {}).get("salario_familia"))
                        or marcou((d or {}).get("dependente_ir")) for d in varios)
        else:
            algum = False
        algum = algum or marcou(pedido.get("salario_familia")) \
            or marcou(pedido.get("dependente_ir"))
        return vinculo == "CTPS" and algum
    return False


def _pedido_ligado(s: Session, sol: dict[str, Any], template: str):
    return s.execute(text("""
        SELECT id, numero, situacao FROM dp_pedidos
         WHERE template = :t AND situacao <> 'CANCELADO'
           AND (solicitacao_id = :sid
                OR (solicitacao_id IS NULL AND colaborador_id = :c
                    AND criado_em >= :desde))
         ORDER BY (solicitacao_id = :sid) DESC NULLS LAST, id DESC LIMIT 1"""),
        {"t": template, "sid": sol["id"], "c": sol.get("colaborador_id"),
         "desde": sol["criado_em"]}).mappings().first()


def passos(s: Session, sol: dict[str, Any]) -> dict[str, Any]:
    template = TEMPLATE_DO_TIPO[sol["tipo"]]
    from app.apps.erp.core import contabilidade as svc_ct
    nome = svc_ct.TEMPLATES[template]["nome"]
    atendido = sol.get("situacao") in (EM_ANDAMENTO, "ATENDIDA")
    ped = _pedido_ligado(s, sol, template)
    saida = []

    def item(codigo, rotulo, quem, estado, detalhe, **extra):
        saida.append({"codigo": codigo, "rotulo": rotulo, "quem": quem,
                      "como": "lido", "estado": estado, "detalhe": detalhe,
                      "por": "", "em": None, "observacao": "", **extra})

    item("ATENDIDO", "O DP analisou e registrou", "DP",
         "FEITO" if atendido else "PENDENTE",
         "atendido" if atendido else "atenda o pedido")
    # ⚑ O DESCONTO EM FOLHA PRECISA DA AUTORIZAÇÃO ASSINADA (CLT art. 462),
    # colhida pela obra como o aviso prévio (`termos.py`), antes de ir à
    # contabilidade.
    if sol["tipo"] == "DESCONTO_EM_FOLHA":
        from app.apps.erp.core import termos
        tm = termos.do_pedido(s, int(sol["id"]))
        if tm is None:
            item("AUTORIZACAO", "Autorização de desconto assinada pela pessoa",
                 "obra colhe, DP confere", "PENDENTE",
                 "gere o documento para a obra colher a assinatura",
                 gerar_termo=True)
        elif termos.concluido(tm):
            item("AUTORIZACAO", "Autorização de desconto assinada pela pessoa",
                 "obra colhe, DP confere", "FEITO",
                 f"{tm['numero']}: " + termos.FORMA_ROTULOS.get(tm.get("forma") or "", ""),
                 termo={"id": tm["id"], "numero": tm["numero"],
                        "anexo_id": tm.get("anexo_id")})
        else:
            item("AUTORIZACAO", "Autorização de desconto assinada pela pessoa",
                 "obra colhe, DP confere",
                 "AGUARDANDO" if tm["situacao"] == "ABERTO" else "PENDENTE",
                 (f"{tm['numero']} com a obra para colher a assinatura"
                  if tm["situacao"] == "ABERTO" else
                  f"{tm['numero']} voltou — abra e valide"),
                 termo={"id": tm["id"], "numero": tm["numero"],
                        "anexo_id": tm.get("anexo_id")},
                 termo_a_validar=tm["situacao"] != "ABERTO")
    if ped is None:
        item("CONTABILIDADE", f"Pedido à contabilidade: {nome}", "DP", "PENDENTE",
             "monte o pedido — ele sai preenchido com o que a solicitação tem")
    else:
        enviado = ped["situacao"] != "RASCUNHO"
        item("CONTABILIDADE", f"Pedido à contabilidade: {nome}", "DP",
             "FEITO" if enviado else "PENDENTE",
             f"{ped['numero']} " + ("enviado" if enviado
                                    else "montado e ainda não enviado"),
             pedido={"id": ped["id"], "numero": ped["numero"]})
    voltou = ped is not None and ped["situacao"] == "CONCLUIDO"
    item("RETORNO", "A contabilidade devolveu", "Contabilidade",
         "FEITO" if voltou else ("AGUARDANDO" if ped is not None
                                 and ped["situacao"] != "RASCUNHO" else "PENDENTE"),
         "concluído — o pedido fecha sozinho" if voltou else
         ("esperando a contabilidade" if ped is not None else "depois do pedido"))
    # ⚑ O AVISO DE FÉRIAS ASSINADO NA TELA (20/11/2026, noite). Ele: *"Aviso
    # de férias em tela."* Depois do retorno, a pessoa dá ciência no celular.
    from app.apps.erp.core import ferias as svc_ferias
    if sol["tipo"] in svc_ferias.TIPOS_COM_AVISO:
        from app.apps.erp.core import termos
        tm = termos.do_pedido(s, int(sol["id"]))
        rot = "Aviso de férias assinado pela pessoa"
        quem_ = "obra colhe, DP confere"
        if not voltou:
            item("AVISO_FERIAS", rot, quem_, "PENDENTE",
                 "depois do retorno da contabilidade, vai para a obra")
        elif tm is None:
            item("AVISO_FERIAS", rot, quem_, "PENDENTE",
                 "gere o aviso para a obra colher a assinatura", gerar_termo=True)
        elif termos.concluido(tm):
            item("AVISO_FERIAS", rot, quem_, "FEITO",
                 f"{tm['numero']}: " + termos.FORMA_ROTULOS.get(tm.get("forma") or "", ""),
                 termo={"id": tm["id"], "numero": tm["numero"],
                        "anexo_id": tm.get("anexo_id")})
        else:
            item("AVISO_FERIAS", rot, quem_,
                 "AGUARDANDO" if tm["situacao"] == "ABERTO" else "PENDENTE",
                 (f"{tm['numero']} com a obra para colher a assinatura"
                  if tm["situacao"] == "ABERTO" else
                  f"{tm['numero']} voltou — abra e valide"),
                 termo={"id": tm["id"], "numero": tm["numero"],
                        "anexo_id": tm.get("anexo_id")},
                 termo_a_validar=tm["situacao"] != "ABERTO")
    cumpridos = sum(1 for x in saida if x["estado"] == "FEITO")
    proximo = next((x for x in saida if x["estado"] != "FEITO"), None)
    return {"passos": saida, "cumpridos": cumpridos, "total": len(saida),
            "pode_encerrar": False, "fecha_sozinho": True,
            "proximo": proximo["rotulo"] if proximo else "",
            "marcacoes_disponiveis": True}


def _dados_do_pedido(s: Session, sol: dict[str, Any]) -> dict[str, Any]:
    """O que a solicitação já sabe, no vocabulário do modelo da contabilidade."""
    d = sol.get("dados") or {}
    tipo = sol["tipo"]
    if tipo == "ALTERACAO_CARGO":
        cargo = d.get("cargo_deferido_id") or d.get("cargo_pleiteado_id")
        nome_cargo = s.execute(text("SELECT nome FROM cargos WHERE id = :i"),
                               {"i": int(cargo)}).scalar() if str(cargo or "").isdigit() else ""
        return {"cargo_novo": nome_cargo or "", "salario_novo": d.get("salario"),
                "vigencia": d.get("data_inicio"), "motivo": d.get("justificativa")}
    if tipo == "DEPENDENTE":
        um = (d.get("dependentes") or [d])[0] if isinstance(d.get("dependentes"), list) \
            else d
        return {"dependente_nome": um.get("nome"),
                "dependente_nascimento": um.get("data_nascimento"),
                "dependente_cpf": um.get("cpf") or d.get("cpf"),
                "parentesco": um.get("parentesco"),
                "salario_familia": "sim" if str(um.get("salario_familia") or
                                                d.get("salario_familia")) == "1" else "",
                "dependente_ir": "sim" if str(um.get("dependente_ir") or
                                              d.get("dependente_ir")) == "1" else ""}
    if tipo == "FERIAS":
        return {"inicio": d.get("ferias_inicio"), "dias": d.get("ferias_dias"),
                "abono": "sim" if str(d.get("abono_pecuniario")) == "1" else "não",
                "adiantamento_13": "sim" if str(d.get("adiantamento_13")) == "1"
                else "não", "observacao": d.get("observacao")}
    if tipo in ("ALTERACAO_FERIAS", "CANCELAMENTO_FERIAS"):
        from app.apps.erp.core import ferias as svc_ferias
        atual = svc_ferias._da_solicitacao(s, sol)
        saida = {"motivo": d.get("observacao")}
        if atual:
            saida.update({"inicio_anterior": str(atual["inicio"]),
                          "dias_anteriores": atual["dias"]})
        if tipo == "ALTERACAO_FERIAS":
            saida.update({"inicio": d.get("ferias_inicio"),
                          "dias": d.get("ferias_dias"),
                          "abono": "sim" if str(d.get("abono_pecuniario")) == "1"
                          else "não",
                          "adiantamento_13": "sim" if str(d.get("adiantamento_13"))
                          == "1" else "não"})
        return saida
    if tipo == "CANCELAR_DESLIGAMENTO":
        return {"comunicado_em": d.get("desligamento_comunicado_em"),
                "motivo": d.get("observacao")}
    if tipo == "AFASTAMENTO":
        from app.apps.erp.core.cadastros import afastamentos
        nat = str(d.get("afastamento_natureza") or "").upper()
        return {"natureza": afastamentos.TIPOS.get(nat, {}).get("nome", nat),
                "inicio": d.get("afast_inicio"), "previsao": d.get("afast_previsao"),
                "acidente": "sim" if nat == "ACIDENTE" else "",
                "motivo": d.get("observacao")}
    if tipo == "RETORNO_AFASTAMENTO":
        return {"retorno": d.get("retorno_data"), "motivo": d.get("observacao")}
    if tipo == "DESCONTO_EM_FOLHA":
        return {"valor_total": d.get("valor_total"), "parcelas": d.get("parcelas"),
                "valor_parcela": d.get("valor_parcela"),
                "primeira_competencia": d.get("primeira_competencia"),
                "motivo": d.get("observacao")}
    return {}


def montar_pedido(s: Session, sol: dict[str, Any],
                  usuario: Optional[Usuario]) -> dict[str, Any]:
    """Monta (rascunho) o pedido à contabilidade desta solicitação."""
    from app.apps.erp.core import contabilidade as svc_ct
    from app.apps.erp.core.comum.auditoria import ErroValidacao
    from app.apps.erp.core.pessoal_solicitacoes import _evento

    template = TEMPLATE_DO_TIPO.get(sol["tipo"])
    if not template:
        raise ErroValidacao("Este pedido não passa pela contabilidade.")
    if sol["tipo"] == "DESCONTO_EM_FOLHA":
        from app.apps.erp.core import termos
        if termos.tem_tabela(s) and not termos.concluido(
                termos.do_pedido(s, int(sol["id"]))):
            raise ErroValidacao("A autorização de desconto ainda não voltou "
                                "assinada pela pessoa (CLT art. 462).")
    if not sol.get("colaborador_id"):
        raise ErroValidacao("O pedido não tem pessoa ligada.")
    if _pedido_ligado(s, sol, template) is not None:
        raise ErroValidacao("O pedido à contabilidade já foi montado.")
    r = svc_ct.abrir(s, {"template": template,
                         "colaborador_id": sol["colaborador_id"],
                         "solicitacao_id": sol["id"],
                         "dados": {k: v for k, v in _dados_do_pedido(s, sol).items()
                                   if v not in (None, "")}}, usuario)
    _evento(s, sol["id"], "PEDIDO_A_CONTABILIDADE",
            {"numero": r["numero"], "template": template}, usuario)
    return r


def fechar_pelo_pedido(s: Session, pedido: dict[str, Any],
                       usuario: Optional[Usuario] = None) -> int:
    """A contabilidade devolveu e o pedido foi concluído: as solicitações em
    andamento que esperavam por ele estão completamente atendidas."""
    from app.apps.erp.core.pessoal_solicitacoes import _evento

    if pedido.get("template") in EXTRAS:
        return fechar_extra(s, pedido, usuario)
    tipos = [t for t, tpl in TEMPLATE_DO_TIPO.items()
             if tpl == pedido["template"] and e_processo(t)]
    if not tipos:
        return 0
    from app.apps.erp.core import ferias as svc_ferias

    sols = s.execute(text("""
        SELECT id, numero, tipo, colaborador_id, dados FROM dp_solicitacoes
         WHERE situacao = :sit AND tipo = ANY(:t)
           AND (id = CAST(:sid AS INTEGER)
                OR (CAST(:sid AS INTEGER) IS NULL AND colaborador_id = :c))"""),
        {"sit": EM_ANDAMENTO, "t": tipos, "sid": pedido.get("solicitacao_id"),
         "c": pedido.get("colaborador_id")}).mappings().all()
    for sol in sols:
        # A programação de férias só muda AQUI, com o retorno — ver
        # `core/ferias.py`.
        feito = svc_ferias.contabilidade_devolveu(s, dict(sol), pedido)
        # As férias ainda esperam a pessoa assinar o aviso: o pedido segue.
        if sol["tipo"] in svc_ferias.TIPOS_COM_AVISO:
            aviso = svc_ferias.gerar_aviso(s, {**dict(sol), "obra_id": None}, usuario)
            if aviso is not None:
                _evento(s, sol["id"], "CONTABILIDADE_DEVOLVEU",
                        {"texto": f"{pedido.get('numero')} concluído"
                                  + (f"; {feito}" if feito else "")
                                  + f" — aviso {aviso['numero']} foi para a obra "
                                    "colher a assinatura"}, usuario)
                continue
        # Com correção ou cancelamento esperando a ciência, o pedido não fecha
        # aqui — fecha quando ela confirmar (`fechar_extra`).
        if extras_abertos(s, int(sol["id"])):
            _evento(s, sol["id"], "CONTABILIDADE_DEVOLVEU",
                    {"texto": f"{pedido.get('numero')} concluído"
                              + (f"; {feito}" if feito else "")
                              + " — falta a ciência da correção"}, usuario)
            continue
        s.execute(text("UPDATE dp_solicitacoes SET situacao = 'ATENDIDA', "
                       "atualizado_em = now() WHERE id = :i"), {"i": sol["id"]})
        _evento(s, sol["id"], "CONTABILIDADE_DEVOLVEU",
                {"texto": f"{pedido.get('numero')} concluído — pedido atendido"
                          + (f"; {feito}" if feito else "")},
                usuario)
    return len(sols)


def em_andamento(s: Session, *, limite: int = 300) -> list[dict[str, Any]]:
    """As solicitações em andamento que não são desligamento — para o vigia."""
    linhas = s.execute(text("""
        SELECT d.id, d.numero, d.tipo, d.colaborador_id, d.dados, d.criado_em,
               d.situacao, d.atendido_em, COALESCE(c.nome, d.nome_informado) AS pessoa
          FROM dp_solicitacoes d
          LEFT JOIN colaboradores c ON c.id = d.colaborador_id
         WHERE d.situacao = :sit AND d.tipo <> 'DESLIGAMENTO'
         ORDER BY d.atendido_em NULLS LAST, d.id LIMIT :l"""),
        {"sit": EM_ANDAMENTO, "l": limite}).mappings().all()
    saida = []
    hoje = date.today()
    for l in linhas:
        sol = dict(l)
        if e_processo(sol["tipo"]):
            est = passos(s, sol)
        else:
            from app.apps.erp.core import admissao_processo
            est = admissao_processo.passos(s, sol)
        saida.append({"id": l["id"], "numero": l["numero"], "tipo": l["tipo"],
                      "colaborador_id": l["colaborador_id"],
                      "pessoa": l["pessoa"] or "", "dados": l["dados"] or {},
                      "proximo": est["proximo"],
                      "dias": ((hoje - l["atendido_em"].date()).days
                               if l["atendido_em"] else None)})
    return saida


# ===========================================================================
# CORRIGIR E CANCELAR O QUE JÁ FOI PARA A CONTABILIDADE (20/11/2026, noite)
#
# Ele: *"praticamente tudo que se pede pode vir a ser necessário ser
# cancelado, aquele pleito, ou de repente alterado alguma coisa (…) se eu já
# tiver encaminhado alguma coisa para a contabilidade? Então eu vou precisar
# fazer aquela correção. Preciso comunicar a correção, preciso ter confirmar
# que há a ciência daquela alteração."*
#
# A REGRA, a mesma para todo pedido:
#   · o pedido à contabilidade ainda em RASCUNHO: é só refazer — ele é
#     cancelado e montado de novo com o dado certo;
#   · já ENVIADO: nasce um pedido de CORREÇÃO (ou de CANCELAMENTO) à
#     contabilidade, e a solicitação NÃO FECHA enquanto ela não confirmar a
#     ciência. O passo aparece na lista, e o agente cobra.
#   · nada foi à contabilidade: corrige ou cancela aqui, e pronto.
# ===========================================================================
EXTRAS = ("RETIFICACAO", "CANCELAMENTO_SOLICITACAO")


def _pedidos(s: Session, solicitacao_id: int) -> list[dict[str, Any]]:
    return [dict(l) for l in s.execute(text("""
        SELECT id, numero, template, situacao, dados FROM dp_pedidos
         WHERE solicitacao_id = :s AND situacao <> 'CANCELADO'
         ORDER BY id"""), {"s": solicitacao_id}).mappings().all()]


def extras_abertos(s: Session, solicitacao_id: int) -> list[dict[str, Any]]:
    return [p for p in _pedidos(s, solicitacao_id)
            if p["template"] in EXTRAS and p["situacao"] != "CONCLUIDO"]


def passos_extras(s: Session, solicitacao_id: int) -> list[dict[str, Any]]:
    """Os passos da correção / do cancelamento comunicados à contabilidade."""
    saida = []
    for p in _pedidos(s, solicitacao_id):
        if p["template"] not in EXTRAS:
            continue
        cancel = p["template"] == "CANCELAMENTO_SOLICITACAO"
        rotulo = ("Cancelamento comunicado à contabilidade, com a ciência dela"
                  if cancel else
                  "Correção comunicada à contabilidade, com a ciência dela")
        if p["situacao"] == "CONCLUIDO":
            estado, det = "FEITO", f"{p['numero']}: a contabilidade confirmou"
        elif p["situacao"] == "RASCUNHO":
            estado, det = "PENDENTE", f"{p['numero']} montado e ainda não enviado"
        else:
            estado, det = "AGUARDANDO", f"{p['numero']} enviado — esperando a ciência"
        saida.append({"codigo": f"EXTRA_{p['id']}", "rotulo": rotulo,
                      "quem": "DP e contabilidade", "como": "lido",
                      "estado": estado, "detalhe": det, "por": "", "em": None,
                      "observacao": "", "pedido": {"id": p["id"],
                                                   "numero": p["numero"]}})
    return saida


def _o_que_mudou(mudou: list[dict[str, Any]]) -> str:
    from app.apps.erp.core.pessoal_solicitacoes import ROTULOS
    return "; ".join(f"{ROTULOS.get(m['campo'], m['campo'])}: "
                     f"{m.get('de') or '(vazio)'} → {m.get('para') or '(vazio)'}"
                     for m in mudou)


def comunicar_correcao(s: Session, sol: dict[str, Any], mudou: list[dict[str, Any]],
                       usuario: Optional[Usuario]) -> dict[str, Any]:
    """O DP corrigiu um pedido que já tinha andado. Devolve o que fez."""
    from app.apps.erp.core import contabilidade as svc_ct
    from app.apps.erp.core.pessoal_solicitacoes import _evento
    principais = [p for p in _pedidos(s, int(sol["id"]))
                  if p["template"] not in EXTRAS]
    feito: dict[str, Any] = {}
    for p in principais:
        if p["situacao"] == "RASCUNHO":
            svc_ct.cancelar(s, p["id"], usuario,
                            motivo="o DP corrigiu a solicitação: montar de novo")
            feito.setdefault("refazer", []).append(p["numero"])
    enviados = [p for p in principais if p["situacao"] not in ("RASCUNHO",)]
    if enviados and sol.get("colaborador_id"):
        r = svc_ct.abrir(s, {"template": "RETIFICACAO",
                             "colaborador_id": sol["colaborador_id"],
                             "solicitacao_id": sol["id"],
                             "dados": {"pedido_original": ", ".join(
                                 p["numero"] for p in enviados),
                                 "o_que_mudou": _o_que_mudou(mudou)}}, usuario)
        feito["retificacao"] = r["numero"]
        if sol.get("situacao") == "ATENDIDA":
            s.execute(text("UPDATE dp_solicitacoes SET situacao = 'EM_ANDAMENTO', "
                           "atualizado_em = now() WHERE id = :i"), {"i": sol["id"]})
        _evento(s, sol["id"], "CORRECAO_A_CONTABILIDADE",
                {"numero": r["numero"], "texto": _o_que_mudou(mudou)}, usuario)
    return feito


def cancelar_em_andamento(s: Session, sol: dict[str, Any], motivo: str,
                          usuario: Optional[Usuario]) -> dict[str, Any]:
    """Cancelar um pedido que o DP já atendeu. O que foi à contabilidade é
    desfeito com ela: a solicitação só vira Cancelada com a ciência."""
    from app.apps.erp.core import contabilidade as svc_ct
    from app.apps.erp.core.pessoal_solicitacoes import _evento
    if len((motivo or "").strip()) < 5:
        raise ErroValidacao("Diga por que está cancelando — vai junto para a "
                            "contabilidade e para quem pediu.")
    if sol["tipo"] in ("DESLIGAMENTO", "DESLIGAMENTO_PRESTADOR"):
        raise ErroValidacao("Desligamento se desfaz pelo pedido de cancelamento "
                            "de desligamento — ele apaga a saída e segura a "
                            "virada do dia.")
    if sol.get("situacao") not in (EM_ANDAMENTO, "ATENDIDA"):
        raise ErroValidacao("Este pedido não está em andamento.")
    if extras_abertos(s, int(sol["id"])):
        raise ErroValidacao("Já há uma correção ou um cancelamento esperando a "
                            "contabilidade neste pedido.")
    principais = [p for p in _pedidos(s, int(sol["id"]))
                  if p["template"] not in EXTRAS]
    for p in principais:
        if p["situacao"] == "RASCUNHO":
            svc_ct.cancelar(s, p["id"], usuario, motivo=f"pedido cancelado: {motivo}")
    enviados = [p for p in principais if p["situacao"] != "RASCUNHO"]
    import json
    s.execute(text("UPDATE dp_solicitacoes SET dados = dados || CAST(:j AS jsonb) "
                   "WHERE id = :i"),
              {"i": sol["id"], "j": json.dumps({"cancelando": {
                  "motivo": motivo.strip(), "em": date.today().isoformat()}})})
    if enviados and sol.get("colaborador_id"):
        r = svc_ct.abrir(s, {"template": "CANCELAMENTO_SOLICITACAO",
                             "colaborador_id": sol["colaborador_id"],
                             "solicitacao_id": sol["id"],
                             "dados": {"pedido_original": ", ".join(
                                 p["numero"] for p in enviados),
                                 "motivo": motivo.strip()}}, usuario)
        if sol.get("situacao") == "ATENDIDA":
            s.execute(text("UPDATE dp_solicitacoes SET situacao = 'EM_ANDAMENTO', "
                           "atualizado_em = now() WHERE id = :i"), {"i": sol["id"]})
        _evento(s, sol["id"], "CANCELAMENTO_A_CONTABILIDADE",
                {"numero": r["numero"], "motivo": motivo}, usuario)
        return {"cancelamento": r["numero"], "falta": "a ciência da contabilidade"}
    _finalizar_cancelamento(s, sol, motivo, usuario)
    return {"cancelada": True}


def _finalizar_cancelamento(s: Session, sol: dict[str, Any], motivo: str,
                            usuario: Optional[Usuario]) -> None:
    from app.apps.erp.core import ferias as svc_ferias
    from app.apps.erp.core.pessoal_solicitacoes import _cancelar_pre_cadastro, _evento
    s.execute(text("""UPDATE dp_solicitacoes
                         SET situacao = 'CANCELADA', cancelado_por = :u,
                             cancelado_em = now(), atualizado_em = now(),
                             motivo_cancelamento = :m, resposta = :m
                       WHERE id = :i"""),
              {"u": usuario.id if usuario else None, "i": sol["id"],
               "m": f"cancelado: {motivo}"[:500]})
    _evento(s, sol["id"], "CANCELADA", {"motivo": motivo}, usuario)
    # O que o atendimento tinha feito e se desfaz sozinho.
    if sol["tipo"] == "FERIAS" and svc_ferias.tem_tabela(s):
        s.execute(text("UPDATE ferias_programadas SET situacao = 'CANCELADA', "
                       "cancelada_em = now() WHERE solicitacao_id = :s "
                       "AND situacao <> 'CANCELADA'"), {"s": sol["id"]})
    if sol["tipo"] in ("CADASTRO", "ADMISSAO_PRESTADOR") and sol.get("colaborador_id"):
        etapa = s.execute(text("SELECT etapa FROM colaboradores WHERE id = :c"),
                          {"c": sol["colaborador_id"]}).scalar()
        if (etapa or "").upper() in ("CADASTRO_INICIAL", "NA_CONTABILIDADE",
                                     "CONTRATO_A_ASSINAR"):
            _cancelar_pre_cadastro(s, {**sol, "dados": {**(sol.get("dados") or {}),
                                                        "pre_cadastro": "1"}},
                                   f"admissão cancelada: {motivo}", usuario)
    if sol["tipo"] not in ("FERIAS", "CADASTRO", "ADMISSAO_PRESTADOR"):
        _evento(s, sol["id"], "AVISO", {"texto": "o que o atendimento gravou no "
                                        "cadastro NÃO foi desfeito sozinho — "
                                        "confira a ficha da pessoa"}, usuario)


def fechar_extra(s: Session, pedido: dict[str, Any],
                 usuario: Optional[Usuario]) -> int:
    """A contabilidade confirmou a correção ou o cancelamento."""
    from app.apps.erp.core.pessoal_solicitacoes import _evento
    sid = pedido.get("solicitacao_id")
    if not sid:
        return 0
    sol = s.execute(text("SELECT * FROM dp_solicitacoes WHERE id = :i"),
                    {"i": sid}).mappings().first()
    if sol is None:
        return 0
    sol = dict(sol)
    if pedido["template"] == "CANCELAMENTO_SOLICITACAO":
        if sol["tipo"] == "FERIAS":
            from app.apps.erp.core import ferias as svc_ferias
            if svc_ferias.tem_tabela(s):
                s.execute(text("UPDATE ferias_programadas SET situacao = 'CANCELADA', "
                               "cancelada_em = now() WHERE solicitacao_id = :s "
                               "AND situacao <> 'CANCELADA'"), {"s": sid})
        motivo = ((sol.get("dados") or {}).get("cancelando") or {}).get("motivo") or ""
        _finalizar_cancelamento(s, sol, f"{motivo} — confirmado pela "
                                        f"contabilidade ({pedido.get('numero')})",
                                usuario)
        return 1
    _evento(s, sid, "CONTABILIDADE_CONFIRMOU_CORRECAO",
            {"texto": f"{pedido.get('numero')} concluído"}, usuario)
    # A correção era a última coisa que faltava? Então o pedido fecha.
    principais = [p for p in _pedidos(s, sid) if p["template"] not in EXTRAS]
    from app.apps.erp.core import ferias as svc_ferias, termos
    if sol["tipo"] in svc_ferias.TIPOS_COM_AVISO and \
            not termos.concluido(termos.do_pedido(s, sid)):
        return 1        # ainda falta a pessoa assinar o aviso de férias
    if sol["situacao"] == EM_ANDAMENTO and e_processo(sol["tipo"]) \
            and principais and all(p["situacao"] == "CONCLUIDO" for p in principais) \
            and not extras_abertos(s, sid):
        s.execute(text("UPDATE dp_solicitacoes SET situacao = 'ATENDIDA', "
                       "atualizado_em = now() WHERE id = :i"), {"i": sid})
        _evento(s, sid, "ATENDIDA", {"texto": "a correção foi confirmada"}, usuario)
    return 1

