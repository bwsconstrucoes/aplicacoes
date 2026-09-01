# ============================================================================
# ERP — core/titulos/enquadramento.py
# Impede que a despesa seja lançada pelo caminho errado.
#
# A trava óbvia já existe: a conta de locação exige contrato. O problema é a
# burla — lançar a locação como "outras despesas" ou como serviço de terceiros
# e escapar da exigência. O mesmo vale para empreita com medição virando
# "serviço técnico PJ", e para gasto miúdo que deveria ir por fundo fixo.
#
# Aqui o sistema lê o que está sendo lançado e diz: isto é locação, vá pelo
# caminho da locação. A evidência mais forte não é a palavra na descrição — é
# o CADASTRO: se este credor tem contrato de locação ativo, uma despesa dele
# fora do contrato é quase sempre a parcela sendo lançada por fora.
#
# Três níveis: BLOQUEIA (evidência de cadastro), CRITICA (texto do documento
# mais o histórico) e ALERTA (só o texto). O bloqueio pode ser vencido por
# quem tem alçada, mas fica registrado quem passou por cima e por quê.
# ============================================================================
from __future__ import annotations

import logging
import re
import unicodedata
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.apps.erp.db.models.cadastros import Categoria, Fornecedor, Usuario
from app.apps.erp.db.models.financeiro import (
    ContratoLocacao, ContratoServico, Titulo,
)

logger = logging.getLogger(__name__)

# caminho → (rótulo, onde lançar)
CAMINHOS = {
    "LOCACAO": ("Locação de equipamento", "aba Locações — contrato e parcela prevista"),
    "EMPREITA": ("Empreita com medição", "aba Empreitas — contrato e medição"),
    "FUNDO_FIXO": ("Fundo fixo", "aba Fundo fixo e cartão — prestação de contas"),
    "CARTAO": ("Fatura de cartão", "aba Fundo fixo e cartão — fatura"),
}

_TERMOS = {
    "LOCACAO": [r"\bLOCA[CÇ][AÃ]O\b", r"\bALUGUEL\b", r"\bALUGADO\b", r"\bLOCADO\b",
                r"\bLOCA[CÇ][AÃ]O\s+DE\s+EQUIP", r"\bDI[AÁ]RIA\s+DE\s+EQUIP",
                r"\bANDAIME", r"\bESCORA", r"\bBETONEIRA", r"\bCOMPACTADOR",
                r"\bBANHEIRO\s+QU[IÍ]MICO", r"\bCONTAINER", r"\bGERADOR\b",
                r"\bPER[IÍ]ODO\s+DE\s+LOCA"],
    "EMPREITA": [r"\bEMPREITA", r"\bM[AÃ]O\s+DE\s+OBRA\b", r"\bMEDI[CÇ][AÃ]O\b",
                 r"\bSUBEMPREIT", r"\bEXECU[CÇ][AÃ]O\s+DE\s+SERVI",
                 r"\bM2\b|\bM²\b|\bMETRO\s+QUADRADO", r"\bPERCENTUAL\s+EXECUTADO"],
    "FUNDO_FIXO": [r"\bREEMBOLSO\b", r"\bRESSARCIMENTO\s+DE\s+DESPESA",
                   r"\bDESPESAS?\s+MI[UÚ]DAS?\b", r"\bCAIXA\s+PEQUEN",
                   r"\bFUNDO\s+FIXO\b", r"\bPRESTA[CÇ][AÃ]O\s+DE\s+CONTAS?\b"],
    "CARTAO": [r"\bFATURA\s+(DO\s+)?CART[AÃ]O", r"\bCART[AÃ]O\s+DE\s+CR[EÉ]DITO"],
}

# contas que já são o caminho certo — se a pessoa escolheu, não há o que apontar
_CONTAS_ESPERADAS = {
    "LOCACAO": ("3.3.",),
    "EMPREITA": ("3.2.01",),
    "FUNDO_FIXO": ("3.4.08", "5.3.10"),
}


def _norm(t: str) -> str:
    t = unicodedata.normalize("NFKD", (t or "").upper())
    return "".join(c for c in t if not unicodedata.combining(c))


def _texto_do_lancamento(dados: dict[str, Any]) -> str:
    partes = [dados.get("descricao"), dados.get("emitente_nome"),
              dados.get("observacoes"), dados.get("justificativa_excecao")]
    for i in (dados.get("itens") or [])[:10]:
        partes.append(i.get("descricao") if isinstance(i, dict) else str(i))
    return _norm(" · ".join(str(p) for p in partes if p))


def avaliar(s: Session, dados: dict[str, Any],
            usuario: Optional[Usuario] = None) -> list[dict[str, Any]]:
    """Diz se o lançamento está indo pelo caminho certo."""
    achados: list[dict[str, Any]] = []
    modalidade = (dados.get("modalidade") or "NORMAL").upper()
    tipo = (dados.get("tipo") or "").upper()

    categoria = None
    if dados.get("categoria_id"):
        categoria = s.get(Categoria, int(dados["categoria_id"]))
    codigo = categoria.codigo if categoria else ""

    def ja_esta_certo(caminho: str) -> bool:
        if caminho == "LOCACAO":
            return (tipo.startswith("T4") or bool(dados.get("locacao_contrato_id"))
                    or any(codigo.startswith(p) for p in _CONTAS_ESPERADAS["LOCACAO"]))
        if caminho == "EMPREITA":
            return (tipo.startswith("T5") or bool(dados.get("contrato_servico_id"))
                    or codigo in _CONTAS_ESPERADAS["EMPREITA"])
        if caminho == "FUNDO_FIXO":
            return modalidade == "FUNDO_FIXO" or codigo in _CONTAS_ESPERADAS["FUNDO_FIXO"]
        if caminho == "CARTAO":
            return modalidade == "CARTAO"
        return False

    forn_id = dados.get("fornecedor_id")

    # Se o lançamento JÁ está num caminho específico, não se aponta outro por
    # evidência de cadastro: um mesmo credor pode ter locação e empreita ao
    # mesmo tempo, e cobrar os dois caminhos ao mesmo tempo seria só ruído.
    enquadrado = any(ja_esta_certo(k) for k in CAMINHOS)
    # a despesa com colaborador já é um caminho próprio, com sua cadeia de
    # aprovação; não faz sentido cobrar dela contrato de locação ou empreita
    if dados.get("despesa_colaborador_id"):
        enquadrado = True
    # pagamento de pessoa (rescisão, guia, verba) também é caminho próprio:
    # não se cobra dele contrato de locação nem de empreita
    if (dados.get("colaborador_id") or dados.get("colaboradores")
            or tipo.startswith("T7")):
        enquadrado = True

    # ---- evidência de cadastro: o credor TEM contrato ativo
    if forn_id and not enquadrado:
        contratos = s.scalars(select(ContratoLocacao).where(
            ContratoLocacao.fornecedor_id == int(forn_id),
            ContratoLocacao.status == "ATIVO")).all()
        if contratos:
            nums = ", ".join(c.numero for c in contratos[:3])
            achados.append({
                "caminho": "LOCACAO", "gravidade": "BLOQUEIA", "codigo": "E1",
                "msg": f"Este credor tem contrato de locação ativo ({nums}). "
                       f"A cobrança de locação precisa ser lançada como parcela do "
                       f"contrato, não como despesa avulsa.",
                "onde": CAMINHOS["LOCACAO"][1]})

    if forn_id and not enquadrado:
        contratos = s.scalars(select(ContratoServico).where(
            ContratoServico.fornecedor_id == int(forn_id),
            ContratoServico.status == "VIGENTE")).all()
        if contratos:
            nums = ", ".join(c.numero for c in contratos[:3])
            achados.append({
                "caminho": "EMPREITA", "gravidade": "BLOQUEIA", "codigo": "E2",
                "msg": f"Este prestador tem contrato de empreita vigente ({nums}). "
                       f"O pagamento deve sair de uma medição do contrato — é o que "
                       f"impede pagar duas vezes o mesmo serviço.",
                "onde": CAMINHOS["EMPREITA"][1]})

    # ---- evidência de texto
    texto = _texto_do_lancamento(dados)
    for caminho, padroes in _TERMOS.items():
        if ja_esta_certo(caminho) or any(a["caminho"] == caminho for a in achados):
            continue
        encontrados = [p for p in padroes if re.search(p, texto)]
        if not encontrados:
            continue
        gravidade = "CRITICA" if len(encontrados) >= 2 else "ALERTA"
        achados.append({
            "caminho": caminho, "gravidade": gravidade, "codigo": "E3",
            "msg": f"O documento fala em {CAMINHOS[caminho][0].lower()} "
                   f"({len(encontrados)} indício(s) no texto), mas o lançamento está "
                   f"indo por outro caminho.",
            "onde": CAMINHOS[caminho][1]})

    # ---- histórico: este credor sempre foi lançado como locação?
    if forn_id and not enquadrado and not any(
            a["caminho"] == "LOCACAO" for a in achados):
        anteriores = s.execute(
            select(func.count()).select_from(Titulo).join(
                Categoria, Categoria.id == Titulo.categoria_id)
            .where(Titulo.fornecedor_id == int(forn_id),
                   Categoria.codigo.like("3.3.%"),
                   Titulo.criado_em >= date.today() - timedelta(days=365))).scalar() or 0
        if anteriores >= 2:
            achados.append({
                "caminho": "LOCACAO", "gravidade": "CRITICA", "codigo": "E4",
                "msg": f"Este credor teve {anteriores} título(s) de locação no último ano. "
                       f"Confirme se esta despesa não é a mesma locação sem contrato.",
                "onde": CAMINHOS["LOCACAO"][1]})

    # ---- nota de débito: documento típico de locadora
    tipo_doc = (dados.get("tipo_documento") or "").upper()
    parece_nota_debito = ("NOTA DE DEBITO" in texto or "NOTA DEBITO" in texto
                          or "FATURA DE LOCACAO" in texto
                          or (tipo_doc in ("FATURA", "OUTRO") and
                              re.search(r"\bLOCA[CÇ][AÃ]O\b", texto)))
    if parece_nota_debito and not ja_esta_certo("LOCACAO"):
        mensagem = ("Isto é uma nota de débito de locação e não há contrato de locação "
                    "cadastrado para este credor. Cadastre o contrato antes — sem ele "
                    "ninguém acompanha o que está em obra nem quando devolver, que é "
                    "exatamente como o equipamento se perde.")
        existente = next((a for a in achados if a["caminho"] == "LOCACAO"), None)
        if existente is not None:
            # o texto já levantou a suspeita; a nota de débito confirma
            existente["gravidade"] = "BLOQUEIA"
            existente["codigo"] = "E6"
            existente["msg"] = mensagem
        else:
            achados.append({"caminho": "LOCACAO", "gravidade": "BLOQUEIA",
                            "codigo": "E6", "msg": mensagem,
                            "onde": CAMINHOS["LOCACAO"][1]})

    # ---- gasto miúdo pulverizado que deveria ser fundo fixo
    valor = dados.get("valor_bruto") or dados.get("valor")
    try:
        v = Decimal(str(valor).replace(",", ".")) if valor else None
    except Exception:
        v = None
    if (v is not None and v <= Decimal("300") and modalidade == "NORMAL"
            and not dados.get("documento_numero")
            and tipo.startswith("T14")):
        achados.append({
            "caminho": "FUNDO_FIXO", "gravidade": "ALERTA", "codigo": "E5",
            "msg": f"Despesa de R$ {v} sem documento fiscal. Gasto miúdo assim costuma "
                   f"entrar por fundo fixo, com o comprovante anexado à linha.",
            "onde": CAMINHOS["FUNDO_FIXO"][1]})

    ordem = {"BLOQUEIA": 0, "CRITICA": 1, "ALERTA": 2}
    achados.sort(key=lambda a: ordem[a["gravidade"]])
    return achados


def exigir_caminho_correto(s: Session, dados: dict[str, Any], usuario: Usuario) -> None:
    """Chamado na criação do título: recusa o que tem evidência de cadastro."""
    from app.apps.erp.core.auth.permissoes import P
    from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento

    achados = avaliar(s, dados, usuario)
    bloqueios = [a for a in achados if a["gravidade"] == "BLOQUEIA"]
    if not bloqueios:
        return
    justificativa = (dados.get("justificativa_enquadramento") or "").strip()
    pode_vencer = usuario.perfil in (P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO)
    if not (pode_vencer and len(justificativa) >= 15):
        b = bloqueios[0]
        raise ErroValidacao(
            f"{b['msg']} Lance por: {b['onde']}."
            + ("" if pode_vencer else
               " Se houver exceção, peça ao financeiro."))
    logger.info("ERP/enquadramento: %s venceu bloqueio %s", usuario.email,
                [b["codigo"] for b in bloqueios])
    dados["_enquadramento_vencido"] = {
        "bloqueios": bloqueios, "justificativa": justificativa, "por": usuario.email}
