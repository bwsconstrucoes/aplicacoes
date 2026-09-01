# ============================================================================
# ERP — core/pessoal.py
# Colaboradores e Despesas com Colaborador (DC).
#
# Por que a DC existe: pagar 20 pessoas de uma obra não pode virar 20 títulos
# lançados um a um. O pagamento não sai na conta de cada pessoa — sai por
# ARQUIVO (BeeVale ou SomaPay). Então a DC é um lote: várias pessoas, cada uma
# com sua verba, aprovado em cadeia e virando UM título financeiro rateado.
#
# A cadeia tem uma figura a mais que o resto do sistema: o DEPARTAMENTO
# PESSOAL. Ele revisa depois do supervisor porque conhece o cadastro e sabe se
# aquele auxílio-alimentação é devido, se o valor da diária confere. É a
# checagem que o pessoal da obra não tem como fazer.
#
#   administrativo lança → supervisor/gestor revisa → DP revisa → diretor
#   confirma e gera o arquivo de pagamento → nasce o título financeiro
#
# A crítica que mais pega erro é a repetição: a mesma pessoa recebendo a mesma
# verba com valor parecido dentro de dez dias, quando o ciclo é quinzenal.
# ============================================================================
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from app.apps.erp.core.comum.auditoria import ErroPermissao, ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import (
    Categoria, Colaborador, Funcao, Obra, PerfilUsuario, Usuario,
)
from app.apps.erp.db.models.financeiro import (
    DespesaColaborador, DespesaColaboradorItem,
)

logger = logging.getLogger(__name__)
_CENT = Decimal("0.01")
DIAS_REPETICAO = 10          # ciclo quinzenal: repetir em 10 dias é suspeito

# verba → (rótulo, conta do plano, exige quantidade)
VERBAS = {
    "PRODUCAO": ("Produção", "4.1.06", False),
    "DIARIA": ("Diárias", "4.1.05", True),
    "ALIMENTACAO": ("Alimentação (extra)", "4.2.01", False),
    "TRANSPORTE": ("Transporte (extra)", "4.2.02", False),
    "PLR": ("Participação nos lucros", "4.1.08", False),
    "FERIAS": ("Férias", "4.1.03", False),
    "RESCISAO": ("Rescisão", "4.1.04", False),
    "ADIANTAMENTO": ("Adiantamento", "4.1.07", False),
    "OUTRA": ("Outra verba", "4.1.99", False),
}

# quem aprova em cada etapa
CADEIA = [
    ("AGUARDANDO_SUPERVISOR", "aprovado_supervisor",
     (PerfilUsuario.SUPERVISOR_OBRA, PerfilUsuario.GESTOR_OBRA,
      PerfilUsuario.DIRETOR_FINANCEIRO, PerfilUsuario.ADMIN),
     "supervisor ou gestor da obra"),
    ("AGUARDANDO_DP", "aprovado_dp",
     (PerfilUsuario.DEPARTAMENTO_PESSOAL, PerfilUsuario.DIRETOR_FINANCEIRO,
      PerfilUsuario.ADMIN),
     "departamento pessoal"),
    ("AGUARDANDO_DIRETOR", "aprovado_diretor",
     (PerfilUsuario.DIRETOR_FINANCEIRO, PerfilUsuario.ADMIN),
     "diretor financeiro"),
]


def _dec(v: Any, campo: str = "valor") -> Decimal:
    try:
        s = str(v or "0").strip().replace("R$", "").replace(" ", "")
        if "," in s and "." in s:
            s = (s.replace(".", "").replace(",", ".")
                 if s.rfind(",") > s.rfind(".") else s.replace(",", ""))
        elif "," in s:
            s = s.replace(".", "").replace(",", ".")
        elif s.count(".") > 1 or (s.count(".") == 1 and len(s.split(".")[1]) == 3
                                  and len(s.split(".")[0].lstrip("-")) <= 3):
            s = s.replace(".", "")
        return Decimal(s).quantize(_CENT)
    except (InvalidOperation, TypeError):
        raise ErroValidacao(f"Valor inválido em {campo}: {v!r}")


def _data(v: Any) -> Optional[date]:
    if isinstance(v, date):
        return v
    try:
        return date.fromisoformat(str(v)[:10]) if v else None
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Colaboradores
# ---------------------------------------------------------------------------
def salvar_colaborador(s: Session, dados: dict[str, Any], usuario: Usuario) -> Colaborador:
    from app.apps.erp.core.cadastros.validadores import cpf_valido, somente_digitos

    cpf = somente_digitos(dados.get("cpf") or "")
    if not cpf_valido(cpf):
        raise ErroValidacao("CPF inválido.")
    nome = (dados.get("nome") or "").strip()
    if len(nome) < 4:
        raise ErroValidacao("Informe o nome do colaborador.")

    c = s.scalars(select(Colaborador).where(Colaborador.cpf == cpf)).first()
    novo = c is None
    if novo:
        c = Colaborador(cpf=cpf, nome=nome)
        s.add(c)
    c.nome = nome
    for campo in ("matricula", "regime", "situacao", "pix_chave", "pix_tipo",
                  "banco", "agencia", "conta", "telefone", "observacoes"):
        if campo in dados:
            setattr(c, campo, (str(dados[campo]).strip() or None))
    for campo in ("funcao_id", "obra_id"):
        if dados.get(campo):
            setattr(c, campo, int(dados[campo]))
    for campo in ("valor_diaria", "aux_alimentacao", "aux_transporte"):
        if campo in dados:
            valor = str(dados[campo] or "").strip()
            setattr(c, campo, _dec(valor, campo) if valor else None)
    for campo in ("admissao", "demissao"):
        if campo in dados:
            setattr(c, campo, _data(dados[campo]))
    if c.demissao and c.situacao == "ATIVO":
        c.situacao = "DESLIGADO"
    s.flush()
    registrar_evento(s, "colaborador", c.id, "CRIADO" if novo else "ALTERADO",
                     {"nome": c.nome, "cpf": c.cpf, "obra": c.obra_id}, usuario.id)
    return c


def listar_colaboradores(s: Session, obra_id: Optional[int] = None,
                         ativos: bool = True) -> list[dict[str, Any]]:
    stmt = select(Colaborador).options(
        selectinload(Colaborador.funcao), selectinload(Colaborador.obra))
    if obra_id:
        stmt = stmt.where(Colaborador.obra_id == obra_id)
    if ativos:
        stmt = stmt.where(Colaborador.situacao == "ATIVO")
    return [{
        "id": c.id, "nome": c.nome, "cpf": c.cpf, "matricula": c.matricula,
        "funcao": c.funcao.nome if c.funcao else None,
        "funcao_id": c.funcao_id,
        "obra": c.obra.codigo if c.obra else None, "obra_id": c.obra_id,
        "regime": c.regime, "situacao": c.situacao,
        "valor_diaria": float(c.valor_diaria) if c.valor_diaria else (
            float(c.funcao.valor_diaria) if c.funcao and c.funcao.valor_diaria else None),
        "aux_alimentacao": float(c.aux_alimentacao) if c.aux_alimentacao else None,
        "aux_transporte": float(c.aux_transporte) if c.aux_transporte else None,
        "pix_chave": c.pix_chave, "tem_pagamento": bool(c.pix_chave or c.conta),
        "admissao": c.admissao.isoformat() if c.admissao else None,
    } for c in s.scalars(stmt.order_by(Colaborador.nome)).all()]


# ---------------------------------------------------------------------------
# Críticas
# ---------------------------------------------------------------------------
def criticar(s: Session, itens: list[dict[str, Any]], *, obra_id: Optional[int] = None,
             despesa_id: Optional[int] = None) -> dict[str, Any]:
    """As checagens que a planilha não faz — repetição é a que mais pega erro."""
    por_item: dict[int, list[dict[str, str]]] = {}
    gerais: list[dict[str, str]] = []
    hoje = date.today()
    desde = hoje - timedelta(days=DIAS_REPETICAO)

    def marcar(i: int, codigo: str, msg: str, gravidade: str = "ALERTA") -> None:
        por_item.setdefault(i, []).append(
            {"codigo": codigo, "msg": msg, "gravidade": gravidade})

    vistos: dict[tuple, int] = {}
    soma = Decimal("0.00")
    for i, item in enumerate(itens):
        colab = s.get(Colaborador, int(item.get("colaborador_id") or 0))
        if colab is None:
            marcar(i, "P0", "Colaborador não encontrado.", "BLOQUEIA")
            continue
        verba = (item.get("verba") or "").upper()
        if verba not in VERBAS:
            marcar(i, "P1", f"Verba inválida: {verba}.", "BLOQUEIA")
            continue
        valor = _dec(item.get("valor"), f"valor de {colab.nome}")
        soma += valor
        if valor <= 0:
            marcar(i, "P2", "Valor deve ser maior que zero.", "BLOQUEIA")

        if colab.situacao == "DESLIGADO" and verba not in ("RESCISAO", "FERIAS", "PLR"):
            marcar(i, "P3", f"{colab.nome} está desligado — só rescisão, férias ou PLR.",
                   "BLOQUEIA")
        if not (colab.pix_chave or colab.conta):
            marcar(i, "P4", f"{colab.nome} não tem dados de pagamento no cadastro.",
                   "CRITICA")

        # a mesma pessoa, a mesma verba, dentro do ciclo
        anteriores = s.execute(
            select(DespesaColaboradorItem, DespesaColaborador)
            .join(DespesaColaborador,
                  DespesaColaborador.id == DespesaColaboradorItem.despesa_id)
            .where(DespesaColaboradorItem.colaborador_id == colab.id,
                   DespesaColaboradorItem.verba == verba,
                   DespesaColaborador.competencia >= desde,
                   DespesaColaborador.status.not_in(["CANCELADA", "DEVOLVIDA"]),
                   DespesaColaborador.id != (despesa_id or 0))).all()
        for anterior, dc in anteriores:
            dif = abs(Decimal(anterior.valor) - valor)
            if dif <= _CENT:
                marcar(i, "P5", f"{colab.nome} já recebeu {VERBAS[verba][0].lower()} de "
                                f"R$ {anterior.valor} na {dc.numero} "
                                f"({dc.competencia:%d/%m}) — mesmo valor em menos de "
                                f"{DIAS_REPETICAO} dias.", "BLOQUEIA")
                break
            if valor > 0 and dif / valor <= Decimal("0.15"):
                marcar(i, "P6", f"{colab.nome} recebeu R$ {anterior.valor} da mesma verba "
                                f"na {dc.numero} ({dc.competencia:%d/%m}) — valor parecido "
                                f"em menos de {DIAS_REPETICAO} dias.", "CRITICA")
                break

        # repetido dentro da própria DC
        chave = (colab.id, verba)
        if chave in vistos:
            marcar(i, "P7", f"{colab.nome} aparece duas vezes com "
                            f"{VERBAS[verba][0].lower()} nesta mesma despesa.", "BLOQUEIA")
        vistos[chave] = i

        # diária: confere com o cadastro
        if verba == "DIARIA":
            qtd = item.get("quantidade")
            if not qtd or _dec(qtd, "quantidade") <= 0:
                marcar(i, "P8", "Diária exige a quantidade de dias.", "BLOQUEIA")
            else:
                referencia = colab.valor_diaria or (
                    colab.funcao.valor_diaria if colab.funcao else None)
                if referencia:
                    esperado = (Decimal(str(qtd)) * Decimal(referencia)).quantize(_CENT)
                    if abs(esperado - valor) > _CENT:
                        marcar(i, "P9", f"{qtd} diária(s) × R$ {referencia} = R$ {esperado}, "
                                        f"mas foi informado R$ {valor}.", "CRITICA")
                else:
                    marcar(i, "P10", f"{colab.nome} não tem diária cadastrada — "
                                     f"o valor não pôde ser conferido.")

        # auxílios extras acima do cadastrado
        if verba == "ALIMENTACAO" and colab.aux_alimentacao and \
                valor > Decimal(colab.aux_alimentacao):
            marcar(i, "P11", f"Acima do auxílio-alimentação cadastrado "
                             f"(R$ {colab.aux_alimentacao}).")
        if verba == "TRANSPORTE" and colab.aux_transporte and \
                valor > Decimal(colab.aux_transporte):
            marcar(i, "P12", f"Acima do auxílio-transporte cadastrado "
                             f"(R$ {colab.aux_transporte}).")
        if obra_id and colab.obra_id and colab.obra_id != int(obra_id):
            outra = s.get(Obra, colab.obra_id)
            marcar(i, "P13", f"{colab.nome} está lotado em "
                             f"{outra.codigo if outra else 'outra obra'}.")

    if len(itens) >= 25:
        gerais.append({"codigo": "P14", "gravidade": "ALERTA",
                       "msg": f"{len(itens)} pessoas nesta despesa — confira com atenção."})

    bloqueios = sum(1 for l in por_item.values() for c in l if c["gravidade"] == "BLOQUEIA")
    criticas = sum(1 for l in por_item.values() for c in l if c["gravidade"] == "CRITICA")
    return {"por_item": {str(k): v for k, v in por_item.items()}, "gerais": gerais,
            "soma": float(soma), "bloqueios": bloqueios, "criticas": criticas,
            "pessoas": len({i.get("colaborador_id") for i in itens})}


# ---------------------------------------------------------------------------
# Despesa com colaborador
# ---------------------------------------------------------------------------
def _categoria_da_verba(s: Session, verba: Optional[str]) -> Optional[int]:
    """Conta do plano correspondente à verba — a DC entra na análise de custo."""
    codigo = VERBAS.get((verba or "").upper(), (None, None, None))[1]
    if not codigo:
        return None
    cat = s.scalars(select(Categoria).where(Categoria.codigo == codigo)).first()
    return cat.id if cat else None


def categorias_de_pessoal(s: Session) -> list[dict[str, Any]]:
    """Só as contas ligadas a colaboradores — quem lança não vê o plano inteiro."""
    codigos = sorted({v[1] for v in VERBAS.values() if v[1]})
    cats = s.scalars(select(Categoria).where(
        Categoria.ativo.is_(True),
        Categoria.codigo.in_(codigos) | Categoria.codigo.like("4.%"))
        .order_by(Categoria.codigo)).all()
    return [{"id": c.id, "codigo": c.codigo, "descricao": c.descricao,
             "grupo": c.grupo_nome} for c in cats]


def criar_despesa(s: Session, dados: dict[str, Any], usuario: Usuario) -> DespesaColaborador:
    obra = s.get(Obra, int(dados.get("obra_id") or 0))
    if obra is None:
        raise ErroValidacao("Informe a obra.")
    itens = [i for i in (dados.get("itens") or []) if i.get("colaborador_id")]
    if not itens:
        raise ErroValidacao("Adicione ao menos um colaborador.")

    critica = criticar(s, itens, obra_id=obra.id)
    if critica["bloqueios"] and not dados.get("forcar"):
        raise ErroValidacao(
            f"{critica['bloqueios']} bloqueio(s) na conferência — corrija antes de enviar.")

    competencia = _data(dados.get("competencia")) or date.today()
    n = s.scalar(select(func.count()).select_from(DespesaColaborador)) or 0
    d = DespesaColaborador(
        numero=f"DC{n + 1:05d}", obra_id=obra.id, competencia=competencia,
        data_prevista=_data(dados.get("data_prevista")),
        descricao=(dados.get("descricao") or "").strip() or None,
        # o meio de pagamento NÃO é escolhido por quem lança: é o financeiro
        # que decide, na hora de gerar o arquivo
        meio_pagamento=(dados.get("meio_pagamento") or "A_DEFINIR").upper(),
        status="AGUARDANDO_SUPERVISOR",
        valor_total=Decimal(str(critica["soma"])).quantize(_CENT),
        criado_por=usuario.id)
    s.add(d)
    s.flush()

    for i, item in enumerate(itens):
        s.add(DespesaColaboradorItem(
            despesa_id=d.id, colaborador_id=int(item["colaborador_id"]),
            verba=(item.get("verba") or "OUTRA").upper(),
            quantidade=_dec(item["quantidade"], "quantidade") if item.get("quantidade") else None,
            valor_unitario=(_dec(item["valor_unitario"], "valor unitário")
                            if item.get("valor_unitario") else None),
            valor=_dec(item["valor"], "valor"),
            obra_id=int(item.get("obra_id") or obra.id),
            categoria_id=(int(item["categoria_id"]) if item.get("categoria_id")
                          else _categoria_da_verba(s, item.get("verba"))),
            observacao=(item.get("observacao") or "").strip() or None,
            criticas=critica["por_item"].get(str(i), [])))
    s.flush()
    registrar_evento(s, "despesa_colaborador", d.id, "CRIADA", {
        "numero": d.numero, "obra": obra.codigo, "pessoas": critica["pessoas"],
        "itens": len(itens), "total": str(d.valor_total),
        "bloqueios": critica["bloqueios"], "criticas": critica["criticas"]}, usuario.id)
    logger.info("ERP/pessoal: %s com %d item(ns), R$ %s", d.numero, len(itens), d.valor_total)
    return d


def aprovar(s: Session, despesa_id: int, usuario: Usuario,
            observacao: str = "") -> dict[str, Any]:
    """Avança um degrau da cadeia: supervisor → DP → diretor."""
    d = s.get(DespesaColaborador, despesa_id)
    if d is None:
        raise ErroValidacao("Despesa não encontrada.")
    etapa = next((e for e in CADEIA if e[0] == d.status), None)
    if etapa is None:
        raise ErroValidacao(f"Despesa está {d.status} — não há aprovação pendente.")
    _, campo, perfis, quem = etapa
    if usuario.perfil not in perfis:
        raise ErroPermissao(f"Esta etapa é do {quem}.")
    if d.criado_por == usuario.id and usuario.perfil not in (
            PerfilUsuario.ADMIN, PerfilUsuario.DIRETOR_FINANCEIRO):
        raise ErroPermissao("Quem lançou a despesa não a aprova.")

    setattr(d, campo, usuario.id)
    setattr(d, f"{campo}_em", datetime.now(timezone.utc))
    indice = [e[0] for e in CADEIA].index(d.status)
    d.status = CADEIA[indice + 1][0] if indice + 1 < len(CADEIA) else "APROVADA"
    registrar_evento(s, "despesa_colaborador", d.id, "APROVADA_ETAPA", {
        "numero": d.numero, "etapa": quem, "por": usuario.nome,
        "observacao": observacao, "novo_status": d.status}, usuario.id)
    return {"numero": d.numero, "status": d.status,
            "proxima_etapa": next((e[3] for e in CADEIA if e[0] == d.status), None)}


def devolver(s: Session, despesa_id: int, motivo: str, usuario: Usuario) -> dict[str, Any]:
    d = s.get(DespesaColaborador, despesa_id)
    if d is None:
        raise ErroValidacao("Despesa não encontrada.")
    if len((motivo or "").strip()) < 10:
        raise ErroValidacao("Explique o que precisa ser corrigido (mínimo 10 caracteres).")
    d.status = "DEVOLVIDA"
    d.motivo_devolucao = motivo.strip()
    registrar_evento(s, "despesa_colaborador", d.id, "DEVOLVIDA",
                     {"numero": d.numero, "motivo": motivo, "por": usuario.nome}, usuario.id)
    return {"numero": d.numero, "status": d.status}


def gerar_titulo(s: Session, despesa_id: int, dados: dict[str, Any],
                 usuario: Usuario) -> dict[str, Any]:
    """Aprovada, a DC vira UM título financeiro rateado pelas obras dos itens.

    O pagamento sai por arquivo (BeeVale ou SomaPay), então o título nasce em
    Pix: no BeeVale, atualiza-se o QR Code depois; no SomaPay, é a transferência
    para a conta de pagamentos, e a baixa individual acontece lá.
    """
    from app.apps.erp.core.titulos.service import criar_titulo

    d = s.get(DespesaColaborador, despesa_id, options=[
        selectinload(DespesaColaborador.itens)])
    if d is None:
        raise ErroValidacao("Despesa não encontrada.")
    if d.status != "APROVADA":
        raise ErroValidacao(f"Despesa está {d.status} — só se fatura o que foi aprovado.")
    meio = (dados.get("meio_pagamento") or d.meio_pagamento or "").upper()
    if meio in ("", "A_DEFINIR"):
        raise ErroValidacao("Informe o meio de pagamento (BeeVale ou SomaPay) "
                            "antes de gerar o título.")
    d.meio_pagamento = meio
    if d.titulo_id:
        raise ErroValidacao(f"Esta despesa já gerou o título {d.titulo_id}.")

    por_obra: dict[int, Decimal] = {}
    for i in d.itens:
        oid = i.obra_id or d.obra_id
        por_obra[oid] = por_obra.get(oid, Decimal("0")) + Decimal(i.valor)

    # a conta do plano segue a verba predominante do lote
    contagem: dict[str, Decimal] = {}
    for i in d.itens:
        contagem[i.verba] = contagem.get(i.verba, Decimal("0")) + Decimal(i.valor)
    verba_maior = max(contagem, key=contagem.get)
    codigo = VERBAS.get(verba_maior, ("", "4.1.99", False))[1]
    cat = s.scalars(select(Categoria).where(Categoria.codigo == codigo)).first()
    if cat is None:
        cat = s.scalars(select(Categoria).where(Categoria.codigo == "4.1.99")).first()
    if cat is None:
        raise ErroValidacao("Conta de pessoal não encontrada — instale o plano financeiro.")

    verbas_txt = ", ".join(f"{VERBAS[v][0]} {c}" for v, c in
                           sorted(contagem.items(), key=lambda x: -x[1])[:3])
    titulo = criar_titulo(s, {
        # folha e encargos: é o tipo que aceita as contas do grupo 4 sem nota
        "tipo": "T7_FOLHA_ENCARGOS",
        "fornecedor_id": dados.get("fornecedor_id"),
        "categoria_id": cat.id,
        "descricao": f"{d.numero} — {len(d.itens)} colaborador(es) · {verbas_txt}"[:200],
        "valor_bruto": str(d.valor_total),
        "competencia": d.competencia.strftime("%Y-%m"),
        "forma_pagamento": "PIX",
        "fornecedor_conta_id": dados.get("fornecedor_conta_id"),
        "parcelas": [{"vencimento": (dados.get("vencimento")
                                     or (d.data_prevista or date.today()).isoformat()),
                      "valor": str(d.valor_total)}],
        "rateios": [{"obra_id": i.obra_id or d.obra_id, "valor": str(i.valor),
                     "categoria_id": i.categoria_id,
                     "descricao": VERBAS.get(i.verba, (i.verba,))[0]} for i in d.itens],
        "despesa_colaborador_id": d.id,
        "justificativa_excecao":
            f"Despesa com colaboradores {d.numero}, paga por arquivo "
            f"{d.meio_pagamento}. Aprovada por supervisor, DP e diretoria.",
    }, usuario)
    # DC de uma pessoa só: o título aponta direto para ela; de várias, cada uma
    # entra no rateio por colaborador — assim a ficha de todos fica completa
    pessoas = {i.colaborador_id for i in d.itens}
    if len(pessoas) == 1:
        titulo.colaborador_id = next(iter(pessoas))
    else:
        from app.apps.erp.db.models.financeiro import TituloColaborador
        por_pessoa: dict[int, Decimal] = {}
        for i in d.itens:
            por_pessoa[i.colaborador_id] = por_pessoa.get(
                i.colaborador_id, Decimal("0")) + Decimal(i.valor)
        for cid, valor in por_pessoa.items():
            s.add(TituloColaborador(titulo_id=titulo.id, colaborador_id=cid,
                                    valor=valor, observacao=f"Parte na {d.numero}"))

    d.titulo_id = titulo.id
    d.status = "FATURADA"
    s.flush()
    registrar_evento(s, "despesa_colaborador", d.id, "TITULO_GERADO", {
        "numero": d.numero, "titulo": titulo.numero_sp, "valor": str(d.valor_total),
        "meio": d.meio_pagamento, "obras": len(por_obra)}, usuario.id)
    return {
        "numero": d.numero, "titulo": titulo.numero_sp, "titulo_id": titulo.id,
        "valor": float(d.valor_total), "rateios": len(por_obra),
        "proximo_passo": ("Atualize o QR Code Pix do BeeVale neste título."
                          if d.meio_pagamento == "BEEVALE" else
                          "Transferência para a conta SomaPay — a baixa individual "
                          "acontece na plataforma."),
    }


def planilha_pagamento(s: Session, despesa_id: int) -> dict[str, Any]:
    """Linhas prontas para o arquivo de pagamento (BeeVale ou SomaPay)."""
    d = s.get(DespesaColaborador, despesa_id, options=[
        selectinload(DespesaColaborador.itens)])
    if d is None:
        raise ErroValidacao("Despesa não encontrada.")
    linhas = []
    for i in sorted(d.itens, key=lambda x: x.id):
        c = s.get(Colaborador, i.colaborador_id)
        linhas.append({
            "dc": d.numero, "nome": c.nome if c else "—", "cpf": c.cpf if c else "",
            "verba": VERBAS.get(i.verba, (i.verba,))[0],
            "quantidade": float(i.quantidade) if i.quantidade else None,
            "valor": float(i.valor),
            "pix_chave": (c.pix_chave if c else None),
            "banco": c.banco if c else None, "agencia": c.agencia if c else None,
            "conta": c.conta if c else None,
            "observacao": i.observacao or "",
        })
    return {"numero": d.numero, "meio": d.meio_pagamento,
            "competencia": d.competencia.strftime("%m/%Y"),
            "total": float(d.valor_total), "linhas": linhas,
            "sem_dados": [l["nome"] for l in linhas
                          if not (l["pix_chave"] or l["conta"])]}


def detalhar(s: Session, despesa_id: int) -> dict[str, Any]:
    d = s.get(DespesaColaborador, despesa_id, options=[
        selectinload(DespesaColaborador.itens)])
    if d is None:
        raise ErroValidacao("Despesa não encontrada.")
    obra = s.get(Obra, d.obra_id)
    obras = {o.id: o.codigo for o in s.scalars(select(Obra)).all()}

    def nome(uid):
        u = s.get(Usuario, uid) if uid else None
        return u.nome if u else None

    return {
        "id": d.id, "numero": d.numero, "obra": obra.codigo if obra else "—",
        "obra_id": d.obra_id, "competencia": d.competencia.strftime("%m/%Y"),
        "data_prevista": d.data_prevista.isoformat() if d.data_prevista else None,
        "descricao": d.descricao, "meio_pagamento": d.meio_pagamento,
        "status": d.status, "valor_total": float(d.valor_total),
        "titulo_id": d.titulo_id, "motivo_devolucao": d.motivo_devolucao,
        "criado_por": nome(d.criado_por),
        "aprovacoes": [
            {"etapa": "Supervisor/gestor", "por": nome(d.aprovado_supervisor),
             "em": d.aprovado_supervisor_em.strftime("%d/%m/%Y %H:%M")
                   if d.aprovado_supervisor_em else None},
            {"etapa": "Departamento pessoal", "por": nome(d.aprovado_dp),
             "em": d.aprovado_dp_em.strftime("%d/%m/%Y %H:%M") if d.aprovado_dp_em else None},
            {"etapa": "Diretor financeiro", "por": nome(d.aprovado_diretor),
             "em": d.aprovado_diretor_em.strftime("%d/%m/%Y %H:%M")
                   if d.aprovado_diretor_em else None},
        ],
        "proxima_etapa": next((e[3] for e in CADEIA if e[0] == d.status), None),
        "itens": [{
            "id": i.id, "colaborador_id": i.colaborador_id,
            "nome": (s.get(Colaborador, i.colaborador_id).nome
                     if s.get(Colaborador, i.colaborador_id) else "—"),
            "cpf": (s.get(Colaborador, i.colaborador_id).cpf
                    if s.get(Colaborador, i.colaborador_id) else ""),
            "verba": i.verba, "verba_rotulo": VERBAS.get(i.verba, (i.verba,))[0],
            "categoria_id": i.categoria_id,
            "quantidade": float(i.quantidade) if i.quantidade else None,
            "valor_unitario": float(i.valor_unitario) if i.valor_unitario else None,
            "valor": float(i.valor), "obra": obras.get(i.obra_id, ""),
            "observacao": i.observacao, "criticas": i.criticas or [],
            "conferido": bool(i.conferido_em),
        } for i in d.itens],
    }


def historico(s: Session, colaborador_id: int, meses: int = 24) -> dict[str, Any]:
    """Tudo que já se pagou a esta pessoa, venha de onde vier.

    Junta três origens: os itens de DC, os títulos lançados diretamente em nome
    dela (rescisão, exame, vale) e a parte dela em títulos coletivos, como a
    guia de FGTS do mês. Sem isso, o histórico teria buraco justamente nos
    valores maiores.
    """
    from app.apps.erp.db.models.financeiro import (
        StatusTitulo, Titulo, TituloColaborador,
    )

    c = s.get(Colaborador, colaborador_id)
    if c is None:
        raise ErroValidacao("Colaborador não encontrado.")
    corte = date.today() - timedelta(days=meses * 31)
    linhas: list[dict[str, Any]] = []

    for item, dc in s.execute(
            select(DespesaColaboradorItem, DespesaColaborador)
            .join(DespesaColaborador,
                  DespesaColaborador.id == DespesaColaboradorItem.despesa_id)
            .where(DespesaColaboradorItem.colaborador_id == colaborador_id,
                   DespesaColaborador.competencia >= corte,
                   DespesaColaborador.status != "CANCELADA")
            .order_by(DespesaColaborador.competencia.desc())).all():
        linhas.append({
            "origem": "DC", "referencia": dc.numero,
            "data": dc.competencia.isoformat(),
            "descricao": VERBAS.get(item.verba, (item.verba,))[0],
            "quantidade": float(item.quantidade) if item.quantidade else None,
            "valor": float(item.valor), "situacao": dc.status,
            "titulo_id": dc.titulo_id, "id": dc.id,
        })

    for t in s.scalars(select(Titulo).where(
            Titulo.colaborador_id == colaborador_id,
            Titulo.status.not_in([StatusTitulo.CANCELADO, StatusTitulo.ESTORNADO]),
            Titulo.competencia >= corte)).all():
        linhas.append({
            "origem": "TITULO", "referencia": t.numero_sp,
            "data": t.competencia.isoformat(), "descricao": t.descricao,
            "valor": float(t.valor_liquido), "situacao": t.status.value,
            "titulo_id": t.id, "id": t.id,
        })

    for vinculo, t in s.execute(
            select(TituloColaborador, Titulo)
            .join(Titulo, Titulo.id == TituloColaborador.titulo_id)
            .where(TituloColaborador.colaborador_id == colaborador_id,
                   Titulo.status.not_in([StatusTitulo.CANCELADO, StatusTitulo.ESTORNADO]),
                   Titulo.competencia >= corte)).all():
        linhas.append({
            "origem": "RATEADO", "referencia": t.numero_sp,
            "data": t.competencia.isoformat(),
            "descricao": f"{t.descricao} (parte desta pessoa)",
            "valor": float(vinculo.valor) if vinculo.valor else None,
            "situacao": t.status.value, "titulo_id": t.id, "id": t.id,
            "observacao": vinculo.observacao,
        })

    linhas.sort(key=lambda x: x["data"], reverse=True)
    por_verba: dict[str, float] = {}
    total = 0.0
    for l in linhas:
        if l["valor"]:
            total += l["valor"]
            chave = l["descricao"][:40]
            por_verba[chave] = por_verba.get(chave, 0.0) + l["valor"]
    ultimos_12 = sum(l["valor"] or 0 for l in linhas
                     if l["data"] >= (date.today() - timedelta(days=365)).isoformat())
    return {
        "colaborador": {"id": c.id, "nome": c.nome, "cpf": c.cpf,
                        "funcao": c.funcao.nome if c.funcao else None,
                        "obra": c.obra.codigo if c.obra else None,
                        "situacao": c.situacao,
                        "admissao": c.admissao.isoformat() if c.admissao else None,
                        "demissao": c.demissao.isoformat() if c.demissao else None},
        "lancamentos": linhas, "total": round(total, 2),
        "ultimos_12_meses": round(ultimos_12, 2),
        "por_verba": sorted(({"verba": k, "valor": round(v, 2)}
                             for k, v in por_verba.items()),
                            key=lambda x: x["valor"], reverse=True),
    }


def listar(s: Session, usuario: Optional[Usuario] = None) -> list[dict[str, Any]]:
    from app.apps.erp.core.auth.permissoes import obras_do_usuario

    stmt = (select(DespesaColaborador).options(selectinload(DespesaColaborador.itens))
            .order_by(DespesaColaborador.id.desc()).limit(300))
    if usuario is not None:
        permitidas = obras_do_usuario(s, usuario)
        if permitidas is not None:
            stmt = stmt.where(DespesaColaborador.obra_id.in_(permitidas or [0]))
    obras = {o.id: o.codigo for o in s.scalars(select(Obra)).all()}
    saida = []
    for d in s.scalars(stmt).all():
        alertas = sum(1 for i in d.itens for c in (i.criticas or [])
                      if c.get("gravidade") in ("BLOQUEIA", "CRITICA"))
        saida.append({
            "id": d.id, "numero": d.numero, "obra": obras.get(d.obra_id, "—"),
            "competencia": d.competencia.strftime("%m/%Y"),
            "status": d.status, "valor_total": float(d.valor_total),
            "pessoas": len({i.colaborador_id for i in d.itens}),
            "itens": len(d.itens), "meio_pagamento": d.meio_pagamento,
            "alertas": alertas, "titulo_id": d.titulo_id,
            "proxima_etapa": next((e[3] for e in CADEIA if e[0] == d.status), None),
        })
    return saida
