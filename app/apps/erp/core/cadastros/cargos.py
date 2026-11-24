# -*- coding: utf-8 -*-
"""
CARGO, CONVENÇÃO COLETIVA e o PISO — a regra (migração 085).

Pedido do dono, 05/11/2026, sobre a planilha de cargos que ele exportou:

> *"veja que ela tá mal construída. Mistura Cargo e convenção. Isso é pra ser
> distinto. Seleciona Cargo e Selecionar Convenção Coletiva. Ao mesmo tempo tem
> a questão salarial e as alterações salariais pra serem gravadas."*

AS TRÊS COISAS, e por que separá-las resolve o problema dele:

  · **cargo** é o que a pessoa faz (PEDREIRO). Tem CBO, e é um só.
  · **convenção** é o acordo que manda no piso (CE-Fortaleza, BA-Sobradinho).
  · o **cruzamento** é onde vive o piso e a categoria — PEDREIRO em
    CE-Fortaleza paga R$ 2.230; em MA-Imperatriz, R$ 1.608,23.

Na planilha os três estão num texto: `PEDREIRO (CE-Fortaleza)`. São 135 linhas
para 63 cargos — PEDREIRO repetido 11 vezes. Mudar o piso de uma convenção
obrigava a achar e editar linha por linha, e foi assim que apareceram os dois
casos de MESMO cargo e convenção com salários diferentes.

O QUE A IMPORTAÇÃO CONSERTA SOZINHA, e o que ela NÃO conserta:

| defeito na planilha | aqui |
|---|---|
| 16 convenções que são 13 ("MT-Mato Grosso" e "MT- Mato Grosso") | colapsam: a comparação ignora caixa, acento e espaço repetido |
| 19 categorias que são ~13 ("não qualificados" × "NÃO QUALIFICADO") | idem, e o sufixo da convenção ("PROFISSIONAL (MA)") é removido |
| CBO em 5 formatos (`7152-10`, `715505.0`, `72411-0`, vazio) | guardado só com dígitos; a tela exibe com máscara |
| salário por extenso digitado, com erro ("mil quinhetoss") | não é importado: o ERP gera do valor |
| `ALMOXARIFADO` onde era ALMOXARIFE, `PEDEEIRO` onde era PEDREIRO | **NÃO conserta** — vira aviso na tela, para o dono decidir |
| mesmo cargo+convenção com dois salários | **NÃO escolhe** — vira aviso com os dois valores |

Erro de digitação em nome não se adivinha: "ALMOXARIFADO" pode ser cargo de
verdade em outra empresa. Corrigir calado criaria um cargo que ninguém pediu.
"""
from __future__ import annotations

import logging
import re
import unicodedata
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import (
    Cargo, CargoConvencao, CargoConvencaoSalario, Convencao, Usuario,
)

logger = logging.getLogger(__name__)

# As categorias que a planilha usa, já sem o sufixo de convenção e sem variação
# de caixa. Ficam aqui para a tela oferecer lista em vez de texto livre — é o
# texto livre que produziu "não qualificados", "Não qualificados" e
# "NÃO QUALIFICADO" como três categorias.
CATEGORIAS = (
    "PROFISSIONAL", "MEIO-PROFISSIONAL", "QUALIFICADO", "NAO QUALIFICADO",
    "OPERARIO QUALIFICADO", "SERVENTE", "PRATICO", "COMUM",
    "ENCARREGADO DE SETOR", "MESTRE DE OBRAS", "PESSOAL DE APOIO ADM.",
    "SEM CATEGORIA",
)


def _sem_acento(texto: Any) -> str:
    t = unicodedata.normalize("NFKD", str(texto or ""))
    return "".join(c for c in t if not unicodedata.combining(c))


def normalizar_nome(texto: Any) -> str:
    """PURA. Para comparar: sem acento, maiúsculo, um espaço só — e **sem espaço
    ao redor do hífen**.

    O hífen é o detalhe que decide, e eu errei nele na primeira versão: a
    planilha tem "MT-Mato Grosso" e "MT- Mato Grosso", "SP-Feticom" e
    "SP - Feticom". Colapsar só espaço repetido deixa os dois pares de pé, e aí
    a mesma convenção entra duas vezes — com dois pisos para o mesmo cargo, que
    é exatamente o defeito que esta importação existe para consertar.
    """
    limpo = " ".join(_sem_acento(texto).upper().split())
    return re.sub(r"\s*-\s*", "-", limpo)


def normalizar_cbo(bruto: Any) -> Optional[str]:
    """PURA. O CBO só com dígitos.

    A planilha traz cinco formatos, e o pior é o `715505.0`: o Google Sheets
    entendeu `7155-05` como número, dividiu e devolveu decimal. Sem esta
    função, `7152-10`, `715210` e `715210.0` seriam três cargos.

    Mantém até 7 dígitos (o CBO brasileiro tem 6 ou 7); o que sobra é lixo de
    conversão e é descartado com aviso de quem chamou.
    """
    texto = str(bruto or "").strip()
    if not texto:
        return None
    if texto.endswith(".0"):             # 715505.0 -> 715505
        texto = texto[:-2]
    digitos = re.sub(r"\D", "", texto)
    return digitos[:7] or None


def cbo_com_mascara(cbo: Any) -> str:
    """PURA. 715210 -> 7152-10. É só exibição."""
    d = re.sub(r"\D", "", str(cbo or ""))
    if len(d) == 6:
        return f"{d[:4]}-{d[4:]}"
    if len(d) == 7:
        return f"{d[:5]}-{d[5:]}"
    return d


def normalizar_categoria(bruto: Any, convencao: Any = None) -> Optional[str]:
    """PURA. Tira a variação de caixa e o sufixo da convenção.

    "PROFISSIONAL (MA)" é PROFISSIONAL — o "(MA)" repete a convenção, que já é
    a coluna ao lado. Com o sufixo, a mesma categoria viraria uma por estado, e
    nenhuma tela conseguiria somar "quantos profissionais a empresa tem".
    """
    texto = normalizar_nome(bruto)
    if not texto:
        return None
    texto = re.sub(r"\s*\([^)]*\)\s*$", "", texto).strip()
    # "NAO QUALIFICADOS" (plural) -> singular, como as outras
    if texto in ("NAO QUALIFICADOS", "NAO QUALIFICADO"):
        return "NAO QUALIFICADO"
    return texto or None


def _dec(bruto: Any) -> Optional[Decimal]:
    texto = str(bruto or "").strip()
    if not texto:
        return None
    texto = texto.replace("R$", "").strip()
    # "1.910,65" (planilha brasileira) e "1910.65" (exportação) convivem
    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    try:
        return Decimal(texto).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return None


# ---------------------------------------------------------------------------
# Achar ou criar
# ---------------------------------------------------------------------------
def achar_ou_criar_cargo(s: Session, nome: Any, *, cbo: Any = None) -> Cargo:
    limpo = " ".join(str(nome or "").split())
    if len(limpo) < 2:
        raise ErroValidacao("Informe o nome do cargo.")
    alvo = normalizar_nome(limpo)
    for c in s.scalars(select(Cargo)).all():
        if normalizar_nome(c.nome) == alvo:
            if cbo and not c.cbo:             # o CBO que faltava chegou agora
                c.cbo = normalizar_cbo(cbo)
            return c
    c = Cargo(nome=limpo.upper(), cbo=normalizar_cbo(cbo))
    s.add(c)
    s.flush()
    return c


def achar_ou_criar_convencao(s: Session, nome: Any, *, uf: Any = None) -> Convencao:
    limpo = " ".join(str(nome or "").split())
    if not limpo:
        limpo = "Sem Convenção"
    alvo = normalizar_nome(limpo)
    for c in s.scalars(select(Convencao)).all():
        if normalizar_nome(c.nome) == alvo:
            return c
    # A UF sai do próprio nome quando ele começa por ela ("CE-Fortaleza").
    sigla = str(uf or "").strip().upper()[:2] or None
    if not sigla:
        m = re.match(r"^([A-Z]{2})\s*-", limpo.upper())
        sigla = m.group(1) if m else None
    c = Convencao(nome=limpo, uf=sigla)
    s.add(c)
    s.flush()
    return c


def definir_piso(s: Session, cargo_convencao_id: int, *, salario: Any,
                 vigencia: Optional[date] = None, valor_diaria: Any = None,
                 motivo: str = "", usuario: Optional[Usuario] = None) -> CargoConvencao:
    """Troca o piso e FECHA o anterior, na mesma transação.

    O valor vigente fica repetido em `cargo_convencao.salario_piso` porque é o
    que toda tela lê; a história fica na tabela de salários. Manter os dois de
    acordo é trabalho desta função, e de nenhuma outra — se alguém escrever o
    piso direto na coluna, a história passa a mentir.
    """
    cc = s.get(CargoConvencao, cargo_convencao_id)
    if cc is None:
        raise ErroValidacao("Cruzamento de cargo e convenção não encontrado.")
    valor = _dec(salario)
    if valor is None or valor <= 0:
        raise ErroValidacao("Informe o piso salarial.")
    quando = vigencia or date.today()
    aberto = s.scalars(select(CargoConvencaoSalario).where(
        CargoConvencaoSalario.cargo_convencao_id == cc.id,
        CargoConvencaoSalario.vigencia_fim.is_(None))).first()
    if aberto is not None:
        if aberto.salario_piso == valor and _dec(valor_diaria) == aberto.valor_diaria:
            return cc                     # nada mudou: não suja a história
        if quando <= aberto.vigencia_inicio:
            raise ErroValidacao(
                f"O piso vigente começou em {aberto.vigencia_inicio:%d/%m/%Y}. "
                "A vigência do novo precisa ser depois disso.")
        aberto.vigencia_fim = quando
    s.add(CargoConvencaoSalario(
        cargo_convencao_id=cc.id, salario_piso=valor, valor_diaria=_dec(valor_diaria),
        vigencia_inicio=quando, motivo=(motivo or "").strip() or None,
        usuario_id=usuario.id if usuario else None))
    cc.salario_piso = valor
    if _dec(valor_diaria) is not None:
        cc.valor_diaria = _dec(valor_diaria)
    s.flush()
    registrar_evento(s, "cargo_convencao", cc.id, "PISO_DEFINIDO",
                     {"de": str(aberto.salario_piso) if aberto else None,
                      "para": str(valor), "vigencia": str(quando), "motivo": motivo},
                     usuario.id if usuario else None)
    return cc


def piso_em(s: Session, cargo_convencao_id: int, quando: date) -> Optional[Decimal]:
    """O piso que valia NAQUELA data — é o que a conferência da folha usa."""
    linha = s.scalars(select(CargoConvencaoSalario).where(
        CargoConvencaoSalario.cargo_convencao_id == cargo_convencao_id,
        CargoConvencaoSalario.vigencia_inicio <= quando).order_by(
        CargoConvencaoSalario.vigencia_inicio.desc())).first()
    if linha is None:
        return None
    if linha.vigencia_fim is not None and linha.vigencia_fim <= quando:
        return None
    return linha.salario_piso


def cruzamento(s: Session, cargo_id: int, convencao_id: int, *,
               criar: bool = False) -> Optional[CargoConvencao]:
    cc = s.scalars(select(CargoConvencao).where(
        CargoConvencao.cargo_id == cargo_id,
        CargoConvencao.convencao_id == convencao_id)).first()
    if cc is None and criar:
        cc = CargoConvencao(cargo_id=cargo_id, convencao_id=convencao_id)
        s.add(cc)
        s.flush()
    return cc


# ---------------------------------------------------------------------------
# Ler para a tela
# ---------------------------------------------------------------------------
def listar(s: Session, *, so_ativos: bool = True,
           com_salario: bool = True) -> dict[str, Any]:
    """A lista para a tela.

    ⚠️ `com_salario=False` existe por uma brecha que eu mesmo abri e o teste
    `test_permissoes_rotas` pegou: a tela de cadastro precisa da LISTA de cargos
    para montar a caixinha, e ela é aberta a quem tem `ver_pessoal` — o que
    inclui supervisor de obra e PARCEIRO. Mandar o piso junto entregaria a
    tabela salarial da empresa a quem só precisa escolher um cargo numa lista.
    """
    cargos = {c.id: c for c in s.scalars(select(Cargo).order_by(Cargo.nome)).all()}
    convs = {c.id: c for c in s.scalars(select(Convencao).order_by(Convencao.nome)).all()}
    stmt = select(CargoConvencao)
    if so_ativos:
        stmt = stmt.where(CargoConvencao.ativo.is_(True))
    cruzamentos = []
    for cc in s.scalars(stmt).all():
        cargo, conv = cargos.get(cc.cargo_id), convs.get(cc.convencao_id)
        cruzamentos.append({
            "id": cc.id, "cargo_id": cc.cargo_id, "convencao_id": cc.convencao_id,
            "cargo": cargo.nome if cargo else "", "convencao": conv.nome if conv else "",
            "cbo": cbo_com_mascara(cargo.cbo) if cargo else "",
            "categoria": cc.categoria or "",
            "ativo": cc.ativo,
        })
        if com_salario:
            cruzamentos[-1].update({
                "salario_piso": float(cc.salario_piso) if cc.salario_piso else None,
                "valor_diaria": float(cc.valor_diaria) if cc.valor_diaria else None,
            })
    cruzamentos.sort(key=lambda x: (x["cargo"], x["convencao"]))
    return {
        "cargos": [{"id": c.id, "nome": c.nome, "cbo": cbo_com_mascara(c.cbo),
                    "ativo": c.ativo} for c in cargos.values()
                   if c.ativo or not so_ativos],
        "convencoes": [{"id": c.id, "nome": c.nome, "uf": c.uf or "",
                        "mes_data_base": c.mes_data_base, "ativo": c.ativo}
                       for c in convs.values() if c.ativo or not so_ativos],
        "cruzamentos": cruzamentos,
        "categorias": list(CATEGORIAS),
    }


def historico_do_piso(s: Session, cargo_convencao_id: int) -> list[dict[str, Any]]:
    linhas = s.scalars(select(CargoConvencaoSalario).where(
        CargoConvencaoSalario.cargo_convencao_id == cargo_convencao_id).order_by(
        CargoConvencaoSalario.vigencia_inicio.desc())).all()
    return [{
        "salario_piso": float(l.salario_piso),
        "valor_diaria": float(l.valor_diaria) if l.valor_diaria else None,
        "vigencia_inicio": l.vigencia_inicio.isoformat(),
        "vigencia_fim": l.vigencia_fim.isoformat() if l.vigencia_fim else None,
        "vigente": l.vigencia_fim is None,
        "motivo": l.motivo or "",
    } for l in linhas]


# ---------------------------------------------------------------------------
# AS CONVENÇÕES E QUEM ESTÁ NELAS (23/11/2026)
#
# Ele, antes de importar a tabela completa: *"eu queria visualizar quem são os
# colaboradores para cada convenção dessa. Até para entender se todas que foi
# cadastrada tem alguém ou se não. Inclusive, até excluir alguma que não
# precise estar aqui."*
#
# Custo: UMA consulta para o resumo (contagem agrupada), uma por convenção
# aberta. Nada de carregar o cadastro inteiro para contar.
# ---------------------------------------------------------------------------
def convencoes_com_gente(s: Session, *, so_ativas: bool = True) -> list[dict[str, Any]]:
    """Cada convenção com quantos cargos tem e quantas pessoas estão nela."""
    linhas = s.execute(text("""
        SELECT v.id, v.nome, v.uf, v.mes_data_base, v.ativo,
               (SELECT count(*) FROM cargo_convencao cc
                 WHERE cc.convencao_id = v.id AND cc.ativo) AS cargos,
               (SELECT count(*) FROM cargo_convencao cc
                 WHERE cc.convencao_id = v.id AND cc.ativo
                   AND cc.salario_piso IS NULL) AS sem_piso,
               count(c.id) FILTER (WHERE UPPER(COALESCE(c.situacao, '')) <> 'DESLIGADO')
                   AS pessoas,
               count(c.id) FILTER (WHERE UPPER(COALESCE(c.situacao, '')) = 'DESLIGADO')
                   AS desligados
          FROM convencoes v
          LEFT JOIN colaboradores c ON c.convencao_id = v.id
         WHERE (:todas OR v.ativo IS NOT FALSE)
         GROUP BY v.id
         ORDER BY v.nome"""), {"todas": not so_ativas}).mappings().all()
    return [dict(l) for l in linhas]


def pessoas_da_convencao(s: Session, convencao_id: int) -> list[dict[str, Any]]:
    """Quem está na convenção hoje (sem os desligados), com cargo e obra."""
    linhas = s.execute(text("""
        SELECT c.id, c.nome, c.vinculo, c.situacao, g.nome AS cargo,
               o.codigo AS obra
          FROM colaboradores c
          LEFT JOIN cargos g ON g.id = c.cargo_id
          LEFT JOIN obras o ON o.id = c.obra_id
         WHERE c.convencao_id = :v
           AND UPPER(COALESCE(c.situacao, '')) <> 'DESLIGADO'
         ORDER BY c.nome"""), {"v": convencao_id}).mappings().all()
    return [dict(l) for l in linhas]


def excluir_convencao(s: Session, convencao_id: int, usuario: Any) -> dict[str, Any]:
    """Exclui a convenção que não serve — com as três travas que a história pede.

    - Tem gente ATIVA nela: não exclui. Primeiro se troca a convenção dessas
      pessoas; senão a ficha delas ficaria sem piso e sem data-base.
    - Só gente DESLIGADA (ou histórico de vínculo/alteração apontando para
      ela): DESATIVA — some das listas e das caixinhas, mas a rescisão e o
      histórico de quem saiu continuam dizendo em que convenção estavam.
    - Ninguém nunca: apaga de vez, com os cargos dela e o histórico do piso.
    """
    from app.apps.erp.core.comum.auditoria import registrar_evento
    conv = s.get(Convencao, convencao_id)
    if conv is None:
        raise ErroValidacao("Convenção não encontrada.")
    ativas = len(pessoas_da_convencao(s, convencao_id))
    if ativas:
        raise ErroValidacao(
            f"{conv.nome} tem {ativas} pessoa(s) ativa(s). Troque a convenção "
            "delas no cadastro primeiro — senão a ficha fica sem piso e sem "
            "data-base.")
    usada = s.execute(text("""
        SELECT (SELECT count(*) FROM colaboradores WHERE convencao_id = :v)
             + (SELECT count(*) FROM colaborador_vinculos WHERE convencao_id = :v)
             + (SELECT count(*) FROM colaborador_alteracoes
                 WHERE convencao_de_id = :v OR convencao_para_id = :v)"""),
        {"v": convencao_id}).scalar() or 0
    nome = conv.nome
    if usada:
        conv.ativo = False
        s.execute(text("UPDATE cargo_convencao SET ativo = FALSE WHERE convencao_id = :v"),
                  {"v": convencao_id})
        feito = "desativada"
    else:
        s.execute(text("DELETE FROM cargo_convencao WHERE convencao_id = :v"),
                  {"v": convencao_id})
        s.delete(conv)
        feito = "excluida"
    s.flush()
    registrar_evento(s, "convencao", convencao_id,
                     "DESATIVADA" if usada else "EXCLUIDA",
                     {"nome": nome, "historico": int(usada)},
                     getattr(usuario, "id", None))
    return {"feito": feito, "nome": nome}


MESES = ("janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho",
         "agosto", "setembro", "outubro", "novembro", "dezembro")


def mes_da_data_base(bruto: Any) -> Optional[int]:
    """PURA. "5", "05", "maio", "Maio/2026", "01/05" → 5. Vazio ou ilegível → None."""
    t = _sem_acento(str(bruto or "")).strip().lower()
    if not t:
        return None
    for i, nome in enumerate(MESES, start=1):
        if t.startswith(_sem_acento(nome).lower()[:3]):
            return i
    numeros = re.findall(r"\d+", t)
    if len(numeros) >= 2 and len(numeros[0]) <= 2 and len(numeros[1]) <= 2:
        n = int(numeros[1])            # 01/05 = dia/mês
    elif numeros:
        n = int(numeros[0])
    else:
        return None
    return n if 1 <= n <= 12 else None


def planilha_modelo(s: Session) -> bytes:
    """A planilha para importar, JÁ COM o que está cadastrado — quem baixa
    completa e manda de volta. Uma aba com os dados, outra com as regras."""
    from io import BytesIO

    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill

    dados = listar(s, so_ativos=True)
    convs = {c["id"]: c for c in dados["convencoes"]}
    wb = Workbook()
    ws = wb.active
    ws.title = "Cargos"
    cab = ("Cargo", "CBO", "Convenção", "UF", "Mês da data-base", "Categoria",
           "Salário", "Valor diária", "Status")
    ws.append(cab)
    for c in ws[1]:
        c.font = Font(bold=True, color="FFFFFF")
        c.fill = PatternFill("solid", fgColor="0B2C5C")
    for x in dados["cruzamentos"]:
        v = convs.get(x["convencao_id"]) or {}
        ws.append((x["cargo"], x["cbo"], x["convencao"], v.get("uf") or "",
                   MESES[v["mes_data_base"] - 1] if v.get("mes_data_base") else "",
                   x["categoria"], x.get("salario_piso"), x.get("valor_diaria"),
                   "Ativo"))
    if not dados["cruzamentos"]:
        ws.append(("PEDREIRO", "7152-10", "CE-Fortaleza", "CE", "maio",
                   "PROFISSIONAL", 2230.00, None, "Ativo"))
        ws.append(("SERVENTE", "7170-20", "CE-Fortaleza", "CE", "maio",
                   "SERVENTE", 1650.00, None, "Ativo"))
    for col, larg in zip("ABCDEFGHI", (34, 10, 26, 6, 16, 22, 12, 12, 10)):
        ws.column_dimensions[col].width = larg
    for linha in ws.iter_rows(min_row=2, min_col=7, max_col=8):
        for c in linha:
            c.number_format = "#,##0.00"
    ws.freeze_panes = "A2"

    ajuda = wb.create_sheet("Como preencher")
    regras = (
        ("Como preencher a aba Cargos", ""),
        ("", ""),
        ("Uma linha", "é um cargo NUMA convenção. PEDREIRO em CE-Fortaleza e PEDREIRO em "
                      "MA-Imperatriz são duas linhas, cada uma com o seu salário."),
        ("Cargo *", "o nome do cargo. Caixa e acento não importam: “Pedreiro” e "
                    "“PEDREIRO” são o mesmo."),
        ("Convenção *", "o nome da convenção. Se ainda não existir, é criada. Vazio = "
                        "“Sem Convenção” (quem não tem, como autônomo)."),
        ("CBO", "com ou sem traço: 7152-10 ou 715210."),
        ("UF", "a sigla do estado. Se o nome da convenção começar por ela "
               "(“CE-Fortaleza”), pode deixar vazio."),
        ("Mês da data-base", "o mês do reajuste da convenção: “maio” ou 5. É ele que "
                             "avisa o DP do risco de demitir nos 30 dias antes."),
        ("Categoria", "uma de: " + ", ".join(CATEGORIAS) + "."),
        ("Salário", "o piso. 2230,00 ou 2.230,00. Salário novo ABRE uma linha no "
                    "histórico na data que você escolher ao importar — o antigo "
                    "continua valendo para os meses passados."),
        ("Valor diária", "só para quem é pago por dia."),
        ("Status", "“Ativo” ou vazio. Qualquer outra coisa (“Inativo”) entra inativo."),
        ("", ""),
        ("Ao importar", "a tela primeiro CONFERE sem gravar e mostra o que vai "
                        "acontecer e os avisos; só depois você grava."),
        ("Nada é apagado", "cargo ou convenção que não estiver na planilha continua "
                           "como está. Para tirar uma convenção, use o botão Excluir na tela."),
    )
    for a, b in regras:
        ajuda.append((a, b))
    ajuda["A1"].font = Font(bold=True, size=13)
    ajuda.column_dimensions["A"].width = 20
    ajuda.column_dimensions["B"].width = 100
    for linha in ajuda.iter_rows(min_row=3):
        linha[0].font = Font(bold=True)
        linha[1].alignment = Alignment(wrap_text=True, vertical="top")
    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()
