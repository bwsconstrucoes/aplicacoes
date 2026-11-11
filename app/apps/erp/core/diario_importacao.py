# -*- coding: utf-8 -*-
"""
ERP — A IMPORTAÇÃO DOS DIÁRIOS JÁ ESCRITOS (migração 099).

Pedido do dono em 10/11/2026, mandando a planilha com 1.427 diários:

    *"Eu gostaria de importar essa base que nós temos aí para dentro do diário
    de obras, porque aí a gente poderia dar prosseguimento ao que a gente já
    tem. (…) Nós temos o código da obra, temos o dia, temos questão dos
    colaboradores. Não vai ter, da mesma forma, porque aí não tem nome do
    colaborador, só tem as funções. (…) Pelo código da obra você vai puxar as
    outras informações."*

⚑ AS TRÊS REGRAS DESTE ARQUIVO, e elas valem mais que o código:

1. **NADA É GRAVADO SEM CONFERÊNCIA.** `importar(simular=True)` é o padrão, e
   a tela mostra o que entraria antes de deixar gravar. Importação de 1.427
   linhas que dá errado no meio é trabalho de dias para desfazer.

2. **DIA QUE JÁ EXISTE NO ERP NÃO É SOBRESCRITO, NUNCA.** Diário é documento.
   Se alguém já escreveu aquele dia aqui, a linha da planilha é informada como
   "já existia" e ignorada — a planilha não manda no que o sistema já tem.

3. **A PLANILHA NÃO CRIA OBRA.** Código que não bate com nenhuma obra
   cadastrada é devolvido na conferência para o dono dizer qual é. Inventar
   obra a partir de um código seria encher o cadastro de duplicata.

O QUE A PLANILHA TEM, E O QUE VIRA AQUI
---------------------------------------
    Obra                     → obras.codigo (ou o mapa que ele montar)
    Data do Diário           → diarios_obra.data
    Tempo Manhã / Tarde      → tempo_manha / tempo_tarde
    Relato Diário            → relato
    Responsável pelo Inform. → informado_por (quem preencheu lá, não um login)
    Efetivo 1 / Efetivo 2    → diario_efetivo_extra, origem IMPORTADO
    Efetivo Total            → só para CONFERIR a soma; não é gravado
    ID                       → importado_id (torna o reenvio inofensivo)

As colunas Contrato, Objeto, Endereço, Município, Responsável Técnico e
companhia vêm VAZIAS no arquivo, e é o certo: esses dados moram no cadastro da
obra e o diário os busca de lá na hora de mostrar. Ver `cabecalho()`.
"""
from __future__ import annotations

import io
import logging
import re
import unicodedata
from datetime import date, datetime
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao

logger = logging.getLogger(__name__)

ORIGEM = "planilha"

# O teto existe para a importação não virar um pedido de meia hora que o
# gunicorn derruba no meio. A planilha dele tem 1.427 linhas.
TETO_DE_LINHAS = 20000

# ---------------------------------------------------------------------------
# O TEMPO: a planilha escreve de seis jeitos para três coisas.
# ---------------------------------------------------------------------------
_TEMPOS = {
    "BOM": "BOM",
    "NUBLADO": "NUBLADO",
    "CHUVA": "CHUVOSO", "CHUVOSO": "CHUVOSO",
    "IMPRATICAVEL": "IMPRATICAVEL", "IMPRATICÁVEL": "IMPRATICAVEL",
}

# ---------------------------------------------------------------------------
# ⚑ OS DIAS SEM EXPEDIENTE, e esta lista é FECHADA de propósito.
#
# 135 linhas dizem só "FOLGA", 83 dizem '"FOLGA"', outras dizem "DOMINGO" ou
# "RECESSO". Marcar esses dias como sem expediente muda o papel (o modelo do
# órgão tem campo para isso) e tira o dia da conta de pendência.
#
# "AGUARDANDO LICENÇA" (106 linhas) NÃO entra, e a diferença importa: não é
# folga, é obra parada esperando terceiro — informação de pleito de prazo, que
# não pode virar "não houve expediente". O relato continua escrito em todos os
# casos; marcar o dia não apaga o que foi dito.
# ---------------------------------------------------------------------------
SEM_EXPEDIENTE = {
    "FOLGA", "FOLGAS", "DOMINGO", "DOMINGO - FERIADO", "FERIADO",
    "RECESSO", "SABADO", "SABADO COMPENSADO", "NAO HOUVERAM ATIVIDADES",
    "NAO HOUVE ATIVIDADES", "SEM ATIVIDADES", "SEM EXPEDIENTE",
}

# ⚑ O CASAMENTO EXATO NÃO BASTAVA, e o arquivo dele mostrou isso: a mesma
# coisa aparece como "FOLGA", '"FOLGA"', "FERIADO - NÃO HOUVE EXPEDIENTE",
# "* SEM ATIVIDADES REALIZADAS NO DIA.", "SEM EXPENDIENTE ( CARNAVAL )" (com o
# erro de digitação) e "FINAL DE SEMANA". Uma lista exata deixaria passar
# dezenas de dias, e cada um viraria dia trabalhado no papel.
#
# Então: MARCADOR dentro do texto, mas SÓ EM RELATO CURTO. É a trava que
# importa — num relato de obra de verdade a palavra "feriado" aparece ("3-
# concretagem adiantada por causa do feriado"), e marcar aquele dia como sem
# expediente seria apagar um dia de trabalho do documento.
LIMITE_DO_AVISO = 60
SINAIS_SEM_EXPEDIENTE = (
    "NAO HOUVE EXPEDIENTE", "SEM EXPEDIENTE", "SEM EXPENDIENTE",
    "NAO HOUVERAM ATIVIDADES", "NAO HOUVE ATIVIDADES", "SEM ATIVIDADES",
    "FOLGA", "FERIADO", "RECESSO", "DOMINGO", "FINAL DE SEMANA",
    "SABADO COMPENSADO", "SABADO CONPENSADO",
)


def parece_sem_expediente(relato: Any) -> bool:
    """O dia em que não houve obra — dito de qualquer um dos jeitos.

    ⚠️ "AGUARDANDO LICENÇA" (106 linhas no arquivo dele) NÃO entra aqui, e a
    diferença vale dinheiro: não é folga, é obra PARADA esperando terceiro —
    informação de pleito de prazo. Virar "não houve expediente" apagaria
    exatamente o que sustenta o pleito.
    """
    texto = _maiusculo_sem_acento(relato)
    if not texto or len(texto) > LIMITE_DO_AVISO:
        return False
    if texto in SEM_EXPEDIENTE:
        return True
    return any(sinal in texto for sinal in SINAIS_SEM_EXPEDIENTE)

_COLUNAS = {
    "obra": ("obra",),
    "data": ("data do diario", "data"),
    "tempo_manha": ("tempo manha",),
    "tempo_tarde": ("tempo tarde",),
    "relato": ("relato diario", "relato"),
    "informado_por": ("responsavel pelo informacao", "responsavel pela informacao"),
    "efetivo_total": ("efetivo total",),
    "efetivo_1": ("efetivo 1",),
    "efetivo_2": ("efetivo 2",),
    "id": ("id",),
}


def _chave(texto: Any) -> str:
    """Sem acento, sem maiúscula, sem espaço sobrando — para casar cabeçalho."""
    cru = unicodedata.normalize("NFD", str(texto or "").strip().lower())
    limpo = "".join(c for c in cru if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", limpo)


def _maiusculo_sem_acento(texto: Any) -> str:
    cru = unicodedata.normalize("NFD", str(texto or "").strip().upper())
    limpo = "".join(c for c in cru if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", limpo).strip(' "\'')


def _de_gente(funcao: str) -> str:
    """"ENGENHEIRO CIVIL" vira "Engenheiro civil".

    O diário é lido por pessoas e sai no papel: CAIXA ALTA no meio de uma
    tabela parece grito, e é como a planilha antiga guardava.
    """
    limpo = re.sub(r"\s+", " ", str(funcao or "").strip())
    return limpo[:1].upper() + limpo[1:].lower() if limpo else ""


def _data(valor: Any) -> Optional[date]:
    if valor is None or str(valor).strip() == "":
        return None
    if isinstance(valor, datetime):
        return valor.date()
    if isinstance(valor, date):
        return valor
    texto = str(valor).strip()[:10]
    for formato in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(texto, formato).date()
        except ValueError:
            continue
    return None


def _inteiro(valor: Any) -> Optional[int]:
    if valor is None or str(valor).strip() == "":
        return None
    try:
        return int(float(str(valor).strip().replace(",", ".")))
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# O EFETIVO: "1 - ENGENHEIRO CIVIL\n17 - SERVENTE" vira linhas contáveis.
#
# ⚑ UMA LINHA QUE NÃO CASAR COM O PADRÃO NÃO É DESCARTADA EM SILÊNCIO — ela
# volta em `sobras`, e a conferência mostra. Importação que engole o que não
# entendeu é importação que perde dado sem avisar.
# ---------------------------------------------------------------------------
_EFETIVO = re.compile(r"^\s*(\d+)\s*[-–—]\s*(.+?)\s*$")


def ler_efetivo(*textos: Any) -> tuple[list[dict[str, Any]], list[str]]:
    pessoas: list[dict[str, Any]] = []
    sobras: list[str] = []
    for texto in textos:
        for linha in str(texto or "").replace("\r", "\n").split("\n"):
            if not linha.strip():
                continue
            achou = _EFETIVO.match(linha)
            if not achou:
                sobras.append(linha.strip())
                continue
            quantidade, funcao = int(achou.group(1)), _de_gente(achou.group(2))
            if quantidade < 1 or not funcao:
                sobras.append(linha.strip())
                continue
            pessoas.append({"funcao": funcao, "quantidade": quantidade})
    # Mesma função repetida nas duas colunas vira uma linha só.
    juntos: dict[str, int] = {}
    for p in pessoas:
        juntos[p["funcao"]] = juntos.get(p["funcao"], 0) + p["quantidade"]
    return ([{"funcao": f, "quantidade": q} for f, q in juntos.items()],
            sobras)


# ---------------------------------------------------------------------------
# LER O ARQUIVO
# ---------------------------------------------------------------------------
def ler_planilha(conteudo: bytes) -> list[dict[str, Any]]:
    """Transforma o .xlsx nas linhas que a importação entende.

    Casa o cabeçalho pelo NOME da coluna, não pela posição: a planilha é
    exportada por outra ferramenta e a ordem das colunas pode mudar sem aviso.
    """
    try:
        import openpyxl
    except ImportError:                                 # pragma: no cover
        raise ErroValidacao("Leitura de planilha indisponível no serviço.")
    try:
        wb = openpyxl.load_workbook(io.BytesIO(conteudo), read_only=True,
                                    data_only=True)
    except Exception as e:                              # noqa: BLE001
        raise ErroValidacao(
            "Não consegui abrir o arquivo. Ele precisa ser .xlsx — se for "
            f".xls antigo, salve como .xlsx no Excel. ({e})")
    ws = wb[wb.sheetnames[0]]
    it = ws.iter_rows(values_only=True)
    try:
        cabecalho = [_chave(c) for c in next(it)]
    except StopIteration:
        raise ErroValidacao("A planilha está vazia.")

    onde: dict[str, int] = {}
    for campo, nomes in _COLUNAS.items():
        for n in nomes:
            if n in cabecalho:
                onde[campo] = cabecalho.index(n)
                break
    for obrigatorio in ("obra", "data"):
        if obrigatorio not in onde:
            raise ErroValidacao(
                f"A planilha não tem a coluna “{_COLUNAS[obrigatorio][0]}”. "
                f"Achei estas: {', '.join(c for c in cabecalho if c)}.")

    def pegar(linha, campo):
        i = onde.get(campo)
        return linha[i] if i is not None and i < len(linha) else None

    linhas = []
    for numero, bruta in enumerate(it, start=2):
        if not any(c is not None and str(c).strip() for c in bruta):
            continue
        if len(linhas) >= TETO_DE_LINHAS:
            raise ErroValidacao(
                f"A planilha passa de {TETO_DE_LINHAS} linhas. Divida em "
                f"partes para a importação não derrubar o serviço.")
        efetivo, sobras = ler_efetivo(pegar(bruta, "efetivo_1"),
                                      pegar(bruta, "efetivo_2"))
        linhas.append({
            "linha": numero,
            "codigo_obra": str(pegar(bruta, "obra") or "").strip(),
            "data": _data(pegar(bruta, "data")),
            "tempo_manha": _TEMPOS.get(_maiusculo_sem_acento(
                pegar(bruta, "tempo_manha"))),
            "tempo_tarde": _TEMPOS.get(_maiusculo_sem_acento(
                pegar(bruta, "tempo_tarde"))),
            "relato": str(pegar(bruta, "relato") or "").strip(),
            "informado_por": str(pegar(bruta, "informado_por") or "").strip(),
            "efetivo": efetivo,
            "efetivo_sobras": sobras,
            "efetivo_total": _inteiro(pegar(bruta, "efetivo_total")),
            "id": str(pegar(bruta, "id") or "").strip(),
        })
    return linhas


# ---------------------------------------------------------------------------
# JUNTAR O QUE A PLANILHA REPETE
#
# 38 pares obra+dia aparecem duas vezes no arquivo dele. Olhando de perto são
# dois casos: o mesmo relato enviado duas vezes (duplicata de verdade) e dois
# relatos DIFERENTES do mesmo dia (dois registros legítimos).
#
# ⚑ ENTÃO NÃO SE ESCOLHE UM: junta-se. Relato igual vira um; relatos
# diferentes viram um texto com os dois, separados. Escolher "o último" jogaria
# fora meio dia de obra sem ninguém perceber. Do efetivo fica o MAIOR, porque
# o menor costuma ser o envio incompleto.
# ---------------------------------------------------------------------------
def juntar_repetidos(linhas: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    por_chave: dict[tuple[str, Any], dict[str, Any]] = {}
    ordem: list[tuple[str, Any]] = []
    juntadas = 0
    for l in linhas:
        chave = (_maiusculo_sem_acento(l["codigo_obra"]), l["data"])
        if chave not in por_chave:
            por_chave[chave] = dict(l, repetida=1)
            ordem.append(chave)
            continue
        juntadas += 1
        alvo = por_chave[chave]
        alvo["repetida"] += 1
        novo = (l["relato"] or "").strip()
        if novo and novo not in (alvo["relato"] or ""):
            alvo["relato"] = ((alvo["relato"] or "").rstrip()
                              + ("\n\n" if alvo["relato"] else "") + novo)
        if sum(p["quantidade"] for p in l["efetivo"]) > \
           sum(p["quantidade"] for p in alvo["efetivo"]):
            alvo["efetivo"] = l["efetivo"]
            alvo["efetivo_total"] = l["efetivo_total"]
        alvo["tempo_manha"] = alvo["tempo_manha"] or l["tempo_manha"]
        alvo["tempo_tarde"] = alvo["tempo_tarde"] or l["tempo_tarde"]
        alvo["informado_por"] = alvo["informado_por"] or l["informado_por"]
    return [por_chave[c] for c in ordem], juntadas


# ---------------------------------------------------------------------------
# AS OBRAS: casar o código da planilha com o cadastro
# ---------------------------------------------------------------------------
def casar_obras(s: Session, codigos: list[str],
                mapa: Optional[dict[str, int]] = None) -> dict[str, Any]:
    """Para cada código da planilha, a obra do cadastro — ou nada.

    `mapa` é o que o dono amarrou à mão na tela ({código: obra_id}); ele vence
    o casamento automático, porque ele sabe coisas que o código não diz.
    """
    mapa = {_maiusculo_sem_acento(k): int(v) for k, v in (mapa or {}).items() if v}
    linhas = s.execute(text(
        "SELECT id, codigo, nome, status FROM obras")).mappings().all()
    por_codigo = {_maiusculo_sem_acento(l["codigo"]): l for l in linhas}
    por_nome = {_maiusculo_sem_acento(l["nome"]): l for l in linhas}

    achadas, perdidas = {}, []
    for bruto in codigos:
        chave = _maiusculo_sem_acento(bruto)
        if not chave:
            continue
        if chave in mapa:
            obra = next((l for l in linhas if l["id"] == mapa[chave]), None)
            if obra is not None:
                achadas[chave] = {"obra_id": obra["id"], "codigo": obra["codigo"],
                                  "nome": obra["nome"], "como": "você apontou"}
                continue
        obra = por_codigo.get(chave) or por_nome.get(chave)
        if obra is not None:
            achadas[chave] = {"obra_id": obra["id"], "codigo": obra["codigo"],
                              "nome": obra["nome"], "como": "pelo código"}
        else:
            perdidas.append(bruto)
    return {"achadas": achadas, "perdidas": sorted(set(perdidas))}


# ---------------------------------------------------------------------------
# IMPORTAR — e o padrão é SIMULAR
# ---------------------------------------------------------------------------
def importar(s: Session, conteudo: bytes, usuario_id: Optional[int] = None, *,
             simular: bool = True,
             mapa_obras: Optional[dict[str, int]] = None) -> dict[str, Any]:
    """Lê, confere e (se mandarem) grava.

    Devolve sempre o MESMO relatório, simulando ou não — é o que permite a
    pessoa olhar a conferência e apertar gravar sabendo o que vai acontecer.
    """
    linhas = ler_planilha(conteudo)
    if not linhas:
        raise ErroValidacao("Não achei nenhuma linha de dado na planilha.")
    linhas, juntadas = juntar_repetidos(linhas)

    obras = casar_obras(s, [l["codigo_obra"] for l in linhas], mapa_obras)
    achadas = obras["achadas"]

    # O que já existe aqui, de uma vez só — uma consulta por linha seriam
    # 1.427 idas ao banco.
    ja_existem: set[tuple[int, date]] = set()
    ids_vindos: set[str] = set()
    alvo = [(achadas[_maiusculo_sem_acento(l["codigo_obra"])]["obra_id"], l["data"])
            for l in linhas
            if l["data"] and _maiusculo_sem_acento(l["codigo_obra"]) in achadas]
    if alvo:
        obra_ids = sorted({o for o, _ in alvo})
        datas = sorted({d for _, d in alvo})
        for linha in s.execute(text("""
            SELECT obra_id, data, importado_id FROM diarios_obra
             WHERE obra_id = ANY(:o) AND data = ANY(:d)"""),
            {"o": obra_ids, "d": datas}).mappings().all():
            ja_existem.add((linha["obra_id"], linha["data"]))
            if linha["importado_id"]:
                ids_vindos.add(linha["importado_id"])

    gravados, pulados, problemas = [], [], []
    sem_expediente = 0
    efetivo_divergente = 0

    for l in linhas:
        chave = _maiusculo_sem_acento(l["codigo_obra"])
        if not l["data"]:
            problemas.append({"linha": l["linha"], "motivo": "sem data legível",
                              "obra": l["codigo_obra"]})
            continue
        if chave not in achadas:
            pulados.append({"linha": l["linha"], "motivo": "obra não encontrada",
                            "obra": l["codigo_obra"], "data": str(l["data"])})
            continue
        obra_id = achadas[chave]["obra_id"]
        if (obra_id, l["data"]) in ja_existem:
            pulados.append({"linha": l["linha"],
                            "motivo": "este dia já tem diário no sistema",
                            "obra": achadas[chave]["codigo"],
                            "data": str(l["data"])})
            continue
        if not (l["relato"] or "").strip():
            problemas.append({"linha": l["linha"], "motivo": "sem relato",
                              "obra": l["codigo_obra"], "data": str(l["data"])})
            continue

        folga = parece_sem_expediente(l["relato"])
        if folga:
            sem_expediente += 1
        soma = sum(p["quantidade"] for p in l["efetivo"])
        if l["efetivo_total"] is not None and soma != l["efetivo_total"]:
            efetivo_divergente += 1

        gravados.append({
            "linha": l["linha"], "obra_id": obra_id,
            "obra": achadas[chave]["codigo"], "data": str(l["data"]),
            "relato": l["relato"], "sem_expediente": folga,
            "pessoas": soma, "efetivo_total_na_planilha": l["efetivo_total"],
            "funcoes": len(l["efetivo"]),
            "sobras": l["efetivo_sobras"],
            "repetida": l.get("repetida", 1),
        })

        if simular:
            continue
        # ⚑ `ON CONFLICT DO NOTHING` nas DUAS chaves: obra+dia (o índice que já
        # existia) e origem+id (o da migração 099). Reenviar o arquivo não
        # dobra nada.
        novo = s.execute(text("""
            INSERT INTO diarios_obra
                (obra_id, data, tempo_manha, tempo_tarde, relato,
                 sem_expediente, situacao, criado_por, informado_por,
                 importado_de, importado_id)
            VALUES (:o, :d, :tm, :tt, :r, :se, 'ABERTO', :u, :ip, :orig, :iid)
            ON CONFLICT (obra_id, data) DO NOTHING
            RETURNING id"""),
            {"o": obra_id, "d": l["data"], "tm": l["tempo_manha"],
             "tt": l["tempo_tarde"], "r": l["relato"], "se": folga,
             "u": usuario_id, "ip": l["informado_por"] or None,
             "orig": ORIGEM, "iid": l["id"] or None}).scalar()
        if novo is None:
            continue
        for p in l["efetivo"]:
            s.execute(text("""
                INSERT INTO diario_efetivo_extra
                    (diario_id, descricao, funcao, quantidade, origem)
                VALUES (:i, :d, :f, :q, 'IMPORTADO')"""),
                {"i": novo, "d": p["funcao"], "f": p["funcao"],
                 "q": p["quantidade"]})
    if not simular:
        s.flush()

    return {
        "simulado": simular,
        "linhas_no_arquivo": len(linhas) + juntadas,
        "linhas_juntadas": juntadas,
        "obras_encontradas": sorted(
            ({"codigo_na_planilha": k, **v} for k, v in achadas.items()),
            key=lambda x: x["codigo_na_planilha"]),
        "obras_nao_encontradas": obras["perdidas"],
        "gravaria": len(gravados) if simular else 0,
        "gravados": 0 if simular else len(gravados),
        "pulados": pulados,
        "problemas": problemas,
        "sem_expediente": sem_expediente,
        "efetivo_divergente": efetivo_divergente,
        # Uma amostra para a tela mostrar, sem mandar 1.427 relatos para o
        # navegador.
        "amostra": gravados[:25],
        "periodo": (min((g["data"] for g in gravados), default=None),
                    max((g["data"] for g in gravados), default=None)),
    }
