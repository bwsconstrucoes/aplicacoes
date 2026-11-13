# -*- coding: utf-8 -*-
"""
ERP — COMPLETAR O CADASTRO DAS OBRAS COM A C. DIÁRIOS (13/11/2026).

O PEDIDO, e o engano dentro dele. Ele, olhando o diário:

    *"Eu não sei porque a obra — a gente cadastrou as obras, importou lá na
    planilha C. Diários, tem todas as informações das obras — mas não está
    trazendo aqui para o cabeçalho do diário."*

⚑ O CABEÇALHO NÃO ESTAVA ERRADO. Ele lê o CADASTRO DA OBRA (`diario.cabecalho`)
e mostra o que achar; achando vazio, mostra travessão. O que estava vazio era o
cadastro:

1. A planilha de DIÁRIOS que ele mandou (1.427 linhas) TEM as colunas Contrato,
   Objeto, Endereço, Município, Ordem de Serviço, Responsável Técnico, Empresa,
   Data de Início e Data de Término — e as 1.427 linhas vêm com todas elas
   VAZIAS. Conferido coluna a coluna. Não era de lá que o dado viria.
2. A importação de obras por CSV (`planilhas.py`) só recebe o que o arquivo
   trouxer, e o arquivo que criou as obras trazia pouco mais que código e nome.
3. A C. DIÁRIOS, essa sim, tem os dados — e o monorepo já a lê há meses, na
   emissão de NFS-e (`app/apps/emissaonf/cdiarios.py`, aba "Centro de Custo").
   O ERP é que nunca tinha olhado para ela.

Então é daqui que o cabeçalho se completa.

O QUE A C. DIÁRIOS DÁ, E O QUE ELA NÃO DÁ
-----------------------------------------
DÁ: contrato, objeto, município, UF, CNO, cliente, CNPJ do cliente, valor do
contrato, alíquota de ISS e tributação.

NÃO DÁ, e por isso continuam a ser digitados no cadastro da obra: responsável
técnico, CREA, ART, ordem de serviço e sua data, início e término da obra.
Esses cinco é que fazem o quadro de PRAZO do diário aparecer — sem data de
início não há "dia 37 de 180". O relatório diz isso em voz alta, para ninguém
importar e achar que acabou.

⚑ TRÊS REGRAS, e valem mais que o código:

1. **NÃO CRIA OBRA.** Código que não existe no ERP é devolvido na conferência.
   A planilha tem obras de anos, e encher o cadastro de duplicata para depois
   limpar à mão é trabalho que não acaba.
2. **NÃO SOBRESCREVE O QUE JÁ ESTÁ PREENCHIDO** — a não ser que mandem
   (`regravar=True`). O que alguém digitou aqui dentro vale mais que a
   planilha; quando os dois divergem, o relatório MOSTRA os dois lados.
3. **CONFERE ANTES DE GRAVAR**, como todas as outras importações da casa.
"""
from __future__ import annotations

import io
import logging
import re
import unicodedata
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao

logger = logging.getLogger(__name__)

TETO_DE_LINHAS = 5000

# O que a aba "Centro de Custo" chama cada coisa → o campo do cadastro.
# Casado pelo NOME do cabeçalho, nunca pela posição: a coluna de hoje vira
# outra amanhã, e foi assim que a emissão de NFS-e aprendeu a ler esta mesma
# planilha.
_COLUNAS = {
    "codigo": ("codigo primario", "código primário", "codigo", "centro de custo"),
    "contrato": ("contrato",),
    "objeto": ("objeto",),
    "municipio": ("municipio", "município"),
    "uf": ("uf",),
    "cno": ("cno",),
    "cliente": ("cliente",),
    "cnpj_cliente": ("cnpj cliente",),
    "valor_contrato": ("valor",),
    "aliquota_iss": ("aliquota iss", "alíquota iss"),
    "tributacao": ("tributacao", "tributação"),
}

# ⚑ COLUNAS QUE EXISTEM E NÃO SÃO APROVEITADAS — ditas, nunca caladas.
#
# "Endereço Cliente" é o endereço DO CLIENTE, não o da obra. No diário, o
# campo Endereço é onde o serviço acontece; pôr ali a sede da prefeitura seria
# escrever mentira num documento que a fiscalização assina. Fica de fora até o
# dono dizer de onde vem o endereço da obra.
NAO_APROVEITADAS = {
    "endereco cliente": "é o endereço do cliente, não o da obra — no diário o "
                        "Endereço é o lugar onde o serviço acontece",
    "empresa": "a empresa que executa é um cadastro à parte aqui (CNPJ), e "
               "casar por nome escrito à mão trocaria obra de empresa",
    "conta de pagamento": "pagamento não entra no cadastro da obra",
    "codigo omie": "o código do departamento no OMIE já vem da importação de "
                   "obras e é chave única — mexer nele desalinha o rateio",
}

# O que NENHUMA planilha traz, e o diário precisa. Vai no relatório.
SO_NO_CADASTRO = [
    ("responsavel_tecnico", "Responsável técnico"),
    ("crea_obra", "CREA"),
    ("art_rrt", "ART/RRT"),
    ("ordem_servico", "Ordem de serviço"),
    ("data_ordem_servico", "Data da ordem de serviço"),
    ("data_inicio", "Início da obra"),
    ("data_termino", "Término previsto"),
]

_TEXTOS = ("contrato", "objeto", "municipio", "uf", "cno", "cliente",
           "cnpj_cliente", "tributacao")
_NUMEROS = ("valor_contrato", "aliquota_iss")

ROTULOS = {
    "contrato": "Contrato", "objeto": "Objeto", "municipio": "Município",
    "uf": "UF", "cno": "CNO", "cliente": "Cliente",
    "cnpj_cliente": "CNPJ do cliente", "valor_contrato": "Valor do contrato",
    "aliquota_iss": "Alíquota de ISS", "tributacao": "Tributação",
}


def _chave(texto: Any) -> str:
    cru = unicodedata.normalize("NFD", str(texto or "").strip().lower())
    limpo = "".join(c for c in cru if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", limpo)


def _codigo(texto: Any) -> str:
    """O código como se compara: sem acento, sem espaço, tudo maiúsculo."""
    return re.sub(r"\s+", "", _chave(texto)).upper()


def _numero(valor: Any) -> Optional[Decimal]:
    """"R$ 1.234.567,89" e "5,00%" viram número. O que não der, vira nada.

    Nunca levanta erro: uma célula escrita torta não pode derrubar a leitura
    de 300 obras — ela vira um campo não preenchido, que o relatório mostra.
    """
    if valor is None or isinstance(valor, bool):
        return None
    if isinstance(valor, (int, float, Decimal)):
        return Decimal(str(valor))
    bruto = re.sub(r"[^\d,.\-]", "", str(valor))
    if not bruto or bruto in ("-", ".", ","):
        return None
    # 1.234.567,89 (nosso) × 1,234,567.89 (deles): manda a ÚLTIMA pontuação.
    if "," in bruto and "." in bruto:
        if bruto.rfind(",") > bruto.rfind("."):
            bruto = bruto.replace(".", "").replace(",", ".")
        else:
            bruto = bruto.replace(",", "")
    elif "," in bruto:
        bruto = bruto.replace(",", ".")
    try:
        return Decimal(bruto)
    except InvalidOperation:
        return None


def _municipio_e_uf(municipio: str, uf: str) -> tuple[str, str]:
    """"Exu-PE" vira ("Exu", "PE"). A coluna UF, quando vem, manda.

    A C. Diários escreve o município com a UF colada; o cadastro guarda os
    dois separados, porque é assim que o papel do diário imprime.
    """
    nome = str(municipio or "").strip()
    sigla = str(uf or "").strip().upper()[:2]
    achado = re.match(r"^(.*?)[\s]*[-/][\s]*([A-Za-z]{2})$", nome)
    if achado:
        nome = achado.group(1).strip()
        sigla = sigla or achado.group(2).upper()
    return nome, sigla


def ler_planilha(conteudo: bytes) -> list[dict[str, Any]]:
    """O .xlsx exportado da C. Diários vira as linhas que a importação entende."""
    try:
        import openpyxl
    except ImportError:                                 # pragma: no cover
        raise ErroValidacao("Leitura de planilha indisponível no serviço.")
    try:
        wb = openpyxl.load_workbook(io.BytesIO(conteudo), read_only=True,
                                    data_only=True)
    except Exception as e:                              # noqa: BLE001
        raise ErroValidacao(
            "Não consegui abrir o arquivo. Ele precisa ser .xlsx — se estiver "
            f"no Google Planilhas, baixe como Excel (.xlsx). ({e})")
    # A aba certa é a "Centro de Custo"; não tendo esse nome, vale a primeira.
    nome_aba = next((n for n in wb.sheetnames
                     if _chave(n) == "centro de custo"), wb.sheetnames[0])
    ws = wb[nome_aba]
    it = ws.iter_rows(values_only=True)
    try:
        bruto_cab = list(next(it))
    except StopIteration:
        raise ErroValidacao("A planilha está vazia.")
    # Os dois: o normalizado para CASAR, e o original para a mensagem de erro
    # — quem lê "achei estas: contrato, objeto" tem de reconhecer a própria
    # planilha, e não uma versão minúscula e sem acento dela.
    cabecalho = [_chave(c) for c in bruto_cab]
    como_escrito = [str(c or "").strip() for c in bruto_cab]

    onde: dict[str, int] = {}
    for campo, nomes in _COLUNAS.items():
        for n in nomes:
            if _chave(n) in cabecalho:
                onde[campo] = cabecalho.index(_chave(n))
                break
    if "codigo" not in onde:
        raise ErroValidacao(
            "A planilha não tem a coluna “Código Primário” (nem “Centro de "
            f"Custo”). Achei estas: {', '.join(c for c in como_escrito if c)}.")

    def pegar(linha, campo):
        i = onde.get(campo)
        return linha[i] if i is not None and i < len(linha) else None

    linhas = []
    for numero, bruta in enumerate(it, start=2):
        if not any(c is not None and str(c).strip() for c in bruta):
            continue
        if len(linhas) >= TETO_DE_LINHAS:
            raise ErroValidacao(
                f"A planilha passa de {TETO_DE_LINHAS} linhas. Divida em partes.")
        # A primeira coluna guarda o OUTRO código da mesma obra — por posição,
        # como na emissão de NFS-e, porque o rótulo dela varia.
        codigo_2 = str(bruta[0] or "").strip() if bruta else ""
        municipio, uf = _municipio_e_uf(pegar(bruta, "municipio"),
                                        pegar(bruta, "uf"))
        linha: dict[str, Any] = {
            "linha": numero,
            "codigo": str(pegar(bruta, "codigo") or "").strip(),
            "codigo_2": codigo_2,
            "municipio": municipio,
            "uf": uf,
        }
        for campo in _TEXTOS:
            if campo in ("municipio", "uf"):
                continue
            linha[campo] = str(pegar(bruta, campo) or "").strip()
        for campo in _NUMEROS:
            linha[campo] = _numero(pegar(bruta, campo))
        if not linha["codigo"] and not linha["codigo_2"]:
            continue
        linhas.append(linha)
    return linhas


def colunas_ignoradas(conteudo: bytes) -> list[dict[str, str]]:
    """As colunas que existem no arquivo e NÃO são usadas — com o motivo."""
    try:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(conteudo), read_only=True,
                                    data_only=True)
    except Exception:                                   # noqa: BLE001
        return []
    nome_aba = next((n for n in wb.sheetnames
                     if _chave(n) == "centro de custo"), wb.sheetnames[0])
    try:
        cabecalho = [_chave(c) for c in next(wb[nome_aba].iter_rows(values_only=True))]
    except StopIteration:
        return []
    usadas = {_chave(n) for nomes in _COLUNAS.values() for n in nomes}
    saida = []
    for i, c in enumerate(cabecalho):
        # ⚑ A PRIMEIRA COLUNA É USADA, por POSIÇÃO: é o segundo código da obra
        # (o rótulo dela varia, o lugar não). Listá-la como "não aproveitada"
        # era dizer que eu ignoro justamente o que me faz achar a obra.
        if not c or c in usadas or i == 0:
            continue
        saida.append({"coluna": c,
                      "motivo": NAO_APROVEITADAS.get(c, "não tem par no cadastro")})
    return saida


def _obras_do_erp(s: Session) -> list[dict[str, Any]]:
    # Os campos que a planilha preenche E os que só o cadastro tem: estes
    # últimos entram para o relatório poder dizer quantas obras seguem sem
    # responsável técnico, sem ART, sem data de início.
    campos = ", ".join(["id", "codigo", "nome"] + list(ROTULOS)
                       + [c for c, _ in SO_NO_CADASTRO])
    return [dict(l) for l in s.execute(text(
        f"SELECT {campos} FROM obras")).mappings().all()]


def _vazio(valor: Any) -> bool:
    return valor is None or (isinstance(valor, str) and not valor.strip())


def _mesmo(a: Any, b: Any) -> bool:
    if isinstance(a, Decimal) or isinstance(b, Decimal):
        na, nb = _numero(a), _numero(b)
        return na is not None and nb is not None and na == nb
    return _chave(a) == _chave(b)


def _como_se_le(campo: str, valor: Any) -> str:
    """O valor como aparece na conferência — não como o banco o guarda.

    "2500000.00" é um número de banco de dados. Quem confere uma importação de
    300 obras precisa bater o olho e reconhecer "R$ 2.500.000,00".
    """
    if valor is None:
        return ""
    if campo == "valor_contrato":
        inteiro, _, centavos = f"{Decimal(valor):.2f}".partition(".")
        sinal = "-" if inteiro.startswith("-") else ""
        inteiro = inteiro.lstrip("-")
        com_ponto = ""
        while len(inteiro) > 3:
            com_ponto = "." + inteiro[-3:] + com_ponto
            inteiro = inteiro[:-3]
        return f"R$ {sinal}{inteiro}{com_ponto},{centavos}"
    if campo == "aliquota_iss":
        return f"{Decimal(valor):.2f}".replace(".", ",") + "%"
    return str(valor)


def importar(s: Session, conteudo: bytes, usuario_id: Optional[int] = None, *,
             simular: bool = True, regravar: bool = False) -> dict[str, Any]:
    """Lê a C. Diários, confere e (se mandarem) completa o cadastro das obras."""
    linhas = ler_planilha(conteudo)
    if not linhas:
        raise ErroValidacao("Não achei nenhuma obra na planilha.")

    obras = _obras_do_erp(s)
    por_codigo: dict[str, dict[str, Any]] = {}
    for o in obras:
        por_codigo.setdefault(_codigo(o["codigo"]), o)
        por_codigo.setdefault(_codigo(o["nome"]), o)

    preenchidas, conflitos, nao_encontradas = [], [], []
    ja_completas = 0
    vistas: set[int] = set()

    for l in linhas:
        obra = (por_codigo.get(_codigo(l["codigo"]))
                or por_codigo.get(_codigo(l["codigo_2"])))
        if obra is None:
            nao_encontradas.append(l["codigo"] or l["codigo_2"])
            continue
        if obra["id"] in vistas:
            # A planilha repete a obra em mais de uma linha (os dois códigos).
            # A primeira linha manda; as outras não desfazem o que ela fez.
            continue
        vistas.add(obra["id"])

        novos: dict[str, Any] = {}
        for campo in ROTULOS:
            valor = l.get(campo)
            if _vazio(valor):
                continue
            atual = obra.get(campo)
            if _vazio(atual):
                novos[campo] = valor
            elif not _mesmo(atual, valor):
                conflitos.append({
                    "obra": obra["codigo"], "campo": campo,
                    "rotulo": ROTULOS[campo],
                    "no_erp": _como_se_le(campo, atual),
                    "na_planilha": _como_se_le(campo, valor),
                    "regravado": bool(regravar)})
                if regravar:
                    novos[campo] = valor
        if not novos:
            ja_completas += 1
            continue

        preenchidas.append({
            "obra_id": obra["id"], "obra": obra["codigo"], "nome": obra["nome"],
            "campos": {ROTULOS[k]: _como_se_le(k, v) for k, v in novos.items()}})
        if simular:
            continue
        pedacos = ", ".join(f"{k} = :{k}" for k in novos)
        s.execute(text(f"UPDATE obras SET {pedacos} WHERE id = :id"),
                  {**novos, "id": obra["id"]})
    if not simular:
        s.flush()
        logger.info("ERP/obras: C. Diários completou %d obra(s) (usuário %s)",
                    len(preenchidas), usuario_id)

    # O que o diário continua sem ter, e de onde vem. Isto é metade do valor
    # do relatório: importar e achar que acabou é o jeito de o cabeçalho
    # seguir pela metade sem ninguém entender por quê.
    faltando = []
    for campo, rotulo in SO_NO_CADASTRO:
        sem = [o["codigo"] for o in obras if _vazio(o.get(campo))]
        faltando.append({"campo": campo, "rotulo": rotulo,
                         "obras_sem": len(sem), "exemplos": sorted(sem)[:8]})

    return {
        "simulado": simular,
        "linhas_no_arquivo": len(linhas),
        "obras_no_erp": len(obras),
        "completaria": len(preenchidas) if simular else 0,
        "completadas": 0 if simular else len(preenchidas),
        "ja_estavam_completas": ja_completas,
        "nao_encontradas": sorted(set(c for c in nao_encontradas if c)),
        "preenchidas": preenchidas[:50],
        "conflitos": conflitos[:50],
        "conflitos_total": len(conflitos),
        "colunas_ignoradas": colunas_ignoradas(conteudo),
        "so_no_cadastro": faltando,
    }
