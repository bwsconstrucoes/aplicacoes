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

from app.apps.erp.core.cadastros import obras as svc_obras
from app.apps.erp.core.comum.auditoria import ErroValidacao

logger = logging.getLogger(__name__)

TETO_DE_LINHAS = 5000

# O que a aba "Centro de Custo" chama cada coisa → o campo do cadastro.
# Casado pelo NOME do cabeçalho, nunca pela posição: a coluna de hoje vira
# outra amanhã, e foi assim que a emissão de NFS-e aprendeu a ler esta mesma
# planilha.
_COLUNAS = {
    "codigo": ("codigo primario", "código primário", "codigo", "centro de custo"),
    # ⚑ O NÚMERO DA CONTABILIDADE (16/11/2026). Ele: *"em relação a setor, eu
    # criei, está na planilha C. Diários, tem a coluna Centro de Custo da
    # Contabilidade. E é exatamente esse setor aí."*
    #
    # Esta coluna já existia na planilha e era LIDA E DESCARTADA, por não ter
    # onde morar no cadastro. Agora ela é a ponte entre o nome que a BWS dá à
    # obra ("094") e o que a contabilidade dá ("164.01") — e é por ela que a
    # ficha de registro do Fortes acha a obra de cada pessoa.
    #
    # ⚠️ O CASAMENTO É EXATO (o cabeçalho tem de ser igual ao alias), e é por
    # isso que ela não colide com "centro de custo" ali em cima, que é outro
    # nome do código primário. São duas colunas diferentes com nome parecido —
    # exatamente o tipo de coisa que casamento "por pedaço do nome" trocaria.
    "centro_custo_contabil": ("centro de custo da contabilidade",
                              "centro de custo contabilidade",
                              "centro de custo contabil",
                              "centro de custo contábil"),
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
    # ⚑ O QUE ENTROU EM 24/11/2026, e por quê. Ele: *"os dados reais e
    # atualizados estão lá (…) qualquer outro dado ele deve ser atualizado
    # para o ERP (…) lá o trabalho manual já foi feito. Eu quero só replicar
    # ele pro ERP. Porque senão o ERP vai nascer desatualizado."*
    #
    # ⚠️ CINCO DESTES ESTAVAM ESCRITOS AQUI COMO "a planilha não dá" —
    # responsável técnico, ART, ordem de serviço, início e término. A frase
    # vinha da planilha de DIÁRIOS (a de 1.427 linhas), onde essas colunas
    # vêm vazias mesmo. Na C. DIÁRIOS elas existem e estão preenchidas. Eram
    # justamente as que fazem o quadro de PRAZO do diário aparecer.
    "endereco": ("endereco", "endereço"),
    "data_inicio": ("data de inicio", "data de início"),
    "data_termino": ("data de termino", "data de término"),
    "ordem_servico": ("ordem de servico", "ordem de serviço"),
    "responsavel_tecnico": ("responsavel tecnico", "responsável técnico"),
    "engenheiro_fiscal": ("fiscalizacao", "fiscalização"),
    "vigencia_inicio": ("data de inicio vigencia", "data de início vigência"),
    "vigencia_fim": ("data de termino vigencia", "data de término vigência"),
    "art_rrt": ("no art", "nº art", "n art", "art"),
    "orgao_resumido": ("orgao resumido", "órgão resumido"),
    "data_base_orcamento": ("data base p/ reajuste", "data base para reajuste",
                            "data base reajuste"),
    # A FASE DA OBRA. Vale o mapa de `FASES_DA_PLANILHA`, logo abaixo — o
    # texto da planilha não é o nome que o ERP usa.
    "fase_texto": ("status da obra", "status", "fase"),
    # "-3.7319, -38.5267" numa célula só; o cadastro guarda em dois campos.
    "coordenadas": ("coordenadas geograficas", "coordenadas geográficas",
                    "coordenadas"),
}

# ⚑ O QUE A PLANILHA ESCREVE → A FASE DO ERP (24/11/2026).
#
# Os oito valores foram CONTADOS na planilha de verdade, não supostos:
# Em Execução (80), Concluída (50), Acervo Técnico (31), Concluída com Dívida
# (14), Distratada (3), Criação Centros (3), Especial (1), Aguardando O.S (1).
#
# ⚑ "ESPECIAL" VIRA CONCLUÍDA — decisão dele, em 24/11/2026, depois de ver a
# pergunta na tela: *"Especial tratar como obra Concluída tb, que não aparece
# mais diário."* É 1 obra. O que ele quer é que ela saia do diário, e
# "Concluída" é a fase que faz isso.
#
# ⚠️ E TRÊS FASES DO ERP NUNCA VÊM DAQUI — Paralisada, Recebimento provisório
# e Recebimento definitivo não existem na planilha. Quem usar essas fases
# marca na mão, e a sincronização não as desfaz sem valor equivalente.
FASES_DA_PLANILHA = {
    "em execucao": "EM_EXECUCAO",
    "concluida": "CONCLUIDA",
    "concluida com divida": "CONCLUIDA_COM_DIVIDA",
    "acervo tecnico": "ACERVO_TECNICO",
    "distratada": "DISTRATADA",
    "criacao centros": "CRIACAO",
    "aguardando o.s": "AGUARDANDO_OS",
    "aguardando os": "AGUARDANDO_OS",
    "aguardando o.s.": "AGUARDANDO_OS",
    "especial": "CONCLUIDA",
    "paralisada": "PARALISADA",
    "recebimento provisorio": "RECEBIMENTO_PROVISORIO",
    "recebimento definitivo": "RECEBIMENTO_DEFINITIVO",
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

# O que a C. Diários NÃO traz, e o diário precisa. Vai no relatório.
#
# ⚠️ ENCOLHEU EM 24/11/2026. Cinco itens saíram daqui — responsável técnico,
# ART, ordem de serviço, início e término — porque estão na C. Diários e
# agora são lidos. A frase "nenhuma planilha traz" vinha da planilha de
# DIÁRIOS, onde vêm vazios mesmo; nunca tinha sido conferida contra esta.
SO_NO_CADASTRO = [
    ("crea_obra", "CREA"),
    ("data_ordem_servico", "Data da ordem de serviço"),
]

_TEXTOS = ("contrato", "objeto", "municipio", "uf", "cno", "cliente",
           "cnpj_cliente", "tributacao", "centro_custo_contabil",
           "endereco", "ordem_servico", "responsavel_tecnico",
           "engenheiro_fiscal", "art_rrt", "orgao_resumido")
_NUMEROS = ("valor_contrato", "aliquota_iss", "latitude", "longitude")
_DATAS = ("data_inicio", "data_termino", "vigencia_inicio", "vigencia_fim",
          "data_base_orcamento")

ROTULOS = {
    "contrato": "Contrato", "objeto": "Objeto", "municipio": "Município",
    "uf": "UF", "cno": "CNO", "cliente": "Cliente",
    "cnpj_cliente": "CNPJ do cliente", "valor_contrato": "Valor do contrato",
    "aliquota_iss": "Alíquota de ISS", "tributacao": "Tributação",
    "centro_custo_contabil": "Centro de custo da contabilidade",
    "endereco": "Endereço da obra",
    "data_inicio": "Início da obra", "data_termino": "Término previsto",
    "ordem_servico": "Ordem de serviço",
    "responsavel_tecnico": "Responsável técnico",
    "engenheiro_fiscal": "Fiscalização",
    "vigencia_inicio": "Início da vigência",
    "vigencia_fim": "Fim da vigência",
    "art_rrt": "ART/RRT", "orgao_resumido": "Órgão resumido",
    "data_base_orcamento": "Data base para reajuste",
    "latitude": "Latitude", "longitude": "Longitude",
}


# ⚠️ OS NOMES QUE A ABA PODE TER — e isto é CONSERTO de 24/11/2026.
#
# A aba se chama **"C. Diários"** na planilha de verdade. A busca aqui aceitava
# só "Centro de Custo" e, não achando, **caía na PRIMEIRA aba do arquivo** —
# que no arquivo baixado inteiro do Google é `ClientesOmie`. Resultado: a
# importação lia a aba errada, não casava obra nenhuma e devolvia "nenhuma obra
# encontrada" sem ninguém entender por quê. Foi o que deixou o cadastro das
# obras pela metade.
#
# A lista é a MESMA que a emissão de NFS-e já tolerava há meses
# (`app/apps/emissaonf/worker.py`, `ABA_CDIARIOS`) — lá o nome certo estava, e
# aqui não. Dois lugares lendo a mesma planilha com régua diferente.
#
# ⚑ E NÃO CAI MAIS NA PRIMEIRA ABA: não achando nenhum destes nomes, a leitura
# RECUSA e diz quais abas viu. Adivinhar a aba é como se lê a planilha errada
# calado.
ABA_DA_PLANILHA = ("centro de custo", "centro de custos", "c. diarios",
                   "c.diarios", "c diarios", "cdiarios")


def _e_a_aba_certa(nome: str) -> bool:
    return _chave(nome) in ABA_DA_PLANILHA

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


def _data(valor: Any):
    """A célula de data vira `date`. O que não der, vira nada.

    A planilha mistura os dois jeitos: célula de data de verdade (o openpyxl
    devolve `datetime`) e texto "12/09/2026". Nunca levanta erro — data
    escrita torta vira campo não preenchido, que o relatório mostra, e não
    derruba a leitura das outras 182 obras.

    ⚠️ DIA/MÊS, NUNCA MÊS/DIA: "03/04/2026" é 3 de abril. Lido ao contrário,
    o diário imprimiria "dia 37 de 180" errado e ninguém desconfiaria.
    """
    from datetime import date as _d, datetime as _dt
    if valor is None or isinstance(valor, bool):
        return None
    if isinstance(valor, _dt):
        return valor.date()
    if isinstance(valor, _d):
        return valor
    texto = str(valor).strip()
    if not texto:
        return None
    for forma in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%y"):
        try:
            return _dt.strptime(texto[:10], forma).date()
        except ValueError:
            continue
    return None


def _coordenadas(valor: Any) -> tuple[Optional[Decimal], Optional[Decimal]]:
    """"-3,7319, -38,5267" vira (latitude, longitude).

    A planilha guarda as duas numa célula só. Vem com vírgula decimal ou com
    ponto, com e sem espaço. Qualquer coisa fora do par de números vira nada:
    coordenada errada põe a obra no meio do oceano no mapa.
    """
    texto = str(valor or "").strip()
    if not texto:
        return None, None
    achados = re.findall(r"-?\d+[.,]?\d*", texto)
    if len(achados) != 2:
        return None, None
    try:
        lat = Decimal(achados[0].replace(",", "."))
        lon = Decimal(achados[1].replace(",", "."))
    except InvalidOperation:
        return None, None
    # Fora do planeta não entra: é erro de digitação, não coordenada.
    if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
        return None, None
    return lat, lon


def _fase_da_planilha(valor: Any) -> Optional[str]:
    """"Em Execução" vira EM_EXECUCAO. O que não estiver no mapa vira nada.

    Devolver `None` é a decisão: a obra fica com a fase que já tem e o
    relatório avisa. Chutar uma fase mexe no que aparece no diário.
    """
    return FASES_DA_PLANILHA.get(_chave(valor))


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
    nome_aba = next((n for n in wb.sheetnames if _e_a_aba_certa(n)), None)
    if nome_aba is None:
        raise ErroValidacao(
            "Não achei a aba da C. Diários neste arquivo. Procuro por "
            "“C. Diários” ou “Centro de Custo”. As abas deste arquivo são: "
            + ", ".join(wb.sheetnames) + ".")
    ws = wb[nome_aba]
    it = ws.iter_rows(values_only=True)
    try:
        bruto_cab = list(next(it))
    except StopIteration:
        raise ErroValidacao("A planilha está vazia.")
    # Os dois: o normalizado para CASAR, e o original para a mensagem de erro
    # — quem lê "achei estas: contrato, objeto" tem de reconhecer a própria
    # planilha, e não uma versão minúscula e sem acento dela.
    return _entender(bruto_cab, list(it))


def _entender(bruto_cab, brutas) -> list[dict[str, Any]]:
    """Do cabeçalho e das linhas CRUAS para as linhas que a importação usa.

    ⚑ MORA SEPARADO DESDE 24/11/2026, quando a leitura passou a ter duas
    fontes: o arquivo .xlsx e a planilha ONLINE. Ele, com razão: *"pra que eu
    vou fazer download de planilha se a planilha é online? Qual o sentido de
    eu ficar fazendo download para fazer importação?"*

    As duas fontes entregam a mesma coisa — um cabeçalho e um monte de
    linhas. Tudo o que vem depois (casar coluna, ler data, partir
    coordenada, traduzir a fase) é regra, e regra fica num lugar só. Duas
    cópias divergiriam no primeiro ajuste.
    """
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
    for numero, bruta in enumerate(brutas, start=2):
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
            if campo in ("latitude", "longitude"):
                continue                       # vêm juntas, da mesma célula
            linha[campo] = _numero(pegar(bruta, campo))
        for campo in _DATAS:
            linha[campo] = _data(pegar(bruta, campo))
        linha["latitude"], linha["longitude"] = _coordenadas(
            pegar(bruta, "coordenadas"))
        # A fase viaja nos dois formatos: o do ERP (para gravar) e o texto
        # cru (para o relatório poder dizer "não conheço 'Especial'").
        linha["fase_texto"] = str(pegar(bruta, "fase_texto") or "").strip()
        linha["fase"] = _fase_da_planilha(linha["fase_texto"])
        if not linha["codigo"] and not linha["codigo_2"]:
            continue
        linhas.append(linha)
    return linhas


# ===========================================================================
# LER A PLANILHA ONLINE — sem download, sem upload
#
# Ele, em 24/11/2026: *"pra que eu vou fazer download de planilha se a
# planilha é online? Qual o sentido de eu ficar fazendo download para fazer
# importação? Por que tu não atualiza direto a planilha que está online?"*
#
# Razão nenhuma. A emissão de NFS-e lê ESTA MESMA planilha ao vivo há meses,
# e a importação de pessoas do DP também (`planilha_dp.carregar`). Só esta
# pedia arquivo.
#
# ⚑ A CREDENCIAL É A QUE JÁ EXISTE (`GOOGLE_CREDENTIALS_BASE64`), pela mesma
# porta que o resto do monorepo usa. Não se cria variável nova para
# credencial.
#
# ⚑ O ERRO VOLTA POR EXTENSO, nunca só no log. É a lição de 17/11/2026, que
# custou uma rodada dele: *"Meu, não tá funcionando. Você precisa me mostrar
# o que tá acontecendo."* Log é para quem abre o Render; ele abre a TELA.
#
# CUSTO: UMA leitura da planilha por importação — não uma por obra. Vai na
# cota do Google que é dividida com o painel, o Análise de SPs e a emissão
# de nota.
# ===========================================================================
import os

# O id da planilha "Bases de Dados Pipefy", onde mora a aba C. Diários. É a
# mesma que a emissão de NFS-e abre (`app/apps/emissaonf/worker.py`).
PLANILHA_ID = os.getenv("ERP_CDIARIOS_SHEET_ID",
                        "1C7MWQmr5uFGWuJ18osUNDapiojVXzQ_GxMMDQqxPsBk")


def ler_online(planilha_id: str = "") -> dict[str, Any]:
    """A aba C. Diários lida direto do Google.

    Devolve `{"linhas": [...], "aba": ..., "planilha": ..., "erro": ...}` —
    o erro vem preenchido quando não deu, para a tela poder mostrar.
    """
    chave = planilha_id or PLANILHA_ID
    try:
        from app.apps.analisesps.credenciais import cliente, com_retry
        livro = com_retry(lambda: cliente().open_by_key(chave))
        nome_aba = next((w.title for w in com_retry(lambda: livro.worksheets())
                         if _e_a_aba_certa(w.title)), None)
        if nome_aba is None:
            abas = ", ".join(w.title for w in com_retry(lambda: livro.worksheets()))
            return {"linhas": [], "planilha": chave, "aba": "",
                    "erro": "Não achei a aba da C. Diários nesta planilha. "
                            f"As abas são: {abas}."}
        aba = com_retry(lambda: livro.worksheet(nome_aba))
        brutas = com_retry(lambda: aba.get_all_values())
    except Exception as e:                              # noqa: BLE001
        logger.warning("ERP/obras: não deu para ler a C. Diários online (%s)", e)
        return {"linhas": [], "planilha": chave, "aba": "",
                "erro": str(e)[:500]}
    if len(brutas) < 2:
        return {"linhas": [], "planilha": chave, "aba": nome_aba,
                "erro": f'A aba "{nome_aba}" veio vazia.'}
    try:
        linhas = _entender(brutas[0], brutas[1:])
    except ErroValidacao as e:
        return {"linhas": [], "planilha": chave, "aba": nome_aba,
                "erro": str(e)}
    return {"linhas": linhas, "planilha": chave, "aba": nome_aba, "erro": ""}


def colunas_ignoradas(conteudo: bytes) -> list[dict[str, str]]:
    """As colunas que existem no arquivo e NÃO são usadas — com o motivo."""
    try:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(conteudo), read_only=True,
                                    data_only=True)
    except Exception:                                   # noqa: BLE001
        return []
    nome_aba = next((n for n in wb.sheetnames if _e_a_aba_certa(n)), None)
    if nome_aba is None:
        return []
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
    # `fase` entra fora de ROTULOS: ela não é campo que se escreve por UPDATE
    # solto — passa por `obras.mudar_fase`, que guarda o histórico.
    campos = ", ".join(["id", "codigo", "nome", "fase"] + list(ROTULOS)
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


def importar(s: Session, conteudo: Optional[bytes] = None,
             usuario_id: Optional[int] = None, *,
             simular: bool = True, regravar: bool = False,
             linhas: Optional[list[dict[str, Any]]] = None) -> dict[str, Any]:
    """Confere e (se mandarem) completa o cadastro das obras.

    As linhas vêm do arquivo .xlsx (`conteudo`) ou já prontas da planilha
    online (`linhas`, de `ler_online`). Daqui para baixo é a mesma coisa.
    """
    if linhas is None:
        if conteudo is None:
            raise ErroValidacao("Nada para importar.")
        linhas = ler_planilha(conteudo)
    if not linhas:
        raise ErroValidacao("Não achei nenhuma obra na planilha.")

    obras = _obras_do_erp(s)
    por_codigo: dict[str, dict[str, Any]] = {}
    for o in obras:
        por_codigo.setdefault(_codigo(o["codigo"]), o)
        por_codigo.setdefault(_codigo(o["nome"]), o)

    preenchidas, conflitos, nao_encontradas = [], [], []
    fases_mudadas: list[dict[str, Any]] = []
    fases_desconhecidas: dict[str, list[str]] = {}
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
        # ⚑ A FASE ANDA POR FORA DO UPDATE (24/11/2026), e tem de ser assim:
        # mudar de fase guarda histórico, trilha de auditoria e as datas de
        # conclusão e recebimento. Escrevê-la com um UPDATE solto, junto dos
        # outros campos, perderia tudo isso em 183 obras de uma vez.
        if l.get("fase_texto") and not l.get("fase"):
            fases_desconhecidas.setdefault(l["fase_texto"], []).append(
                obra["codigo"])
        elif l.get("fase") and l["fase"] != obra.get("fase"):
            # ⚑ O RÓTULO VAI PRONTO. "EM_EXECUCAO" é o banco falando; quem
            # confere 179 mudanças de fase lê "Em execução". Mesma régua do
            # assistente, e a tradução mora num lugar só.
            from app.apps.erp.core.cadastros.obras import fase_em_portugues
            fases_mudadas.append({
                "obra": obra["codigo"], "de": obra.get("fase"),
                "de_rotulo": (fase_em_portugues(obra["fase"])
                              if obra.get("fase") else ""),
                "para": l["fase"],
                "para_rotulo": fase_em_portugues(l["fase"]),
                "na_planilha": l["fase_texto"]})
            if not simular:
                # ⚑ A OBSERVAÇÃO LEVA O TEXTO DA PLANILHA JUNTO. Seis meses
                # depois, "por que esta obra está Concluída?" se responde
                # abrindo o histórico de fase dela: "C. Diários: Especial".
                # Sem isso, a trilha diz de onde veio mas não o quê.
                svc_obras.mudar_fase(
                    s, obra["id"], l["fase"], None,
                    observacao=f"C. Diários: {l['fase_texto']}".strip(": "))

        if not novos:
            if not (l.get("fase") and l["fase"] != obra.get("fase")):
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
        # A FASE SAI SEPARADA no relatório, e não misturada nos campos: é ela
        # que decide o que aparece na tela do diário, então quem confere tem
        # de ver quantas obras mudam de fase sem caçar numa lista de campos.
        "fases_mudadas": fases_mudadas[:50],
        "fases_mudadas_total": len(fases_mudadas),
        "fases_desconhecidas": [
            {"na_planilha": k, "obras": len(v), "exemplos": sorted(v)[:6]}
            for k, v in sorted(fases_desconhecidas.items())],
        "colunas_ignoradas": colunas_ignoradas(conteudo),
        "so_no_cadastro": faltando,
    }
