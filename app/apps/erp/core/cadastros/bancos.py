# ============================================================================
# ERP — core/cadastros/bancos.py
# A lista de bancos (código FEBRABAN/COMPE + nome), para o cadastro de conta.
#
# PEDIDO DO DONO, 10/09/2026: *"eu queria que tivesse uma base, você já puxasse
# essa base com as informações bancárias da FEBRABAN"* — em vez de digitar
# "237" de cabeça e torcer.
#
# DE ONDE VEM O NÚMERO
#
# O código de compensação (COMPE) é publicado pelo **Banco Central**, na
# relação de participantes do STR. É lista pública, sem cadastro e sem chave —
# a mesma lógica do INCC em `core/indices/bcb.py`.
#
# POR QUE A LISTA TAMBÉM MORA AQUI DENTRO
#
# Porque banco não pode depender de internet. A relação abaixo é a base de
# partida e funciona sozinha, para sempre, mesmo que o Banco Central saia do ar
# ou mude o endereço do arquivo. O botão "Atualizar no Banco Central" troca
# essa base pela oficial e guarda o resultado no banco de dados — a partir daí
# vale a atualizada.
#
# ⚠️ A lista embutida NÃO é a relação oficial completa: são os bancos e
# instituições de pagamento que aparecem em conta e em comprovante no Brasil.
# Banco que faltar se resolve de dois jeitos: apertando o botão de atualizar,
# ou digitando o código na mão, que continua permitido.
# ============================================================================
from __future__ import annotations

import json
import logging
import re
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento

logger = logging.getLogger(__name__)

CHAVE_PARAMETRO = "bancos.lista"        # a lista atualizada, em JSON
CHAVE_ATUALIZADA_EM = "bancos.atualizada_em"

# A relação de participantes do STR, em CSV, publicada pelo Banco Central.
URL_STR = "https://www.bcb.gov.br/pom/spb/estatistica/port/ParticipantesSTRport.csv"
TEMPO_LIMITE = 25

# código COMPE → nome usual. Ordenado pelo código, para conferir com o olho.
EMBUTIDOS: dict[str, str] = {
    "001": "Banco do Brasil",
    "003": "Banco da Amazônia",
    "004": "Banco do Nordeste do Brasil",
    "021": "Banestes",
    "025": "Banco Alfa",
    "033": "Santander",
    "036": "Banco Bradesco BBI",
    "037": "Banpará",
    "041": "Banrisul",
    "047": "Banese",
    "062": "Hipercard",
    "063": "Bradescard",
    "069": "Crefisa",
    "070": "BRB — Banco de Brasília",
    "077": "Banco Inter",
    "079": "Banco Original do Agronegócio",
    "082": "Banco Topázio",
    "084": "Uniprime do Norte do Paraná",
    "085": "Ailos (Cecred)",
    "089": "Cooperativa Credisan",
    "091": "Unicred Central RS",
    "092": "BRK CFI",
    "094": "Banco Finaxis",
    "096": "Banco B3",
    "097": "Credisis",
    "099": "Uniprime Central",
    "104": "Caixa Econômica Federal",
    "107": "Banco Bocom BBM",
    "121": "Banco Agibank",
    "133": "Cresol Confederação",
    "136": "Unicred",
    "173": "BRL Trust DTVM",
    "184": "Banco Itaú BBA",
    "197": "Stone Pagamentos",
    "208": "Banco BTG Pactual",
    "212": "Banco Original",
    "213": "Banco Arbi",
    "218": "Banco BS2",
    "222": "Banco Crédit Agricole Brasil",
    "224": "Banco Fibra",
    "233": "Banco Cifra",
    "237": "Bradesco",
    "241": "Banco Clássico",
    "243": "Banco Master",
    "246": "Banco ABC Brasil",
    "254": "Paraná Banco",
    "260": "Nu Pagamentos (Nubank)",
    "265": "Banco Fator",
    "266": "Banco Cédula",
    "274": "Money Plus",
    "290": "PagBank (PagSeguro)",
    "299": "Banco Afinz",
    "301": "BPP Instituição de Pagamento",
    "318": "Banco BMG",
    "320": "China Construction Bank Brasil",
    "323": "Mercado Pago",
    "326": "Parati CFI",
    "329": "QI Sociedade de Crédito Direto",
    "332": "Acesso Soluções de Pagamento",
    "335": "Banco Digio",
    "336": "Banco C6",
    "340": "Super Pagamentos",
    "341": "Itaú Unibanco",
    "348": "Banco XP",
    "352": "Toro CTVM",
    "355": "Ótimo Sociedade de Crédito Direto",
    "366": "Banco Société Générale Brasil",
    "370": "Banco Mizuho do Brasil",
    "376": "Banco J.P. Morgan",
    "380": "PicPay",
    "389": "Banco Mercantil do Brasil",
    "394": "Banco Bradesco Financiamentos",
    "399": "Kirton Bank (HSBC)",
    "403": "Cora Sociedade de Crédito Direto",
    "404": "SumUp Sociedade de Crédito Direto",
    "412": "Banco Capital",
    "413": "Banco BV",
    "422": "Banco Safra",
    "456": "Banco MUFG Brasil",
    "464": "Banco Sumitomo Mitsui Brasileiro",
    "473": "Banco Caixa Geral Brasil",
    "477": "Citibank N.A.",
    "479": "Banco ItauBank",
    "487": "Deutsche Bank",
    "488": "JPMorgan Chase Bank",
    "492": "ING Bank N.V.",
    "505": "Banco Credit Suisse Brasil",
    "600": "Banco Luso Brasileiro",
    "604": "Banco Industrial do Brasil",
    "610": "Banco VR",
    "611": "Banco Paulista",
    "612": "Banco Guanabara",
    "613": "Omni Banco",
    "623": "Banco PAN",
    "626": "Banco C6 Consignado",
    "630": "Banco Smartbank",
    "633": "Banco Rendimento",
    "634": "Banco Triângulo",
    "637": "Banco Sofisa",
    "643": "Banco Pine",
    "652": "Itaú Unibanco Holding",
    "653": "Banco Voiter",
    "654": "Banco Digimais",
    "655": "Banco Votorantim",
    "707": "Banco Daycoval",
    "712": "Banco Ourinvest",
    "739": "Banco Cetelem",
    "741": "Banco Ribeirão Preto",
    "743": "Banco Semear",
    "745": "Citibank S.A.",
    "746": "Banco Modal",
    "747": "Rabobank Brasil",
    "748": "Sicredi",
    "751": "Scotiabank Brasil",
    "752": "BNP Paribas Brasil",
    "755": "Bank of America Merrill Lynch",
    "756": "Sicoob",
    "757": "Banco KEB HANA do Brasil",
}


def normalizar_codigo(bruto: Any) -> str:
    """Três dígitos, com o zero à esquerda que todo mundo esquece.

    Quem digita costuma escrever "1" para o Banco do Brasil e "33" para o
    Santander. Guardar assim faria a mesma conta aparecer com dois códigos
    diferentes conforme quem cadastrou.
    """
    d = re.sub(r"\D", "", str(bruto or ""))
    if not d:
        return ""
    return d[-3:].rjust(3, "0") if len(d) <= 3 else d[:3]


def _parametro(s: Session, chave: str) -> str:
    from app.apps.erp.db.models.cadastros import Parametro
    linha = s.get(Parametro, chave)
    return (linha.valor if linha is not None else "") or ""


def _gravar_parametro(s: Session, chave: str, valor: str) -> None:
    from app.apps.erp.db.models.cadastros import Parametro
    linha = s.get(Parametro, chave)
    if linha is None:
        s.add(Parametro(chave=chave, valor=valor))
    else:
        linha.valor = valor


def mapa(s: Optional[Session] = None) -> dict[str, str]:
    """código → nome. A lista atualizada quando existir; a embutida quando não.

    Aceita não receber sessão: há lugar que só quer traduzir um código e não
    tem banco à mão (formatação de relatório, por exemplo).
    """
    if s is None:
        return dict(EMBUTIDOS)
    bruto = _parametro(s, CHAVE_PARAMETRO)
    if not bruto:
        return dict(EMBUTIDOS)
    try:
        guardado = json.loads(bruto)
    except ValueError:
        logger.warning("ERP/bancos: lista guardada ilegível — usando a embutida")
        return dict(EMBUTIDOS)
    if not isinstance(guardado, dict) or not guardado:
        return dict(EMBUTIDOS)
    # A embutida entra por baixo: se a oficial não trouxer algum código que
    # já está em uso numa conta cadastrada, o nome dele não some da tela.
    junto = dict(EMBUTIDOS)
    junto.update({normalizar_codigo(k): str(v) for k, v in guardado.items()})
    return junto


def nome(codigo: Any, s: Optional[Session] = None) -> str:
    return mapa(s).get(normalizar_codigo(codigo), "")


def rotulo(codigo: Any, s: Optional[Session] = None) -> str:
    """"237 · Bradesco" — ou só o código, quando não se conhece o nome."""
    c = normalizar_codigo(codigo)
    n = nome(c, s)
    return f"{c} · {n}" if n else c


def listar(s: Optional[Session] = None) -> list[dict[str, str]]:
    """A lista para a tela montar o campo de escolha, ordenada por nome."""
    return sorted(
        ({"codigo": c, "nome": n} for c, n in mapa(s).items()),
        key=lambda b: (b["nome"].lower(), b["codigo"]))


def estado(s: Session) -> dict[str, Any]:
    return {
        "quantidade": len(mapa(s)),
        "atualizada_em": _parametro(s, CHAVE_ATUALIZADA_EM),
        "oficial": bool(_parametro(s, CHAVE_PARAMETRO)),
    }


# ---------------------------------------------------------------------------
# A atualização no Banco Central — pelo BOTÃO, nunca sozinha
# ---------------------------------------------------------------------------
def _baixar_csv() -> str:                                  # pragma: no cover
    """Isolado numa função só para o teste poder dublar. Não há como bater no
    Banco Central dentro da suíte, e nem seria desejável."""
    import requests
    resposta = requests.get(URL_STR, timeout=TEMPO_LIMITE)
    resposta.raise_for_status()
    # O arquivo vem em latin-1, como quase tudo que o BCB publica em CSV.
    return resposta.content.decode("latin-1", errors="replace")


def interpretar_csv(texto: str) -> dict[str, str]:
    """Extrai código → nome do CSV de participantes do STR.

    As colunas do arquivo já mudaram de ordem no passado, então nada de
    "coluna 3": procura-se, em cada linha, a coluna que é um código COMPE de
    três dígitos e a que tem o nome mais longo. Ler por posição é o tipo de
    coisa que quebra calada.
    """
    import csv
    import io

    achados: dict[str, str] = {}
    leitor = csv.reader(io.StringIO(texto), delimiter=",")
    for linha in leitor:
        if len(linha) < 2:
            leitor2 = csv.reader(io.StringIO(",".join(linha)), delimiter=";")
            linha = next(leitor2, linha)
        celulas = [c.strip().strip('"') for c in linha if c is not None]
        codigos = [c for c in celulas if re.fullmatch(r"\d{3}", c)]
        if not codigos:
            continue
        nomes = [c for c in celulas
                 if not re.fullmatch(r"[\d./-]*", c) and len(c) > 3]
        if not nomes:
            continue
        codigo = normalizar_codigo(codigos[0])
        # O nome curto ("BCO DO BRASIL S.A.") é o que cabe numa tela; o longo
        # é a razão social inteira. Fica o primeiro que não seja o ISPB.
        achados[codigo] = max(nomes, key=len)[:80]
    return achados


def atualizar(s: Session, usuario: Optional[Any] = None,
              baixar=None) -> dict[str, Any]:
    """Troca a lista pela oficial do Banco Central. Só pelo botão.

    Falha aqui não é catástrofe, de propósito: a lista embutida continua
    valendo e o cadastro de conta continua funcionando. O que muda é a
    mensagem na tela, em português.
    """
    try:
        texto = (baixar or _baixar_csv)()
    except Exception as e:
        logger.warning("ERP/bancos: não consegui falar com o Banco Central — %s", e)
        raise ErroValidacao(
            "Não consegui falar com o Banco Central agora. A lista que já "
            "existe continua valendo — tente de novo mais tarde.")

    achados = interpretar_csv(texto)
    if len(achados) < 50:
        raise ErroValidacao(
            f"O Banco Central respondeu num formato que eu não reconheci "
            f"(só entendi {len(achados)} banco(s)). A lista que já existe "
            f"continua valendo.")

    from datetime import datetime
    antes = len(mapa(s))
    _gravar_parametro(s, CHAVE_PARAMETRO, json.dumps(achados, ensure_ascii=False))
    _gravar_parametro(s, CHAVE_ATUALIZADA_EM, datetime.now().strftime("%d/%m/%Y %H:%M"))
    s.flush()
    depois = len(mapa(s))
    registrar_evento(s, "parametro", 0, "BANCOS_ATUALIZADOS",
                     {"antes": antes, "depois": depois},
                     getattr(usuario, "id", None))
    logger.info("ERP/bancos: lista atualizada — %d bancos", depois)
    return {"quantidade": depois, "novos": max(0, depois - antes)}
