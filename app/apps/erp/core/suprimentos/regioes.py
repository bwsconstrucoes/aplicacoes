# ============================================================================
# ERP — core/suprimentos/regioes.py
# ATÉ ONDE O FORNECEDOR VENDE, de um jeito que cruza com o município da obra.
#
# PEDIDO DO DONO, 21/09/2026:
#
#   "O fornecedor tem a região que atende, e existe o local da obra. Se não é
#   um fornecedor que atenda a nível nacional, eu tenho que buscar na região da
#   obra. Da forma que está, a gente simplesmente escreve de qualquer jeito,
#   sem padronização. Como vamos cruzar obra x fornecedor?"
#
# O PROBLEMA, no dado real: a coluna "Região de Atuação" da planilha tem 27
# grafias e QUATRO NÍVEIS misturados — "BR", "NE", "PE", "RMF", "CARIRI",
# "BARBALHA - CE", "TAUÁ CE". Nenhum deles se compara com outro por igualdade
# de texto, e é por isso que o disparo automático não filtrava por região: ele
# só PONTUAVA quem era da mesma cidade, e deixava entrar fornecedor de São
# Paulo numa obra do Cariri.
#
# O QUE ESTE MÓDULO FAZ
#
#   1. TRADUZ o texto livre para (abrangência, UFs, municípios) — e diz o que
#      NÃO reconheceu, em vez de chutar;
#   2. RESPONDE "este fornecedor atende esta obra?", que é a pergunta que o
#      disparo automático faz a cada bloco.
#
# AS LISTAS ESTÃO AQUI, E NÃO NUMA MIGRAÇÃO, porque vão precisar de ajuste:
# município novo aparece, e o dono vai querer acrescentar um apelido que a
# equipe usa. Mexer aqui é mexer em código testado; mexer num `.sql` já
# aplicado é impossível.
# ============================================================================
from __future__ import annotations

import logging
import re
import unicodedata
from typing import Any, Optional

logger = logging.getLogger(__name__)

NACIONAL = "NACIONAL"
ESTADUAL = "ESTADUAL"
REGIONAL = "REGIONAL"
LOCAL = "LOCAL"
NAO_INFORMADA = "NAO_INFORMADA"

ROTULOS = {
    NACIONAL: "Todo o Brasil",
    ESTADUAL: "Estado(s) inteiro(s)",
    REGIONAL: "Uma região (vários municípios)",
    LOCAL: "Só a cidade dele",
    NAO_INFORMADA: "Não informada",
}

UFS = ("AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS",
       "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC",
       "SE", "SP", "TO")

# As macrorregiões, para quem escreveu "NE" — que na planilha aparece 256 vezes.
MACRORREGIOES = {
    "NORTE": ("AC", "AM", "AP", "PA", "RO", "RR", "TO"),
    "NORDESTE": ("AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"),
    "CENTRO OESTE": ("DF", "GO", "MS", "MT"),
    "SUDESTE": ("ES", "MG", "RJ", "SP"),
    "SUL": ("PR", "RS", "SC"),
}
APELIDOS_DE_MACRORREGIAO = {
    "NE": "NORDESTE", "N": "NORTE", "CO": "CENTRO OESTE",
    "SE": "SUDESTE", "S": "SUL", "SUDESTE": "SUDESTE", "SUL": "SUL",
    "NORDESTE": "NORDESTE", "NORTE": "NORTE", "CENTRO OESTE": "CENTRO OESTE",
}

# As regiões que a BWS usa no dia a dia. São as duas que aparecem na planilha
# — "RMF" 204 vezes e "CARIRI" 17 — e é aqui que se acrescenta a próxima.
REGIOES_NOMEADAS: dict[str, tuple[str, ...]] = {
    "RMF": (
        "FORTALEZA", "CAUCAIA", "MARACANAU", "MARANGUAPE", "EUSEBIO", "AQUIRAZ",
        "PACATUBA", "HORIZONTE", "ITAITINGA", "PACAJUS", "CHOROZINHO",
        "GUAIUBA", "SAO GONCALO DO AMARANTE", "PARACURU", "PARAIPABA",
        "SAO LUIS DO CURU", "TRAIRI", "CASCAVEL", "PINDORETAMA",
    ),
    "CARIRI": (
        "JUAZEIRO DO NORTE", "CRATO", "BARBALHA", "MISSAO VELHA", "CARIRIACU",
        "FARIAS BRITO", "JARDIM", "NOVA OLINDA", "SANTANA DO CARIRI",
    ),
}
APELIDOS_DE_REGIAO = {
    "RMF": "RMF", "REGIAO METROPOLITANA": "RMF",
    "REGIAO METROPOLITANA DE FORTALEZA": "RMF", "GRANDE FORTALEZA": "RMF",
    "CARIRI": "CARIRI", "REGIAO DO CARIRI": "CARIRI",
    "RM CARIRI": "CARIRI", "RM DO CARIRI": "CARIRI",
}

PALAVRAS_DE_BRASIL = ("BR", "BRASIL", "NACIONAL", "TODO O BRASIL", "NAC")


def normalizar(texto: Optional[str]) -> str:
    """Sem acento, em maiúsculas, sem espaço sobrando.

    É a forma em que os municípios são GUARDADOS e COMPARADOS. Guardar
    "Tauá" e comparar com "TAUA" é o tipo de diferença que faz o cruzamento
    falhar sem dar erro nenhum.
    """
    bruto = unicodedata.normalize("NFKD", (texto or "").strip().upper())
    sem_acento = "".join(c for c in bruto if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", sem_acento.replace("\xa0", " ")).strip()


def _partes(texto: str) -> list[str]:
    return [p.strip() for p in re.split(r"[;,/|]", texto or "") if p.strip()]


def _tirar_uf_do_fim(termo: str) -> tuple[str, str]:
    """"BARBALHA - CE" e "TAUÁ CE" viram ("BARBALHA", "CE").

    A planilha escreve a cidade com a UF colada de três jeitos, e sem isso
    "TAUA CE" nunca casaria com o município "TAUA" da obra.
    """
    t = normalizar(termo)
    achado = re.search(r"^(.*?)[\s\-]+([A-Z]{2})$", t)
    if achado and achado.group(2) in UFS and achado.group(1).strip():
        return achado.group(1).strip(), achado.group(2)
    return t, ""


def traduzir(textos: list[str]) -> dict[str, Any]:
    """O texto livre vira (abrangência, UFs, municípios) — ou volta como
    desconhecido, sem chute.

    A abrangência final é a MAIOR encontrada: quem escreveu "BR, CE" atende o
    Brasil, e restringir ao Ceará por causa da segunda palavra seria tirá-lo de
    cotações que ele atende. Errar para o lado de incluir custa um e-mail que o
    fornecedor ignora; errar para o lado de excluir custa um preço que nunca
    foi pedido — e ninguém percebe que faltou.
    """
    ufs: set[str] = set()
    municipios: set[str] = set()
    nivel = NAO_INFORMADA
    nacional = False
    desconhecidos: list[str] = []

    def subir(novo: str) -> None:
        nonlocal nivel
        ordem = {NAO_INFORMADA: 0, LOCAL: 1, REGIONAL: 2, ESTADUAL: 3, NACIONAL: 4}
        if ordem[novo] > ordem[nivel]:
            nivel = novo

    for bruto in textos or []:
        for termo in _partes(bruto):
            t = normalizar(termo)
            if not t:
                continue
            if t in PALAVRAS_DE_BRASIL:
                nacional = True
                subir(NACIONAL)
                continue
            macro = APELIDOS_DE_MACRORREGIAO.get(t)
            if macro:
                ufs.update(MACRORREGIOES[macro])
                subir(ESTADUAL)
                continue
            if t in UFS:
                ufs.add(t)
                subir(ESTADUAL)
                continue
            regiao = APELIDOS_DE_REGIAO.get(t)
            if regiao:
                municipios.update(REGIOES_NOMEADAS[regiao])
                subir(REGIONAL)
                continue
            cidade, uf_do_fim = _tirar_uf_do_fim(termo)
            if uf_do_fim:
                municipios.add(cidade)
                subir(LOCAL)
                continue
            # Sobrou um nome sozinho. Pode ser cidade ("SOBRAL", "BARRO") e
            # quase sempre é — mas ACEITAR QUALQUER COISA como município é o
            # que encheria o cadastro de "MATRIZ" e "A COMBINAR". Só entra se
            # parecer nome de lugar: letras e espaços, sem número.
            if re.fullmatch(r"[A-Z ]{3,}", t):
                municipios.add(t)
                subir(LOCAL)
            else:
                desconhecidos.append(termo.strip())

    if nacional:
        # Nacional não precisa de lista: a lista seria o país inteiro, e mantê-la
        # daria a impressão de que só aqueles lugares valem.
        return {"abrangencia": NACIONAL, "ufs": [], "municipios": [],
                "desconhecidos": desconhecidos}
    if nivel is NAO_INFORMADA or (not ufs and not municipios):
        return {"abrangencia": NAO_INFORMADA, "ufs": [], "municipios": [],
                "desconhecidos": desconhecidos}
    return {"abrangencia": nivel, "ufs": sorted(ufs),
            "municipios": sorted(municipios), "desconhecidos": desconhecidos}


# ---------------------------------------------------------------------------
# A pergunta que o disparo automático faz
# ---------------------------------------------------------------------------
def atende(forn, municipio: str, uf: str) -> tuple[bool, str]:
    """(atende?, por quê) — para a tela poder mostrar o motivo.

    O "por quê" não é enfeite: é o que permite ao comprador discordar com
    fundamento quando um fornecedor que ele sabe que atende ficou de fora.
    """
    abrangencia = (getattr(forn, "abrangencia", None) or NAO_INFORMADA)
    cidade = normalizar(municipio)
    sigla = normalizar(uf)

    if abrangencia == NACIONAL:
        return True, "atende todo o Brasil"
    if abrangencia == ESTADUAL:
        ufs = [normalizar(x) for x in (getattr(forn, "ufs_atendidas", None) or [])]
        if sigla and sigla in ufs:
            return True, f"atende o estado ({sigla})"
        return False, f"não atende {sigla or 'a UF da obra'}"
    if abrangencia in (REGIONAL, LOCAL):
        cidades = [normalizar(x)
                   for x in (getattr(forn, "municipios_atendidos", None) or [])]
        if cidade and cidade in cidades:
            return True, ("está na região que ele atende" if abrangencia == REGIONAL
                          else "é da própria cidade da obra")
        return False, f"não atende {municipio or 'o município da obra'}"
    # SEM REGIÃO DEFINIDA fica de fora do automático, e é isso que torna o
    # buraco visível: enquanto ele entrava "porque sim", ninguém arrumava o
    # cadastro. A tela conta quantos são e leva até eles.
    return False, "sem região definida no cadastro"
