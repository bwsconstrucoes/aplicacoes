# -*- coding: utf-8 -*-
"""
FATURAMENTO — as notas fiscais emitidas, numa tela (09/10/2026, migração 053).

O dono: *"essa atualização de planilha eu quero eliminar (…) numa nova tela no
Análise de SPs, que a gente pode chamar de Faturamento, eu quero fazer o
controle de notas — ver faturamento, fazer o download da nota, uma parte
gráfica de evolução, poder fazer toda essa gestão."*

O desenho inteiro está em `app/apps/emissaonf/FATURAMENTO.md`. Em resumo:

- a FONTE é a aba "Base Faturamento" da planilha das notas, que o emissor
  grava (uma linha por nota, cabeçalho com os nomes dos campos);
- atributos da OBRA (empresa, SCP, contrato, município, tributação) NÃO estão
  na base: vêm da C. Diários ("Centro de Custo"), cruzados pelo código da obra —
  *"informação que vem da C. Diários não precisa entrar na base, a gente vai
  cruzar"*;
- a carga traz as duas abas para o banco (`carregar`, no processo separado), e
  a tela lê só do banco.

⚠️ AS TRÊS VERDADES DE UM TRIBUTO na base, e a tela respeita: VAZIO = não se
sabe (nota antiga não equalizada); 0,00 com retém N = não reteve; valor com
retém S = reteve. Vazio NÃO vira zero aqui.
"""
from __future__ import annotations

import json
import logging

from .tabela import Coluna

logger = logging.getLogger("analisesps.faturamento")

# A planilha "Controle de Impostos e Emissão de Nota" e a "Bases de Dados
# Pipefy" — os mesmos ids que o emissor usa (`emissaonf/worker.py`).
PLANILHA_NOTAS = "1NOEzey3vKleRuX7Jm8GylRBjGDFmQYi5l0LxtpPpEbU"
ABA_BASE = "Base Faturamento"
PLANILHA_OBRAS = "1C7MWQmr5uFGWuJ18osUNDapiojVXzQ_GxMMDQqxPsBk"
ABAS_OBRAS = ["Centro de Custo", "Centro de Custos", "C. Diários"]

# Os atributos da obra que a tela mostra, pelo NOME do cabeçalho na C. Diários
# (o mesmo mapa do emissor, `emissaonf/cdiarios.py`).
CAMPOS_DA_OBRA = {
    "codigo_primario": "Código Primário",
    "centro_custo": "Centro de Custo",
    "municipio": "Município",
    "uf": "UF",
    "tributacao": "Tributação",
    "cliente": "Cliente",
    "contrato": "Contrato",
    "objeto": "Objeto",
    "empresa": "Empresa",
    "empresa_cnpj": "CNPJ Empresa",
    "scp": "SCP",
    "scp_cnpj": "CNPJ SCP",
}

# AS COLUNAS DA LISTA DE NOTAS, que cada pessoa escolhe (09/10/2026 — *"na aba
# Solicitações você consegue definir quais colunas exibir; quero a mesma coisa
# para essa de notas (…) de repente se eu quiser que apareça menos"*). A NOTA
# não entra na lista: é a identidade da linha, e é nela que o duplo clique
# abre a ficha. Mesma mecânica e mesma guarda da tabela de SPs (`tabela.py`).
COLUNAS = [
    Coluna("emissao",     "Emissão",              "data",    True),
    Coluna("obra",        "Obra",                 "texto",   True),
    Coluna("empresa",     "Empresa",              "texto",   True),
    Coluna("tomador",     "Tomador",              "texto",   True),
    Coluna("medicao",     "Medição",              "texto",   True),
    # O PERÍODO DA MEDIÇÃO (09/10/2026 — *"colunas que tragam o período da
    # medição; não vai ter para todos, mas a gente tem lá na planilha
    # Protocolos"*). A competência saiu da lista: *"nem precisa ter coluna, ela
    # tem que estar no filtro"*.
    Coluna("periodo_ini", "Início da medição",    "data",    True),
    Coluna("periodo_fim", "Fim da medição",       "data",    True),
    Coluna("valor",       "Valor",                "moeda",   True),
    Coluna("tributos",    "Tributos (PIS a ISS)", "moeda",   True),
    Coluna("liquido",     "Líquido",              "moeda",   True),
    Coluna("recebido_em", "Recebido em",          "data",    True),
    Coluna("recebido",    "Valor recebido",       "moeda",   True),
    Coluna("situacao",    "Situação",             "texto",   True),
    Coluna("omie",        "Omie",                 "texto",   True),
    Coluna("arquivos",    "Arquivos",             "link",    True),
]
COLUNAS_POR_CHAVE = {c.chave: c for c in COLUNAS}
PREFERENCIA_COLUNAS = "colunas_faturamento"

STATUS_VALIDA = "valida"
# A lista é "como a planilha" (pedido do dono, 09/10/2026): muitas linhas por
# página, cabeçalho fixo e rolagem.
POR_PAGINA = 300
CHAVE_META = "faturamento_carregado_em"


def pronto() -> bool:
    """A migração 053 já rodou?"""
    from .db import tem_coluna
    return tem_coluna("faturamento_nota", "dados")


def _chave_obra(v) -> str:
    return "".join(str(v or "").split()).upper()


# ---------------------------------------------------------------------------
# A carga (processo separado)
# ---------------------------------------------------------------------------
def competencia_normalizada(valor, data_emissao="") -> str:
    """A competência como "AAAA-MM" (é assim que ordena), venha como vier:
    "2026-09", "2026-09-01", "09/2026", "01/09/2026". Sem ela, o mês da
    emissão — a mesma regra da consolidação do emissor."""
    import re
    t = str(valor or "").strip()
    m = re.match(r"^(\d{4})-(\d{1,2})", t)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}"
    m = re.match(r"^(?:\d{1,2}/)?(\d{1,2})/(\d{4})$", t)
    if m:
        return f"{m.group(2)}-{int(m.group(1)):02d}"
    if data_emissao:
        from .formatos import para_data
        d = para_data(data_emissao)
        if d:
            return d.strftime("%Y-%m")
    return ""


def competencia_br(valor) -> str:
    """"2026-09" → "09/2026" (09/10/2026 — *"é para ser primeiro mês, barra,
    depois o ano"*)."""
    t = str(valor or "")
    return f"{t[5:7]}/{t[:4]}" if len(t) >= 7 and t[4] == "-" else t


# ---------------------------------------------------------------------------
# O PERÍODO DA MEDIÇÃO pela aba "Protocolos" (09/10/2026).
#
# A base só tem o período das notas que o emissor gravou depois de 09/10; para
# as antigas ele está na "Protocolos" (por OBRA-MEDIÇÃO, a mesma chave que a
# consolidação usa para o card e o código do Omie). Aqui só se LÊ: o período
# completa a tela, e a base não é tocada.
#
# ⚠️ AS COLUNAS SÃO ACHADAS PELO NOME do cabeçalho, não pela posição: o mapa do
# emissor só conhece A (chave), C (card) e D (código do Omie). Não achando, a
# carga diz quais cabeçalhos viu — para acertar o nome, e não chutar a coluna.
# ---------------------------------------------------------------------------
ABA_PROTOCOLOS = "Protocolos"


def _sem_acento(t) -> str:
    import unicodedata
    t = unicodedata.normalize("NFD", str(t or "")).encode("ascii", "ignore").decode()
    return " ".join(t.lower().split())


def periodos_dos_protocolos(valores: list) -> tuple[dict, str]:
    """({CHAVE OBRA-MEDIÇÃO: (início, fim)}, aviso). Aviso vazio = tudo certo."""
    import re
    if not valores:
        return {}, "aba Protocolos vazia."

    def e_inicio(n):
        return "inicio" in n and ("medic" in n or "period" in n)

    def e_fim(n):
        return (("termino" in n or "fim" in n or "final" in n)
                and ("medic" in n or "period" in n))

    # o cabeçalho pode não estar na primeira linha (importação de outra planilha)
    for pos, cab in enumerate(valores[:5]):
        nomes = [_sem_acento(c) for c in cab]
        i_ini = next((i for i, n in enumerate(nomes) if e_inicio(n)), None)
        i_fim = next((i for i, n in enumerate(nomes) if e_fim(n)), None)
        i_unico = next((i for i, n in enumerate(nomes) if "periodo" in n), None)
        if (i_ini is not None and i_fim is not None) or i_unico is not None:
            break
    else:
        vistos = [str(c).strip() for c in valores[0] if str(c).strip()][:20]
        return {}, ("Protocolos: não achei as colunas do período da medição "
                    f"(cabeçalhos vistos: {', '.join(vistos) or 'nenhum'}).")
    i_chave = next((i for i, n in enumerate(nomes) if "chave" in n), 0)

    def celula(linha, i):
        return str(linha[i]).strip() if i is not None and i < len(linha) else ""

    saida = {}
    for linha in valores[pos + 1:]:
        chave = celula(linha, i_chave).upper()
        if not chave or chave in saida:
            continue                       # a PRIMEIRA vale, como no emissor
        if i_ini is not None and i_fim is not None:
            ini, fim = celula(linha, i_ini), celula(linha, i_fim)
        else:
            # uma coluna só: "01/09/2026 a 30/09/2026"
            partes = re.split(r"\s+(?:a|à|ate|até)\s+", celula(linha, i_unico))
            ini, fim = (partes + ["", ""])[:2] if len(partes) == 2 else ("", "")
        if ini or fim:
            saida[chave] = (ini, fim)
    return saida, ""


def completar_periodos(notas: list[dict], periodos: dict) -> int:
    """Põe o período da Protocolos nas notas que não o têm. Devolve quantas."""
    postos = 0
    for d in notas:
        if d.get("medicao_periodo_ini") or d.get("medicao_periodo_fim"):
            continue
        chave = (f"{str(d.get('obra_codigo') or '').strip().upper()}-"
                 f"{str(d.get('medicao_numero') or '').strip().upper()}")
        achado = periodos.get(chave)
        if achado:
            d["medicao_periodo_ini"], d["medicao_periodo_fim"] = achado
            d["_periodo_da_protocolos"] = "S"
            postos += 1
    return postos


def _assinatura(d: dict) -> tuple:
    """O que faz duas linhas com o MESMO número serem a mesma nota: mesma
    emissão, mesma obra, mesmo valor."""
    from .formatos import para_data, para_numero
    return (para_data(d.get("data_emissao")), _chave_obra(d.get("obra_codigo")),
            para_numero(d.get("valor_total")))


def separar_repetidas(notas: list[dict], contagem: dict | None = None) -> list[dict]:
    """Tira as linhas REPETIDAS (mesmo número E mesma nota) e dá chave própria
    a nota DIFERENTE que repete um número já usado.

    ⚠️ 09/10/2026 — o dono: *"3.468 notas levadas à base, 3.284 notas fiscais
    (…) ela não está completa"*. A primeira versão guardava uma nota por
    número e jogava fora as outras 184 linhas sem dizer nada. Número repetido
    pode ser duas coisas, e elas pedem tratamentos opostos:

      - a MESMA nota escrita duas vezes (mesma emissão, obra e valor) — fica uma;
      - OUTRA nota com o mesmo número (outra data, obra ou valor: outra série,
        outro ano, outra empresa) — entra, com a chave "número-2", "número-3"…

    `contagem` recebe quantas foram de cada tipo, para a carga dizer."""
    contagem = contagem if contagem is not None else {}
    contagem.setdefault("repetidas", 0)
    contagem.setdefault("mesmo_numero", 0)
    vistas: dict = {}
    saida = []
    for d in notas:
        numero = d.get("nota_numero", "")
        assinatura = _assinatura(d)
        ja = vistas.setdefault(numero, [])
        if assinatura in ja:
            contagem["repetidas"] += 1
            continue
        ja.append(assinatura)
        if len(ja) > 1:
            contagem["mesmo_numero"] += 1
            d["_chave"] = f"{numero}-{len(ja)}"
        saida.append(d)
    return saida


def notas_das_linhas(valores: list, contagem: dict | None = None) -> list[dict]:
    """As linhas da aba viram dicionários pelo NOME do cabeçalho. Linha sem
    número de nota fica de fora; número repetido — ver `separar_repetidas`.

    Cada nota guarda a linha dela na aba (`_linha_base`): é por ela que a
    conferência no Omie acha a linha certa, mesmo quando o número se repete."""
    if not valores:
        return []
    cabecalho = [str(c).strip() for c in valores[0]]
    notas = []
    for i, linha in enumerate(valores[1:], start=2):
        dados = {nome: (str(linha[j]).strip() if j < len(linha) else "")
                 for j, nome in enumerate(cabecalho) if nome}
        if not dados.get("nota_numero", ""):
            continue
        dados["_linha_base"] = i
        notas.append(dados)
    return separar_repetidas(notas, contagem)


def obras_das_linhas(valores: list) -> dict:
    """{CÓDIGO: atributos} pelos DOIS códigos da obra — o primário ("Código
    Primário") e o secundário (coluna A). O primário nunca é encoberto: é a
    mesma regra de `emissaonf/cdiarios.carregar_obras`."""
    if not valores:
        return {}
    norm = [" ".join(str(c).split()).lower() for c in valores[0]]
    idx = {campo: (norm.index(nome.lower()) if nome.lower() in norm else None)
           for campo, nome in CAMPOS_DA_OBRA.items()}
    obras, secundarios = {}, []
    for linha in valores[1:]:
        def g(campo):
            i = idx.get(campo)
            return str(linha[i]).strip() if i is not None and i < len(linha) else ""
        dados = {campo: g(campo) for campo in CAMPOS_DA_OBRA}
        primario = _chave_obra(dados["codigo_primario"])
        secundario = _chave_obra(linha[0] if linha else "")
        if primario:
            obras[primario] = dados
        if secundario and secundario != primario:
            secundarios.append((secundario, dados))
    for codigo, dados in secundarios:
        obras.setdefault(codigo, dados)
    return obras


def carregar(anotar=None) -> dict:
    """Traz a "Base Faturamento" e a C. Diários para o banco, por inteiro.

    UMA TRANSAÇÃO: apaga e regrava as duas tabelas juntas. Quem abrir a tela no
    meio vê a carga anterior inteira, nunca metade."""
    from . import formatos
    from .credenciais import com_retry
    from .db import conexao
    from .horario import agora
    from .sincronizacao import _aba, _explicar_aba, _meta_gravar

    anotar = anotar or (lambda *a, **k: None)
    avisos = []
    anotar("trazendo as notas fiscais", ABA_BASE)
    try:
        contagem = {}
        notas = notas_das_linhas(com_retry(_aba(PLANILHA_NOTAS, ABA_BASE).get_all_values),
                                 contagem)
    except Exception as e:  # noqa: BLE001 — a frase vai para a tela
        raise RuntimeError(_explicar_aba(PLANILHA_NOTAS, ABA_BASE, e)) from e

    anotar("trazendo o período da medição", ABA_PROTOCOLOS)
    try:
        periodos, aviso_protocolos = periodos_dos_protocolos(
            com_retry(_aba(PLANILHA_NOTAS, ABA_PROTOCOLOS).get_all_values))
    except Exception as e:  # noqa: BLE001 — sem a Protocolos, a tela vive sem período
        periodos, aviso_protocolos = {}, _explicar_aba(PLANILHA_NOTAS, ABA_PROTOCOLOS, e)
    if aviso_protocolos:
        avisos.append(aviso_protocolos)
    com_periodo = completar_periodos(notas, periodos)

    anotar("trazendo as obras", "C. Diários")
    obras, erro_obras = {}, None
    for nome in ABAS_OBRAS:
        try:
            obras = obras_das_linhas(com_retry(_aba(PLANILHA_OBRAS, nome).get_all_values))
            break
        except Exception as e:  # noqa: BLE001 — tenta o próximo nome da aba
            erro_obras = e
    if not obras:
        # Sem obras a tela funciona — só não mostra empresa/SCP. Dizer.
        avisos.append("C. Diários: " + _explicar_aba(PLANILHA_OBRAS, ABAS_OBRAS[0],
                                                       erro_obras or "vazia"))

    if contagem.get("mesmo_numero"):
        avisos.append(f"{contagem['mesmo_numero']} nota(s) repetem o número de outra "
                      "(data, obra ou valor diferentes) — entram como \"número-2\".")
    if contagem.get("repetidas"):
        avisos.append(f"{contagem['repetidas']} linha(s) da base são a mesma nota "
                      "escrita duas vezes — contadas uma vez só.")

    def numero(v):
        return formatos.para_numero(v)

    with conexao() as conn:
        conn.execute("DELETE FROM analisesps.faturamento_nota")
        conn.execute("DELETE FROM analisesps.faturamento_obra")
        for n in notas:
            conn.execute(
                "INSERT INTO analisesps.faturamento_nota (nota_numero, "
                " nota_sequencial, data_emissao, competencia, status, obra_codigo, "
                " tomador_nome, valor_total, valor_liquido, valor_recebido, "
                " data_recebimento, dados) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)",
                (n.get("_chave") or n["nota_numero"], n.get("nota_sequencial", ""),
                 formatos.para_data(n.get("data_emissao")),
                 competencia_normalizada(n.get("competencia"), n.get("data_emissao")),
                 (n.get("status") or STATUS_VALIDA).lower(),
                 _chave_obra(n.get("obra_codigo")),
                 n.get("tomador_nome", ""),
                 numero(n.get("valor_total")),
                 numero(n.get("valor_liquido_previsto")),
                 numero(n.get("valor_recebido")),
                 formatos.para_data(n.get("data_recebimento")),
                 json.dumps(n, ensure_ascii=False)))
        for codigo, dados in obras.items():
            conn.execute(
                "INSERT INTO analisesps.faturamento_obra (codigo, dados) "
                "VALUES (?, ?::jsonb)", (codigo, json.dumps(dados, ensure_ascii=False)))
        conn.commit()
        _meta_gravar(conn, CHAVE_META, agora().isoformat())
    logger.info("Faturamento: %d nota(s) e %d código(s) de obra carregados.",
                len(notas), len(obras))
    return {"notas": len(notas), "obras": len(obras), "avisos": avisos,
            "periodos_da_protocolos": com_periodo,
            "repetidas": contagem.get("repetidas", 0),
            "mesmo_numero": contagem.get("mesmo_numero", 0)}


# ---------------------------------------------------------------------------
# As notas ANTIGAS (09/10/2026): *"as notas anteriores, como faço para importar
# elas? Não estão aparecendo."*
#
# Quem leva as notas antigas da "Notas BWS" para a "Base Faturamento" é a
# CONSOLIDAÇÃO DO EMISSOR (`emissaonf/base_faturamento.consolidar`) — a regra é
# dele, e não é copiada aqui. Pela tela do emissor ela roda um lote por clique
# (com o token do link na URL); daqui ela roda TODOS os lotes, no processo
# separado, sem prender o serviço. Não apaga nada e não emite nada: só lê as
# abas antigas e escreve na aba nova. Rodar de novo não duplica.
# ---------------------------------------------------------------------------
def _emissor():
    """Os módulos do emissor que guardam as regras da base e do Omie.

    O emissor se importa de forma PLANA (`import base_faturamento`), como
    scripts — ver `emissaonf/README.md`. A pasta dele entra no caminho, como o
    web.py dele faz. As regras NÃO são copiadas para cá: são dele."""
    import os
    import sys
    pasta = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "emissaonf"))
    if pasta not in sys.path:
        sys.path.insert(0, pasta)
    import base_faturamento as bfat
    import omie as emissor_omie
    import omie_conferencia as ocf
    return bfat, emissor_omie, ocf


LOTE_DA_IMPORTACAO = 1000
MAX_RODADAS = 20


def importar_antigas(anotar=None) -> dict:
    from .credenciais import cliente, com_retry

    anotar = anotar or (lambda *a, **k: None)
    bfat, _, _ = _emissor()

    planilha = com_retry(lambda: cliente().open_by_key(PLANILHA_NOTAS))
    gravadas, r = 0, {"faltam": None}
    for rodada in range(1, MAX_RODADAS + 1):
        anotar("trazendo as notas antigas para a Base Faturamento",
               f"lote {rodada}" + (f" — faltam {r['faltam']}" if r["faltam"] else ""))
        r = bfat.consolidar(planilha, limite=LOTE_DA_IMPORTACAO)
        gravadas += r["gravadas"]
        logger.info("Faturamento: lote %d da importação — %d gravada(s), faltam %d.",
                    rodada, r["gravadas"], r["faltam"])
        if not r["faltam"] or not r["gravadas"]:
            break
    return {"gravadas": gravadas, "faltam": r["faltam"] or 0,
            "total_na_base": r.get("total_na_base")}


# ---------------------------------------------------------------------------
# CONFERIR OS TÍTULOS NO OMIE (09/10/2026) — *"a gente precisa poder fazer
# aquela consulta do título ao Omie, para compatibilizar"*.
#
# É a "Conferir" da tela do emissor (`/emissao/omie`), trazida para cá: as
# regras são as DELE (`emissaonf/omie_conferencia.py`) e não são copiadas —
#
#   - um título cobre VÁRIAS notas: o valor do título é RATEADO entre elas pelo
#     valor bruto, fechando ao centavo;
#   - só o que foi RETIDO entra na soma; cancelada e substituída ficam fora;
#   - nota sem tributo registrado não é "divergente": é "falta equalizar".
#
# ⚠️ AQUI SÓ SE LÊ O OMIE. O que se escreve é na base ("Base Faturamento"): os
# campos `omie_*`, a data da conferência e as duas divergências — exatamente o
# que a "Conferir" do emissor escreve. EQUALIZAR (gravar no Omie) continua só
# na tela do emissor, com a confirmação marcada e a lista do que vai mudar.
#
# A consulta usa o cliente do painel (`OmieClient`), que já sabe esperar
# quando o Omie pede pausa — com a MESMA credencial (OMIE_KEY/OMIE_SECRET do
# ambiente, que o emissor também prefere à aba Credenciais).
# ---------------------------------------------------------------------------
# Num processo separado, sozinho, mas não sem fim: o Omie tem cota, e a base
# tem milhares de títulos. Primeiro os nunca conferidos; depois os mais velhos.
MAX_TITULOS_POR_RODADA = 800
GRAVAR_A_CADA = 40


def _consultar_titulo(cliente_omie, codigo: str) -> dict:
    from app.apps.painel.sync.omie_client import URL_CONTARECEBER
    _, emissor_omie, _ = _emissor()
    resposta = cliente_omie._call(URL_CONTARECEBER, "ConsultarContaReceber",
                                  {"codigo_lancamento_integracao": codigo})
    return emissor_omie.ler_titulo(resposta or {})


def conferir_grupo(notas: list[dict], titulo: dict) -> dict:
    """Escreve nas linhas da base o que o Omie tem (rateado) e diz o que não
    bate. Não fala com ninguém: recebe o título já consultado."""
    bfat, _, ocf = _emissor()
    conferivel = ocf.tem_tributos_declarados(notas)
    desejado = ocf.somar_tributos(notas)
    fora = ocf.precisa_equalizar(titulo, desejado) if conferivel else []
    partes = ocf.ratear_titulo(notas, titulo)
    for d in notas:
        ocf.aplicar_no_registro(d, titulo, partes.get(bfat._txt(d.get("nota_numero"))))
    return {"conferivel": conferivel, "fora": fora, "desejado": desejado}


def _ordem_da_conferencia(notas: list[dict]):
    """Nunca conferido vem primeiro; depois, o conferido há mais tempo."""
    import datetime as dt
    momentos = []
    for d in notas:
        texto = str(d.get("omie_conferido_em") or "").strip()
        if not texto:
            return (0, dt.datetime.min)
        try:
            momentos.append(dt.datetime.strptime(texto, "%d/%m/%Y %H:%M"))
        except ValueError:
            return (0, dt.datetime.min)
    return (1, min(momentos) if momentos else dt.datetime.min)


def _gravar_no_banco(registros: list[dict]) -> None:
    """A tela mostra a conferência sem esperar a próxima carga da planilha."""
    from .db import conexao
    with conexao() as conn:
        for d in registros:
            dados = {k: v for k, v in d.items() if not str(k).startswith("_")}
            # Período vazio na aba não apaga o que veio da Protocolos.
            for k in ("medicao_periodo_ini", "medicao_periodo_fim"):
                if not str(dados.get(k) or "").strip():
                    dados.pop(k, None)
            # MESCLA (`||`): o que a carga guardou só para si (`_chave`,
            # `_linha_base`) fica. E acha a nota pela LINHA da aba — o número
            # pode se repetir (`separar_repetidas`).
            cur = conn.execute(
                "UPDATE analisesps.faturamento_nota SET dados = dados || ?::jsonb "
                " WHERE dados->>'_linha_base' = ?",
                (json.dumps(dados, ensure_ascii=False), str(d.get("_linha") or "")))
            if not cur.rowcount:
                # carga de antes da `_linha_base`: pelo número
                conn.execute(
                    "UPDATE analisesps.faturamento_nota SET dados = dados || ?::jsonb "
                    " WHERE nota_numero = ?",
                    (json.dumps(dados, ensure_ascii=False),
                     str(d.get("nota_numero") or "")))
        conn.commit()


def conferir_no_omie(anotar=None, cliente_omie=None, planilha=None) -> dict:
    """Confere no Omie os títulos da base inteira (no processo separado)."""
    from app.apps.painel.sync.omie_client import OmieAPIError, OmieBloqueada, OmieClient
    from .credenciais import cliente, com_retry

    anotar = anotar or (lambda *a, **k: None)
    bfat, _, ocf = _emissor()
    planilha = planilha or com_retry(lambda: cliente().open_by_key(PLANILHA_NOTAS))
    ws = bfat._ws(planilha)
    anotar("lendo a Base Faturamento", "")
    linhas = com_retry(lambda: bfat.ler_linhas(ws))
    # A mesma nota escrita duas vezes somaria o tributo dela duas vezes no
    # título: fica uma, como na tela.
    linhas = separar_repetidas(linhas)
    grupos = ocf.agrupar_por_titulo(linhas)
    sem_codigo = len(linhas) - sum(len(v) for v in grupos.values())
    ordem = sorted(grupos.items(), key=lambda kv: _ordem_da_conferencia(kv[1]))
    da_vez = ordem[:MAX_TITULOS_POR_RODADA]
    cli = cliente_omie or OmieClient.de_ambiente()

    r = {"titulos": len(grupos), "conferidos": 0, "divergentes": 0,
         "sem_tributo": 0, "nao_achados": 0, "erros": 0, "sem_codigo": sem_codigo,
         "faltam": max(len(ordem) - len(da_vez), 0), "bloqueio": "", "gravadas": 0}
    pendentes = []

    def gravar():
        if pendentes:
            r["gravadas"] += com_retry(lambda: bfat.gravar_lote(ws, pendentes))
            _gravar_no_banco(pendentes)
            pendentes.clear()

    for i, (codigo, notas) in enumerate(da_vez, start=1):
        anotar("conferindo os títulos no Omie", f"{i} de {len(da_vez)}")
        try:
            titulo = _consultar_titulo(cli, codigo)
        except OmieBloqueada as e:
            # Insistir prolonga o bloqueio: para aqui, guarda o que já foi.
            r["bloqueio"] = str(e)
            r["faltam"] += len(da_vez) - i + 1
            break
        except OmieAPIError as e:
            if getattr(e, "definitivo", False):
                r["nao_achados"] += 1
            else:
                r["erros"] += 1
            logger.info("Faturamento: título %s não conferido: %s", codigo, e)
            continue
        except Exception:  # noqa: BLE001 — um título ruim não para os outros
            r["erros"] += 1
            logger.exception("Faturamento: falha ao conferir o título %s", codigo)
            continue
        c = conferir_grupo(notas, titulo)
        r["conferidos"] += 1
        if not c["conferivel"]:
            r["sem_tributo"] += 1
        elif c["fora"]:
            r["divergentes"] += 1
        pendentes.extend(notas)
        if len(pendentes) >= GRAVAR_A_CADA:
            gravar()
    gravar()
    logger.info("Faturamento: conferência no Omie — %s", r)
    return r


def conferir_uma_no_omie(numero: str, cliente_omie=None, planilha=None) -> dict:
    """Confere AGORA o título de uma nota — o botão da ficha.

    Lê só as linhas daquele título na aba (não a aba inteira) e uma vez o Omie.
    As notas irmãs (mesmo título) entram junto: o rateio é entre elas."""
    from app.apps.painel.sync.omie_client import OmieAPIError, OmieBloqueada
    from .conciliacao_omie import _cliente as cliente_de_tela
    from .credenciais import cliente, com_retry
    from .db import consultar

    bfat, _, _ = _emissor()
    nota = uma(numero)
    if not nota:
        return {"ok": False, "erro": "Nota não encontrada."}
    codigo = str(nota.get("omie_codigo") or "").strip()
    if not codigo:
        return {"ok": False, "erro": (
            "Esta nota não tem o código do título no Omie na base. Ele vem da aba "
            "\"Protocolos\" (por obra e medição) — sem ele não há título para consultar.")}
    # As notas do mesmo título, cada uma com a SUA linha na aba (o número pode
    # se repetir — `separar_repetidas`; a linha não).
    irmas = {str(l[1]): str(l[0]) for l in consultar(
        "SELECT dados->>'nota_numero', coalesce(dados->>'_linha_base', '') "
        "  FROM analisesps.faturamento_nota "
        " WHERE dados->>'omie_codigo_integracao' = ?", (codigo,))}

    planilha = planilha or com_retry(lambda: cliente().open_by_key(PLANILHA_NOTAS))
    ws = bfat._ws(planilha)
    if "" in irmas:
        # carga de antes da `_linha_base`: acha pelo número
        mapa = com_retry(lambda: bfat.numeros_na_base(ws))
        irmas = {str(mapa[n]): n for n in irmas.values() if n in mapa}
    linhas_da_aba = sorted(int(l) for l in irmas if l)
    if not linhas_da_aba:
        return {"ok": False, "erro": "A nota não foi achada na aba \"Base Faturamento\" "
                                     "— aperte \"Atualizar da planilha\" e tente de novo."}
    ultima_coluna = bfat._col(len(bfat.CAB) - 1)
    blocos = com_retry(lambda: ws.batch_get(
        [f"A{l}:{ultima_coluna}{l}" for l in linhas_da_aba]))
    registros = []
    for linha, bloco in zip(linhas_da_aba, blocos):
        valores = list(bloco[0]) if bloco else []
        d = {nome: bfat._txt(valores[i]) if i < len(valores) else ""
             for i, nome in enumerate(bfat.CAB)}
        d["_linha"] = linha
        if d.get("nota_numero") != irmas.get(str(linha)):
            # Alguém mexeu na ordem da aba depois da carga: regravar aqui poria
            # o resultado na nota errada.
            return {"ok": False, "erro": "A aba \"Base Faturamento\" mudou desde a "
                    "última carga — aperte \"Atualizar da planilha\" e tente de novo."}
        registros.append(d)

    try:
        titulo = _consultar_titulo(cliente_omie or cliente_de_tela(), codigo)
    except OmieBloqueada as e:
        return {"ok": False, "erro": str(e)}
    except OmieAPIError as e:
        return {"ok": False, "erro": (
            f"O título {codigo} não foi achado no Omie." if getattr(e, "definitivo", False)
            else f"O Omie não respondeu: {str(e)[:200]}")}
    c = conferir_grupo(registros, titulo)
    com_retry(lambda: bfat.gravar_lote(ws, registros))
    _gravar_no_banco(registros)
    return {"ok": True, "codigo": codigo, "notas": len(registros),
            "conferivel": c["conferivel"], "fora": c["fora"],
            "valor_titulo": titulo.get("valor_titulo"),
            "numero_documento": titulo.get("numero_documento", "")}


# ---------------------------------------------------------------------------
# O que a tela pergunta
# ---------------------------------------------------------------------------
_JUNTA_OBRA = (" FROM analisesps.faturamento_nota n "
               " LEFT JOIN analisesps.faturamento_obra o ON o.codigo = n.obra_codigo ")


def _where(f: dict, com_datas: bool = True) -> tuple[str, list]:
    condicoes, params = ["TRUE"], []
    competencias = [c for c in (f.get("competencias") or []) if str(c).strip()]
    if competencias:
        # Competência marcada manda no período: a medição de 09/2024 faturada
        # em 10/2024 não pode sumir porque a emissão ficou fora das datas.
        com_datas = False
        condicoes.append(f"n.competencia IN ({', '.join('?' for _ in competencias)})")
        params += competencias
    if com_datas and f.get("de"):
        condicoes.append("n.data_emissao >= ?")
        params.append(f["de"])
    if com_datas and f.get("ate"):
        condicoes.append("n.data_emissao <= ?")
        params.append(f["ate"])
    if f.get("status", STATUS_VALIDA) != "todas":
        condicoes.append("n.status = ?")
        params.append(f.get("status") or STATUS_VALIDA)
    obras = [_chave_obra(o) for o in (f.get("obras") or []) if str(o).strip()]
    if obras:
        condicoes.append(f"n.obra_codigo IN ({', '.join('?' for _ in obras)})")
        params += obras
    empresas = [e for e in (f.get("empresas") or []) if str(e).strip()]
    if empresas:
        condicoes.append("coalesce(nullif(o.dados->>'scp',''), o.dados->>'empresa', '') "
                         f"IN ({', '.join('?' for _ in empresas)})")
        params += empresas
    # RETENÇÃO POR TRIBUTO (09/10/2026 — *"às vezes preciso saber quais notas
    # têm retenção de INSS e quais não têm"*). A marca é o "retém" da base:
    # S = retido, N = não retido, vazio = não informado (nota antiga ainda não
    # equalizada — que não é "sem retenção").
    for tributo, escolha in (f.get("retencoes") or {}).items():
        if tributo not in TRIBUTOS:
            continue
        marca = f"upper(coalesce(n.dados->>'retem_{tributo}', ''))"
        if escolha == "com":
            condicoes.append(f"{marca} LIKE 'S%'")
        elif escolha == "sem":
            condicoes.append(f"{marca} LIKE 'N%'")
        elif escolha == "vazio":
            condicoes.append(f"{marca} = ''")
    tributacoes = [t for t in (f.get("tributacoes") or []) if str(t).strip()]
    if tributacoes:
        condicoes.append("coalesce(o.dados->>'tributacao', '') "
                         f"IN ({', '.join('?' for _ in tributacoes)})")
        params += tributacoes
    if f.get("recebimento") == "recebidas":
        condicoes.append("n.data_recebimento IS NOT NULL")
    elif f.get("recebimento") == "a_receber":
        condicoes.append("n.data_recebimento IS NULL")
    if f.get("busca"):
        termo = "%" + str(f["busca"]).strip().lower() + "%"
        condicoes.append("(lower(n.nota_numero) LIKE ? OR lower(n.nota_sequencial) LIKE ? "
                         " OR lower(n.tomador_nome) LIKE ? OR lower(n.obra_codigo) LIKE ? "
                         " OR lower(coalesce(n.dados->>'discriminacao','')) LIKE ?)")
        params += [termo] * 5
    return " WHERE " + " AND ".join(condicoes), params


def resumo(f: dict) -> dict:
    from .db import consultar_um
    where, params = _where(f)
    linha = consultar_um(
        "SELECT count(*), coalesce(sum(n.valor_total), 0), "
        "       coalesce(sum(n.valor_liquido), 0), "
        "       coalesce(sum(n.valor_recebido) FILTER (WHERE n.data_recebimento IS NOT NULL), 0), "
        "       count(*) FILTER (WHERE n.data_recebimento IS NULL), "
        "       coalesce(sum(coalesce(n.valor_liquido, n.valor_total)) "
        "                FILTER (WHERE n.data_recebimento IS NULL), 0) "
        + _JUNTA_OBRA + where, tuple(params))
    quantidade, bruto, liquido, recebido, abertas, a_receber = linha
    return {"quantidade": int(quantidade or 0), "bruto": bruto, "liquido": liquido,
            "recebido": recebido, "abertas": int(abertas or 0), "a_receber": a_receber}


AGRUPAMENTOS = {"mes": ("month", "Mês"), "trimestre": ("quarter", "Trimestre"),
                "ano": ("year", "Ano")}


def _rotulo_do_periodo(inicio, agrupar: str) -> str:
    if agrupar == "ano":
        return f"{inicio.year}"
    if agrupar == "trimestre":
        return f"{(inicio.month - 1) // 3 + 1}º tri/{inicio.year}"
    return f"{inicio.month:02d}/{inicio.year}"


def _seguinte(inicio, agrupar: str):
    meses = {"mes": 1, "trimestre": 3, "ano": 12}[agrupar]
    total = inicio.year * 12 + inicio.month - 1 + meses
    return inicio.replace(year=total // 12, month=total % 12 + 1, day=1)


def por_periodo(f: dict, agrupar: str = "mes") -> list[dict]:
    """O faturamento por período (mês, trimestre ou ano, pela data de emissão):
    notas, faturado, líquido previsto, recebido e a receber.

    ⚠️ PERÍODO SEM NOTA APARECE COM ZERO: pular faria dois meses vizinhos
    parecerem seguidos, e o "buraco" é justamente a informação."""
    import datetime as dt
    from .db import consultar
    agrupar = agrupar if agrupar in AGRUPAMENTOS else "mes"
    trunc = AGRUPAMENTOS[agrupar][0]
    where, params = _where(f)
    linhas = consultar(
        f"SELECT date_trunc('{trunc}', n.data_emissao)::date AS ini, count(*), "
        "       coalesce(sum(n.valor_total), 0), coalesce(sum(n.valor_liquido), 0), "
        "       coalesce(sum(n.valor_recebido) FILTER (WHERE n.data_recebimento IS NOT NULL), 0), "
        "       coalesce(sum(coalesce(n.valor_liquido, n.valor_total)) "
        "                FILTER (WHERE n.data_recebimento IS NULL), 0) "
        + _JUNTA_OBRA + where + " AND n.data_emissao IS NOT NULL "
        " GROUP BY 1 ORDER BY 1", tuple(params))
    achados = {ini: (q, b, l, r, a) for ini, q, b, l, r, a in linhas}
    if not achados:
        return []
    saida, atual, fim = [], min(achados), max(achados)
    while atual <= fim and len(saida) < 400:
        q, b, l, r, a = achados.get(atual, (0, 0, 0, 0, 0))
        saida.append({"inicio": atual,
                      "fim": _seguinte(atual, agrupar) - dt.timedelta(days=1),
                      "rotulo": _rotulo_do_periodo(atual, agrupar),
                      "quantidade": int(q), "bruto": b, "liquido": l,
                      "recebido": r, "a_receber": a})
        atual = _seguinte(atual, agrupar)
    return saida


def listar(f: dict, pagina: int = 1) -> list[dict]:
    from .db import consultar
    where, params = _where(f)
    pagina = max(1, int(pagina or 1))
    linhas = consultar(
        "SELECT n.dados, o.dados, n.data_emissao, n.valor_total, n.valor_liquido, "
        "       n.valor_recebido, n.data_recebimento, n.status "
        + _JUNTA_OBRA + where +
        " ORDER BY n.data_emissao DESC NULLS LAST, n.nota_sequencial DESC "
        " LIMIT ? OFFSET ?", tuple(params) + (POR_PAGINA, (pagina - 1) * POR_PAGINA))
    return [_linha_da_tela(*l) for l in linhas]


def uma(numero: str) -> dict | None:
    from .db import consultar
    linhas = consultar(
        "SELECT n.dados, o.dados, n.data_emissao, n.valor_total, n.valor_liquido, "
        "       n.valor_recebido, n.data_recebimento, n.status "
        + _JUNTA_OBRA + " WHERE n.nota_numero = ?", (str(numero),))
    return _linha_da_tela(*linhas[0]) if linhas else None


def opcoes() -> dict:
    """As listas dos filtros: obras e empresas que aparecem nas notas."""
    from .db import consultar
    obras = [o for (o,) in consultar(
        "SELECT DISTINCT obra_codigo FROM analisesps.faturamento_nota "
        " WHERE obra_codigo <> '' ORDER BY 1")]
    empresas = [e for (e,) in consultar(
        "SELECT DISTINCT coalesce(nullif(o.dados->>'scp',''), o.dados->>'empresa', '') "
        + _JUNTA_OBRA + " WHERE coalesce(nullif(o.dados->>'scp',''), "
        "                              o.dados->>'empresa', '') <> '' ORDER BY 1")]
    competencias = [c for (c,) in consultar(
        "SELECT DISTINCT competencia FROM analisesps.faturamento_nota "
        " WHERE competencia <> '' ORDER BY 1 DESC")]
    tributacoes = [t for (t,) in consultar(
        "SELECT DISTINCT coalesce(o.dados->>'tributacao', '') "
        + _JUNTA_OBRA + " WHERE coalesce(o.dados->>'tributacao', '') <> '' ORDER BY 1")]
    return {"obras": obras, "empresas": empresas,
            # (valor, rótulo): o filtro manda "2026-09" e mostra "09/2026"
            "competencias": [(c, competencia_br(c)) for c in competencias],
            "tributacoes": tributacoes}


def carregado_em():
    from .db import consultar_um
    try:
        linha = consultar_um("SELECT valor FROM analisesps.meta WHERE chave = ?",
                             (CHAVE_META,))
    except Exception:  # noqa: BLE001
        return None
    return linha[0] if linha else None


def total_no_banco() -> int:
    """Quantas notas a última carga trouxe, SEM filtro nenhum. É o que separa
    "a aba está vazia" de "nenhuma nota neste filtro" (09/10/2026 — o dono viu
    "notas trazidas às 18:38" e a tela vazia, sem saber qual dos dois era)."""
    from .db import consultar_um
    try:
        linha = consultar_um("SELECT count(*) FROM analisesps.faturamento_nota")
    except Exception:  # noqa: BLE001
        return 0
    return int(linha[0] or 0) if linha else 0


TRIBUTOS = ("pis", "cofins", "ir", "csll", "inss", "iss")
LINKS = (("link_nfse_nacional", "DANFSe (nacional)"),
         ("link_nfse_municipal", "NFS-e (municipal)"),
         ("link_xml", "XML"), ("link_recibo", "Recibo"))


def _linha_da_tela(dados, obra, data_emissao, valor_total, valor_liquido,
                   valor_recebido, data_recebimento, status) -> dict:
    """A nota como a tela usa: os campos da base + o que veio da obra."""
    dados = dados if isinstance(dados, dict) else json.loads(dados or "{}")
    obra = obra if isinstance(obra, dict) else json.loads(obra or "{}")
    from .formatos import para_data, para_numero
    tributos = []
    for t in TRIBUTOS:
        bruto = dados.get(t, "")
        tributos.append({
            "nome": t.upper(),
            # ⚠️ vazio fica VAZIO: "não se sabe" não é zero
            "valor": para_numero(bruto) if str(bruto).strip() else None,
            "retem": str(dados.get("retem_" + t, "")).strip().upper(),
            "omie": (para_numero(dados.get("omie_" + t))
                     if str(dados.get("omie_" + t, "")).strip() else None),
        })
    empresa = obra.get("scp") or obra.get("empresa") or ""
    return {
        # A CHAVE da nota na tela (link da ficha): o número, ou "número-2"
        # quando outra nota já usa esse número (`separar_repetidas`).
        "chave": dados.get("_chave") or dados.get("nota_numero", ""),
        "numero_repetido": bool(dados.get("_chave")),
        "numero": dados.get("nota_numero", ""),
        "sequencial": dados.get("nota_sequencial", ""),
        "modelo": dados.get("modelo", ""),
        "chave": dados.get("chave_acesso", ""),
        "data_emissao": data_emissao,
        "competencia": competencia_br(competencia_normalizada(
            dados.get("competencia"), dados.get("data_emissao"))),
        "status": status,
        "observacao": dados.get("observacao", ""),
        "obra": dados.get("obra_codigo", ""),
        "medicao": dados.get("medicao_numero", ""),
        "periodo_ini": para_data(dados.get("medicao_periodo_ini")),
        "periodo_fim": para_data(dados.get("medicao_periodo_fim")),
        "periodo_da_protocolos": bool(dados.get("_periodo_da_protocolos")),
        "tomador": dados.get("tomador_nome", ""),
        "tomador_cnpj": dados.get("tomador_cnpj", ""),
        "valor_total": valor_total,
        "valor_liquido": valor_liquido,
        "valor_recebido": valor_recebido,
        "data_recebimento": data_recebimento,
        "banco_conta": dados.get("banco_conta", ""),
        "aliquota_iss": dados.get("aliquota_iss", ""),
        "tributos": tributos,
        "ibs": dados.get("ibs", ""), "cbs": dados.get("cbs", ""),
        "divergencia_tributos": dados.get("divergencia_tributos", ""),
        "omie_codigo": dados.get("omie_codigo_integracao", ""),
        "omie_conferido_em": dados.get("omie_conferido_em", ""),
        "omie_valor_titulo": (para_numero(dados.get("omie_valor_titulo"))
                              if str(dados.get("omie_valor_titulo", "")).strip() else None),
        "omie_numero_documento": dados.get("omie_numero_documento", ""),
        "divergencia_recebimento": dados.get("divergencia_recebimento", ""),
        "discriminacao": dados.get("discriminacao", ""),
        "link_card": dados.get("link_card", ""),
        "links": [(rotulo, dados.get(campo, "")) for campo, rotulo in LINKS
                  if str(dados.get(campo, "")).strip()],
        "empresa": empresa,
        "e_scp": bool(obra.get("scp")),
        "cliente": obra.get("cliente", ""),
        "contrato": obra.get("contrato", ""),
        "municipio": obra.get("municipio", ""),
        "tributacao": obra.get("tributacao", ""),
        "centro_custo": obra.get("centro_custo", ""),
    }
