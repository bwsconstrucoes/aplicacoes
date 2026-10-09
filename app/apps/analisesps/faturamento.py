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
def notas_das_linhas(valores: list) -> list[dict]:
    """As linhas da aba viram dicionários pelo NOME do cabeçalho. Linha sem
    número de nota fica de fora. Repetida: vale a PRIMEIRA (a regra da
    consolidação, para a base não mudar entre duas rodadas)."""
    if not valores:
        return []
    cabecalho = [str(c).strip() for c in valores[0]]
    vistas, saida = set(), []
    for linha in valores[1:]:
        dados = {nome: (str(linha[i]).strip() if i < len(linha) else "")
                 for i, nome in enumerate(cabecalho) if nome}
        numero = dados.get("nota_numero", "")
        if not numero or numero in vistas:
            continue
        vistas.add(numero)
        saida.append(dados)
    return saida


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
        notas = notas_das_linhas(com_retry(_aba(PLANILHA_NOTAS, ABA_BASE).get_all_values))
    except Exception as e:  # noqa: BLE001 — a frase vai para a tela
        raise RuntimeError(_explicar_aba(PLANILHA_NOTAS, ABA_BASE, e)) from e

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
                (n["nota_numero"], n.get("nota_sequencial", ""),
                 formatos.para_data(n.get("data_emissao")),
                 n.get("competencia", ""),
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
    return {"notas": len(notas), "obras": len(obras), "avisos": avisos}


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
LOTE_DA_IMPORTACAO = 1000
MAX_RODADAS = 20


def importar_antigas(anotar=None) -> dict:
    import os
    import sys
    from .credenciais import cliente, com_retry

    anotar = anotar or (lambda *a, **k: None)
    # O emissor se importa de forma PLANA (`import worker`), como scripts — ver
    # `emissaonf/README.md`. A pasta dele entra no caminho, como o web.py dele faz.
    pasta = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "emissaonf"))
    if pasta not in sys.path:
        sys.path.insert(0, pasta)
    import base_faturamento as bfat

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
# O que a tela pergunta
# ---------------------------------------------------------------------------
_JUNTA_OBRA = (" FROM analisesps.faturamento_nota n "
               " LEFT JOIN analisesps.faturamento_obra o ON o.codigo = n.obra_codigo ")


def _where(f: dict, com_datas: bool = True) -> tuple[str, list]:
    condicoes, params = ["TRUE"], []
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
    return {"obras": obras, "empresas": empresas}


def carregado_em():
    from .db import consultar_um
    try:
        linha = consultar_um("SELECT valor FROM analisesps.meta WHERE chave = ?",
                             (CHAVE_META,))
    except Exception:  # noqa: BLE001
        return None
    return linha[0] if linha else None


TRIBUTOS = ("pis", "cofins", "ir", "csll", "inss", "iss")
LINKS = (("link_nfse_nacional", "DANFSe (nacional)"),
         ("link_nfse_municipal", "NFS-e (municipal)"),
         ("link_xml", "XML"), ("link_recibo", "Recibo"))


def _linha_da_tela(dados, obra, data_emissao, valor_total, valor_liquido,
                   valor_recebido, data_recebimento, status) -> dict:
    """A nota como a tela usa: os campos da base + o que veio da obra."""
    dados = dados if isinstance(dados, dict) else json.loads(dados or "{}")
    obra = obra if isinstance(obra, dict) else json.loads(obra or "{}")
    from .formatos import para_numero
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
        "numero": dados.get("nota_numero", ""),
        "sequencial": dados.get("nota_sequencial", ""),
        "modelo": dados.get("modelo", ""),
        "chave": dados.get("chave_acesso", ""),
        "data_emissao": data_emissao,
        "competencia": dados.get("competencia", ""),
        "status": status,
        "observacao": dados.get("observacao", ""),
        "obra": dados.get("obra_codigo", ""),
        "medicao": dados.get("medicao_numero", ""),
        "periodo": " a ".join(p for p in (dados.get("medicao_periodo_ini", ""),
                                           dados.get("medicao_periodo_fim", "")) if p),
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
