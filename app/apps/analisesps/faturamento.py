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
POR_PAGINA = 100
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
    if f.get("obra"):
        condicoes.append("n.obra_codigo = ?")
        params.append(_chave_obra(f["obra"]))
    if f.get("empresa"):
        condicoes.append("coalesce(nullif(o.dados->>'scp',''), o.dados->>'empresa', '') = ?")
        params.append(f["empresa"])
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


def evolucao(f: dict, meses: int = 13) -> list[dict]:
    """O faturamento mês a mês (pela data de emissão): o bruto e o recebido.

    As datas do filtro limitam a janela; sem elas, os últimos `meses` meses."""
    from .db import consultar
    where, params = _where(f)
    linhas = consultar(
        "SELECT to_char(date_trunc('month', n.data_emissao), 'YYYY-MM') AS mes, "
        "       count(*), coalesce(sum(n.valor_total), 0), "
        "       coalesce(sum(n.valor_recebido) FILTER (WHERE n.data_recebimento IS NOT NULL), 0) "
        + _JUNTA_OBRA + where + " AND n.data_emissao IS NOT NULL "
        " GROUP BY 1 ORDER BY 1 DESC LIMIT ?", tuple(params) + (meses,))
    achados = {m: {"mes": m, "quantidade": int(q), "bruto": b, "recebido": r}
               for m, q, b, r in linhas}
    if not achados:
        return []
    # ⚠️ MÊS SEM NOTA APARECE COM ZERO: pular o mês faria duas barras vizinhas
    # parecerem meses seguidos, e o "buraco" é justamente a informação.
    ano, mes = map(int, min(achados).split("-"))
    fim = max(achados)
    saida = []
    while True:
        chave = f"{ano:04d}-{mes:02d}"
        saida.append(achados.get(chave) or {"mes": chave, "quantidade": 0,
                                            "bruto": 0, "recebido": 0})
        if chave >= fim:
            break
        ano, mes = (ano + 1, 1) if mes == 12 else (ano, mes + 1)
    return saida[-meses:]


def por_obra(f: dict, quantos: int = 12) -> list[dict]:
    from .db import consultar
    where, params = _where(f)
    linhas = consultar(
        "SELECT n.obra_codigo, max(coalesce(o.dados->>'cliente','')), count(*), "
        "       coalesce(sum(n.valor_total), 0) "
        + _JUNTA_OBRA + where + " GROUP BY 1 ORDER BY 4 DESC LIMIT ?",
        tuple(params) + (quantos,))
    return [{"obra": o or "(sem obra)", "cliente": c, "quantidade": int(q), "bruto": b}
            for o, c, q, b in linhas]


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
