# -*- coding: utf-8 -*-
"""
Cenários da prestação de contas — guardar um jeito de dividir, com nome.

O pedido do dono, em 22/09/2026, depois de olhar o que existia espalhado em
quatro telas:

    "Imagina você ter todo um trabalho de fazer um rateio, pá, pá, pá, de um
     jeito. Salvei, aí eu quero que seja salvo. Mas, e se de repente eu quiser
     um outro cenário? (…) Esse cenário eu vou utilizar 50% da mão de obra.
     Esse cenário eu vou utilizar o faturamento."

Um cenário guarda TUDO o que muda o resultado: a régua do rateio (mão de obra
ou faturamento), quanto de cada conta da matriz entra no bolo, a régua dos
juros, a taxa de administração e quem divide o quê. Trocar de cenário troca o
resultado inteiro — e o anterior continua lá, intacto, para comparar.

**Por que os percentuais são uma HIERARQUIA e não uma lista de regras:** a tela
antiga pedia um cadastro por regra, com nome, escopo e vigência. Dava trabalho,
e o dono disse isso com todas as letras. Aqui a conta da matriz herda o
percentual padrão do cenário; marcar um GRUPO sobrepõe; marcar uma CATEGORIA
sobrepõe o grupo; marcar um LANÇAMENTO sobrepõe a categoria. Configurar é
tocar em poucas linhas, não em todas.

Nada aqui calcula: isto lê e grava. A conta mora em `prestacao.py`.
"""
from __future__ import annotations

from .db import conexao, consultar, consultar_um

CRITERIOS = {
    "pessoal": "Mão de obra — o custo de pessoal de cada obra",
    "faturamento": "Faturamento — o que cada obra recebeu",
}

# Quantos meses somar para medir a participação de cada obra. Janela curta
# reage rápido e balança; janela longa é estável e demora a reagir.
JANELAS = {"1": "o próprio mês", "3": "3 meses", "12": "12 meses",
           "acumulado": "tudo desde o início"}

NIVEIS = ("grupo", "categoria", "lancamento")

_CAMPOS = ("id", "nome", "criterio", "janela", "pct_padrao", "juros_por_deficit",
           "juros_sem_deficit", "taxa_adm_pct", "medida", "observacao")

_SELECT = ("SELECT id, nome, criterio, janela, pct_padrao, juros_por_deficit, "
           "       juros_sem_deficit, taxa_adm_pct, medida, observacao "
           "  FROM cenario")


def _linha(t) -> dict:
    cenario = dict(zip(_CAMPOS, t))
    for campo in ("pct_padrao", "taxa_adm_pct"):
        cenario[campo] = float(cenario[campo] or 0)
    cenario["juros_por_deficit"] = int(cenario["juros_por_deficit"] or 0)
    return cenario


# ----------------------------------------------------------------------- ler
def listar() -> list[dict]:
    return [_linha(t) for t in consultar(_SELECT + " ORDER BY nome")]


def buscar(cenario_id) -> dict | None:
    t = consultar_um(_SELECT + " WHERE id = ?", [int(cenario_id)])
    return _linha(t) if t else None


def pesos(cenario_id) -> dict:
    """{nível: {chave: percentual}} — o que foi marcado neste cenário."""
    saida = {nivel: {} for nivel in NIVEIS}
    for nivel, chave, pct in consultar(
            "SELECT nivel, chave, pct FROM cenario_peso WHERE cenario_id = ?",
            [int(cenario_id)]):
        if nivel in saida:
            saida[nivel][chave] = float(pct or 0)
    return saida


def participacoes(cenario_id) -> list[dict]:
    """Quem divide, já com o nome do sócio — a tela nunca mostra número de
    cadastro para quem está lendo."""
    return [{"id": i, "obra": obra, "socio_id": s, "socio": nome,
             "tipo": tipo, "pct": float(pct or 0)}
            for i, obra, s, nome, tipo, pct in consultar(
                "SELECT p.id, p.obra, p.socio_id, s.nome, s.tipo, p.pct "
                "  FROM cenario_participacao p "
                "  JOIN socios s ON s.id = p.socio_id "
                " WHERE p.cenario_id = ? AND s.ativo = 1 "
                " ORDER BY p.obra, s.nome", [int(cenario_id)])]


def excluidas(cenario_id) -> list[str]:
    """O que fica FORA da análise: "obra:NOME" ou "projeto:NOME"."""
    return [i for (i,) in consultar(
        "SELECT item FROM cenario_excluida WHERE cenario_id = ? ORDER BY item",
        [int(cenario_id)])]


def completo(cenario_id) -> dict | None:
    """O cenário com tudo o que ele guarda — uma leitura só para a tela."""
    cenario = buscar(cenario_id)
    if not cenario:
        return None
    cenario["pesos"] = pesos(cenario_id)
    cenario["participacoes"] = participacoes(cenario_id)
    cenario["excluidas"] = excluidas(cenario_id)
    return cenario


# --------------------------------------------------------------------- gravar
def _limpar(valor, padrao: float = 0.0, teto: float = 100.0) -> float:
    """Percentual digitado por gente: vírgula vale, e fora da faixa é preso no
    limite em vez de virar erro na cara de quem está configurando."""
    try:
        v = float(str(valor).replace(",", ".").strip())
    except (TypeError, ValueError, AttributeError):
        return padrao
    return min(max(v, 0.0), teto)


def criar(nome: str, **campos) -> int:
    """Cria um cenário. Nome repetido é recusado pelo banco, de propósito:
    dois cenários com o mesmo nome tornam a comparação inútil."""
    nome = (nome or "").strip() or "Sem nome"
    with conexao() as conn:
        cur = conn.execute(
            "INSERT INTO cenario (nome, criterio, janela, pct_padrao, "
            "                     juros_por_deficit, juros_sem_deficit, "
            "                     taxa_adm_pct, medida, observacao) "
            "VALUES (?,?,?,?,?,?,?,?,?) RETURNING id",
            (nome,
             campos.get("criterio", "pessoal"),
             str(campos.get("janela", "1")),
             _limpar(campos.get("pct_padrao", 100), 100.0),
             1 if str(campos.get("juros_por_deficit", 1)) in ("1", "True", "on") else 0,
             campos.get("juros_sem_deficit", "sobra"),
             _limpar(campos.get("taxa_adm_pct", 1.5), 1.5),
             campos.get("medida", "comprometido"),
             (campos.get("observacao") or "").strip()))
        novo = cur.fetchone()[0]
        conn.commit()
    return int(novo)


def atualizar(cenario_id, **campos) -> None:
    """Grava só os campos informados — a tela salva um bloco de cada vez."""
    permitidos = {
        "nome": lambda v: (str(v).strip() or "Sem nome"),
        "criterio": lambda v: (v if v in CRITERIOS else "pessoal"),
        "janela": lambda v: (str(v) if str(v) in JANELAS else "1"),
        "pct_padrao": lambda v: _limpar(v, 100.0),
        "juros_por_deficit": lambda v: 1 if str(v) in ("1", "True", "on") else 0,
        "juros_sem_deficit": lambda v: (v if v in ("sobra", "estrutura") else "sobra"),
        "taxa_adm_pct": lambda v: _limpar(v, 1.5),
        "medida": lambda v: ("executado" if v == "executado" else "comprometido"),
        "observacao": lambda v: (str(v) or "").strip(),
    }
    pares = [(c, f(campos[c])) for c, f in permitidos.items() if c in campos]
    if not pares:
        return
    sql = ("UPDATE cenario SET " + ", ".join(f"{c} = ?" for c, _ in pares)
           + ", atualizado_em = now() WHERE id = ?")
    with conexao() as conn:
        conn.execute(sql, [v for _c, v in pares] + [int(cenario_id)])
        conn.commit()


def marcar_peso(cenario_id, nivel: str, chave: str, pct) -> None:
    """Marca (ou desmarca) quanto de uma conta entra no bolo.

    Vazio APAGA a marcação em vez de gravar zero: são coisas diferentes — zero
    é "esta conta não se divide", vazio é "esta conta segue o nível de cima"."""
    if nivel not in NIVEIS or not (chave or "").strip():
        return
    chave = chave.strip()
    vazio = pct is None or str(pct).strip() == ""
    with conexao() as conn:
        if vazio:
            conn.execute("DELETE FROM cenario_peso "
                         " WHERE cenario_id = ? AND nivel = ? AND chave = ?",
                         (int(cenario_id), nivel, chave))
        else:
            conn.execute(
                "INSERT INTO cenario_peso (cenario_id, nivel, chave, pct) "
                "VALUES (?,?,?,?) ON CONFLICT (cenario_id, nivel, chave) "
                "DO UPDATE SET pct = excluded.pct",
                (int(cenario_id), nivel, chave, _limpar(pct)))
        conn.commit()


def marcar_pesos(cenario_id, nivel: str, chaves, pct) -> int:
    """O mesmo, para vários de uma vez — é assim que a tela marca em lote.

    O dono foi explícito: "não adianta uma coisa que eu tenho que digitar coisa
    por coisa". Marcar trinta lançamentos com 0% tem de ser um gesto só."""
    quantos = 0
    for chave in chaves:
        if (chave or "").strip():
            marcar_peso(cenario_id, nivel, chave, pct)
            quantos += 1
    return quantos


def salvar_participacao(cenario_id, socio_id, pct, obra: str = "") -> None:
    with conexao() as conn:
        conn.execute(
            "INSERT INTO cenario_participacao (cenario_id, obra, socio_id, pct) "
            "VALUES (?,?,?,?) ON CONFLICT (cenario_id, obra, socio_id) "
            "DO UPDATE SET pct = excluded.pct",
            (int(cenario_id), (obra or "").strip(), int(socio_id), _limpar(pct)))
        conn.commit()


def apagar_participacao(participacao_id) -> None:
    with conexao() as conn:
        conn.execute("DELETE FROM cenario_participacao WHERE id = ?",
                     [int(participacao_id)])
        conn.commit()


def excluir(cenario_id, item: str) -> None:
    """Tira uma obra ("obra:NOME") ou um projeto inteiro ("projeto:NOME") da
    análise deste cenário. Repetir não dá erro."""
    item = (item or "").strip()
    if not (item.startswith("obra:") or item.startswith("projeto:")) or len(item) < 6:
        return
    with conexao() as conn:
        conn.execute("INSERT INTO cenario_excluida (cenario_id, item) VALUES (?,?) "
                     "ON CONFLICT DO NOTHING", (int(cenario_id), item))
        conn.commit()


def reincluir(cenario_id, item: str) -> None:
    with conexao() as conn:
        conn.execute("DELETE FROM cenario_excluida WHERE cenario_id = ? AND item = ?",
                     (int(cenario_id), (item or "").strip()))
        conn.commit()


def duplicar(cenario_id, nome: str) -> int:
    """Copia um cenário inteiro com outro nome.

    É o que torna barato perguntar "e se fosse faturamento?": parte-se do que
    já está pronto e troca-se uma coisa só, em vez de configurar tudo de novo.
    """
    origem = buscar(cenario_id)
    if not origem:
        raise ValueError("Cenário não encontrado.")
    novo = criar(nome, **{c: origem[c] for c in _CAMPOS if c not in ("id", "nome")})
    with conexao() as conn:
        conn.execute(
            "INSERT INTO cenario_peso (cenario_id, nivel, chave, pct) "
            "SELECT ?, nivel, chave, pct FROM cenario_peso WHERE cenario_id = ?",
            (novo, int(cenario_id)))
        conn.execute(
            "INSERT INTO cenario_participacao (cenario_id, obra, socio_id, pct) "
            "SELECT ?, obra, socio_id, pct FROM cenario_participacao "
            " WHERE cenario_id = ?",
            (novo, int(cenario_id)))
        conn.execute(
            "INSERT INTO cenario_excluida (cenario_id, item) "
            "SELECT ?, item FROM cenario_excluida WHERE cenario_id = ?",
            (novo, int(cenario_id)))
        conn.commit()
    return novo


def apagar(cenario_id) -> None:
    """Apaga o cenário e tudo o que pende dele (o banco cuida do resto)."""
    with conexao() as conn:
        conn.execute("DELETE FROM cenario WHERE id = ?", [int(cenario_id)])
        conn.commit()
