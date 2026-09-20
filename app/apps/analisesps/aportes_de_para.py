# -*- coding: utf-8 -*-
"""
De-para dos aportes: do nome que o dono usa para o código que o OMIE exige.

⚠️ ESTE ARQUIVO EXISTE POR UMA FRASE DO BRIEFING, e ela vale a pena repetir:
*"Não escrever código nenhum fixo no programa. Eu mexo no plano financeiro;
código chumbado vira lançamento errado silencioso no dia em que eu mexer."*

Lançamento errado silencioso é o pior defeito que esta funcionalidade pode
ter: o título entra no OMIE, a tela diz "gravado", e o número aparece no
lugar errado de um relatório que ninguém confere linha a linha.

Então o caminho é: **descobrir pela descrição**, no espelho do OMIE que o
painel já mantém no banco (schema `painel`), e **guardar o resultado para o
dono conferir**. Descrição que não aparece, ou que aparece mais de uma vez,
PARA e pede a decisão dele — nunca escolhe uma por conta própria.

De onde sai cada coisa (tudo já existe, nada foi criado para isto):

  · `painel.cat`               — plano financeiro: código, descrição e a marca
                                 de transferência. É daqui que sai o código
                                 da categoria.
  · `painel.contas_correntes`  — as contas, com o código que o OMIE usa.
  · `painel.clientes`          — fornecedores (e clientes; vêm juntos).
  · `painel.rateio`            — de onde sai a lista de obras (departamentos):
                                 não há catálogo de departamentos no espelho,
                                 e os que já foram usados em algum título são
                                 exatamente os que interessam.
  · `painel.titulos` + `painel.movimentos` — para a crítica de transferência,
                                 sem precisar consultar o OMIE.

⚠️ NADA AQUI DERRUBA TELA. O espelho do painel pode estar vazio (carga nunca
rodada) e a migração 018 pode não ter sido aplicada. Nos dois casos a tela
tem de abrir e DIZER o que falta — a tela de configuração é justamente a que
conserta o módulo quando algo quebra, e ela não pode ser a próxima a cair.
"""
from __future__ import annotations

import logging

from .aportes import CATEGORIAS, MATRIZ, PARCERIA, achatar

logger = logging.getLogger("analisesps.aportes_de_para")

PAPEIS = (MATRIZ, PARCERIA)

# Teto das listas que vão para a tela. A base tem milhares de fornecedores;
# mandar todos para um <select> trava o navegador e não ajuda ninguém.
MAX_LISTA = 300


class SemEspelho(Exception):
    """O espelho do OMIE não pôde ser lido — e a tela diz isso, em português."""


def _consultar(sql: str, params=()) -> list:
    from .db import consultar
    return consultar(sql, params)


# ---------------------------------------------------------------------------
# AS CONTAS
# ---------------------------------------------------------------------------
def contas_do_omie() -> list:
    """Todas as contas correntes do espelho, para o dono apontar quais são.

    Resposta à pergunta que ele mesmo fez no briefing: *"7011 e 22069 são
    número de conta bancária ou código de conta do OMIE? Me mostre a lista de
    contas com código e descrição para eu apontar quais são."* A lista traz os
    dois números justamente porque é olhando os dois que ele reconhece.
    """
    try:
        linhas = _consultar(
            "SELECT codigo, COALESCE(descricao, ''), COALESCE(numero_conta, ''),"
            "       COALESCE(inativa, '') "
            "  FROM painel.contas_correntes "
            " ORDER BY COALESCE(inativa, '') , descricao")
    except Exception as e:  # noqa: BLE001 — a tela tem de dizer o que houve
        logger.exception("Aportes: não consegui ler as contas correntes")
        raise SemEspelho(
            "Não consegui ler as contas do OMIE. Elas vêm da carga do painel; "
            "se ela nunca rodou, rode-a primeiro.") from e
    return [{"codigo": l[0], "descricao": l[1], "numero_conta": l[2],
             "inativa": str(l[3]).upper().startswith("S")} for l in linhas]


def contas_configuradas() -> dict:
    """{papel: {codigo, descricao}} — vazio quando ainda não foi apontado."""
    try:
        linhas = _consultar(
            "SELECT papel, codigo_conta, COALESCE(descricao, '') "
            "  FROM analisesps.aporte_conta")
    except Exception:  # noqa: BLE001 — migração 018 ainda não aplicada
        logger.exception("Aportes: não consegui ler o de-para das contas")
        return {}
    return {l[0]: {"codigo": l[1], "descricao": l[2]} for l in linhas if l[1]}


def guardar_conta(papel: str, codigo, quem: str = "") -> None:
    """Aponta qual conta do OMIE faz este papel. Código vazio desfaz."""
    if papel not in PAPEIS:
        raise ValueError(f"Papel desconhecido: {papel}")
    from .db import conexao

    descricao = ""
    if codigo:
        for c in contas_do_omie():
            if int(c["codigo"]) == int(codigo):
                descricao = c["descricao"]
                break
        else:
            raise SemEspelho(
                f"A conta {codigo} não está no espelho do OMIE. Se ela é nova, "
                f"rode a carga do painel antes de apontá-la aqui.")

    with conexao() as con:
        con.execute(
            "INSERT INTO analisesps.aporte_conta "
            "       (papel, codigo_conta, descricao, definido_por) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT (papel) DO UPDATE SET codigo_conta = EXCLUDED.codigo_conta,"
            "    descricao = EXCLUDED.descricao, definido_em = now(),"
            "    definido_por = EXCLUDED.definido_por",
            (papel, int(codigo) if codigo else None, descricao, quem or ""))
        con.commit()


# ---------------------------------------------------------------------------
# AS CATEGORIAS
# ---------------------------------------------------------------------------
def procurar_categoria(descricao: str) -> list:
    """Candidatas no plano financeiro, casando pela descrição achatada.

    Devolve TODAS as que casam — inclusive quando são várias. Escolher uma
    aqui seria exatamente o que o dono proibiu.
    """
    alvo = achatar(descricao)
    if not alvo:
        return []
    try:
        linhas = _consultar(
            "SELECT codigo, COALESCE(descricao, ''), COALESCE(transferencia, ''),"
            "       COALESCE(codigo_dre, ''), COALESCE(conta_inativa, '') "
            "  FROM painel.cat ORDER BY codigo")
    except Exception as e:  # noqa: BLE001
        logger.exception("Aportes: não consegui ler o plano financeiro")
        raise SemEspelho(
            "Não consegui ler o plano financeiro do OMIE. Ele vem da carga do "
            "painel; se ela nunca rodou, rode-a primeiro.") from e

    exatas, parecidas = [], []
    for l in linhas:
        nome = achatar(l[1])
        if not nome:
            continue
        item = {"codigo": l[0], "descricao": l[1], "transferencia": l[2],
                "codigo_dre": l[3], "inativa": str(l[4]).upper().startswith("S")}
        if nome == alvo:
            exatas.append(item)
        elif alvo in nome or nome in alvo:
            parecidas.append(item)
    # Havendo casamento exato, as "parecidas" só atrapalham: "Aportes BWS" é
    # pedaço de "Devolução de Aportes BWS", e mostrar as duas como igualmente
    # prováveis empurraria o dono para o erro.
    return exatas or parecidas


def descobrir_categorias() -> dict:
    """{chave: {situacao, candidatos, codigo, descricao, transferencia}}.

    `situacao` é 'achou', 'ambigua' ou 'nao_achou'. Só 'achou' traz código
    pronto; as outras duas param e pedem a decisão do dono, que é o que o
    briefing manda: *"se a descrição não for encontrada, ou se aparecer mais
    de uma categoria com nome parecido, a tela tem de parar e me dizer, em vez
    de escolher uma."*
    """
    guardadas = categorias_configuradas()
    saida = {}
    for chave, descricao in CATEGORIAS.items():
        try:
            candidatos = procurar_categoria(descricao)
            erro = ""
        except SemEspelho as e:
            candidatos, erro = [], str(e)

        escolhida = guardadas.get(chave) or {}
        if escolhida.get("codigo"):
            situacao = "escolhida"
        elif erro:
            situacao = "sem_espelho"
        elif not candidatos:
            situacao = "nao_achou"
        elif len(candidatos) > 1:
            situacao = "ambigua"
        else:
            situacao = "achou"

        unica = candidatos[0] if len(candidatos) == 1 else {}
        saida[chave] = {
            "chave": chave,
            "procurada": descricao,
            "situacao": situacao,
            "erro": erro,
            "candidatos": candidatos,
            "codigo": escolhida.get("codigo") or unica.get("codigo") or "",
            "descricao": (escolhida.get("descricao")
                          or unica.get("descricao") or ""),
            "transferencia": (escolhida.get("transferencia")
                              or unica.get("transferencia") or ""),
            "confirmada": bool(escolhida.get("codigo")),
        }
    return saida


def categorias_configuradas() -> dict:
    """{chave: {codigo, descricao, transferencia}} — o que o dono confirmou."""
    try:
        linhas = _consultar(
            "SELECT chave, COALESCE(codigo, ''), COALESCE(descricao_encontrada, ''),"
            "       COALESCE(transferencia, '') "
            "  FROM analisesps.aporte_categoria")
    except Exception:  # noqa: BLE001 — migração 018 ainda não aplicada
        logger.exception("Aportes: não consegui ler o de-para das categorias")
        return {}
    return {l[0]: {"codigo": l[1], "descricao": l[2], "transferencia": l[3]}
            for l in linhas if l[1]}


def guardar_categoria(chave: str, codigo: str, quem: str = "") -> None:
    """Confirma qual código do plano financeiro é esta categoria."""
    if chave not in CATEGORIAS:
        raise ValueError(f"Categoria desconhecida: {chave}")
    from .db import conexao

    descricao, transferencia = "", ""
    codigo = str(codigo or "").strip()
    if codigo:
        achou = _consultar(
            "SELECT COALESCE(descricao, ''), COALESCE(transferencia, '') "
            "  FROM painel.cat WHERE codigo = ?", (codigo,))
        if not achou:
            raise SemEspelho(
                f"O código {codigo} não está no plano financeiro do espelho. "
                f"Se a categoria é nova no OMIE, rode a carga do painel antes.")
        descricao, transferencia = achou[0][0], achou[0][1]

    with conexao() as con:
        con.execute(
            "INSERT INTO analisesps.aporte_categoria "
            "  (chave, descricao_procurada, codigo, descricao_encontrada, "
            "   transferencia, definido_por) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (chave) DO UPDATE SET codigo = EXCLUDED.codigo,"
            "    descricao_encontrada = EXCLUDED.descricao_encontrada,"
            "    transferencia = EXCLUDED.transferencia, definido_em = now(),"
            "    definido_por = EXCLUDED.definido_por",
            (chave, CATEGORIAS[chave], codigo, descricao, transferencia,
             quem or ""))
        con.commit()


def categorias_resolvidas() -> dict:
    """O código de cada categoria, SEM exigir que o dono confirme nada.

    ⚠️ MUDANÇA DE 20/09/2026, e ela veio dele olhando a tela: *"eu não entendi
    esse gravar o de-para. Eu acho que não precisaria."*

    Ele tem razão. Quando a descrição aparece UMA vez só no plano financeiro,
    não há decisão a tomar — pedir um clique de confirmação é cerimônia, e
    cerimônia que se repete vira clique automático, que é pior do que não ter
    conferência nenhuma.

    O de-para continua existindo e continua sendo o que impede código chumbado.
    Só deixou de ser um PASSO: agora ele só aparece quando há de fato uma
    decisão — descrição repetida, ou descrição que não existe. Nesses dois
    casos a tela para e pergunta, exatamente como antes.

    O que ele confirmou à mão continua valendo por cima do que foi descoberto:
    é assim que ele conserta um caso que o sistema leria errado.
    """
    confirmadas = categorias_configuradas()
    resolvidas = dict(confirmadas)
    for chave, descricao in CATEGORIAS.items():
        if resolvidas.get(chave, {}).get("codigo"):
            continue
        try:
            candidatos = procurar_categoria(descricao)
        except SemEspelho:
            continue
        if len(candidatos) == 1:
            resolvidas[chave] = {
                "codigo": candidatos[0]["codigo"],
                "descricao": candidatos[0]["descricao"],
                "transferencia": candidatos[0]["transferencia"],
                "descoberta": True,
            }
    return resolvidas


def falta_configurar() -> list:
    """Frases em português sobre o que ainda impede um lançamento."""
    faltas = []
    contas = contas_configuradas()
    for papel in PAPEIS:
        if not (contas.get(papel) or {}).get("codigo"):
            from .aportes import PAPEL_ROTULO
            faltas.append(f"Falta apontar qual conta do OMIE é a "
                          f"{PAPEL_ROTULO[papel]}.")
    categorias = categorias_resolvidas()
    for chave, descricao in CATEGORIAS.items():
        if not (categorias.get(chave) or {}).get("codigo"):
            faltas.append(f"Não sei qual é o código da categoria "
                          f"\"{descricao}\" no seu plano financeiro.")
    return faltas


# ---------------------------------------------------------------------------
# FORNECEDORES E OBRAS
# ---------------------------------------------------------------------------
def fornecedores(busca: str = "") -> list:
    """Cadastros do OMIE que casam com o que foi digitado.

    Busca simples por pedaço do nome ou do documento. Não há acento a tratar
    aqui: razão social no OMIE vem em maiúsculas sem acento na esmagadora
    maioria dos casos, e o teto de resultados protege a tela do resto.
    """
    termo = str(busca or "").strip()
    try:
        linhas = _consultar(
            "SELECT codigo, COALESCE(razao_social, ''), COALESCE(cnpj_cpf, '') "
            "  FROM painel.clientes "
            " WHERE LOWER(COALESCE(razao_social, '')) LIKE ? "
            "    OR COALESCE(cnpj_cpf, '') LIKE ? "
            " ORDER BY razao_social LIMIT ?",
            (f"%{termo.lower()}%", f"%{termo}%", MAX_LISTA))
    except Exception as e:  # noqa: BLE001
        logger.exception("Aportes: não consegui ler os fornecedores")
        raise SemEspelho(
            "Não consegui ler os fornecedores do OMIE. Eles vêm da carga "
            "do painel.") from e
    return [{"codigo": l[0], "nome": l[1], "documento": l[2]} for l in linhas]


def obras() -> list:
    """As obras (departamentos) já usadas em algum título.

    Não há catálogo de departamentos no espelho do OMIE — o painel só guarda o
    rateio. Na prática isso basta e até ajuda: departamento que nunca foi
    usado em título nenhum dificilmente é onde um aporte deve entrar.
    """
    try:
        linhas = _consultar(
            "SELECT ccoddep, MAX(COALESCE(cdesdep, '')) "
            "  FROM painel.rateio WHERE COALESCE(ccoddep, '') <> '' "
            " GROUP BY ccoddep ORDER BY 2")
    except Exception as e:  # noqa: BLE001
        logger.exception("Aportes: não consegui ler as obras")
        raise SemEspelho(
            "Não consegui ler as obras. Elas vêm da carga do painel.") from e
    return [{"codigo": l[0], "nome": l[1] or l[0]} for l in linhas]


# ---------------------------------------------------------------------------
# A CRÍTICA DE TRANSFERÊNCIA — consulta ao espelho, não ao OMIE
# ---------------------------------------------------------------------------
def consultar_semelhantes(valor: float, data, codigos_contas: list) -> list:
    """Movimentações do MESMO dia e do MESMO valor entre contas da BWS.

    Olha as baixas (`painel.movimentos`), não os títulos: transferência entre
    contas aparece como dinheiro que de fato andou, e é a data do movimento —
    não a do vencimento — que o dono tem na cabeça quando diz "no mesmo dia".

    Tolerância de um centavo no valor: o dono digita "12.500,00" e o
    movimento pode ter vindo com arredondamento de outro caminho. Exigir
    igualdade perfeita faria a crítica calar justamente quando ela importa.
    """
    codigos = [int(c) for c in (codigos_contas or []) if c]
    if not codigos or not valor:
        return []
    from .aportes import data_br
    dia = data_br(data) if not isinstance(data, str) else data

    marcadores = ", ".join(["?"] * len(codigos))
    try:
        linhas = _consultar(
            f"SELECT m.ncodcc, COALESCE(cc.descricao, ''), m.nvalpago, "
            f"       m.ddtpagamento, COALESCE(c.descricao, ''), "
            f"       COALESCE(c.transferencia, '') "
            f"  FROM painel.movimentos m "
            f"  LEFT JOIN painel.contas_correntes cc ON cc.codigo = m.ncodcc "
            f"  LEFT JOIN painel.cat c ON c.codigo = m.ccodcateg "
            f" WHERE m.ddtpagamento = ? "
            f"   AND m.ncodcc IN ({marcadores}) "
            f"   AND ABS(COALESCE(m.nvalpago, 0) - ?) <= 0.01 "
            f" ORDER BY m.ncodcc LIMIT 20",
            tuple([dia] + codigos + [float(valor)]))
    except Exception:  # noqa: BLE001 — a crítica é conforto; não derruba a tela
        logger.exception("Aportes: não consegui rodar a crítica de transferência")
        return []

    # Só interessa quando o mesmo valor apareceu em DUAS contas no mesmo dia —
    # é isso que tem cara de transferência. Uma perna só é um pagamento comum.
    por_conta = {}
    for l in linhas:
        por_conta.setdefault(l[0], (l[1] or str(l[0]), l[4], l[5]))
    if len(por_conta) < 2:
        return []

    nomes = [v[0] for v in por_conta.values()]
    categoria = next((v[1] for v in por_conta.values() if v[1]), "")
    return [{
        "valor": float(valor),
        "data": dia,
        "conta_a": nomes[0],
        "conta_b": nomes[1],
        "categoria": categoria,
        "e_transferencia": any(
            str(v[2]).upper().startswith("S") for v in por_conta.values()),
    }]
