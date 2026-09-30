# -*- coding: utf-8 -*-
"""
A folha que a contabilidade mandou, guardada.

POR QUE ESTE MÓDULO EXISTE, e é o que destrava todo o resto: até 27/09/2026 a
Folha Sintética era lida do `.xls` e calculada em memória, e morria com a
requisição. O dono pediu um painel com os totais por obra e por conta — e **não
há como somar o que não está gravado**. Sem isto não existe painel, não existe
tela por verba, não existe arquivo de pagamento e não existe log do que foi
gerado.

A DIVISÃO DE TRABALHO, que importa manter:

    folha_sintetica.py   lê o arquivo e entende (nenhum banco)
    folha_arquivo.py     guarda, lista e apaga (este)
    folha_apropriacao.py decide para qual obra vai cada real (nenhum banco)

⚠️ O QUE FICA GUARDADO É O QUE A CONTABILIDADE MANDOU, CRU. A apropriação — para
qual obra vai cada real — **não entra aqui**, de propósito: ela depende do ponto,
do rateio e do ajuste à mão, e muda depois de a folha estar importada. Misturar
as duas faria uma reimportação apagar o ajuste fino que alguém fez, e o ajuste é
o trabalho mais caro do processo.

REIMPORTAR É NORMAL: o dono corrige algo na contabilidade e manda de novo. A
segunda importação da mesma competência e tipo **substitui** a primeira — duas
folhas de 08/2026 quinzena deixariam qualquer total ambíguo.
"""
from __future__ import annotations

import hashlib
import logging
from decimal import Decimal

from . import folha_sintetica

logger = logging.getLogger("analisesps.folha_arquivo")

# Quanto o arquivo pode pesar. A Folha Sintética de 500 pessoas tem ~100 KB; o
# teto existe porque a instância divide 2 GB com 17 módulos e já morreu de falta
# de memória em julho de 2026.
MAXIMO_DO_ARQUIVO = 10 * 1024 * 1024        # 10 MB

TIPOS = (folha_sintetica.QUINZENA, folha_sintetica.FIM_DE_MES)

ROTULO_DO_TIPO = {
    folha_sintetica.QUINZENA: "Quinzena (dia 1 ao 15)",
    folha_sintetica.FIM_DE_MES: "Fim de mês (16 ao último dia)",
}


class ErroDaImportacao(RuntimeError):
    """Não deu para importar. A frase vai inteira para a tela."""


def _pronto() -> bool:
    """A migração 029 já rodou? Enquanto não, a tela avisa em vez de estourar.

    O código sobe para o Render ANTES de alguém apertar "Aplicar atualizações do
    banco" — sem esta guarda, a tela cairia no intervalo entre as duas coisas."""
    from .db import consultar_um
    try:
        consultar_um("SELECT 1 FROM analisesps.folha LIMIT 1")
        return True
    except Exception:  # noqa: BLE001 — tabela que ainda não existe é normal
        return False


def impressao_do_arquivo(conteudo: bytes) -> str:
    """A impressão do conteúdo, para dizer "é o MESMO arquivo".

    É diferente de "é o mesmo mês": o dono pode mandar o arquivo corrigido da
    mesma competência, e aí o conteúdo muda. Saber os dois permite a tela dizer
    "você já importou ESTE arquivo" em vez de só "já existe folha deste mês"."""
    return hashlib.sha256(conteudo or b"").hexdigest()


# ---------------------------------------------------------------------------
# Importar
# ---------------------------------------------------------------------------
def importar(conteudo: bytes, nome_do_arquivo: str = "", tipo: str = "",
             quem: str = "", ano: int | None = None,
             mes: int | None = None) -> dict:
    """Lê o `.xls` e guarda. Devolve o resumo que a tela mostra.

    `tipo` só precisa vir quando o título do relatório não deixa claro — e aí a
    tela pergunta, em vez de adivinhar. Adivinhar erraria o PERÍODO DO PONTO, e
    o período errado apropria os dias errados nas obras.

    `ano` e `mes` idem: o relatório traz a competência, mas se não trouxer, a
    tela pergunta em vez de assumir o mês corrente.
    """
    if not _pronto():
        raise ErroDaImportacao(
            "a tabela da folha ainda não existe. Aperte "
            '"Aplicar atualizações do banco" em Configurações e tente de novo.')

    if not conteudo:
        raise ErroDaImportacao("o arquivo chegou vazio.")
    if len(conteudo) > MAXIMO_DO_ARQUIVO:
        raise ErroDaImportacao(
            f"o arquivo tem {len(conteudo) / 1024 / 1024:.1f} MB e o teto é "
            f"{MAXIMO_DO_ARQUIVO // 1024 // 1024} MB. A Folha Sintética de "
            "500 pessoas tem menos de um MB — confira se não trocou o arquivo.")

    try:
        lida = folha_sintetica.ler(conteudo)
    except folha_sintetica.ErroDaFolha:
        raise
    except Exception as e:  # noqa: BLE001
        raise ErroDaImportacao(f"não consegui ler o arquivo: {e}") from e

    if not lida.linhas:
        raise ErroDaImportacao(
            "não achei nenhuma pessoa no arquivo. Confira se é a Folha "
            "Sintética do Fortes, e não outro relatório.")

    # A COMPETÊNCIA: a do arquivo manda; a da tela só entra quando o arquivo não
    # diz. O contrário faria a tela sobrescrever o que o relatório afirma — e o
    # relatório é a fonte.
    ano_final = lida.ano or ano
    mes_final = lida.mes or mes
    if not ano_final or not mes_final:
        raise ErroDaImportacao(
            "o arquivo não diz de que mês é a folha. Escolha a competência na "
            "tela e importe de novo.")

    tipo_final = lida.tipo_sugerido or str(tipo or "").strip()
    if tipo_final not in TIPOS:
        raise ErroDaImportacao(
            'não deu para saber se esta folha é da QUINZENA ou do FIM DE MÊS '
            f'pelo título ("{lida.titulo or "sem título"}"). Escolha na tela e '
            "importe de novo — o tipo decide o período do ponto, então não dá "
            "para adivinhar.")

    total = lida.total
    pessoas = len(lida.linhas)
    avisos = list(lida.avisos)
    # ⚠️ NÃO FECHAR É AVISO, NÃO É RECUSA. O dono precisa poder importar uma
    # folha que não fecha para DESCOBRIR por que não fecha — recusar deixaria o
    # arquivo do lado de fora, onde ninguém investiga.
    if not lida.fecha:
        avisos.append(
            f"a soma das pessoas ({total}) é diferente da soma dos subtotais "
            f"por filial ({lida.total_das_filiais}). Confira o arquivo.")
    if lida.total_declarado is not None and lida.total_declarado != total:
        avisos.append(
            f"o rodapé do relatório declara {lida.total_declarado} e as linhas "
            f"somam {total}.")
    if (lida.pessoas_declaradas is not None
            and lida.pessoas_declaradas != pessoas):
        avisos.append(
            f"o rodapé declara {lida.pessoas_declaradas} pessoa(s) e eu li "
            f"{pessoas}.")

    from .db import conexao

    with conexao() as conn:
        # SUBSTITUI a folha daquela competência e tipo. O CASCADE leva as linhas
        # junto — e é numa transação só, porque uma folha sem linhas (a antiga
        # apagada, a nova não gravada) mostraria total zero como se fosse
        # verdade.
        cur = conn.execute(
            "DELETE FROM analisesps.folha "
            " WHERE ano = ? AND mes = ? AND tipo = ?",
            (int(ano_final), int(mes_final), tipo_final))
        substituiu = bool(cur.rowcount and cur.rowcount > 0)
        cur.close()

        cur = conn.execute(
            "INSERT INTO analisesps.folha "
            "  (ano, mes, tipo, titulo, empresa, cnpj, arquivo_nome, impressao,"
            "   total, total_declarado, pessoas, pessoas_declaradas, avisos,"
            "   importado_por) "
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING id",
            (int(ano_final), int(mes_final), tipo_final, lida.titulo,
             lida.empresa, lida.cnpj, str(nome_do_arquivo or "")[:200],
             impressao_do_arquivo(conteudo), total, lida.total_declarado,
             pessoas, lida.pessoas_declaradas, " | ".join(avisos),
             str(quem or "")[:120]))
        folha_id = cur.fetchone()[0]
        cur.close()

        conn.executemany(
            "INSERT INTO analisesps.folha_linha "
            "  (folha_id, id_fortes, nome, valor, filial_codigo, filial_nome) "
            " VALUES (?,?,?,?,?,?)",
            [(folha_id, l.id_fortes, l.nome, l.valor, l.filial_codigo,
              l.filial_nome) for l in lida.linhas])
        conn.commit()

    # CASA COM O CADASTRO NA HORA: a tela que vem depois já mostra quem está
    # pendente. Num `try` porque a folha já está gravada — um tropeço no
    # casamento não pode desfazer a importação, e o casamento roda de novo a cada
    # visita à tela.
    casamento = {"casadas": 0, "pendentes": 0}
    try:
        casamento = casar_com_o_cadastro(folha_id)
    except Exception as e:  # noqa: BLE001
        logger.exception("Análise de SPs: falhou casar a folha com o cadastro")
        avisos.append(f"não deu para casar a folha com o cadastro: {e}")

    logger.info("Análise de SPs: folha %02d/%d (%s) importada — %d pessoa(s), "
                "total %s.", mes_final, ano_final, tipo_final, pessoas, total)
    return {"casadas": casamento["casadas"], "pendentes": casamento["pendentes"],
            "id": folha_id, "ano": int(ano_final), "mes": int(mes_final),
            "tipo": tipo_final, "pessoas": pessoas, "total": total,
            "avisos": avisos, "substituiu": substituiu, "fecha": lida.fecha}


# ---------------------------------------------------------------------------
# Ler de volta
# ---------------------------------------------------------------------------
CAMPOS = ("id", "ano", "mes", "tipo", "titulo", "empresa", "cnpj",
          "arquivo_nome", "impressao", "total", "total_declarado", "pessoas",
          "pessoas_declaradas", "avisos", "importado_em", "importado_por")


def _dicionario(linha) -> dict:
    folha = {campo: linha[i] for i, campo in enumerate(CAMPOS)}
    folha["competencia"] = f"{folha['mes']:02d}/{folha['ano']}"
    folha["rotulo_do_tipo"] = ROTULO_DO_TIPO.get(folha["tipo"], folha["tipo"])
    # ⚠️ AS FOLHAS JÁ IMPORTADAS guardaram o separador de setor ("001.01 -
    # CONSTRUTORA/ESCRITORIO") como "linha que não reconheci" — o leitor não o
    # conhecia até 30/09/2026. Filtrado aqui, elas deixam de acusar erro sem
    # precisar reimportar. Ver `folha_sintetica.PADRAO_SETOR`.
    from .folha_sintetica import PADRAO_SETOR
    prefixo = "linha que não reconheci: "
    folha["lista_de_avisos"] = [
        a for a in (folha["avisos"] or "").split(" | ")
        if a and not (a.startswith(prefixo)
                      and PADRAO_SETOR.match(a[len(prefixo):].strip()))]
    # `fecha` não é guardado: ele é a mesma pergunta que os avisos já
    # respondem, e um campo derivado guardado é um campo que pode mentir depois.
    folha["fecha"] = not folha["lista_de_avisos"]
    return folha


def listar(teto: int = 60) -> list:
    """As folhas importadas, da mais recente para a mais antiga."""
    from .db import consultar
    if not _pronto():
        return []
    linhas = consultar(
        "SELECT " + ", ".join(CAMPOS) + " FROM analisesps.folha "
        " ORDER BY ano DESC, mes DESC, tipo LIMIT ?", (int(teto),))
    return [_dicionario(l) for l in linhas]


def abrir(folha_id: int) -> dict | None:
    """Uma folha e as pessoas dela. None quando não existe."""
    from .db import consultar, consultar_um
    if not _pronto():
        return None
    linha = consultar_um(
        "SELECT " + ", ".join(CAMPOS) + " FROM analisesps.folha WHERE id = ?",
        (int(folha_id),))
    if not linha:
        return None
    folha = _dicionario(linha)
    folha["linhas"] = [
        {"id": r[0], "id_fortes": r[1], "nome": r[2], "cpf": r[3],
         "valor": r[4], "filial_codigo": r[5], "filial_nome": r[6]}
        for r in consultar(
            "SELECT id, id_fortes, nome, cpf, valor, filial_codigo, filial_nome"
            "  FROM analisesps.folha_linha WHERE folha_id = ? "
            " ORDER BY lower(nome)", (int(folha_id),))]
    return folha


def totais_por_filial(folha_id: int) -> list:
    """Quanto cada filial soma nesta folha, da maior para a menor.

    É o primeiro corte do painel que o dono pediu. O total por OBRA depende da
    apropriação (ponto + rateio), que ainda não está guardada — então o que dá
    para responder hoje é por filial, que é o que o arquivo traz."""
    from .db import consultar
    if not _pronto():
        return []
    linhas = consultar(
        "SELECT filial_codigo, max(filial_nome), count(*), sum(valor) "
        "  FROM analisesps.folha_linha WHERE folha_id = ? "
        " GROUP BY filial_codigo ORDER BY sum(valor) DESC",
        (int(folha_id),))
    return [{"codigo": l[0], "nome": l[1], "pessoas": l[2], "total": l[3]}
            for l in linhas]


# ---------------------------------------------------------------------------
# CASAR A FOLHA COM AS PESSOAS
# ---------------------------------------------------------------------------
def casar_com_o_cadastro(folha_id: int) -> dict:
    """Preenche o CPF de cada linha, pelo ID Fortes do cadastro.

    ⚠️ É O PASSO QUE FAZ A FOLHA CONVERSAR COM O RESTO DO SISTEMA. A Folha
    Sintética traz **código do empregado e nome**; o ponto, o cadastro, o rateio
    e o pagamento são todos por **CPF**. Sem casar, a folha é uma lista de nomes.

    NÃO CASA POR NOME, e isso é decisão com preço pago: as planilhas cruzam por
    nome hoje, e é frágil — dois "JOSE DA SILVA", um acento diferente, um nome do
    meio abreviado, e o salário vai para a pessoa errada. Aqui é pelo código, e
    quem não tem código **fica pendente, visível**, em vez de casar com um
    parecido.

    Roda sozinho depois de importar, e de novo a cada visita à tela: o cadastro
    pode ter sido atualizado no meio, e aí gente que estava pendente passa a
    casar sem ninguém reimportar nada.

    Devolve `{"casadas", "pendentes"}`.
    """
    from . import colaboradores
    from .db import conexao

    if not _pronto():
        return {"casadas": 0, "pendentes": 0}
    from .db import consultar
    de_para = colaboradores.de_para_do_fortes()
    linhas = consultar(
        "SELECT id, id_fortes, cpf FROM analisesps.folha_linha "
        " WHERE folha_id = ?", (int(folha_id),))

    if not de_para:
        # Sem de/para não há o que casar, e zerar o que já estava casado seria
        # perder informação por causa de uma planilha fora do ar.
        #
        # ⚠️ MAS A CONTAGEM SAI CERTA MESMO ASSIM, e o teste pegou isto: eu
        # devolvia "0 pendentes" quando TODO MUNDO estava pendente. Número com o
        # significado errado é pior que número nenhum — ele diria que a folha
        # está pronta para pagar.
        faltam = sum(1 for _i, _f, cpf in linhas if not cpf)
        return {"casadas": len(linhas) - faltam, "pendentes": faltam}

    mudar = []
    pendentes = 0
    for linha_id, id_fortes, cpf_atual in linhas:
        achado = de_para.get(colaboradores.normalizar_id_fortes(id_fortes))
        cpf_novo = achado["cpf"] if achado else ""
        if not cpf_novo:
            pendentes += 1
        if cpf_novo != (cpf_atual or ""):
            mudar.append((cpf_novo, linha_id))

    if mudar:
        with conexao() as conn:
            conn.executemany(
                "UPDATE analisesps.folha_linha SET cpf = ? WHERE id = ?", mudar)
            conn.commit()

    casadas = len(linhas) - pendentes
    return {"casadas": casadas, "pendentes": pendentes}


def criticas(folha_id: int) -> dict:
    """O que precisa da mão de alguém nesta folha, antes de pagar.

    ⚠️ AS TRÊS PERGUNTAS QUE DECIDEM DINHEIRO:

      1. **quem está na folha e não está no cadastro** — sem CPF não há
         apropriação, não há auxílio, não há pagamento. Correção do dono em
         26/09/2026: essa pessoa **não pode ficar escondida** numa lista à parte;
         ela fica na folha, marcada, e o total não fecha enquanto ela estiver
         assim — que é exatamente o que tem de acontecer;
      2. **quem já saiu** e está na folha — *"não podemos pagar salário ou
         diárias pra quem saiu"*;
      3. **quem está saindo** — pode haver valor devido até o último dia, então é
         aviso para conferir, não trava.

    A situação de cada pessoa vem de `colaboradores.situacao_no_pagamento`, a
    MESMA função das outras telas — duas respostas para "esta pessoa pode
    receber?" divergiriam no primeiro caso de borda.
    """
    from . import colaboradores
    from .db import consultar

    vazio = {"pendentes": [], "sairam": [], "saindo": [], "total_pendente": 0,
             "total_de_quem_saiu": 0}
    if not _pronto():
        return vazio

    folha = consultar(
        "SELECT id_fortes, nome, cpf, valor FROM analisesps.folha_linha "
        " WHERE folha_id = ? ORDER BY lower(nome)", (int(folha_id),))
    if not folha:
        return vazio

    # O FIM DO PERÍODO é o que decide "saiu" × "está saindo": quem saiu depois do
    # fim da quinzena trabalhou a quinzena inteira e recebe. Ver
    # `situacao_no_pagamento`.
    cabeca = consultar(
        "SELECT ano, mes, tipo FROM analisesps.folha WHERE id = ?",
        (int(folha_id),))
    ate = None
    if cabeca:
        ano, mes, tipo = cabeca[0]
        from . import folha_apropriacao
        periodo = folha_apropriacao.periodo_do_pagamento(ano, mes, tipo)
        ate = periodo[1] if periodo else None

    fichas = colaboradores.muitos_por_cpf(
        [c for _i, _n, c, _v in folha if c], ate=ate)

    saida = {"pendentes": [], "sairam": [], "saindo": [],
             "total_pendente": 0, "total_de_quem_saiu": 0}
    for id_fortes, nome, cpf, valor in folha:
        if not cpf:
            saida["pendentes"].append(
                {"id_fortes": id_fortes, "nome": nome, "valor": valor})
            saida["total_pendente"] += valor or 0
            continue
        ficha = fichas.get(cpf)
        if not ficha:
            # Tem CPF pelo de/para, mas o cadastro não devolveu — cadastro
            # apagado depois do casamento. É pendente do mesmo jeito.
            saida["pendentes"].append(
                {"id_fortes": id_fortes, "nome": nome, "valor": valor})
            saida["total_pendente"] += valor or 0
            continue
        situacao = ficha.get("situacao")
        item = {"id_fortes": id_fortes, "nome": ficha.get("nome") or nome,
                "cpf": cpf, "valor": valor, "motivo": ficha.get("motivo", ""),
                "link_pipefy": ficha.get("link_pipefy", "")}
        if situacao == colaboradores.SITUACAO_SAIU:
            saida["sairam"].append(item)
            saida["total_de_quem_saiu"] += valor or 0
        elif situacao == colaboradores.SITUACAO_SAINDO:
            saida["saindo"].append(item)
    return saida


# ---------------------------------------------------------------------------
# O PAINEL
# ---------------------------------------------------------------------------
def panorama() -> dict:
    """Os totais que o dono pediu, no que já dá para responder hoje.

    Palavras dele, em 27/09/2026:

        "Tem que ter informação gerencial, tipo dashboard, para poder estar vendo
         qual é o total por obra, porque isso já ajuda nessa questão do rateio.
         (…) a folha da contabilidade, conseguir em um ambiente visualizar tudo."

    ⚠️ O QUE FALTA, DITO AQUI PARA NÃO PARECER ESQUECIMENTO: o total **por obra**
    depende da apropriação (ponto + rateio + ajuste), que ainda não está
    guardada. O que o arquivo da contabilidade traz é **filial**, e é isso que
    esta função responde. Quando a apropriação existir, o total por obra entra
    aqui — no mesmo lugar, e não numa tela paralela.

    E as outras verbas (alimentação, transporte, diaristas, GM) também não
    entram ainda: elas não têm de onde vir. A tela diz isso, em vez de mostrar
    um total que parece completo e não é.
    """
    from .db import consultar, consultar_um
    vazio = {"folhas": [], "total": Decimal("0"), "pessoas": 0,
             "competencias": 0, "pendentes": 0, "por_filial": [],
             "pronto": False}
    if not _pronto():
        return vazio

    folhas = listar()
    if not folhas:
        return {**vazio, "pronto": True}

    # ⚠️ O TOTAL GERAL SOMA AS FOLHAS, e não é a soma de tudo o que a empresa
    # paga: são só as folhas da contabilidade que foram importadas. Dizer
    # "total da folha" sem essa ressalva faria o número parecer o custo de
    # pessoal inteiro — que ainda inclui alimentação, transporte e diaristas.
    total = sum((f["total"] for f in folhas), Decimal("0"))
    pessoas = sum(f["pessoas"] for f in folhas)

    # `consultar_um` devolve a LINHA; `consultar` devolve a lista de linhas. Usar
    # o segundo aqui dava `int(tupla)` — e o teste pegou na primeira rodada.
    pendentes = consultar_um(
        "SELECT count(*) FROM analisesps.folha_linha WHERE cpf = ''")
    por_filial = consultar(
        "SELECT l.filial_codigo, max(l.filial_nome), count(*), sum(l.valor) "
        "  FROM analisesps.folha_linha l "
        " GROUP BY l.filial_codigo ORDER BY sum(l.valor) DESC LIMIT 60")

    return {
        "folhas": folhas,
        "total": total,
        "pessoas": pessoas,
        "competencias": len({(f["ano"], f["mes"]) for f in folhas}),
        "pendentes": int((pendentes or [0])[0] or 0),
        "por_filial": [{"codigo": l[0], "nome": l[1], "pessoas": l[2],
                        "total": l[3]} for l in por_filial],
        "pronto": True,
    }


def apagar(folha_id: int, quem: str = "") -> bool:
    """Apaga a folha importada e as linhas dela. Devolve se apagou algo.

    ⚠️ APAGAR AQUI É SEGURO, e é a diferença desta tabela para as outras: o que
    está guardado é a CÓPIA de um arquivo que a contabilidade tem. Apagar e
    importar de novo não perde decisão nenhuma — a apropriação e o ajuste fino
    moram em outro lugar, justamente para isto."""
    from .db import conexao
    if not _pronto():
        return False
    with conexao() as conn:
        cur = conn.execute("DELETE FROM analisesps.folha WHERE id = ?",
                           (int(folha_id),))
        apagou = bool(cur.rowcount and cur.rowcount > 0)
        cur.close()
        conn.commit()
    if apagou:
        logger.info("Análise de SPs: folha %s apagada por %s.", folha_id,
                    quem or "(sem nome)")
    return apagou
