# -*- coding: utf-8 -*-
"""
Alterar a classificação de títulos no OMIE, em lote, com rede de proteção.

O painel inteiro só lê. Esta é a única parte que ESCREVE num sistema de fora, e
o dono disse a frase que define o tom: *"é realmente algo sério, eu não posso
falhar nem errar"*.

As quatro proteções, e o que cada uma evita:

1. **SENHA PRÓPRIA** (`PAINEL_SENHA_ESCRITA`). Não é a senha de entrar no
   painel: é uma segunda, só para executar. Sem ela configurada no ambiente,
   alterar fica **desligado** — o padrão é NEGAR, como no resto do painel.
2. **SIMULAÇÃO POR PADRÃO.** Enquanto ninguém pedir o contrário, o painel
   consulta o título, monta o que enviaria e mostra o "de → para" — sem enviar.
3. **TRAVA DO RATEIO.** Trocar o departamento coloca 100% no novo e desfaz o
   rateio anterior. Num título dividido entre obras isso apaga informação que
   ninguém reconstrói. O painel RECUSA esses títulos, a menos que quem está
   alterando diga explicitamente que sabe.
4. **REGISTRO DE TUDO**, no banco (o disco do Render some), inclusive dos
   envios que deram errado — que é justamente quando o registro importa.
"""
from __future__ import annotations

import hmac
import logging
import os
import time
import unicodedata

from .db import conexao, consultar

logger = logging.getLogger("painel.saneamento")

# O CADASTRO LIDO NO ENSAIO SERVE PARA O ENVIO.
#
# 22/09/2026, o dono, ao alterar UM título: "A Omie bloqueou as chamadas por
# consumo excessivo e pediu 59 segundos. 0 título(s) alterado(s)." O fluxo da
# tela é ensaio → executar, e cada um consultava o título no OMIE. Duas
# consultas iguais em menos de um minuto é o que a Omie chama de "consumo
# redundante" — e bloqueia. Ou seja: alterar um título logo depois de ensaiá-lo
# NUNCA funcionava. Foi por isso que "a primeira escrita no OMIE nunca
# aconteceu".
#
# Agora o cadastro lido fica guardado por alguns minutos, e o envio usa o que
# o ensaio leu: a única chamada nova é a alteração, que é outra chamada. O
# preço: se alguém mexer no título no OMIE nesses minutos, o envio parte do
# cadastro de antes. Cinco minutos é curto o bastante para isso ser raro e
# longo o bastante para ler o ensaio e clicar. Depois de alterado, o cadastro
# guardado é esquecido — ele já não descreve o título.
VALIDADE_DO_CADASTRO = 300.0
_CADASTROS_RECENTES: dict[tuple, tuple[float, dict]] = {}


def esquecer_cadastros() -> None:
    """Zera o que o ensaio guardou. Os testes usam; a tela não precisa."""
    _CADASTROS_RECENTES.clear()


def _consultar_lembrando(cliente, codigo, tipo) -> dict:
    """O cadastro do título — do ensaio de há pouco, ou do OMIE."""
    chave = (int(codigo), tipo)
    agora = time.monotonic()
    guardado = _CADASTROS_RECENTES.get(chave)
    if guardado and agora - guardado[0] < VALIDADE_DO_CADASTRO:
        return guardado[1]
    if tipo == TIPO_LANC_CC:
        cadastro = cliente.consultar_lancamento_cc(codigo)
    else:
        cadastro = cliente.consultar_titulo(codigo, tipo)
    # limpa o que venceu, para o dicionário não crescer para sempre
    for k, (quando, _c) in list(_CADASTROS_RECENTES.items()):
        if agora - quando >= VALIDADE_DO_CADASTRO:
            _CADASTROS_RECENTES.pop(k, None)
    _CADASTROS_RECENTES[chave] = (agora, cadastro)
    return cadastro

# Quantos títulos por vez. O limite não é técnico: é para um engano de seleção
# não virar um estrago de mil títulos antes de alguém perceber.
TETO_POR_LOTE = 200


def escrita_configurada() -> bool:
    """Sem a senha própria no ambiente, alterar no OMIE fica desligado."""
    return bool(os.getenv("PAINEL_SENHA_ESCRITA", "").strip())


def _para_comparar(valor) -> bytes:
    """Mesmo cuidado do login: `compare_digest` recusa texto fora do ASCII e
    ESTOURA em vez de devolver False. Ver `auth.py`."""
    return unicodedata.normalize("NFC", str(valor or "")).encode("utf-8")


def senha_de_escrita_confere(digitada: str) -> bool:
    esperada = os.getenv("PAINEL_SENHA_ESCRITA", "").strip()
    if not esperada:
        return False
    return hmac.compare_digest(_para_comparar(digitada), _para_comparar(esperada))


def rateio_do_titulo(codigo: int) -> list[str]:
    """Em quantos departamentos este título está rateado, pelo espelho.

    É a pergunta que decide se trocar o departamento é seguro ou destrutivo."""
    return [d for (d,) in consultar(
        "SELECT DISTINCT cdesdep FROM rateio "
        " WHERE codigo_lancamento_omie = ? AND COALESCE(TRIM(cdesdep),'') <> ''",
        (int(codigo),))]


# O LANÇAMENTO FEITO DIRETO NA CONTA (tarifa, IOF, aporte) NÃO É TÍTULO.
#
# 10/11/2026: o dono filtrou "Taxas, Tarifas Bancárias e IOF" (~600 linhas),
# trocou a obra de uma e o OMIE respondeu "Lançamento não cadastrado para o
# Código [11266605823]". O painel perguntava por ela no endereço dos títulos.
# Ela é reconhecida pela marca que o fato põe na observação, e vai pelo
# endereço dos lançamentos de conta (`omie_escrita.preparar_alteracao_cc`).
TIPO_LANC_CC = "cc"


def _marca_cc() -> str:
    from .sync.fato import MARCA_LANCAMENTO_CC
    return MARCA_LANCAMENTO_CC


def _obras_do_lancamento_cc(codigo: int) -> list[str]:
    """Em quantas obras o lançamento de conta está dividido, pelo fato (é lá
    que a apropriação lida do OMIE vira linha)."""
    from .sync.fato import NAO_APROP, OBRA_NAO_LIDA, TRANSFERENCIA_ENTRE_CONTAS
    return [d for (d,) in consultar(
        "SELECT DISTINCT departamento FROM fato"
        " WHERE codigo_lancamento = ? AND observacao = ?"
        "   AND COALESCE(TRIM(departamento),'') NOT IN ('', ?, ?, ?)",
        (int(codigo), _marca_cc(), NAO_APROP, OBRA_NAO_LIDA,
         TRANSFERENCIA_ENTRE_CONTAS))]


def titulos_para_alterar(codigos) -> list[dict]:
    """Os dados que a tela precisa mostrar antes de alterar: o que o título é
    hoje e se ele tem rateio em mais de uma obra."""
    codigos = [int(c) for c in codigos if str(c).strip().isdigit()]
    if not codigos:
        return []
    marcas = ",".join(["?"] * len(codigos))
    # o título vem antes do lançamento de conta (FALSE < TRUE): se um dia o
    # mesmo número existir nos dois, vale o título, que é o caminho de sempre
    linhas = consultar(
        "SELECT DISTINCT ON (codigo_lancamento) codigo_lancamento, tipo, "
        "       numero_documento, categoria, codigo_categoria, departamento, "
        "       razao_social, COALESCE(observacao = ?, FALSE) "
        f"  FROM fato WHERE codigo_lancamento IN ({marcas}) "
        " ORDER BY codigo_lancamento, COALESCE(observacao = ?, FALSE)",
        [_marca_cc(), *codigos, _marca_cc()])
    campos = ("codigo", "tipo", "documento", "categoria", "codigo_categoria",
              "departamento", "razao_social", "lancamento_cc")
    saida = []
    for bruta in linhas:
        titulo = dict(zip(campos, bruta))
        titulo["lancamento_cc"] = bool(titulo["lancamento_cc"])
        titulo["rateado_em"] = (_obras_do_lancamento_cc(titulo["codigo"])
                                if titulo["lancamento_cc"]
                                else rateio_do_titulo(titulo["codigo"]))
        titulo["tem_rateio_multiplo"] = len(titulo["rateado_em"]) > 1
        saida.append(titulo)
    return saida


# EXCLUIR TEM TETO PROPRIO, e menor.
#
# Alterar 200 titulos de uma vez, errando, custa alterar 200 de volta. Excluir
# 200 errado custa redigitar 200 — quando se sabe o que havia. Um lote pequeno e
# a diferenca entre um erro chato e um irreparavel.
TETO_DE_EXCLUSAO = 50


def titulos_para_excluir(codigos) -> list[dict]:
    """O que a tela mostra ANTES de apagar: valor, data, quem, e se tem baixa.

    A baixa aparece porque apagar titulo conciliado desmonta o extrato. O dono
    escolheu, em 21/09/2026, que o painel NAO recusa por conta propria — quem
    decide e o OMIE. Mas ele tem de ver, antes de confirmar, no que esta
    mexendo: recusar e uma coisa, esconder e outra."""
    codigos = [int(c) for c in codigos if str(c).strip().isdigit()]
    if not codigos:
        return []
    marcas = ",".join(["?"] * len(codigos))
    linhas = consultar(
        "SELECT DISTINCT ON (f.codigo_lancamento) f.codigo_lancamento, f.tipo,"
        "       f.numero_documento, f.categoria, f.departamento, f.razao_social,"
        "       f.data, f.situacao, f.pago_recebido, f.a_pagar_receber,"
        "       EXISTS (SELECT 1 FROM movimentos m"
        "                WHERE m.ncodtitulo = f.codigo_lancamento"
        "                  AND UPPER(COALESCE(m.cliquidado,'')) = 'S'),"
        "       COALESCE(f.observacao = ?, FALSE)"
        f"  FROM fato f WHERE f.codigo_lancamento IN ({marcas})"
        " ORDER BY f.codigo_lancamento, COALESCE(f.observacao = ?, FALSE)",
        [_marca_cc(), *codigos, _marca_cc()])
    campos = ("codigo", "tipo", "documento", "categoria", "departamento",
              "razao_social", "data", "situacao", "pago_recebido",
              "a_pagar_receber", "tem_baixa", "lancamento_cc")
    saida = []
    for bruta in linhas:
        titulo = dict(zip(campos, bruta))
        titulo["pago_recebido"] = float(titulo["pago_recebido"] or 0)
        titulo["a_pagar_receber"] = float(titulo["a_pagar_receber"] or 0)
        titulo["tem_baixa"] = bool(titulo["tem_baixa"])
        titulo["rateado_em"] = rateio_do_titulo(titulo["codigo"])
        saida.append(titulo)
    return saida


def _apagar_do_painel(conn, codigo: int) -> None:
    """Tira o titulo da base local, depois de o OMIE ter confirmado.

    Sem isto, o titulo apagado no OMIE continuaria no painel ate a proxima
    ATUALIZACAO COMPLETA — a varredura de excluidos roda uma vez por semana, nao
    todo dia. O dono apagaria, olharia a tela, veria o titulo la, e concluiria
    que nao funcionou."""
    conn.execute("DELETE FROM fato WHERE codigo_lancamento = ?", (codigo,))
    conn.execute("DELETE FROM fato_recebimentos WHERE codigo_lancamento = ?", (codigo,))
    conn.execute("DELETE FROM movimentos WHERE ncodtitulo = ?", (codigo,))
    conn.execute("DELETE FROM rateio WHERE codigo_lancamento_omie = ?", (codigo,))
    conn.execute("DELETE FROM titulos WHERE codigo_lancamento_omie = ?", (codigo,))


def excluir(codigos, *, simulacao: bool = True, cliente=None) -> dict:
    """Apaga os titulos escolhidos NO OMIE. Nao ha desfazer.

    Mesma forma da `aplicar`: uma linha por titulo, igual no ensaio e no envio
    de verdade, para o ensaio mostrar exatamente o que a execucao vai fazer."""
    from .sync import omie_client, omie_escrita

    codigos = sorted({int(c) for c in (codigos or [])
                      if str(c).strip().isdigit()})
    if not codigos:
        return {"ok": False, "erro": "Nenhum título foi escolhido."}

    titulos = titulos_para_excluir(codigos)
    if not titulos:
        return {"ok": False, "erro": "Nenhum título válido foi escolhido."}
    if len(titulos) > TETO_DE_EXCLUSAO:
        return {"ok": False,
                "erro": f"São {len(titulos)} títulos e o máximo por vez é "
                        f"{TETO_DE_EXCLUSAO}. Exclusão não tem desfazer — vá "
                        "em lotes menores."}
    if not simulacao and not escrita_configurada():
        return {"ok": False,
                "erro": "A exclusão no OMIE está desligada: falta configurar a "
                        "senha de execução (PAINEL_SENHA_ESCRITA) no serviço."}

    if cliente is None:
        try:
            cliente = omie_escrita.OmieEscrita.de_ambiente()
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "erro": f"Sem acesso ao OMIE: {e}"}

    resultados = []
    bloqueio = None
    apagados = []
    for titulo in titulos:
        tipo = omie_escrita.tipo_do_titulo(titulo["tipo"])
        titulo["tipo_omie"] = tipo
        linha = {**titulo, "ok": False, "resultado": ""}

        if bloqueio is not None:
            linha["resultado"] = "NÃO TENTADO: o OMIE bloqueou as chamadas."
            resultados.append(linha)
            continue

        # o número de um lançamento de conta, mandado ao endereço dos títulos,
        # no melhor caso dá erro; no pior, apaga o título que tiver o mesmo
        # número. Excluir lançamento de conta não é desta tela.
        if titulo.get("lancamento_cc"):
            linha["resultado"] = ("RECUSADO: é um lançamento feito direto na "
                                  "conta corrente, não um título. Exclua no OMIE.")
            resultados.append(linha)
            continue

        if simulacao:
            linha["resultado"] = "Ensaio: nada foi excluído."
            linha["ok"] = True
            resultados.append(linha)
            continue

        try:
            retorno = cliente.excluir_titulo(titulo["codigo"], tipo)
            linha["resultado"] = "Excluído no OMIE."
            linha["ok"] = True
            apagados.append(titulo["codigo"])
            registrar(titulo, False, "", "", ["EXCLUÍDO no OMIE"], True,
                      str(retorno)[:500])
        except omie_client.OmieBloqueada as e:
            bloqueio = e
            linha["resultado"] = f"NÃO EXCLUÍDO: {e}"
            logger.warning("Painel: OMIE bloqueou o lote de exclusão — %s", e)
        except Exception as e:  # noqa: BLE001 — um título com erro não para o lote
            linha["resultado"] = f"ERRO: {e}"
            logger.exception("Painel: falha ao excluir o titulo %s", titulo["codigo"])
            registrar(titulo, False, "", "", ["EXCLUSÃO recusada"], False, str(e))
        resultados.append(linha)

    # So depois de o OMIE confirmar. Apagar aqui antes seria perder o registro
    # de um titulo que continua existindo la.
    if apagados:
        try:
            with conexao() as conn:
                for codigo in apagados:
                    _apagar_do_painel(conn, codigo)
                conn.commit()
        except Exception:  # noqa: BLE001
            logger.exception("Painel: excluí no OMIE mas não consegui limpar a "
                             "base local dos títulos %s", apagados)

    return {"ok": True, "simulacao": simulacao, "linhas": resultados,
            "quantos": len(resultados), "excluidos": len(apagados),
            "bloqueio": str(bloqueio) if bloqueio else "",
            "bloqueio_segundos": bloqueio.segundos if bloqueio else 0}


def registrar(titulo: dict, simulacao: bool, categoria_nova, departamento_novo,
              mudancas, ok: bool, retorno: str) -> None:
    """Grava o que foi feito. Falhar aqui não pode derrubar a alteração — mas
    aparece no log do serviço, porque alteração sem rastro é o pior caso."""
    try:
        with conexao() as conn:
            conn.execute(
                "INSERT INTO alteracoes_omie (codigo_lancamento, tipo, documento,"
                " simulacao, categoria_nova, departamento_novo, mudancas, ok,"
                " retorno) VALUES (?,?,?,?,?,?,?,?,?)",
                (int(titulo["codigo"]), titulo.get("tipo_omie", ""),
                 titulo.get("documento") or "", bool(simulacao),
                 str(categoria_nova or ""), str(departamento_novo or ""),
                 " · ".join(mudancas or []), bool(ok), str(retorno or "")[:4000]))
            conn.commit()
    except Exception:  # noqa: BLE001
        logger.exception("Painel: NAO consegui registrar a alteracao do titulo %s",
                         titulo.get("codigo"))


def historico(limite: int = 200) -> list[dict]:
    """O que já foi alterado, do mais recente para o mais antigo."""
    from .horario import para_brasilia
    campos = ("quando", "codigo", "tipo", "documento", "simulacao", "mudancas",
              "ok", "retorno")
    return [dict(zip(campos, (para_brasilia(l[0]), l[1], l[2], l[3], l[4],
                              l[5], l[6], l[7])))
            for l in consultar(
                "SELECT quando, codigo_lancamento, tipo, documento, simulacao,"
                " mudancas, ok, retorno FROM alteracoes_omie"
                f" ORDER BY quando DESC LIMIT {int(limite)}")]


def normalizar_alvos(alvos, categoria_nova="", departamento_novo="") -> dict:
    """O que vai mudar em CADA título, no formato `{codigo: (categoria, obra)}`.

    Duas formas chegam aqui, e as duas são legítimas:

    - **um valor para todos** — `[501, 502]` mais `categoria_nova="2.02"`. É o
      lote: marcar várias linhas e mandar todas para a mesma obra.
    - **um valor por título** — `[{"codigo": 501, "categoria": "2.02"},
      {"codigo": 502, "departamento": "D9"}]`. É a edição na própria lista, em
      que cada linha pode ir para um lugar diferente.

    A tela usa a segunda; a primeira continua valendo porque é a que funciona
    sem JavaScript, e porque é ela que os testes exercitam há mais tempo.

    Título repetido não soma: o último pedido vale. Duas linhas do mesmo título
    rateado nunca podem virar duas chamadas ao OMIE — é um cadastro só."""
    pedido = {}
    for alvo in alvos or []:
        if isinstance(alvo, dict):
            codigo = str(alvo.get("codigo", "")).strip()
            categoria = (alvo.get("categoria") or "").strip()
            departamento = (alvo.get("departamento") or "").strip()
        else:
            codigo = str(alvo).strip()
            categoria = (categoria_nova or "").strip()
            departamento = (departamento_novo or "").strip()
        if not codigo.isdigit() or not (categoria or departamento):
            continue
        pedido[int(codigo)] = (categoria, departamento)
    return pedido


def aplicar(alvos, categoria_nova="", departamento_novo="", *,
            simulacao: bool = True, aceita_desfazer_rateio: bool = False,
            cliente=None) -> dict:
    """Altera (ou simula alterar) a classificação dos títulos escolhidos.

    Devolve uma linha por título, com o "de → para" e o resultado — a mesma
    forma na simulação e no envio de verdade, para que o ensaio mostre
    exatamente o que a execução vai fazer."""
    from .sync import omie_client, omie_escrita

    pedido = normalizar_alvos(alvos, categoria_nova, departamento_novo)
    if not pedido:
        return {"ok": False, "erro": "Escolha a categoria nova, o departamento "
                                     "novo, ou os dois."}

    titulos = titulos_para_alterar(pedido.keys())
    if not titulos:
        return {"ok": False, "erro": "Nenhum título válido foi escolhido."}
    if len(titulos) > TETO_POR_LOTE:
        return {"ok": False,
                "erro": f"São {len(titulos)} títulos e o limite por vez é "
                        f"{TETO_POR_LOTE}. Estreite o filtro."}

    if not simulacao and not escrita_configurada():
        return {"ok": False,
                "erro": "A alteração no OMIE está desligada: falta configurar a "
                        "senha de execução (PAINEL_SENHA_ESCRITA) no serviço."}

    if cliente is None and not simulacao:
        cliente = omie_escrita.OmieEscrita.de_ambiente()
    elif cliente is None:
        try:
            cliente = omie_escrita.OmieEscrita.de_ambiente()
        except Exception as e:  # noqa: BLE001 — simular sem credencial ainda ajuda
            return {"ok": False, "erro": f"Sem acesso ao OMIE para consultar: {e}"}

    resultados = []
    # A Omie bloqueando o acesso PARA o lote, e isso nao e desistencia.
    #
    # 21/09/2026: ela devolveu "API bloqueada por consumo indevido. Tente
    # novamente em 664 segundos". Tentar o proximo titulo cairia no mesmo
    # bloqueio e o PROLONGARIA — e é exatamente esse tipo de insistência que ela
    # esta punindo. Melhor parar, dizer quanto falta esperar, e listar o que
    # nao chegou a ser tentado.
    bloqueio = None
    refletidos = []

    for titulo in titulos:
        # cada título carrega o SEU destino: na edição linha a linha, um vai
        # para uma categoria e o outro para outra, no mesmo envio
        categoria_do_titulo, departamento_do_titulo = pedido[titulo["codigo"]]
        tipo = (TIPO_LANC_CC if titulo["lancamento_cc"]
                else omie_escrita.tipo_do_titulo(titulo["tipo"]))
        titulo["tipo_omie"] = tipo
        linha = {**titulo, "tipo_omie": tipo, "mudancas": [], "ok": False,
                 "resultado": "",
                 "categoria_pedida": categoria_do_titulo,
                 "departamento_pedido": departamento_do_titulo}

        if bloqueio is not None:
            linha["resultado"] = "NÃO TENTADO: o OMIE bloqueou as chamadas."
            resultados.append(linha)
            continue

        # a trava do rateio: recusar ANTES de consultar o OMIE
        if departamento_do_titulo and titulo["tem_rateio_multiplo"] and not aceita_desfazer_rateio:
            linha["resultado"] = (
                "RECUSADO: " + ("este lançamento" if titulo["lancamento_cc"]
                                else "este título") + " está rateado entre "
                + ", ".join(titulo["rateado_em"])
                + ". Trocar o departamento apagaria esse rateio.")
            resultados.append(linha)
            continue

        try:
            cadastro = _consultar_lembrando(cliente, titulo["codigo"], tipo)
            if tipo == TIPO_LANC_CC:
                novo, mudancas = omie_escrita.preparar_alteracao_cc(
                    cadastro, titulo["codigo"], categoria_do_titulo or None,
                    departamento_do_titulo or None)
            else:
                novo, mudancas = omie_escrita.preparar_alteracao(
                    cadastro, categoria_do_titulo or None,
                    departamento_do_titulo or None)
            linha["mudancas"] = mudancas
            if not mudancas:
                linha["resultado"] = "Nada a mudar: já está assim."
                linha["ok"] = True
            elif simulacao:
                linha["resultado"] = "Ensaio: nada foi enviado."
                linha["ok"] = True
            else:
                if tipo == TIPO_LANC_CC:
                    retorno = cliente.alterar_lancamento_cc(novo)
                else:
                    retorno = cliente.alterar_titulo(novo, tipo)
                # o cadastro guardado já não descreve o título: fora com ele
                _CADASTROS_RECENTES.pop((int(titulo["codigo"]), tipo), None)
                linha["resultado"] = "Alterado no OMIE."
                linha["ok"] = True
                refletidos.append((titulo, categoria_do_titulo,
                                   departamento_do_titulo, novo))
                registrar(titulo, False, categoria_do_titulo,
                          departamento_do_titulo, mudancas, True,
                          str(retorno)[:500])
        except omie_client.OmieBloqueada as e:
            bloqueio = e
            linha["resultado"] = f"NÃO ALTERADO: {e}"
            logger.warning("Painel: OMIE bloqueou o lote — %s", e)
        except Exception as e:  # noqa: BLE001 — um título com erro não para o lote
            linha["resultado"] = f"ERRO: {e}"
            logger.exception("Painel: falha ao alterar o titulo %s", titulo["codigo"])
            if not simulacao:
                registrar(titulo, False, categoria_do_titulo,
                          departamento_do_titulo, linha["mudancas"], False, str(e))
        resultados.append(linha)

    if refletidos:
        refletir_no_painel(refletidos)

    enviados = sum(1 for r in resultados if r["ok"] and r["mudancas"])
    return {"ok": True, "simulacao": simulacao, "linhas": resultados,
            "quantos": len(resultados), "alterados": enviados,
            "bloqueio": str(bloqueio) if bloqueio else "",
            "bloqueio_segundos": bloqueio.segundos if bloqueio else 0,
            "recusados": sum(1 for r in resultados
                             if r["resultado"].startswith("RECUSADO")),
            "titulos_alterados": sum(1 for t, *_ in refletidos
                                     if not t["lancamento_cc"])}


def refletir_no_painel(alterados) -> None:
    """Põe no painel, NA HORA, o que o OMIE acabou de aceitar.

    10/11/2026, o dono: *"depois que eu altero, qual é a atualização que eu tenho
    que fazer para ele aparecer no painel?"*. Para o lançamento de conta a
    resposta era pior que "uma atualização": NENHUMA trazia — a apropriação
    guardada (migração 022) não é perguntada de novo, e a categoria só voltava
    quando o pagamento daquele dia fosse relido.

    O que muda aqui, e por quê:
    - nas linhas do fato: a obra (e o projeto dela) e a categoria — é o que as
      telas leem. A lista do Explorador já volta mostrando a obra nova, e o
      próximo lote não manda de novo o que já foi;
    - no lançamento de conta, também o espelho: a apropriação guardada passa a
      ser o lançamento COMO FOI ENVIADO (sem consultar de novo — duas consultas
      iguais em segundos são o que o OMIE pune como "consumo redundante"), e a
      categoria do movimento. Assim refazer os números não desfaz nada;
    - no título, o espelho fica: a "Atualização do dia" relê o título alterado
      no OMIE e confirma.

    Falhar aqui NÃO desfaz o que foi para o OMIE: fica no log, e a atualização
    seguinte traz de lá."""
    import json
    marca = _marca_cc()
    try:
        with conexao() as conn:
            from .sync.fato import carregar_catalogos
            from .sync.apropriacao_cc import tabela_existe
            from .sync.espelho import _mst_guarda_bruto
            cat, _cli, proj_map, _cc = carregar_catalogos(conn)
            tem_apropriacao = tabela_existe(conn)
            mst_com_bruto = _mst_guarda_bruto(conn)
            nomes = {c: n for c, n in conn.execute(
                "SELECT ccoddep, MAX(cdesdep) FROM rateio"
                " WHERE COALESCE(ccoddep,'') <> '' GROUP BY 1").fetchall()}
            for titulo, categoria, departamento, enviado in alterados:
                codigo = int(titulo["codigo"])
                do_cc = bool(titulo["lancamento_cc"])
                filtro = ("observacao = ?" if do_cc
                          else "observacao IS DISTINCT FROM ?")
                if departamento:
                    conn.execute(
                        f"UPDATE fato SET departamento = ?, projeto = ?"
                        f" WHERE codigo_lancamento = ? AND {filtro}",
                        (nomes.get(departamento) or departamento,
                         proj_map.get(departamento, ""), codigo, marca))
                if categoria and str(categoria) in cat:
                    desc, grupo, analise = cat[str(categoria)]
                    conn.execute(
                        f"UPDATE fato SET categoria = ?, codigo_categoria = ?,"
                        f"       grupo = ?, analise = COALESCE(?, analise)"
                        f" WHERE codigo_lancamento = ? AND {filtro}",
                        (desc, str(categoria), grupo, analise, codigo, marca))
                if not do_cc:
                    continue
                if tem_apropriacao:
                    conn.execute(
                        "INSERT INTO apropriacao_lancamentos_cc"
                        " (ncodmovcc, bruto, erro, tentativas, lido_em)"
                        " VALUES (?, ?, NULL, 1, now())"
                        " ON CONFLICT (ncodmovcc) DO UPDATE SET"
                        "   bruto = EXCLUDED.bruto, erro = NULL, lido_em = now()",
                        (codigo, json.dumps(enviado, ensure_ascii=False, default=str)))
                if categoria and mst_com_bruto:
                    conn.execute(
                        "UPDATE movimentos_sem_titulo SET ccodcateg = ?"
                        " WHERE COALESCE(bruto::json->'detalhes'->>'nCodMovCC',"
                        "                bruto::json->'detalhes'->>'nCodLanc') = ?",
                        (str(categoria), str(codigo)))
            conn.commit()
        from . import consultas
        consultas.esquecer_listas()
    except Exception:  # noqa: BLE001
        logger.exception("Painel: alterei no OMIE mas não consegui pôr no painel "
                         "os lançamentos %s", [t["codigo"] for t, *_ in alterados])
