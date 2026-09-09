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
import unicodedata

from .db import conexao, consultar

logger = logging.getLogger("painel.saneamento")

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


def titulos_para_alterar(codigos) -> list[dict]:
    """Os dados que a tela precisa mostrar antes de alterar: o que o título é
    hoje e se ele tem rateio em mais de uma obra."""
    codigos = [int(c) for c in codigos if str(c).strip().isdigit()]
    if not codigos:
        return []
    marcas = ",".join(["?"] * len(codigos))
    linhas = consultar(
        "SELECT DISTINCT ON (codigo_lancamento) codigo_lancamento, tipo, "
        "       numero_documento, categoria, codigo_categoria, departamento, "
        "       razao_social "
        f"  FROM fato WHERE codigo_lancamento IN ({marcas}) "
        " ORDER BY codigo_lancamento", codigos)
    campos = ("codigo", "tipo", "documento", "categoria", "codigo_categoria",
              "departamento", "razao_social")
    saida = []
    for bruta in linhas:
        titulo = dict(zip(campos, bruta))
        titulo["rateado_em"] = rateio_do_titulo(titulo["codigo"])
        titulo["tem_rateio_multiplo"] = len(titulo["rateado_em"]) > 1
        saida.append(titulo)
    return saida


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


def aplicar(codigos, categoria_nova="", departamento_novo="", *,
            simulacao: bool = True, aceita_desfazer_rateio: bool = False,
            cliente=None) -> dict:
    """Altera (ou simula alterar) a classificação dos títulos escolhidos.

    Devolve uma linha por título, com o "de → para" e o resultado — a mesma
    forma na simulação e no envio de verdade, para que o ensaio mostre
    exatamente o que a execução vai fazer."""
    from .sync import omie_escrita

    if not categoria_nova and not departamento_novo:
        return {"ok": False, "erro": "Escolha a categoria nova, o departamento "
                                     "novo, ou os dois."}

    titulos = titulos_para_alterar(codigos)
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
    for titulo in titulos:
        tipo = omie_escrita.tipo_do_titulo(titulo["tipo"])
        titulo["tipo_omie"] = tipo
        linha = {**titulo, "tipo_omie": tipo, "mudancas": [], "ok": False,
                 "resultado": ""}

        # a trava do rateio: recusar ANTES de consultar o OMIE
        if departamento_novo and titulo["tem_rateio_multiplo"] and not aceita_desfazer_rateio:
            linha["resultado"] = (
                "RECUSADO: este título está rateado entre "
                + ", ".join(titulo["rateado_em"])
                + ". Trocar o departamento apagaria esse rateio.")
            resultados.append(linha)
            continue

        try:
            cadastro = cliente.consultar_titulo(titulo["codigo"], tipo)
            novo, mudancas = omie_escrita.preparar_alteracao(
                cadastro, categoria_nova or None, departamento_novo or None)
            linha["mudancas"] = mudancas
            if not mudancas:
                linha["resultado"] = "Nada a mudar: já está assim."
                linha["ok"] = True
            elif simulacao:
                linha["resultado"] = "Ensaio: nada foi enviado."
                linha["ok"] = True
            else:
                retorno = cliente.alterar_titulo(novo, tipo)
                linha["resultado"] = "Alterado no OMIE."
                linha["ok"] = True
                registrar(titulo, False, categoria_nova, departamento_novo,
                          mudancas, True, str(retorno)[:500])
        except Exception as e:  # noqa: BLE001 — um título com erro não para o lote
            linha["resultado"] = f"ERRO: {e}"
            logger.exception("Painel: falha ao alterar o titulo %s", titulo["codigo"])
            if not simulacao:
                registrar(titulo, False, categoria_nova, departamento_novo,
                          linha["mudancas"], False, str(e))
        resultados.append(linha)

    enviados = sum(1 for r in resultados if r["ok"] and r["mudancas"])
    return {"ok": True, "simulacao": simulacao, "linhas": resultados,
            "quantos": len(resultados), "alterados": enviados,
            "recusados": sum(1 for r in resultados
                             if r["resultado"].startswith("RECUSADO"))}
