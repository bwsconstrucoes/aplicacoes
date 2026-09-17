# -*- coding: utf-8 -*-
"""O DOCUMENTO DA NOTA: baixar da Receita, guardar no Drive, mostrar na tela.

Pedido do dono em 16/09/2026: *"tem como visualizar fácil a partir da tela de
associação, clicar e ver a nota fiscal? (…) que ficasse um processamento após a
baixa das notas, baixando, gerando esses PDFs e salvando no Google Drive."*

E a autorização, no dia seguinte: *"pode baixar as notas dando essa ciência."*

COMO A NOTA CHEGA AQUI, e por que são duas etapas e não uma:

  1. a distribuição entrega o **resumo** (chave, emitente, valor, situação) —
     é o que a tela já usa para conciliar;
  2. o **XML completo** só é liberado ao destinatário DEPOIS da ciência da
     operação. Dada a ciência, ele aparece num lote seguinte da distribuição,
     e é aí que dá para guardar.

Ou seja: **ciência hoje, documento na próxima rodada.** Não é atraso do
sistema, é como a Receita funciona — e a tela precisa dizer isso, senão quem
clica acha que quebrou.

⚠️ O ARQUIVO NÃO FICA NO BANCO. Um XML tem de 10 a 50 KB; seis mil notas
seriam centenas de megabytes num Postgres de 0,25 GB de RAM que já morreu de
memória uma vez (`CONTEXTO.md` §9). Vai para o Drive — o mesmo caminho que já
guarda os comprovantes — e aqui fica só o endereço.

⚠️ O QUE TEM VALOR FISCAL É O XML, não o PDF. O DANFE é representação
imprimível; quem guarda documento guarda o XML. Por isso é ele que sobe, e o
tipo fica explícito na tabela: no dia em que houver PDF, os dois convivem.
"""
from __future__ import annotations

import logging
import os
import re

logger = logging.getLogger("analisesps.notas_arquivo")

MIME_XML = "text/xml"

# Quantas notas por rodada. A Receita limita consultas seguidas, e a ciência é
# uma escrita — ir devagar aqui é o que evita o bloqueio por consumo indevido
# que já aconteceu com dois CNPJs em 15/09.
CIENCIAS_POR_RODADA = int(os.getenv("ANALISESPS_CIENCIAS_POR_RODADA", "40"))


class SemDocumento(LookupError):
    """A nota existe, mas o documento dela ainda não. NÃO é erro.

    É o estado normal de toda nota antes da ciência, e a tela precisa dizer
    isso com outras palavras que não "deu erro" — senão quem lê vai procurar
    defeito onde só falta esperar a rodada seguinte."""


def _pasta_do_drive() -> str:
    """A pasta onde os XMLs ficam.

    REUSA O MESMO CAMINHO DO BEEVALE (`beevale.pasta_do_drive`), que já resolve
    os três lugares em que a pasta pode estar configurada: a tela, o Render e a
    planilha de credenciais. Uma segunda resolução divergiria no dia em que o
    dono mudasse a pasta num lugar só.

    `DRIVE_FOLDER_NOTAS` vem na frente para quem quiser separar as notas dos
    comprovantes; sem ela, vale a pasta geral — melhor guardar junto do que não
    guardar."""
    propria = (os.getenv("DRIVE_FOLDER_NOTAS") or "").strip()
    if propria:
        return propria
    try:
        from . import beevale
        return (beevale.pasta_do_drive() or ("", ""))[0]
    except Exception:  # noqa: BLE001 — sem pasta, quem chama diz o que falta
        logger.exception("Análise de SPs: não consegui descobrir a pasta do Drive")
        return ""


def guardar_xml(chave: str, xml: str) -> dict:
    """Sobe o XML da nota no Drive e registra o endereço. Devolve {'ok','link'}.

    NÃO SOBE DUAS VEZES: se já houver arquivo guardado para aquela chave, sai
    na hora. Cada subida é uma ida ao Google, e o documento não muda."""
    from .db import conexao, consultar_um
    from . import drive

    chave = re.sub(r"\D", "", str(chave or ""))
    if len(chave) != 44 or not str(xml or "").strip():
        return {"ok": False, "erro": "chave ou XML inválido"}

    try:
        ja = consultar_um(
            "SELECT link FROM analisesps.nota_arquivo "
            " WHERE chave = ? AND tipo = 'xml'", (chave,))
    except Exception:  # noqa: BLE001 — migração 017 ainda não aplicada
        logger.exception("Análise de SPs: não consegui ler o arquivo da nota")
        return {"ok": False, "erro": "a atualização do banco ainda não foi "
                                     "aplicada"}
    if ja and ja[0]:
        return {"ok": True, "link": ja[0], "ja_tinha": True}

    conteudo = xml.encode("utf-8")
    try:
        subiu = drive.subir_arquivo(conteudo, f"{chave}.xml",
                                    _pasta_do_drive(), MIME_XML)
    except Exception as e:  # noqa: BLE001 — Drive fora, cota, pasta errada
        logger.exception("Análise de SPs: falhou subir o XML de %s", chave)
        return {"ok": False, "erro": str(e)[:300]}

    with conexao() as conn:
        conn.execute(
            "INSERT INTO analisesps.nota_arquivo "
            "  (chave, tipo, link, arquivo_id, tamanho) "
            "VALUES (?, 'xml', ?, ?, ?) "
            "ON CONFLICT (chave, tipo) DO UPDATE SET "
            "  link = EXCLUDED.link, arquivo_id = EXCLUDED.arquivo_id, "
            "  tamanho = EXCLUDED.tamanho, guardado_em = now()",
            (chave, subiu.get("link", ""), subiu.get("id", ""), len(conteudo)))
        conn.commit()
    logger.info("Análise de SPs: XML da nota %s guardado no Drive.", chave)
    return {"ok": True, "link": subiu.get("link", ""), "ja_tinha": False}


def baixar_xml(chave: str) -> str:
    """O XML guardado daquela nota, como texto. Levanta se não houver.

    Quem chama é a tela que abre a nota: ela precisa distinguir "ainda não
    tenho o documento" (que é o normal antes da ciência) de "tenho, mas não
    consegui buscar" — e as duas frases são diferentes para quem lê."""
    from .db import consultar_um
    from . import drive

    chave = re.sub(r"\D", "", str(chave or ""))
    if len(chave) != 44:
        raise ValueError("Chave de acesso inválida.")
    try:
        linha = consultar_um(
            "SELECT arquivo_id, link FROM analisesps.nota_arquivo "
            " WHERE chave = ? AND tipo = 'xml'", (chave,))
    except Exception as e:  # noqa: BLE001 — migração 017 ainda não aplicada
        logger.exception("Análise de SPs: não consegui ler o arquivo da nota")
        raise drive.ErroDoDrive(
            "Esta tela precisa da atualização do banco. Vá em Configurações e "
            "aperte \"Aplicar atualizações do banco\".") from e
    if not linha or not linha[0]:
        raise SemDocumento(
            "O documento desta nota ainda não chegou. A Receita só entrega a "
            "nota inteira depois da ciência da operação — e ela chega na busca "
            "seguinte, não na hora.")
    return drive.baixar_arquivo(linha[0]).decode("utf-8", errors="replace")


def arquivos_das_notas(chaves: list) -> dict:
    """{chave: {'xml': link, 'pdf': link}} — para a tela, numa consulta só.

    Uma consulta por nota, com 200 notas na tela, seriam 200 idas ao banco. É a
    mesma lição da conciliação, que já custou 28 segundos de tela."""
    from .db import consultar

    limpas = [re.sub(r"\D", "", str(c or "")) for c in (chaves or [])]
    limpas = [c for c in limpas if len(c) == 44]
    if not limpas:
        return {}
    marcadores = ",".join(["?"] * len(limpas))
    try:
        linhas = consultar(
            "SELECT chave, tipo, link FROM analisesps.nota_arquivo "
            f" WHERE chave IN ({marcadores})", tuple(limpas))
    except Exception:  # noqa: BLE001 — migração 017 ainda não aplicada
        logger.exception("Análise de SPs: não consegui ler os arquivos das notas")
        return {}
    saida: dict = {}
    for chave, tipo, link in linhas:
        saida.setdefault(chave, {})[tipo] = link
    return saida


def ciencia_das_notas(chaves: list) -> dict:
    """{chave: {'ok', 'motivo', 'enviado_em'}} — o que já foi manifestado."""
    from .db import consultar

    limpas = [re.sub(r"\D", "", str(c or "")) for c in (chaves or [])]
    limpas = [c for c in limpas if len(c) == 44]
    if not limpas:
        return {}
    marcadores = ",".join(["?"] * len(limpas))
    try:
        linhas = consultar(
            "SELECT chave, ok, motivo, enviado_em FROM analisesps.nota_evento "
            f" WHERE tipo = ? AND chave IN ({marcadores})",
            ("210210",) + tuple(limpas))
    except Exception:  # noqa: BLE001 — migração 017 ainda não aplicada
        logger.exception("Análise de SPs: não consegui ler a ciência das notas")
        return {}
    return {l[0]: {"ok": bool(l[1]), "motivo": l[2], "enviado_em": l[3]}
            for l in linhas}


def registrar_evento(chave: str, resultado: dict, quem: str = "") -> None:
    """Grava a tentativa — deu certo ou não.

    ⚠️ O REGISTRO É A PARTE AUDITÁVEL. A ciência é uma declaração em nome da
    empresa; sem saber quando foi enviada e o que a Receita respondeu, ninguém
    consegue explicar o que a BWS declarou."""
    from .db import conexao

    try:
        with conexao() as conn:
            conn.execute(
                "INSERT INTO analisesps.nota_evento "
                "  (chave, tipo, ok, codigo, motivo, protocolo, quem) "
                "VALUES (?, '210210', ?, ?, ?, ?, ?) "
                "ON CONFLICT (chave, tipo) DO UPDATE SET "
                "  enviado_em = now(), ok = EXCLUDED.ok, "
                "  codigo = EXCLUDED.codigo, motivo = EXCLUDED.motivo, "
                "  protocolo = EXCLUDED.protocolo, quem = EXCLUDED.quem",
                (re.sub(r"\D", "", str(chave or "")), bool(resultado.get("ok")),
                 str(resultado.get("codigo", ""))[:20],
                 str(resultado.get("motivo", ""))[:400],
                 str(resultado.get("protocolo", ""))[:60], str(quem or "")[:80]))
            conn.commit()
    except Exception:  # noqa: BLE001 — registrar não pode virar outra falha
        logger.exception("Análise de SPs: não consegui registrar a ciência de %s",
                         chave)


def notas_para_manifestar(limite: int = 0) -> list:
    """As notas que ainda esperam ciência, das mais novas para as mais velhas.

    OS QUATRO CORTES, e cada um evita uma chamada que seria recusada:

      1. **só NF-e** — a manifestação do destinatário não existe para CT-e;
      2. **dentro do prazo** (90 dias da emissão) — fora dele a Receita recusa;
      3. **não cancelada** — dar ciência em nota cancelada não serve a nada;
      4. **ainda não tentada** — a segunda ciência é recusada, e insistir é o
         caminho do bloqueio por consumo indevido.

    E exige o **destinatário conhecido**: quem declara é o CNPJ que recebeu a
    nota, com o certificado dele. Sem saber qual é, não há como assinar."""
    from .db import consultar
    from . import sefaz

    limite = int(limite or CIENCIAS_POR_RODADA)
    try:
        linhas = consultar(
            "SELECT n.chave, n.destinatario_doc, n.numero, n.emissao "
            "  FROM analisesps.notas_fiscais n "
            " WHERE substring(n.chave from 21 for 2) = '55' "
            "   AND upper(trim(coalesce(n.status, ''))) <> 'CANCELADA' "
            "   AND n.emissao IS NOT NULL "
            "   AND n.emissao >= current_date - ? "
            "   AND length(regexp_replace(coalesce(n.destinatario_doc, ''), "
            "                             '\\D', '', 'g')) = 14 "
            "   AND NOT EXISTS (SELECT 1 FROM analisesps.nota_evento e "
            "                    WHERE e.chave = n.chave AND e.tipo = '210210') "
            "   AND NOT EXISTS (SELECT 1 FROM analisesps.nota_arquivo a "
            "                    WHERE a.chave = n.chave AND a.tipo = 'xml') "
            " ORDER BY n.emissao DESC LIMIT ?",
            (sefaz.DIAS_PARA_CIENCIA, limite))
    except Exception:  # noqa: BLE001 — migração 017 ainda não aplicada
        logger.exception("Análise de SPs: não consegui listar as notas a "
                         "manifestar")
        return []
    return [{"chave": l[0], "destinatario": re.sub(r"\D", "", str(l[1] or "")),
             "numero": l[2], "emissao": l[3]} for l in linhas]


def manifestar_pendentes(anotar=None, quem: str = "rotina",
                         limite: int = 0) -> dict:
    """Dá ciência nas notas que faltam. É o que a tarefa longa chama.

    ⚠️ UMA NOTA POR VEZ, com teto por rodada. A Receita limita consultas
    seguidas, e esta é uma ESCRITA. Em 15/09 dois CNPJs foram bloqueados por
    uma hora por consultas demais — e aquilo era só leitura."""
    from . import sefaz

    anotar = anotar or (lambda *a, **k: None)
    if not sefaz.configurado():
        return {"manifestadas": 0, "falhas": 0, "erro": "sem certificado"}

    pendentes = notas_para_manifestar(limite)
    feitas, falhas, ja_existiam = 0, 0, 0
    for n, nota in enumerate(pendentes, start=1):
        anotar("dando ciência nas notas na Receita",
               f"{n} de {len(pendentes)} — nota {nota['numero']}")
        try:
            resultado = sefaz.manifestar_ciencia(nota["destinatario"],
                                                 nota["chave"])
        except Exception as e:  # noqa: BLE001 — uma nota não derruba a rodada
            logger.exception("Análise de SPs: falhou a ciência de %s",
                             nota["chave"])
            resultado = {"ok": False, "codigo": "", "motivo": str(e)[:300]}
        registrar_evento(nota["chave"], resultado, quem)
        if resultado.get("ok"):
            feitas += 1
            if resultado.get("ja_existia"):
                ja_existiam += 1
        else:
            falhas += 1
    return {"manifestadas": feitas, "falhas": falhas,
            "ja_existiam": ja_existiam, "olhadas": len(pendentes)}
