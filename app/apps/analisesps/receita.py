# -*- coding: utf-8 -*-
"""
Quem é, de verdade, o dono de um CNPJ — perguntado à Receita.

Pedido do dono em 13/09/2026, na tela de nomes de credor: *"quando você dá
sugestão aqui, esse CNPJ é o quê? Eu quero que você faça a consulta via API do
credor desse CNPJ."*

O QUE ISSO RESOLVE. Hoje a tela compara os nomes que a EQUIPE digitou entre si.
Quando os dois são diferentes de verdade — razão social contra nome de fantasia,
ou empresa que mudou de nome —, nenhuma regra sabe qual vale, e a decisão sobra
para gente. Perguntando à Receita, passa a haver uma **terceira opinião que não
veio de ninguém daqui**: a razão social registrada.

E RESOLVE UM CASO PIOR, que ele descreveu na mesma mensagem: *"pode ser que a
pessoa digitou errado o CNPJ (…) ela confundiu, olhou na nota e olhou o CNPJ da
BWS, da empresa que ela trabalha, aí digitou o nome da empresa ao invés do CNPJ
ao qual a nota fazia referência."* Nesse caso os NOMES não ajudam — o erro está
no número. Mas a razão social do CNPJ digitado não vai parecer nenhum dos nomes
escritos, e é isso que acusa.

⚠️ TRÊS CUIDADOS, e nenhum é opcional:

  1. **NUNCA AUTOMÁTICO, E NUNCA EM MASSA.** A consulta sai por pedido de uma
     pessoa, um CNPJ por vez. Varrer novecentos fornecedores de uma vez é o
     jeito certo de ser bloqueado por uso excessivo — e aí a consulta para de
     funcionar para todo mundo, inclusive no caso em que ela importa.
  2. **A RESPOSTA FICA GUARDADA.** CNPJ não muda de dono; perguntar duas vezes
     a mesma coisa é desperdício e é o que leva ao bloqueio.
  3. **NUNCA DECIDE SOZINHA.** A Receita informa; quem escolhe o nome continua
     sendo o dono. Um serviço de fora ditando o que fica gravado na base seria
     trocar um problema conhecido por um invisível.

O SERVIÇO. `brasilapi.com.br` — público, sem cadastro e sem chave, mantido por
uma comunidade e alimentado pelos dados abertos da Receita. Escolhido por não
exigir credencial nova (ver `CONTEXTO.md`: credencial nova é decisão do dono) e
por não cobrar. **Se ele sair do ar, esta tela continua funcionando** — a
consulta é um extra; nada aqui depende dela.
"""
from __future__ import annotations

import logging

logger = logging.getLogger("analisesps.receita")

ENDERECO = "https://brasilapi.com.br/api/cnpj/v1/{cnpj}"

# Curto de propósito: é uma pessoa esperando na frente da tela. Melhor dizer
# "não consegui" em cinco segundos do que deixá-la olhando para o nada.
SEGUNDOS = 6


class ErroDeConsulta(RuntimeError):
    """Falha com a mensagem já pronta para a tela."""


def _so_digitos(valor) -> str:
    import re
    return re.sub(r"\D", "", str(valor or ""))


def guardada(cnpj: str) -> dict:
    """O que já se perguntou antes sobre este CNPJ. Vazio se nunca."""
    from .db import consultar_um

    limpo = _so_digitos(cnpj)
    if len(limpo) != 14:
        return {}
    try:
        linha = consultar_um(
            "SELECT cnpj, razao_social, fantasia, situacao, municipio, uf, "
            "       consultado_em, erro "
            "  FROM analisesps.receita_cnpj WHERE cnpj = ?", (limpo,))
    except Exception:  # noqa: BLE001 — migração 011 ainda não aplicada
        logger.exception("Análise de SPs: não consegui ler a consulta guardada")
        return {}
    if not linha:
        return {}
    nomes = ["cnpj", "razao_social", "fantasia", "situacao", "municipio", "uf",
             "consultado_em", "erro"]
    return dict(zip(nomes, linha))


def _guardar(cnpj: str, dados: dict, erro: str = "") -> None:
    from .db import conexao
    with conexao() as conn:
        conn.execute(
            "INSERT INTO analisesps.receita_cnpj "
            "  (cnpj, razao_social, fantasia, situacao, municipio, uf, "
            "   consultado_em, erro) "
            "VALUES (?, ?, ?, ?, ?, ?, now(), ?) "
            "ON CONFLICT (cnpj) DO UPDATE SET "
            "  razao_social = EXCLUDED.razao_social, "
            "  fantasia = EXCLUDED.fantasia, situacao = EXCLUDED.situacao, "
            "  municipio = EXCLUDED.municipio, uf = EXCLUDED.uf, "
            "  consultado_em = now(), erro = EXCLUDED.erro",
            (_so_digitos(cnpj), dados.get("razao_social", ""),
             dados.get("fantasia", ""), dados.get("situacao", ""),
             dados.get("municipio", ""), dados.get("uf", ""), erro))
        conn.commit()


def consultar(cnpj: str, forcar: bool = False) -> dict:
    """Pergunta quem é o dono deste CNPJ. Devolve o que ficou guardado.

    Já perguntado antes com sucesso, devolve o guardado sem falar com ninguém —
    CNPJ não muda de dono. `forcar` refaz a pergunta, e é o que serve para uma
    consulta que falhou por rede."""
    import json
    import urllib.error
    import urllib.request

    limpo = _so_digitos(cnpj)
    if len(limpo) != 14:
        raise ErroDeConsulta(
            "só dá para consultar CNPJ (14 números). CPF não tem consulta "
            "pública de nome.")

    antes = guardada(limpo)
    if antes and not antes.get("erro") and not forcar:
        return antes

    pedido = urllib.request.Request(
        ENDERECO.format(cnpj=limpo),
        headers={"User-Agent": "BWS-AnaliseSPs/1.0", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(pedido, timeout=SEGUNDOS) as resposta:
            bruto = json.loads(resposta.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        # 404 É RESPOSTA, NÃO FALHA: quer dizer que este CNPJ não existe na
        # base da Receita — e isso é justamente o achado mais útil quando o
        # número foi digitado errado.
        if e.code == 404:
            _guardar(limpo, {}, "CNPJ não encontrado na Receita")
            return guardada(limpo)
        motivo = f"a consulta respondeu {e.code}"
        _guardar(limpo, {}, motivo)
        raise ErroDeConsulta(motivo) from e
    except Exception as e:  # noqa: BLE001 — rede, tempo esgotado, serviço fora
        motivo = f"não consegui falar com a consulta pública: {e}"
        _guardar(limpo, {}, motivo)
        raise ErroDeConsulta(motivo) from e

    dados = {
        "razao_social": str(bruto.get("razao_social") or "").strip(),
        "fantasia": str(bruto.get("nome_fantasia") or "").strip(),
        "situacao": str(bruto.get("descricao_situacao_cadastral") or "").strip(),
        "municipio": str(bruto.get("municipio") or "").strip(),
        "uf": str(bruto.get("uf") or "").strip(),
    }
    _guardar(limpo, dados)
    logger.info("Análise de SPs: CNPJ %s consultado — %s", limpo,
                dados["razao_social"] or "(sem razão social)")
    return guardada(limpo)


def guardadas(cnpjs) -> dict:
    """O que já se sabe de VÁRIOS CNPJs, numa consulta só.

    A tela de credores pode ter dezenas de casos; uma consulta por linha seriam
    dezenas de idas ao banco numa tela só — e este banco tem um décimo de um
    núcleo."""
    limpos = sorted({_so_digitos(c) for c in (cnpjs or [])
                     if len(_so_digitos(c)) == 14})
    if not limpos:
        return {}
    try:
        from .db import consultar
        marcas = ",".join(["?"] * len(limpos))
        linhas = consultar(
            "SELECT cnpj, razao_social, fantasia, situacao, municipio, uf, "
            "       consultado_em, erro "
            f"  FROM analisesps.receita_cnpj WHERE cnpj IN ({marcas})",
            tuple(limpos))
    except Exception:  # noqa: BLE001 — migração 011 ainda não aplicada
        logger.exception("Análise de SPs: não consegui ler as consultas")
        return {}
    nomes = ["cnpj", "razao_social", "fantasia", "situacao", "municipio", "uf",
             "consultado_em", "erro"]
    return {l[0]: dict(zip(nomes, l)) for l in linhas}
