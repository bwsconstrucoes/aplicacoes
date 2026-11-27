# -*- coding: utf-8 -*-
"""
A VARREDURA DOS PAPÉIS — descobrir o que o assistente NÃO sabe, sem o dono
precisar digitar uma pergunta sequer.

⚑ A IDEIA É DELE (27/11/2026):

> *"De repente a gente, não sei, disparar… imagina aí, alguém se colocar no
> papel de um diretor de uma empresa que tem interesse em saber informações da
> empresa. Às vezes se colocar no papel de um coordenador de suprimento ou de
> um supervisor de obras. Então a gente tem que se colocar em papéis
> diferentes, de repente uma IA, para poder fazer esses questionamentos e
> compreender o sistema. Agora lembrando que o sistema está em constante
> mudança, né? E ampliação. Isso tem que estar casado também."*

⚠️ **ISTO NÃO "TREINA" NADA, e dizer o contrário seria vender fumaça.** Nenhum
modelo aprende aqui; nada fica mais esperto sozinho. O que esta varredura faz
é outra coisa, e é a que presta: **gera a lista de perguntas no lugar do
dono**, e mede quanto o sistema responde hoje.

O QUE SAI DELA, e é o valor:

  1. **O que ele já responde** — vira confiança, e mostra onde não precisa mexer.
  2. **O que ele NÃO responde, e por quê**: o dado não existe no sistema, ou
     existe e o mapa não alcança. São dois trabalhos diferentes.
  3. **Onde a mesma palavra quer dizer duas coisas** — a fonte dos números
     errados com cara de certos.

⚑ **É ELA QUE CASA COM O SISTEMA EM MUDANÇA.** Cresceu uma área, mudou um
campo, nasceu uma tela? Roda de novo e compara com a varredura anterior: o que
o assistente perdeu aparece na diferença. É uma prova de cobertura, não um
relatório bonito.

COMO RODAR (precisa de OPENAI_API_KEY e de um banco COM DADOS, de teste):

    DATABASE_URL=postgresql://...erp_teste \\
    OPENAI_API_KEY=... PYTHONPATH=. \\
    python3 app/apps/erp/scripts/varrer_perguntas.py --por-papel 12

A saída é um relatório em Markdown na tela (redirecione para um arquivo se
quiser guardar). **Nada é gravado no banco** — a varredura só lê.

⚠️ **CUSTO:** uma chamada de IA para gerar as perguntas de cada papel, mais
UMA POR PERGUNTA para montar a consulta. Com 4 papéis × 12 perguntas são 52
chamadas. Não rode isso sem querer.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

PAPEIS = {
    "dono": "o dono da construtora, que quer saber de gente, custo e risco, "
            "e pergunta em voz alta, sem termo técnico",
    "dp": "quem cuida do departamento pessoal: admissões, desligamentos, "
          "férias, afastamentos, documentos e a fila de pedidos",
    "administrativo_de_obra": "quem toca o administrativo de uma obra: a "
                              "equipe que está no canteiro, o que ele pediu "
                              "ao DP e o que está parado",
    "contabilidade": "quem confere a folha: quem entrou, quem saiu, quem está "
                     "afastado, quem mudou de cargo",
}

INSTRUCAO_PAPEL = """Você é {descricao}.

Escreva {quantos} perguntas que VOCÊ faria a um sistema de gestão da
construtora, sobre PESSOAS e sobre os pedidos ao departamento pessoal.

Regras:
- escreva como se fala, não como se consulta um banco;
- perguntas diferentes entre si (não repita a mesma com outras palavras);
- misture: contagem, lista, "quem está", "o que está parado", prazo;
- algumas devem ser de coisas que um sistema desses PODERIA não ter.

Responda SOMENTE um JSON: {{"perguntas": ["...", "..."]}}"""


def _cliente():
    chave = os.getenv("OPENAI_API_KEY", "").strip()
    if not chave:
        sys.exit("RECUSADO: falta OPENAI_API_KEY. A varredura usa IA para "
                 "gerar as perguntas e para montar as consultas.")
    from openai import OpenAI
    return OpenAI(api_key=chave)


def perguntas_do_papel(cliente, papel: str, quantos: int) -> list[str]:
    from app.apps.erp.core.perguntas.na_hora import MODELO
    resp = cliente.chat.completions.create(
        model=MODELO, temperature=0.8,
        messages=[{"role": "user", "content": INSTRUCAO_PAPEL.format(
            descricao=PAPEIS[papel], quantos=quantos)}])
    bruto = resp.choices[0].message.content or "{}"
    bruto = bruto.strip().removeprefix("```json").removeprefix("```").removesuffix("```")
    try:
        return list(json.loads(bruto).get("perguntas") or [])
    except Exception:                                      # noqa: BLE001
        return []


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--por-papel", type=int, default=10)
    ap.add_argument("--papel", action="append", choices=sorted(PAPEIS))
    args = ap.parse_args()

    if not os.getenv("DATABASE_URL"):
        sys.exit("RECUSADO: falta DATABASE_URL (use um banco de TESTE).")
    if "teste" not in os.environ["DATABASE_URL"].lower():
        sys.exit("RECUSADO: a varredura roda contra um banco com 'teste' no "
                 "nome. Ela lê dado de gente — não é para apontar na produção.")

    from sqlalchemy import select

    from app.apps.erp.core.perguntas import entender, catalogo, mapa, na_hora
    from app.apps.erp.db.database import get_session
    from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario

    cliente = _cliente()
    papeis = args.papel or sorted(PAPEIS)
    linhas: list[dict[str, Any]] = []

    with get_session() as s:
        # Quem pergunta na varredura é um ADMIN: o objetivo é medir o MAPA,
        # não o recorte. O recorte tem testes próprios, com banco.
        quem = s.scalars(select(Usuario).where(
            Usuario.perfil == PerfilUsuario.ADMIN, Usuario.ativo.is_(True))).first()
        if quem is None:
            sys.exit("RECUSADO: não há um usuário ADMIN neste banco para "
                     "fazer as perguntas.")
        visoes = mapa.visoes_para(s, quem)

        for papel in papeis:
            for pergunta in perguntas_do_papel(cliente, papel, args.por_papel):
                leitura = entender.entender(pergunta, catalogo.CATALOGO)
                if leitura.get("entendi"):
                    linhas.append({"papel": papel, "pergunta": pergunta,
                                   "como": "catálogo",
                                   "detalhe": leitura.get("pergunta", "")})
                    continue
                try:
                    r = na_hora.responder(s, pergunta, quem)
                except Exception as e:                      # noqa: BLE001
                    linhas.append({"papel": papel, "pergunta": pergunta,
                                   "como": "erro", "detalhe": str(e)[:160]})
                    continue
                linhas.append({
                    "papel": papel, "pergunta": pergunta,
                    "como": "montada" if r.get("respondeu") else (r.get("tipo") or "nao_sei"),
                    "detalhe": (r.get("caminho", {}).get("entendi")
                                if r.get("respondeu") else r.get("recado", ""))[:200],
                })
        s.rollback()          # a varredura só lê; nada do que ela tocou fica

    _relatorio(linhas)


def _relatorio(linhas: list[dict[str, Any]]) -> None:
    total = len(linhas) or 1
    por_como: dict[str, int] = {}
    for l in linhas:
        por_como[l["como"]] = por_como.get(l["como"], 0) + 1

    print("# Varredura dos papéis — o que o assistente alcança\n")
    print(f"{len(linhas)} perguntas, geradas por IA nos papéis: "
          f"{', '.join(sorted({l['papel'] for l in linhas}))}\n")
    print("| Como terminou | Quantas | % |")
    print("|---|---:|---:|")
    for como, n in sorted(por_como.items(), key=lambda x: -x[1]):
        print(f"| {como} | {n} | {n * 100 // total}% |")
    print("\n## O que ele NÃO respondeu — é esta a lista que vira trabalho\n")
    for l in linhas:
        if l["como"] in ("catálogo", "montada"):
            continue
        print(f"- **{l['pergunta']}** ({l['papel']}) — {l['como']}: {l['detalhe']}")
    print("\n## O que ele respondeu\n")
    for l in linhas:
        if l["como"] in ("catálogo", "montada"):
            print(f"- {l['pergunta']} → _{l['como']}_: {l['detalhe']}")


if __name__ == "__main__":
    main()
