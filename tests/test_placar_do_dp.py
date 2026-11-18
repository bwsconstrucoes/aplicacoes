# -*- coding: utf-8 -*-
"""O PLACAR DO DP NÃO APODRECE EM SILÊNCIO.

⚑ POR QUE ESTE ARQUIVO EXISTE (17/11/2026). Ele:

> *"Eu estou tendo que corrigir minúcia a minúcia, porque nada está ficando
> 100%. E está muito desgastante, cansativo e demorado. Eu estou fazendo errado
> do jeito que a gente está fazendo aqui. Tem um método melhor?"*

**O diagnóstico, medido:** a especificação do Pipefy existia no repositório
desde 04/11/2026 (`DE_PARA_PIPEFY_DP.md`, a exportação dos dois pipes), e o
sistema tinha 13 dos tipos. **Mas nada comparava os dois.** Então o único
detector de vão era ele abrir uma tela e sentir falta — um por um, operando.

`PLACAR_DO_DP.md` é a comparação, com um veredito por item. Este teste é o que
impede o placar de virar enfeite:

  · item do Pipefy sem linha no placar → FALHA (o vão volta a ser invisível);
  · linha do placar sem veredito → FALHA (lista sem resposta não decide nada);
  · tipo que o ERP tem e o placar diz que não existe → FALHA (placar mentindo).

⚠️ ESTE TESTE NÃO EXIGE QUE TUDO ESTEJA PRONTO. ❌ e ❓ são vereditos
legítimos: o ERP não é espelho do Pipefy, e o que morre com ele é resposta tão
boa quanto o que foi construído. O que o teste recusa é o SILÊNCIO.
"""
from __future__ import annotations

import re
import unicodedata
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1] / "app" / "apps" / "erp"
DE_PARA = RAIZ / "DE_PARA_PIPEFY_DP.md"
PLACAR = RAIZ / "PLACAR_DO_DP.md"

VEREDITOS = ("✅", "🔀", "❌", "⚰️", "❓")


def _simples(s: str) -> str:
    """Sem acento, sem maiúscula, sem o rabicho de contagem de campos."""
    # o de-para escreve "— 44 campo(s)"; o placar, "(44 campos)". É a mesma coisa.
    s = re.sub(r"—\s*\d+\s*campo\(s\)\s*$", "", s).strip()
    s = re.sub(r"\(\s*\d+\s*campos?\s*\)\s*$", "", s).strip()
    sem = "".join(c for c in unicodedata.normalize("NFKD", s.lower())
                  if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", sem).strip()


def _itens_do_pipefy() -> list[str]:
    """Os títulos de nível 3 do de-para: cada um é um tipo ou uma fase."""
    texto = DE_PARA.read_text(encoding="utf-8")
    return [l[4:].strip() for l in texto.splitlines() if l.startswith("### ")]


def _linhas_do_placar() -> list[tuple[str, str]]:
    """(o que o Pipefy tem, o veredito) de cada linha de tabela do placar."""
    saida = []
    for linha in PLACAR.read_text(encoding="utf-8").splitlines():
        if not linha.startswith("|") or linha.startswith("| ---"):
            continue
        partes = [p.strip() for p in linha.strip("|").split("|")]
        if len(partes) != 2:
            continue
        esquerda, direita = partes
        # o cabeçalho de cada tabela e a legenda não são itens
        if esquerda in ("O que o Pipefy tem", "Fase no Pipefy", "Marca") \
                or set(esquerda) <= set("-: "):
            continue
        # a LEGENDA tem o veredito do lado esquerdo — não é item
        if esquerda in VEREDITOS:
            continue
        saida.append((esquerda, direita))
    return saida


def test_todo_item_do_pipefy_tem_linha_no_placar():
    """Item exportado do Pipe que ninguém triou volta a ser vão invisível."""
    no_placar = {_simples(e) for e, _ in _linhas_do_placar()}
    faltam = [i for i in _itens_do_pipefy() if _simples(i) not in no_placar]

    assert faltam == [], (
        "estes itens do Pipefy não têm linha em PLACAR_DO_DP.md:\n  "
        + "\n  ".join(faltam)
        + "\n  Dê um veredito a cada um (✅ está · 🔀 está em outro lugar · "
        "❌ falta · ⚰️ morre com o Pipefy · ❓ preciso perguntar). "
        "Sem linha, o vão só aparece quando ele abre a tela.")


def test_toda_linha_do_placar_tem_veredito():
    sem = [e for e, d in _linhas_do_placar()
           if not any(v in d for v in VEREDITOS)]
    assert sem == [], (
        "estas linhas do placar não dizem onde a coisa está:\n  "
        + "\n  ".join(sem))


def test_o_placar_nao_diz_que_falta_o_que_o_ERP_JA_TEM():
    """Placar que mente é pior que placar nenhum: manda construir o que existe."""
    import app.apps.erp.core.pessoal_solicitacoes as svc

    nomes_no_erp = {_simples(v.get("nome", "")) for v in svc.TIPOS.values()}
    mentiras = [e for e, d in _linhas_do_placar()
                if "❌" in d and _simples(e) in nomes_no_erp]
    assert mentiras == [], (
        "o placar diz ❌ (não existe) para o que o ERP TEM:\n  "
        + "\n  ".join(mentiras))


def test_todo_tipo_do_ERP_aparece_no_placar():
    """O caminho inverso: tipo construído que o placar não menciona é tipo que
    ninguém sabe se corresponde a algo do Pipe."""
    import app.apps.erp.core.pessoal_solicitacoes as svc

    no_placar = {_simples(e) for e, _ in _linhas_do_placar()}
    fora = [v.get("nome", k) for k, v in svc.TIPOS.items()
            if _simples(v.get("nome", "")) not in no_placar]
    assert fora == [], (
        "estes tipos existem no ERP e não estão no placar:\n  "
        + "\n  ".join(fora))


def test_o_placar_conta_quantos_itens_faltam_de_verdade():
    """Não é regra: é o número aparecendo em cada rodada da suíte, para o vão
    não voltar a ser uma sensação."""
    linhas = _linhas_do_placar()
    conta = {v: sum(1 for _, d in linhas if v in d) for v in VEREDITOS}
    print(f"\nPLACAR DO DP ({len(linhas)} itens): {conta['✅']} no ERP ·"
          f" {conta['🔀']} em outro lugar · {conta['❌']} faltam ·"
          f" {conta['⚰️']} morrem com o Pipefy ·"
          f" {conta['❓']} esperando resposta dele")
    # Uma linha PODE ter duas marcas (🔀 com um ❓ em cima), então a soma passa
    # do total — o que não pode é linha sem marca nenhuma, que é o teste acima.
    assert conta["❓"] > 0 or conta["❌"] == 0, (
        "nenhum ❓ e nenhum ❌: ou o placar está completo (ótimo) ou ninguém o "
        "está mantendo")
