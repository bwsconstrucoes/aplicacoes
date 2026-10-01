# -*- coding: utf-8 -*-
"""
OS DIARISTAS: quem tem dia de DIÁRIA no mês, e quantos.

*"E cadê os diaristas? Não entrou diaristas."* (dono, 28/09/2026)

⚠️ A REGRA JÁ EXISTIA — o que faltava era ligá-la. `folha_vinculo.py` é tradução
fiel de uma fórmula que roda em produção (coluna AH da aba Mobponto), e a
descoberta que ela carrega é esta: **o vínculo é decidido POR DIA, não por
pessoa.** A mesma pessoa tem dias de diária e dias de CTPS no mês em que foi
registrada — os dias entre começar a trabalhar e ser admitida são diária; do
registro em diante, CTPS.

Classificar por pessoa jogaria o mês inteiro de quem foi admitido no meio do
período para um lado só, e metade do dinheiro iria pelo caminho errado.

⚠️ O QUE ISTO JÁ CONSEGUE, E O QUE AINDA NÃO:

  **consegue** — dizer QUEM tem dia de diária e QUANTOS dias, porque para isso
  basta a DATA de cada dia de ponto (que está guardada) mais as duas datas do
  cadastro (Data de Início e Data de Admissão).

  **não consegue** — dizer QUANTO pagar. O valor da diária não está em nenhuma
  coluna que eu tenha confirmado, e os acréscimos (+20 feriado, +10 sábado, +20
  domingo, VIGIA fora) dependem de saber a OBRA e a situação de cada dia — que
  vêm dos campos do ponto cujo nome ainda não é conhecido.

  E a diferença entre as duas coisas fica DITA na tela. Mostrar "R$ 0,00" onde
  falta o valor da diária seria pior do que mostrar "falta o valor": zero tem cara
  de resposta.
"""
from __future__ import annotations

import logging

from . import colaboradores, folha_vinculo, ponto

logger = logging.getLogger("analisesps.folha")


def _cadastro_para_a_regra(ficha: dict) -> dict:
    """A ficha no formato que `folha_vinculo.classificar_dia` espera."""
    return {"inicio": ficha.get("data_inicio"),
            "admissao": ficha.get("data_admissao"),
            "tipo": ficha.get("tipo"),
            "contrato": ficha.get("tipo_contrato")}


def levantar(ano: int, mes: int) -> dict:
    """Quem trabalhou como diarista no mês, e quantos dias cada um.

    Sai da carga do ponto do mês cruzada com o cadastro. Sem carga do ponto não há
    o que dizer — e a tela diz isso, em vez de mostrar lista vazia."""
    from .db import consultar

    carga = None
    try:
        carga = ponto.carga_do_mes(ano, mes)
    except Exception:  # noqa: BLE001 — a tela tem de dizer o que houve
        logger.exception("Diaristas: não consegui ler a carga do ponto")
    if not carga:
        return {"tem_ponto": False, "pessoas": [], "carga": None,
                "dias_de_diaria": 0, "quantos": 0, "sem_cadastro": []}

    # Os dias por pessoa, de uma consulta só. A data é o que a regra precisa.
    linhas = consultar(
        "SELECT cpf, nome, data FROM analisesps.ponto_dia "
        " WHERE carga_id = ? AND data IS NOT NULL ORDER BY cpf, data",
        (carga["id"],))
    dias_por_cpf: dict = {}
    nome_do_ponto: dict = {}
    for cpf, nome, data in linhas:
        dias_por_cpf.setdefault(cpf, []).append(data)
        nome_do_ponto.setdefault(cpf, nome or "")

    fichas = colaboradores.muitos_por_cpf(list(dias_por_cpf))
    obras_por_nome = colaboradores.codigos_das_obras()

    pessoas, sem_cadastro = [], []
    for cpf, dias in dias_por_cpf.items():
        ficha = fichas.get(cpf)
        if not ficha:
            # ⚠️ NÃO DESAPARECE DA LISTA. Quem bate ponto e não está no cadastro é
            # justamente o caso que dá confusão — e esconder faria alguém
            # trabalhar e não receber, sem nada na tela.
            sem_cadastro.append({"cpf": cpf, "nome": nome_do_ponto.get(cpf, ""),
                                 "dias": len(dias)})
            continue
        contagem = folha_vinculo.classificar_dias(
            dias, _cadastro_para_a_regra(ficha))
        de_diaria = contagem.get(folha_vinculo.DIARIA, 0)
        if not de_diaria:
            continue
        pessoas.append({
            "cpf": cpf, "cpf_bonito": ficha.get("cpf_bonito", ""),
            "nome": ficha.get("nome") or nome_do_ponto.get(cpf, ""),
            "cargo": ficha.get("cargo", ""),
            "obra": colaboradores.resolver_obra(ficha, obras_por_nome),
            "link_pipefy": ficha.get("link_pipefy", ""),
            "dias_de_diaria": de_diaria,
            "dias_de_ctps": contagem.get(folha_vinculo.CTPS, 0),
            "dias_sem_decidir": (contagem.get(folha_vinculo.FALTA_DATA, 0)
                                 + contagem.get(folha_vinculo.NAO_ENCONTRADO, 0)),
            "data_inicio": ficha.get("data_inicio"),
            "data_admissao": ficha.get("data_admissao"),
            "tipo_contrato": ficha.get("tipo_contrato", ""),
            "contagem": contagem,
        })

    # Quem tem dia sem decidir vem primeiro: é o cadastro pela metade, e é o que
    # trava o pagamento.
    pessoas.sort(key=lambda p: (p["dias_sem_decidir"] == 0,
                                (p["nome"] or "").lower()))
    return {
        "tem_ponto": True, "carga": carga, "pessoas": pessoas,
        "quantos": len(pessoas),
        "dias_de_diaria": sum(p["dias_de_diaria"] for p in pessoas),
        "com_pendencia": [p for p in pessoas if p["dias_sem_decidir"]],
        "sem_cadastro": sem_cadastro,
        # ⚠️ O QUE FALTA PARA VIRAR DINHEIRO. Fica no resultado para a tela dizer,
        # em vez de mostrar zero com cara de resposta.
        "falta_para_pagar": [
            "o VALOR da diária de cada pessoa (não achei a coluna no cadastro)",
            "o nome dos campos de cada dia do ponto, que é o que diz a OBRA do dia "
            "e se foi sábado, domingo ou feriado (os acréscimos de +10 e +20)",
        ],
    }
