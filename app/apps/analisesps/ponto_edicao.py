# -*- coding: utf-8 -*-
"""
CORRIGIR O PONTO NO MOBPONTO, A PARTIR DA JANELA DO FUNCIONÁRIO — 01/10/2026.

Pedido dele: *"Quero poder fazer a edição da folha do ponto a partir daquela tela
onde detalha as informações do colaborador. Daí o que alterar na base do ponto e
passar, altera no ponto que foi feito download. Exemplo: altera a obra ou adiciona
uma obra que não existia, salva e grava as alterações. Assim fica rápido de
corrigir as possíveis distorções do ponto."*

O caminho é: grava no Mobponto (a fonte) → traz de novo o ponto daquela pessoa
(`ponto.atualizar_pessoa`, que já existe) → a folha recalcula. Corrigir só na
cópia baixada seria apagado pela próxima carga — e o ponto do Mobponto
continuaria errado para todo o resto (DP, banco de horas).

⚠️ O QUE A API CONHECIDA FAZ, E O QUE NÃO FAZ. O contrato vem do
`local_backend.py` que ele mandou em 29/09/2026 (o arquivo NÃO entra no
repositório: tem credencial em texto puro). Lá há UMA ação de ponto:

    type_data = CAD_EDT_PONTO, acao = "C"  → INCLUI uma batida
        cpf_responsavel, nome_responsavel, cpf_funcionario,
        dt_ponto_new = "AAAA-MM-DD HH:MM", justificativa, local

  - **Incluir batida: sim.** É o "adiciona uma obra que não existia".
  - **Mudar a obra de uma batida que já existe: NÃO está no que eu tenho.** Não
    há, no material dele, a ação de editar batida nem o identificador dela.
    Inventar o formato seria gravar coisa errada num sistema de terceiro. Está
    perguntado a ele.

⚠️ NÃO HÁ NOVA TENTATIVA AUTOMÁTICA, ao contrário da leitura. Ler de novo é
inofensivo; GRAVAR de novo depois de um tempo esgotado pode criar a batida duas
vezes (o Mobponto pode ter gravado e a resposta é que se perdeu). Esgotou o tempo
→ a tela diz "não sei se gravou, confira antes de mandar de novo".

⚠️ O RESPONSÁVEL. A API exige CPF e nome de quem faz o ajuste. No script dele são
fixos; aqui vêm de `MOBPONTO_RESPONSAVEL_CPF` e `MOBPONTO_RESPONSAVEL_NOME`, no
Render. Sem eles, o botão não aparece e a tela diz o que falta.

⚠️ NÃO VERIFICADO contra o Mobponto: este ambiente não alcança a produção, e não
há ambiente de teste do Mobponto. O que não se sabe: se `local` aceita o código da
obra como ele aparece no ponto (é o que se manda — o mesmo texto que a leitura
devolve) e se a hora precisa de segundos.
"""
from __future__ import annotations

import datetime as dt
import logging
import os
import re

logger = logging.getLogger("analisesps.ponto")

TIPO_EDITAR = "CAD_EDT_PONTO"
ACAO_INCLUIR = "C"
MAXIMO_DE_BATIDAS = 4
SEGUNDOS_PARA_CONECTAR = 20
SEGUNDOS_DE_ESPERA = 90
MINIMO_DA_JUSTIFICATIVA = 5
PADRAO_DA_HORA = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")


class ErroDaEdicao(RuntimeError):
    """Não deu para gravar no Mobponto. A frase vai inteira para a tela."""


def responsavel() -> tuple:
    """(cpf, nome) de quem assina o ajuste no Mobponto. Vazios se faltar."""
    cpf = re.sub(r"\D", "", os.getenv("MOBPONTO_RESPONSAVEL_CPF") or "")
    nome = " ".join((os.getenv("MOBPONTO_RESPONSAVEL_NOME") or "").split())
    return (cpf if len(cpf) == 11 else ""), nome


def o_que_falta() -> str:
    """Vazio quando dá para gravar; senão, a frase do que configurar."""
    from . import ponto
    faltam = []
    if not ponto.configurado():
        faltam.append("MOBPONTO_AUTHORIZATION e MOBPONTO_API_KEY")
    cpf, nome = responsavel()
    if not cpf:
        faltam.append("MOBPONTO_RESPONSAVEL_CPF (11 números)")
    if not nome:
        faltam.append("MOBPONTO_RESPONSAVEL_NOME")
    if not faltam:
        return ""
    return ("Para gravar no Mobponto por aqui, falta criar no Render: "
            + ", ".join(faltam) + ". O CPF e o nome do responsável são os que o "
            "seu script de ajuste usa.")


def validar(cpf: str, data: str, batidas, justificativa: str) -> tuple:
    """Confere tudo ANTES de mandar a primeira. Devolve (data, batidas limpas).

    Uma batida errada no meio de quatro faria as duas primeiras entrarem e as
    outras não — por isso a recusa é do pedido inteiro, antes de gravar nada."""
    if len(re.sub(r"\D", "", cpf or "")) != 11:
        raise ErroDaEdicao("CPF da pessoa inválido.")
    try:
        dia = dt.date.fromisoformat(str(data or "")[:10])
    except ValueError:
        raise ErroDaEdicao("data do dia inválida.") from None
    limpas = []
    for b in (batidas or []):
        hora = str((b or {}).get("hora") or "").strip()
        obra = " ".join(str((b or {}).get("obra") or "").split()).upper()
        if not hora and not obra:
            continue
        if not PADRAO_DA_HORA.match(hora):
            raise ErroDaEdicao(f'a hora "{hora or "(vazia)"}" não está no formato 07:30.')
        if not obra:
            raise ErroDaEdicao(f"a batida das {hora} está sem obra.")
        if len(obra) > 120:
            raise ErroDaEdicao("o nome da obra está comprido demais.")
        limpas.append({"hora": hora, "obra": obra})
    if not limpas:
        raise ErroDaEdicao("nenhuma batida para incluir — preencha hora e obra.")
    if len(limpas) > MAXIMO_DE_BATIDAS:
        raise ErroDaEdicao(f"no máximo {MAXIMO_DE_BATIDAS} batidas por dia.")
    if len({b["hora"] for b in limpas}) != len(limpas):
        raise ErroDaEdicao("há duas batidas com a mesma hora.")
    texto = " ".join(str(justificativa or "").split())
    if len(texto) < MINIMO_DA_JUSTIFICATIVA:
        raise ErroDaEdicao("escreva a justificativa — ela vai para o Mobponto "
                           "junto com a batida.")
    return dia, sorted(limpas, key=lambda b: b["hora"]), texto[:500]


def _mandar(payload: dict) -> tuple:
    """Uma chamada ao Mobponto. Devolve (ok, resposta em texto). Sem repetir."""
    import requests

    from . import ponto
    try:
        resposta = requests.post(ponto.URL, data=payload,
                                 headers=ponto._cabecalhos(),
                                 verify=ponto._confianca_tls(),
                                 timeout=(SEGUNDOS_PARA_CONECTAR, SEGUNDOS_DE_ESPERA))
    except requests.exceptions.ReadTimeout:
        return None, ("o Mobponto não respondeu a tempo. NÃO SEI SE GRAVOU — "
                      "confira o ponto da pessoa antes de mandar de novo, senão a "
                      "batida pode entrar duas vezes.")
    except Exception as e:  # noqa: BLE001 — rede, certificado
        return False, f"não consegui falar com o Mobponto: {e}"
    texto = (resposta.text or "")[:1000]
    try:
        corpo = resposta.json()
    except Exception:  # noqa: BLE001 — resposta que não é JSON
        corpo = None
    # A mesma regra do script dele: HTTP de sucesso e o corpo sem status=false.
    ok = resposta.ok and not (isinstance(corpo, dict) and corpo.get("status") is False)
    return ok, texto or f"HTTP {resposta.status_code}"


def _registrar(cpf, nome, dia, batida, justificativa, ok, resposta, quem) -> None:
    """Guarda o que foi mandado (migração 040). Sem a tabela, fica só no log."""
    from .db import conexao, tem_coluna
    logger.info("Ponto: batida %s %s %s para %s enviada por %s — ok=%s",
                dia, batida["hora"], batida["obra"], cpf, quem or "(sem nome)", ok)
    if not tem_coluna("ponto_batida_enviada", "cpf"):
        return
    try:
        with conexao() as conn:
            conn.execute(
                "INSERT INTO analisesps.ponto_batida_enviada "
                "  (cpf, nome, data, hora, obra, justificativa, ok, resposta, "
                "   enviado_por) VALUES (?,?,?,?,?,?,?,?,?)",
                (cpf, str(nome or "")[:160], dia, batida["hora"], batida["obra"],
                 justificativa, bool(ok), str(resposta or "")[:1000],
                 str(quem or "")[:120]))
            conn.commit()
    except Exception:  # noqa: BLE001 — o registro não pode esconder o envio
        logger.exception("Ponto: não consegui registrar a batida enviada")


def incluir_batidas(cpf: str, nome: str, data: str, batidas, justificativa: str,
                    quem: str = "") -> dict:
    """Inclui as batidas no Mobponto, uma a uma, e PARA NA PRIMEIRA que falhar.

    Devolve `{enviadas: [...], falhou: {hora, obra, motivo} | None}` — a tela diz
    exatamente o que entrou e o que não, porque o que entrou no Mobponto não se
    desfaz por aqui."""
    falta = o_que_falta()
    if falta:
        raise ErroDaEdicao(falta)
    cpf = re.sub(r"\D", "", cpf or "")
    dia, limpas, texto = validar(cpf, data, batidas, justificativa)
    cpf_resp, nome_resp = responsavel()

    enviadas, falhou = [], None
    for batida in limpas:
        payload = {
            "type_data": TIPO_EDITAR, "acao": ACAO_INCLUIR,
            "cpf_responsavel": cpf_resp, "nome_responsavel": nome_resp,
            "cpf_funcionario": cpf,
            "dt_ponto_new": f"{dia.isoformat()} {batida['hora']}",
            "justificativa": texto, "local": batida["obra"],
        }
        ok, resposta = _mandar(payload)
        _registrar(cpf, nome, dia, batida, texto, ok, resposta, quem)
        if ok:
            enviadas.append(batida)
            continue
        falhou = {**batida, "motivo": resposta,
                  "talvez_gravou": ok is None}
        break
    return {"enviadas": enviadas, "falhou": falhou, "data": dia.isoformat()}
