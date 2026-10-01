# -*- coding: utf-8 -*-
"""
LANÇAR O PONTO NO MOBPONTO, A PARTIR DA JANELA DO FUNCIONÁRIO — 01/10/2026.

Dois pedidos dele no mesmo dia. O primeiro: *"Quero poder fazer a edição da folha
do ponto a partir daquela tela onde detalha as informações do colaborador (…)
altera a obra ou adiciona uma obra que não existia, salva e grava."* O segundo,
que mudou o desenho: *"de forma que eu possa ajustar uma única batida, ou um dia
todo ou um período todo. Todavia se dentro do período já houver batida (…) não
lançar informação que sobreponha o que já existe (…) a gente precisa trabalhar de
forma otimizada e rápida. Imagina, se eu for entrar de um a um, em cada batida
(…) são dezenas de batidas."*

O desenho:
  - um período (De/Até; o mesmo dia é "um dia"), uma obra, uma justificativa;
  - `planejar` decide, dia a dia, o que FALTA no horário padrão e o que fica de
    fora (fim de semana, feriado, férias, falta lançada, dia completo, horário
    que não encaixa) — e a tela mostra isso antes de lançar;
  - "Lançar" entra na fila do ponto (`lancar`): refaz o plano com o ponto já
    baixado, manda uma batida por vez e escreve na cópia baixada o que o
    Mobponto aceitou — a folha recalcula na hora (desde 01/10/2026 não vai mais
    ao Mobponto antes nem depois: levava minutos, e ele decidiu que não);
  - a batida avulsa (uma hora só) fica para pequeno ajuste.

O caminho é gravar no Mobponto (a fonte) e trazer de volta. Corrigir só na cópia
baixada seria apagado pela próxima carga — e o Mobponto continuaria errado para
todo o resto (DP, banco de horas).

⚠️ O QUE A API CONHECIDA FAZ, E O QUE NÃO FAZ. O contrato vem do
`local_backend.py` que ele mandou em 29/09/2026 (o arquivo NÃO entra no
repositório: tem credencial em texto puro). Lá há UMA ação de ponto:

    type_data = CAD_EDT_PONTO, acao = "C"  → INCLUI uma batida
        cpf_responsavel, nome_responsavel, cpf_funcionario,
        dt_ponto_new = "AAAA-MM-DD HH:MM", justificativa, local

  - **Incluir batida: sim.**
  - **Mudar a obra de uma batida que já existe: NÃO está no que eu tenho.** Não
    há, no material dele, a ação de editar batida nem o identificador dela.
    Inventar o formato seria gravar coisa errada num sistema de terceiro.

⚠️ NÃO HÁ NOVA TENTATIVA AUTOMÁTICA, ao contrário da leitura. Ler de novo é
inofensivo; GRAVAR de novo depois de um tempo esgotado pode criar a batida duas
vezes. Esgotou o tempo → para, e o recado diz "confira antes de mandar de novo".

⚠️ O RESPONSÁVEL. A API exige CPF e nome de quem faz o ajuste. No script dele são
fixos; aqui vêm de `MOBPONTO_RESPONSAVEL_CPF` e `MOBPONTO_RESPONSAVEL_NOME`, no
Render. Sem eles, o quadro diz o que falta e não grava.

⚠️ NÃO VERIFICADO contra o Mobponto (não há ambiente de teste dele): se `local`
aceita o código da obra como aparece no ponto, e se o Mobponto põe cada batida
no campo certo (entrada/almoço/retorno/saída) pela hora.
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


# ---------------------------------------------------------------------------
# O HORÁRIO PADRÃO — regra do dono, 01/10/2026, e ela fica ESCRITA aqui:
#
#   *"o padrão é o ponto de entrada 7 horas, o ponto de almoço 12 horas, o ponto
#   de retorno 13 horas do almoço e o ponto de saída às 17 se for segunda a
#   quinta, ou sair às 16 se for na sexta-feira. Aí você tem que deixar essa
#   regra aí anotada."*
#
# E só dia útil: *"obviamente você vai bater só nos dias de semana, não bater no
# fim de semana."* Sábado e domingo ficam fora de período e de dia; quem precisa
# lançar num fim de semana usa a batida avulsa, que é para pequeno ajuste.
# ---------------------------------------------------------------------------
HORARIO_PADRAO = ("07:00", "12:00", "13:00", "17:00")
SAIDA_DE_SEXTA = "16:00"
ROTULO_DA_BATIDA = ("entrada", "almoço", "retorno", "saída")
MAXIMO_DE_DIAS = 31

# O que o ponto escreve num dia em que NÃO se trabalha. Batida em cima disso
# misturaria um dia de folga com um dia trabalhado — o Mobponto pode recusar, ou
# pior, aceitar.
PALAVRAS_DE_DIA_PARADO = ("feriado", "ferias", "férias", "folga", "afast",
                          "atestado", "licen", "compens", "dsr")

SEMANA = ("seg", "ter", "qua", "qui", "sex", "sáb", "dom")


def horario_do_dia(dia: dt.date) -> list:
    """As quatro batidas padrão do dia: 7h, 12h, 13h e 17h — 16h na sexta."""
    padrao = list(HORARIO_PADRAO)
    if dia.weekday() == 4:
        padrao[3] = SAIDA_DE_SEXTA
    return padrao


def _minutos(hora: str) -> int | None:
    texto = str(hora or "").strip()[:5]
    if not PADRAO_DA_HORA.match(texto):
        return None
    return int(texto[:2]) * 60 + int(texto[3:])


def encaixar(existentes, padrao) -> list | None:
    """Quais batidas do padrão faltam, sem passar por cima das que existem.

    *"Digamos que no dia 21 tenha só uma batida. Aí você vai fazer (…) as três
    batidas que faltam."*

    ⚠️ NÃO É POR POSIÇÃO, é por HORÁRIO. Quem bateu só às 13h05 bateu o RETORNO,
    mesmo que o Mobponto tenha posto essa batida no campo de entrada (é a primeira
    do dia). Então cada batida que existe é casada com a batida padrão de horário
    mais próximo, mantendo a ordem do dia, e as que sobram do padrão são as que
    faltam. No exemplo: 13h05 é o retorno → faltam 7h, 12h e 17h.

    Devolve as horas a lançar, ou None quando não há jeito de encaixar sem
    embaralhar a ordem do dia (ex.: alguém bateu às 12h30 e às 12h40) — aí o dia
    fica para ajuste à mão, em vez de o sistema adivinhar."""
    from itertools import combinations

    horas = sorted(m for m in (_minutos(h) for h in existentes) if m is not None)
    alvo = [_minutos(h) for h in padrao]
    if len(horas) >= len(alvo):
        return []
    opcoes = []
    for posicoes in combinations(range(len(alvo)), len(horas)):
        custo = sum(abs(h - alvo[p]) for h, p in zip(horas, posicoes))
        opcoes.append((custo, posicoes))
    for _, posicoes in sorted(opcoes):
        linha = list(alvo)
        for h, p in zip(horas, posicoes):
            linha[p] = h
        # A ordem do dia tem de continuar crescente, com o que já existe no lugar.
        if all(a < b for a, b in zip(linha, linha[1:])):
            return [padrao[i] for i in range(len(alvo)) if i not in posicoes]
    return None


def _dia_parado(dia_do_ponto: dict) -> str:
    """O motivo, quando o ponto diz que o dia não é de trabalho."""
    falta = " ".join(str(dia_do_ponto.get("falta") or "").split())
    if falta:
        return f"o ponto tem falta lançada ({falta})"
    presenca = " ".join(str(dia_do_ponto.get("presenca") or "").split())
    if any(p in presenca.lower() for p in PALAVRAS_DE_DIA_PARADO):
        return f"o ponto diz {presenca}"
    return ""


def planejar(dias_do_ponto, de, ate, feriados=None, ferias=None,
             hora_avulsa: str = "") -> dict:
    """O que vai ser lançado, dia a dia, e o que fica de fora — com o motivo.
    FUNÇÃO PURA: recebe o ponto já baixado, não fala com ninguém.

    *"Se eu tenho um determinado período, ele está todo em branco (…) bateu o
    ponto dia 16 ao dia 30 (…) mas se no dia 21 tenha um ponto batido (…) vamos
    aplicar do dia 16 ao 20, e do dia 22 ao 30."* — e o dia 21 com batida
    incompleta recebe só as que faltam.

    `dias_do_ponto`: `[{data, horas[4], marcacoes[4], presenca, falta}]`.
    `feriados`: `{data: descrição}`. `ferias`: conjunto de datas.
    `hora_avulsa`: quando preenchida, é UMA batida por dia, naquela hora (o
    "um a um" dos pequenos ajustes)."""
    if isinstance(de, str):
        de = dt.date.fromisoformat(de[:10])
    if isinstance(ate, str):
        ate = dt.date.fromisoformat(ate[:10])
    if ate < de:
        raise ErroDaEdicao("o fim do período é antes do começo.")
    if (ate - de).days + 1 > MAXIMO_DE_DIAS:
        raise ErroDaEdicao(f"no máximo {MAXIMO_DE_DIAS} dias por vez.")
    avulsa = str(hora_avulsa or "").strip()[:5]
    if avulsa and not PADRAO_DA_HORA.match(avulsa):
        raise ErroDaEdicao(f'a hora "{avulsa}" não está no formato 07:30.')
    feriados = feriados or {}
    ferias = ferias or set()
    um_dia_so = de == ate
    por_data = {}
    for d in (dias_do_ponto or []):
        data = d.get("data")
        if isinstance(data, str):
            data = dt.date.fromisoformat(data[:10])
        if data:
            por_data[data] = d

    dias = []
    dia = de
    while dia <= ate:
        registro = por_data.get(dia) or {}
        horas = list(registro.get("horas") or ["", "", "", ""])
        obras = list(registro.get("marcacoes") or ["", "", "", ""])
        existentes = [h.strip()[:5] for h, o in zip(horas, obras)
                      if str(h or "").strip() or str(o or "").strip()]
        linha = {"data": dia.isoformat(), "data_br": dia.strftime("%d/%m"),
                 "semana": SEMANA[dia.weekday()], "ja": existentes,
                 "lancar": [], "motivo": ""}

        if dia.weekday() >= 5 and not (avulsa and um_dia_so):
            linha["motivo"] = "fim de semana"
        elif dia in feriados:
            linha["motivo"] = f"feriado ({feriados[dia]})" if feriados[dia] else "feriado"
        elif dia in ferias:
            linha["motivo"] = "férias cadastradas"
        elif _dia_parado(registro):
            linha["motivo"] = _dia_parado(registro)
        elif any(_minutos(h) is None for h in existentes):
            linha["motivo"] = ("tem batida sem hora no ponto — ajuste no "
                               "Mobponto")
        elif avulsa:
            if len(existentes) >= 4:
                linha["motivo"] = "o dia já tem as quatro batidas"
            elif any(abs(_minutos(h) - _minutos(avulsa)) <= 5 for h in existentes):
                linha["motivo"] = f"já há batida perto das {avulsa}"
            else:
                linha["lancar"] = [avulsa]
        else:
            faltam = encaixar(existentes, horario_do_dia(dia))
            if faltam is None:
                linha["motivo"] = ("as batidas que já existem não deixam encaixar "
                                   "o horário padrão — ajuste à mão")
            elif not faltam:
                linha["motivo"] = "o dia já está completo"
            else:
                linha["lancar"] = faltam
        dias.append(linha)
        dia += dt.timedelta(days=1)

    lancar = [d for d in dias if d["lancar"]]
    return {"de": de.isoformat(), "ate": ate.isoformat(), "avulsa": avulsa,
            "dias": dias, "dias_com_lancamento": len(lancar),
            "batidas": sum(len(d["lancar"]) for d in lancar),
            "pulados": [d for d in dias if not d["lancar"]]}


def obras_permitidas() -> list:
    """As obras que podem ir para o Mobponto: os CÓDIGOS PRIMÁRIOS da aba
    "C. Diários".

    O dono, 01/10/2026: *"As obras do Mobponto são as mesmas do cadastro C.
    Diários. Usar elas como base para seleção."* As duas tabelas que vêm dessa aba
    guardam o código primário: `contas_diarios.codigo` e
    `referencias_rateio.nome` (ver `folha_pagamento.conta_por_obra`). O código do
    OMIE, que também está lá, NÃO entra: não é o que o ponto usa.

    Lista vazia quando as planilhas de apoio não foram lidas — e aí nada é
    lançado (ver `validar_pedido`)."""
    from .db import consultar

    def _chave(texto) -> str:
        return " ".join(str(texto or "").split()).upper()

    obras = set()
    for sql in ("SELECT codigo FROM analisesps.contas_diarios",
                "SELECT nome FROM analisesps.referencias_rateio WHERE tipo = 'obra'"):
        try:
            obras.update(_chave(l[0]) for l in consultar(sql) if _chave(l[0]))
        except Exception:  # noqa: BLE001 — tabela pode não existir em base nova
            logger.exception("Ponto: não consegui ler as obras da C. Diários")
    return sorted(obras)


def validar_pedido(obra: str, justificativa: str, permitidas=None) -> tuple:
    """A obra e a justificativa, limpas — ou a recusa, antes de qualquer envio.

    ⚠️ A OBRA TEM DE ESTAR NA C. DIÁRIOS. Texto livre mandaria ao Mobponto um local
    que ele não conhece — com sorte recusado, sem sorte gravado errado."""
    obra = " ".join(str(obra or "").split()).upper()
    if not obra:
        raise ErroDaEdicao("escolha a obra.")
    permitidas = obras_permitidas() if permitidas is None else permitidas
    if not permitidas:
        raise ErroDaEdicao(
            'não tenho a lista de obras da aba "C. Diários" — atualize as '
            "planilhas de apoio em Configurações e tente de novo. Sem ela, nada "
            "é lançado.")
    if obra not in permitidas:
        raise ErroDaEdicao(f'a obra "{obra}" não está na aba "C. Diários". '
                           "Escolha uma da lista.")
    texto = " ".join(str(justificativa or "").split())
    if len(texto) < MINIMO_DA_JUSTIFICATIVA:
        raise ErroDaEdicao("escreva a justificativa — ela vai para o Mobponto "
                           "junto com cada batida.")
    return obra, texto[:500]


def _feriados_e_ferias(cpf: str, de, ate, obra: str) -> tuple:
    """O que o sistema sabe de feriado (nacional ou da obra) e de férias."""
    from . import folha_calendario
    try:
        feriados = {f["data"]: f.get("descricao") or ""
                    for f in folha_calendario.feriados_no_periodo(de, ate, obra)}
    except Exception:  # noqa: BLE001 — sem a lista, segue sem pular feriado
        logger.exception("Ponto: não consegui ler os feriados")
        feriados = {}
    ferias = set()
    try:
        for f in folha_calendario.ferias_que_cruzam(cpf, de, ate):
            d = max(f["inicio"], de)
            while d <= min(f["fim"], ate):
                ferias.add(d)
                d += dt.timedelta(days=1)
    except Exception:  # noqa: BLE001
        logger.exception("Ponto: não consegui ler as férias")
    return feriados, ferias


def plano_da_pessoa(ano: int, mes: int, cpf: str, de, ate, obra: str,
                    hora_avulsa: str = "") -> dict:
    """O plano a partir do ponto BAIXADO desta pessoa (a prévia da tela)."""
    from . import ponto
    de = dt.date.fromisoformat(str(de)[:10])
    ate = dt.date.fromisoformat(str(ate)[:10])
    if (de.year, de.month) != (int(ano), int(mes)) or \
            (ate.year, ate.month) != (int(ano), int(mes)):
        raise ErroDaEdicao(f"o período tem de estar dentro de {int(mes):02d}/{int(ano)}, "
                           "o mês do ponto desta folha.")
    dias = ponto.dias_de_um_cpf(ano, mes, cpf)
    feriados, ferias = _feriados_e_ferias(cpf, de, ate, obra)
    return planejar(dias, de, ate, feriados, ferias, hora_avulsa)


# O pacote de certificados que funcionou, guardado para as próximas batidas do
# mesmo lançamento — completar a cadeia uma vez basta.
_CONFIANCA_REMENDADA = None


def _erro_de_certificado(e) -> bool:
    return "CERTIFICATE_VERIFY_FAILED" in str(e) or "SSLError" in type(e).__name__


def _confianca():
    from . import ponto
    return _CONFIANCA_REMENDADA or ponto._confianca_tls()


def _remendar_confianca():
    """Completa a cadeia de certificados do Mobponto, como a LEITURA já fazia.

    ⚠️ FALHA REAL, 01/10/2026: *"PAROU em 16/09 07:00: não consegui falar com o
    Mobponto: (…) CERTIFICATE_VERIFY_FAILED (…) unable to get local issuer
    certificate"*. A leitura do ponto (`ponto._pedir_pagina`) baixa sozinha o
    certificado do meio da cadeia, que o servidor do Mobponto não manda; o
    envio da batida não fazia isso — e parou na primeira. Ver `ponto._confianca_tls`."""
    global _CONFIANCA_REMENDADA
    from . import ponto
    atual = ponto._confianca_tls()
    if atual is False:
        return None
    remendo = ponto._intermediario_do_servidor(ponto.URL)
    if not remendo:
        return None
    pacote = ponto._pacote_com_o_extra(atual if isinstance(atual, str) else None,
                                       remendo)
    if pacote:
        _CONFIANCA_REMENDADA = pacote
    return pacote


def _mandar(payload: dict) -> tuple:
    """Uma chamada ao Mobponto. Devolve (ok, resposta em texto).

    Repete UMA vez, e só no erro de certificado: ele acontece no aperto de mão,
    ANTES de o pedido sair — a batida não chegou ao Mobponto, e mandar de novo
    não duplica nada. Qualquer outro erro não se repete (ver o topo)."""
    import requests

    from . import ponto

    def enviar(confianca):
        return requests.post(ponto.URL, data=payload, headers=ponto._cabecalhos(),
                             verify=confianca,
                             timeout=(SEGUNDOS_PARA_CONECTAR, SEGUNDOS_DE_ESPERA))

    try:
        try:
            resposta = enviar(_confianca())
        except requests.exceptions.SSLError as e:
            if not _erro_de_certificado(e):
                raise
            remendada = _remendar_confianca()
            if not remendada:
                raise
            resposta = enviar(remendada)
    except requests.exceptions.ReadTimeout:
        return None, ("o Mobponto não respondeu a tempo. NÃO SEI SE GRAVOU — "
                      "confira o ponto da pessoa antes de mandar de novo, senão a "
                      "batida pode entrar duas vezes.")
    except Exception as e:  # noqa: BLE001 — rede, certificado
        if _erro_de_certificado(e):
            return False, (
                "o certificado do Mobponto não pôde ser verificado, e não consegui "
                "completar a cadeia sozinho. Nada foi gravado. O conserto é colar "
                "o certificado do meio da cadeia em MOBPONTO_CA_EXTRA, no Render "
                f"(detalhe: {e})")
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


PAUSA_ENTRE_BATIDAS = 0.5


def lancar(pedido: dict, anotar=None) -> dict:
    """Roda no processo separado (pela fila do ponto). Na ordem:

      1. refaz o plano com o ponto BAIXADO — o que está na cópia agora, que pode
         ter mudado desde a tela;
      2. manda as batidas UMA POR VEZ, e para na primeira que falhar;
      3. escreve na cópia baixada o que o Mobponto aceitou, dia a dia
         (`ponto.aplicar_batidas_na_copia`) — a folha recalcula na hora.

    ⚠️ NÃO VAI MAIS AO MOBPONTO ANTES NEM DEPOIS — decisão do dono, 01/10/2026:
    *"não tem sentido buscar antes, leva muito tempo. A base de informações já
    existe, precisa somente aplicar."* Buscar uma pessoa obriga a varrer páginas
    do mês (a API não filtra por pessoa) e levava minutos.

    ⚠️ O RISCO, aceito por ele: o plano olha a CÓPIA. Batida feita no Mobponto
    depois da última carga do mês não é vista, e o lançamento pode encostar
    nela. A carga automática de hora em hora é o que mantém a cópia fresca, e é
    ela que, na próxima passada, substitui a cópia pelo que o Mobponto tiver."""
    import time

    from . import ponto

    falta = o_que_falta()
    if falta:
        raise ErroDaEdicao(falta)
    anotar = anotar or (lambda *a, **k: None)
    ano, mes = int(pedido["ano"]), int(pedido["mes"])
    cpf = re.sub(r"\D", "", str(pedido.get("cpf") or ""))
    nome = str(pedido.get("nome") or "")
    obra, texto = validar_pedido(pedido.get("obra"), pedido.get("justificativa"))
    cpf_resp, nome_resp = responsavel()

    anotar("lançando o ponto", "montando o que falta, pelo ponto já baixado")
    plano = plano_da_pessoa(ano, mes, cpf, pedido["de"], pedido["ate"], obra,
                            pedido.get("hora_avulsa") or "")

    enviadas, falhou = [], None
    total = plano["batidas"]
    for linha in plano["dias"]:
        aceitas_no_dia = []
        for hora in linha["lancar"]:
            anotar("lançando o ponto",
                   f"batida {len(enviadas) + 1} de {total} — {linha['data_br']} {hora}")
            dia = dt.date.fromisoformat(linha["data"])
            ok, resposta = _mandar({
                "type_data": TIPO_EDITAR, "acao": ACAO_INCLUIR,
                "cpf_responsavel": cpf_resp, "nome_responsavel": nome_resp,
                "cpf_funcionario": cpf,
                "dt_ponto_new": f"{dia.isoformat()} {hora}",
                "justificativa": texto, "local": obra,
            })
            _registrar(cpf, nome, dia, {"hora": hora, "obra": obra}, texto, ok,
                       resposta, pedido.get("quem") or "")
            if not ok:
                falhou = {"data": linha["data_br"], "hora": hora, "motivo": resposta,
                          "talvez_gravou": ok is None}
                break
            enviadas.append((linha["data_br"], hora))
            aceitas_no_dia.append((hora, obra))
            time.sleep(PAUSA_ENTRE_BATIDAS)
        # O QUE O MOBPONTO ACEITOU vai para a cópia, dia a dia — inclusive o dia
        # em que parou no meio: o que entrou, entrou.
        if aceitas_no_dia:
            try:
                ponto.aplicar_batidas_na_copia(ano, mes, cpf, nome, linha["data"],
                                               aceitas_no_dia)
            except Exception:  # noqa: BLE001 — o Mobponto já tem; o recado diz
                logger.exception("Ponto: lancei, mas não consegui pôr na cópia")
        if falhou:
            break
    return {"plano": plano, "enviadas": enviadas, "falhou": falhou}


def recado_do_lancamento(feito: dict) -> str:
    """A frase que fica na execução e que a tela mostra."""
    plano, enviadas, falhou = feito["plano"], feito["enviadas"], feito["falhou"]
    dias = len({d for d, _ in enviadas})
    partes = [f"{len(enviadas)} batida(s) lançada(s) em {dias} dia(s)"]
    if plano["pulados"]:
        partes.append(f"{len(plano['pulados'])} dia(s) não mexidos")
    if falhou:
        partes.append(
            f"PAROU em {falhou['data']} {falhou['hora']}: {falhou['motivo']}"
            + (" — confira no Mobponto se essa entrou antes de mandar de novo"
               if falhou.get("talvez_gravou") else ""))
    return "; ".join(partes)
