# -*- coding: utf-8 -*-
"""
validasp/fila.py — a validação guardada até o "Sim" entrar na SPsBD.

Pedido do dono em 17/10/2026: "se a pessoa sabe que validou, precisamos
garantir que a validação aconteça". Antes, o "Sim" na coluna AH era uma tarefa
solta na memória do serviço — duas tentativas e, falhando, uma linha na aba
FalhaProcessar, do mesmo Google que acabara de falhar. Um reinício do Render
no meio apagava tudo sem rastro.

Agora:

  1. `registrar()` guarda a validação ANTES de a resposta sair — no banco
     (`validasp.pendencias`, migração 084) ou, se o banco não responder, num
     arquivo do próprio serviço. Só sem os dois a validação fica sem garantia,
     e a resposta diz isso.
  2. O vigia (uma linha de execução do próprio serviço, de 30 em 30 s) pega o
     que está na hora, grava o "Sim" e LÊ DE VOLTA a célula. Só a leitura
     confirmando tira o item da fila — gravação "aceita" que não pegou é
     exatamente o defeito que o dono via.
  3. Falhou (Google fora, cota, SP ainda não escrita na SPsBD): tenta de novo
     em 1,5 → 3 → 5 → 10 → 30 → 60 min, e daí de hora em hora. Depois de 7 dias
     sem achar a SP, desiste, deixa registrado e escreve na FalhaProcessar.

O arquivo em disco é a segunda linha de defesa, não a primeira: some numa
publicação. Por isso, assim que o banco responde, o vigia passa o que está em
disco para o banco.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
import uuid
from base64 import b64decode
from datetime import datetime
from typing import Optional

from ..atualizaspbotao.utils import as_string

logger = logging.getLogger(__name__)

SPREADSHEET_ID   = '1lrP1HOvwqyXiVdP2kuTgG7sJjl2QXl0WT4lwkd392DA'
ABA_SPSBD        = 'SPsBD'
ABA_FALHA        = 'FalhaProcessar'
COLUNA_VALIDACAO = 34          # AH — "Validação" (analisesps/colunas.py)

# A primeira tentativa espera 90 s porque a linha da SP pode ainda estar sendo
# escrita na SPsBD pelo processarnovasp — o mesmo intervalo que já existia.
ESPERA_INICIAL_S = 90
ESPERAS_S        = (90, 180, 300, 600, 1800, 3600)   # depois da 1ª, 2ª, … falha
PRAZO_DIAS       = 7
POR_RODADA       = 40
INTERVALO_VIGIA_S = 30

PASTA_DISCO = os.getenv('VALIDASP_FILA_DIR', '/tmp/validasp_fila')

PENDENTE, FEITO, DESISTIU = 'pendente', 'feito', 'desistiu'

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]

_trava_rodada = threading.Lock()


# ---------------------------------------------------------------------------
# Banco
# ---------------------------------------------------------------------------
_banco_ok_ate = 0.0
_banco_falhou_em = 0.0


def _conexao():
    from app.apps.erp.db.database import obter_engine
    return obter_engine().begin()


def banco_disponivel() -> bool:
    """A tabela da fila existe e o banco responde? A resposta "sim" vale 5 min e
    a "não" vale 60 s, para não perguntar a cada validação."""
    global _banco_ok_ate, _banco_falhou_em
    agora = time.time()
    if agora < _banco_ok_ate:
        return True
    if agora - _banco_falhou_em < 60:
        return False
    try:
        from sqlalchemy import text
        with _conexao() as conn:
            existe = conn.execute(text(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'validasp' AND table_name = 'pendencias'")).first() is not None
    except Exception as e:  # noqa: BLE001 — sem banco, a fila vai para o disco
        logger.warning('ValidaSP: banco indisponível (%s); fila no disco', str(e)[:150])
        existe = False
    if existe:
        _banco_ok_ate = agora + 300
    else:
        _banco_falhou_em = agora
    return existe


def esquecer_banco() -> None:
    """Faz a próxima pergunta ir ao banco de novo (testes e depois de erro)."""
    global _banco_ok_ate, _banco_falhou_em
    _banco_ok_ate = 0.0
    _banco_falhou_em = 0.0


def _banco_registrar(sp_id: str, criado_em: Optional[float] = None,
                     tentativas: int = 0, proxima: Optional[float] = None,
                     ultimo_erro: str = '') -> int:
    """Insere a pendência; se já há uma pendente para a mesma SP, devolve a dela."""
    from sqlalchemy import text
    with _conexao() as conn:
        existente = conn.execute(text(
            "SELECT id FROM validasp.pendencias WHERE sp_id = :s AND situacao = :p "
            "ORDER BY id LIMIT 1"), {'s': sp_id, 'p': PENDENTE}).first()
        if existente:
            return int(existente[0])
        criado = criado_em if criado_em is not None else time.time()
        prox = proxima if proxima is not None else criado + ESPERA_INICIAL_S
        return int(conn.execute(text(
            "INSERT INTO validasp.pendencias "
            "(sp_id, tentativas, proxima_tentativa, ultimo_erro, criado_em) "
            "VALUES (:s, :t, to_timestamp(:prox), :e, to_timestamp(:c)) RETURNING id"),
            {'s': sp_id, 't': tentativas, 'prox': prox, 'e': ultimo_erro[:1000],
             'c': criado}).scalar())


def _banco_vencidos(limite: int) -> list[dict]:
    from sqlalchemy import text
    with _conexao() as conn:
        linhas = conn.execute(text(
            "SELECT id, sp_id, tentativas, EXTRACT(EPOCH FROM criado_em) AS criado "
            "FROM validasp.pendencias "
            "WHERE situacao = :p AND proxima_tentativa <= NOW() "
            "ORDER BY proxima_tentativa, id LIMIT :n"), {'p': PENDENTE, 'n': limite}).mappings().all()
    return [{'origem': 'banco', 'id': int(l['id']), 'sp_id': l['sp_id'],
             'tentativas': int(l['tentativas']), 'criado': float(l['criado'])} for l in linhas]


def _banco_feito(item: dict, linha: int) -> None:
    from sqlalchemy import text
    with _conexao() as conn:
        conn.execute(text(
            "UPDATE validasp.pendencias SET situacao = :f, feito_em = NOW(), "
            "linha_planilha = :l, tentativas = tentativas + 1, ultimo_erro = '' "
            "WHERE id = :i"), {'f': FEITO, 'l': linha, 'i': item['id']})


def _banco_falhou(item: dict, erro: str, desistir: bool, espera: float) -> None:
    from sqlalchemy import text
    with _conexao() as conn:
        conn.execute(text(
            "UPDATE validasp.pendencias SET tentativas = tentativas + 1, ultimo_erro = :e, "
            "situacao = :sit, proxima_tentativa = NOW() + make_interval(secs => :w) "
            "WHERE id = :i"),
            {'e': erro[:1000], 'sit': DESISTIU if desistir else PENDENTE,
             'w': espera, 'i': item['id']})


def resumo() -> dict:
    """Quanto está na fila, e desde quando — para conferir sem abrir o banco."""
    saida: dict = {'banco': banco_disponivel(), 'disco_pendentes': len(_disco_listar())}
    if not saida['banco']:
        return saida
    from sqlalchemy import text
    with _conexao() as conn:
        contagem = dict(conn.execute(text(
            "SELECT situacao, COUNT(*) FROM validasp.pendencias GROUP BY situacao")).all())
        mais_antiga = conn.execute(text(
            "SELECT sp_id, to_char(criado_em AT TIME ZONE 'America/Sao_Paulo', 'DD/MM/YYYY HH24:MI'), "
            "tentativas, ultimo_erro FROM validasp.pendencias WHERE situacao = :p "
            "ORDER BY criado_em LIMIT 1"), {'p': PENDENTE}).first()
        desistidas = conn.execute(text(
            "SELECT sp_id, ultimo_erro FROM validasp.pendencias WHERE situacao = :d "
            "ORDER BY id DESC LIMIT 20"), {'d': DESISTIU}).all()
    saida.update({
        'pendentes': int(contagem.get(PENDENTE, 0)),
        'feitas': int(contagem.get(FEITO, 0)),
        'desistidas': int(contagem.get(DESISTIU, 0)),
        'pendente_mais_antiga': ({'sp': mais_antiga[0], 'desde': mais_antiga[1],
                                  'tentativas': mais_antiga[2], 'ultimo_erro': mais_antiga[3]}
                                 if mais_antiga else None),
        'ultimas_desistidas': [{'sp': s, 'erro': e} for s, e in desistidas],
    })
    return saida


# ---------------------------------------------------------------------------
# Disco (segunda linha de defesa)
# ---------------------------------------------------------------------------
def _disco_listar() -> list[str]:
    try:
        return sorted(a for a in os.listdir(PASTA_DISCO) if a.endswith('.json'))
    except FileNotFoundError:
        return []


def _disco_gravar(item: dict, nome: Optional[str] = None) -> str:
    os.makedirs(PASTA_DISCO, exist_ok=True)
    nome = nome or f"{int(time.time())}_{uuid.uuid4().hex[:8]}.json"
    temporario = os.path.join(PASTA_DISCO, nome + '.tmp')
    with open(temporario, 'w', encoding='utf-8') as f:
        json.dump(item, f, ensure_ascii=False)
    os.replace(temporario, os.path.join(PASTA_DISCO, nome))   # nunca meio arquivo
    return nome


def _disco_ler(nome: str) -> Optional[dict]:
    try:
        with open(os.path.join(PASTA_DISCO, nome), encoding='utf-8') as f:
            return json.load(f)
    except (OSError, ValueError) as e:
        logger.error('ValidaSP: arquivo da fila ilegível (%s): %s', nome, e)
        return None


def _disco_apagar(nome: str) -> None:
    try:
        os.remove(os.path.join(PASTA_DISCO, nome))
    except FileNotFoundError:
        pass


def _disco_vencidos(limite: int) -> list[dict]:
    agora = time.time()
    saida = []
    for nome in _disco_listar():
        dado = _disco_ler(nome)
        if not dado or dado.get('situacao', PENDENTE) != PENDENTE:
            continue
        if float(dado.get('proxima', 0)) <= agora:
            saida.append({'origem': 'disco', 'arquivo': nome, 'sp_id': as_string(dado.get('sp_id')),
                          'tentativas': int(dado.get('tentativas', 0)),
                          'criado': float(dado.get('criado', agora)), 'dado': dado})
        if len(saida) >= limite:
            break
    return saida


def _disco_para_banco() -> int:
    """Passa para o banco o que ficou guardado em disco. Só apaga o arquivo
    depois de o banco confirmar a gravação."""
    movidos = 0
    for nome in _disco_listar():
        dado = _disco_ler(nome)
        if not dado or dado.get('situacao', PENDENTE) != PENDENTE:
            continue        # desistida fica no disco como registro; ilegível, para alguém olhar
        try:
            _banco_registrar(as_string(dado.get('sp_id')), criado_em=float(dado.get('criado', time.time())),
                             tentativas=int(dado.get('tentativas', 0)),
                             proxima=float(dado.get('proxima', time.time())),
                             ultimo_erro=as_string(dado.get('ultimo_erro')))
        except Exception as e:  # noqa: BLE001 — fica no disco, tenta na próxima rodada
            logger.warning('ValidaSP: não consegui passar %s para o banco: %s', nome, str(e)[:150])
            esquecer_banco()
            break
        _disco_apagar(nome)
        movidos += 1
    if movidos:
        logger.info('ValidaSP: %d validação(ões) passaram do disco para o banco', movidos)
    return movidos


# ---------------------------------------------------------------------------
# Entrada
# ---------------------------------------------------------------------------
def registrar(sp_id: str) -> dict:
    """Guarda a validação da SP. Nunca levanta erro: devolve onde guardou."""
    sp_id = as_string(sp_id).strip()
    if banco_disponivel():
        try:
            pid = _banco_registrar(sp_id)
            logger.info('ValidaSP: SP %s na fila (banco, pendência %s)', sp_id, pid)
            return {'ok': True, 'onde': 'banco', 'id': pid}
        except Exception as e:  # noqa: BLE001 — cai para o disco
            logger.error('ValidaSP: banco recusou a SP %s (%s); guardando em disco', sp_id, str(e)[:200])
            esquecer_banco()
    try:
        agora = time.time()
        nome = _disco_gravar({'sp_id': sp_id, 'criado': agora, 'tentativas': 0,
                              'proxima': agora + ESPERA_INICIAL_S, 'situacao': PENDENTE,
                              'ultimo_erro': ''})
        logger.warning('ValidaSP: SP %s na fila em DISCO (%s) — some se o serviço for publicado '
                       'antes de o banco voltar', sp_id, nome)
        return {'ok': True, 'onde': 'disco', 'arquivo': nome}
    except Exception as e:  # noqa: BLE001
        logger.error('ValidaSP: SP %s SEM GARANTIA — nem banco nem disco aceitaram: %s', sp_id, e)
        return {'ok': False, 'erro': str(e)[:300]}


# ---------------------------------------------------------------------------
# Planilha
# ---------------------------------------------------------------------------
def _abrir_planilha():
    import gspread
    from google.oauth2.service_account import Credentials
    creds_b64 = os.getenv('GOOGLE_CREDENTIALS_BASE64', '')
    if not creds_b64:
        raise RuntimeError('GOOGLE_CREDENTIALS_BASE64 não configurado.')
    creds = Credentials.from_service_account_info(
        json.loads(b64decode(creds_b64).decode('utf-8')), scopes=SCOPES)
    return gspread.authorize(creds).open_by_key(SPREADSHEET_ID)


def _com_retry(operacao, descricao: str):
    from ..processarnovasp.retry import com_retry
    return com_retry(operacao, descricao=descricao)


def _linhas_por_sp(coluna_a: list) -> dict:
    """SP → número da linha na planilha. Vale a ÚLTIMA ocorrência, como antes."""
    linhas = {}
    for i, valor in enumerate(coluna_a[1:], start=2):     # linha 1 é o cabeçalho
        chave = as_string(valor).strip()
        if chave:
            linhas[chave] = i
    return linhas


def _marcar(ws, linha: int, sp_id: str) -> Optional[str]:
    """Grava o "Sim" e confere lendo de volta. Devolve None se confirmou, ou o erro."""
    _com_retry(lambda: ws.update_cell(linha, COLUNA_VALIDACAO, 'Sim'),
               f'validação SP {sp_id} (linha {linha})')
    lido = _com_retry(lambda: ws.cell(linha, COLUNA_VALIDACAO).value,
                      f'conferência SP {sp_id} (linha {linha})')
    if as_string(lido).strip().lower() == 'sim':
        return None
    return f'gravou na linha {linha}, mas a leitura devolveu "{as_string(lido)}"'


def _espera_depois(tentativas_feitas: int) -> int:
    return ESPERAS_S[min(max(tentativas_feitas - 1, 0), len(ESPERAS_S) - 1)]


def _anotar_falha_processar(ss, sp_id: str) -> None:
    try:
        agora = datetime.now().strftime('%d/%m/%Y %H:%M')
        ss.worksheet(ABA_FALHA).append_row([sp_id, agora, 'Validar'])
    except Exception as e:  # noqa: BLE001 — a desistência já está no banco/log
        logger.error('ValidaSP: não consegui escrever a SP %s na FalhaProcessar: %s', sp_id, e)


def _registrar_resultado(item: dict, linha: Optional[int], erro: Optional[str], ss) -> str:
    sp_id = item['sp_id']
    if erro is None:
        if item['origem'] == 'banco':
            _banco_feito(item, linha)
        else:
            _disco_apagar(item['arquivo'])
        logger.info('ValidaSP: SP %s validada na SPsBD (linha %s, tentativa %d)',
                    sp_id, linha, item['tentativas'] + 1)
        return FEITO

    feitas = item['tentativas'] + 1
    desistir = time.time() - item['criado'] > PRAZO_DIAS * 86400
    espera = _espera_depois(feitas)
    if item['origem'] == 'banco':
        _banco_falhou(item, erro, desistir, espera)
    else:
        dado = dict(item['dado'], tentativas=feitas, ultimo_erro=erro[:1000],
                    proxima=time.time() + espera,
                    situacao=DESISTIU if desistir else PENDENTE)
        _disco_gravar(dado, item['arquivo'])
    if desistir:
        logger.error('ValidaSP: DESISTI da SP %s depois de %d tentativas em %d dias: %s',
                     sp_id, feitas, PRAZO_DIAS, erro)
        if ss is not None:
            _anotar_falha_processar(ss, sp_id)
        return DESISTIU
    logger.warning('ValidaSP: SP %s não validada (tentativa %d): %s — nova tentativa em %d s',
                   sp_id, feitas, erro, espera)
    return PENDENTE


def rodada() -> dict:
    """Uma passada da fila. Nunca levanta erro — o vigia não pode morrer."""
    if not _trava_rodada.acquire(blocking=False):
        return {'ok': True, 'ocupado': True}
    try:
        return _rodada()
    except Exception as e:  # noqa: BLE001
        logger.exception('ValidaSP: rodada da fila falhou')
        return {'ok': False, 'erro': str(e)[:300]}
    finally:
        _trava_rodada.release()


def _rodada() -> dict:
    no_banco = banco_disponivel()
    if no_banco and _disco_listar():
        _disco_para_banco()

    itens: list[dict] = []
    if no_banco:
        try:
            itens = _banco_vencidos(POR_RODADA)
        except Exception as e:  # noqa: BLE001 — o disco ainda pode andar
            logger.warning('ValidaSP: não consegui ler a fila do banco: %s', str(e)[:150])
            esquecer_banco()
    if len(itens) < POR_RODADA:
        itens += _disco_vencidos(POR_RODADA - len(itens))
    if not itens:
        return {'ok': True, 'processados': 0}

    contagem = {FEITO: 0, PENDENTE: 0, DESISTIU: 0}
    ss = None
    try:
        ss = _com_retry(_abrir_planilha, 'abrir planilha de SPs')
        ws = _com_retry(lambda: ss.worksheet(ABA_SPSBD), 'abrir aba SPsBD')
        linhas = _linhas_por_sp(_com_retry(lambda: ws.col_values(1), 'ler coluna A da SPsBD'))
    except Exception as e:  # noqa: BLE001 — o Google falhou: ninguém sai da fila
        erro = f'planilha indisponível: {str(e)[:300]}'
        for item in itens:
            contagem[_registrar_resultado(item, None, erro, ss)] += 1
        return {'ok': True, 'processados': len(itens), **contagem}

    for item in itens:
        linha = linhas.get(item['sp_id'])
        if not linha:
            erro = 'SP não encontrada na SPsBD'
        else:
            try:
                erro = _marcar(ws, linha, item['sp_id'])
            except Exception as e:  # noqa: BLE001
                erro = f'erro do Google ao gravar: {str(e)[:300]}'
        contagem[_registrar_resultado(item, linha, erro, ss)] += 1
    return {'ok': True, 'processados': len(itens), **contagem}


# ---------------------------------------------------------------------------
# O vigia
# ---------------------------------------------------------------------------
_vigia: Optional[threading.Thread] = None
_vigia_trava = threading.Lock()


def _volta_do_vigia() -> None:
    time.sleep(10)
    while True:
        rodada()
        time.sleep(INTERVALO_VIGIA_S)


def garantir_vigia() -> bool:
    """Liga o vigia se ele não estiver rodando neste processo. Não liga nos testes."""
    global _vigia
    if 'pytest' in sys.modules or os.getenv('VALIDASP_SEM_VIGIA'):
        return False
    if _vigia is not None and _vigia.is_alive():
        return True
    with _vigia_trava:
        if _vigia is not None and _vigia.is_alive():
            return True
        _vigia = threading.Thread(target=_volta_do_vigia, name='validasp-vigia', daemon=True)
        _vigia.start()
        logger.info('ValidaSP: vigia da fila ligado')
    return True
