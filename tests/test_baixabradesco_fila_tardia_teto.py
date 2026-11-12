# -*- coding: utf-8 -*-
"""O disparo do cron tem teto, e a rota não segura uma linha de atendimento.

12/11/2026, log do Render que o dono colou junto com *"tá super lento"*:

    AS 4 LINHAS OCUPADAS: POST /api/baixabradesco/processar-fila-tardia há 93 s
    AS 4 LINHAS OCUPADAS: POST /api/baixabradesco/processar-fila-tardia há 124 s

A produção atende com 4 linhas (`--workers 1 --threads 4`). O disparo
reprocessava a fila adiada INTEIRA e drenava a fila de falhas dentro da mesma
requisição — minutos com uma linha presa, a cada cinco minutos.

O que fica travado aqui:

- teto de itens e de tempo por disparo, conferido entre um item e outro, com o
  resto ficando para o próximo disparo (no disco ou na planilha, intacto);
- a parada por cota do Google continua valendo;
- a rota responde na hora e trabalha numa linha própria, um disparo por vez.
"""
import json
import os
import threading

import pytest

from app.apps.baixabradesco import fila as fila_mod
from app.apps.baixabradesco import fila_tardia as mod


class _Relogio:
    """Relógio de mentira para `time.monotonic`: anda quando o teste manda."""

    def __init__(self):
        self.agora = 1000.0

    def __call__(self):
        return self.agora


@pytest.fixture
def relogio(monkeypatch):
    r = _Relogio()
    monkeypatch.setattr(mod.time, 'monotonic', r)
    return r


@pytest.fixture
def fila_em_disco(tmp_path, monkeypatch):
    """Fila adiada num diretório descartável, com N arquivos."""
    monkeypatch.setattr(mod, 'FILA_DIR', str(tmp_path))

    def _encher(n):
        for i in range(n):
            nome = f'{1700000000 + i}_{i:08x}.json'
            with open(os.path.join(tmp_path, nome), 'w', encoding='utf-8') as f:
                json.dump({'payload': {'n': i}, 'tentativas': 0}, f)
    return _encher


@pytest.fixture
def sem_carona(monkeypatch):
    """Mutirão, avisos e fila de falhas dublados: aqui interessa o teto."""
    chamadas = []
    monkeypatch.setattr(mod, '_zerar_atraso_uma_vez',
                        lambda p: chamadas.append('zerar') or {'dispensadas': 0})
    monkeypatch.setattr(mod, '_dispensar_avisos_vencidos',
                        lambda p: chamadas.append('avisos') or {'dispensadas': 0})
    monkeypatch.setattr(mod, '_drenar_a_fila',
                        lambda p: chamadas.append('drenar') or {})
    return chamadas


def _processador(monkeypatch, efeito=None):
    """Dubla `processar_baixabradesco`; `efeito(payload)` roda antes de responder."""
    vistos = []
    import app.apps.baixabradesco.core as core

    def _processar(payload):
        vistos.append(payload.get('n'))
        if efeito:
            efeito(payload)
        return {'resumo': 'ok'}

    monkeypatch.setattr(core, 'processar_baixabradesco', _processar)
    return vistos


# =====================================================================
# teto por disparo
# =====================================================================

def test_fila_grande_anda_no_maximo_o_limite_de_itens(fila_em_disco, sem_carona,
                                                      monkeypatch):
    fila_em_disco(25)
    vistos = _processador(monkeypatch)

    r = mod.processar_fila_tardia({})

    assert len(vistos) == mod.MAX_ADIADOS_POR_DISPARO
    assert r['parou_por'] == 'limite_de_itens'
    assert r['pendentes'] == 25 - mod.MAX_ADIADOS_POR_DISPARO
    assert 'ainda na fila' in r['em_portugues']


def test_o_resto_e_processado_nos_disparos_seguintes(fila_em_disco, sem_carona,
                                                     monkeypatch):
    fila_em_disco(25)
    vistos = _processador(monkeypatch)

    for _ in range(3):
        mod.processar_fila_tardia({})

    assert sorted(vistos) == list(range(25))   # cada um uma vez, na ordem de chegada
    assert vistos == list(range(25))
    assert mod._listar() == []


def test_o_prazo_para_entre_um_item_e_outro(fila_em_disco, sem_carona, relogio,
                                            monkeypatch):
    """Cada item 'leva' 10 s: com prazo de 25 s, o terceiro começa e termina,
    o quarto fica para o próximo disparo."""
    fila_em_disco(8)

    def _demora(_payload):
        relogio.agora += 10

    vistos = _processador(monkeypatch, _demora)

    r = mod.processar_fila_tardia({})

    assert mod.PRAZO_POR_DISPARO_S == 25
    assert len(vistos) == 3
    assert r['parou_por'] == 'prazo'
    assert r['pendentes'] == 5


def test_sem_tempo_o_resto_do_disparo_fica_para_o_proximo(fila_em_disco, sem_carona,
                                                          relogio, monkeypatch):
    fila_em_disco(3)
    _processador(monkeypatch, lambda p: setattr(relogio, 'agora', relogio.agora + 30))

    r = mod.processar_fila_tardia({})

    assert sem_carona == []   # nem mutirão, nem avisos, nem drenagem
    assert r['atraso_zerado'] == {'adiado_para_o_proximo_disparo': True}
    assert r['fila_de_falhas'] == {'adiado_para_o_proximo_disparo': True}
    assert 'próximo' in r['em_portugues']


def test_com_tempo_sobrando_a_carona_roda_e_recebe_o_prazo(fila_em_disco, relogio,
                                                           monkeypatch):
    fila_em_disco(1)
    _processador(monkeypatch)
    recebido = {}
    monkeypatch.setattr(mod, '_zerar_atraso_uma_vez', lambda p: {'dispensadas': 0})
    monkeypatch.setattr(mod, '_dispensar_avisos_vencidos', lambda p: {'dispensadas': 0})
    monkeypatch.setattr(mod, '_drenar_a_fila', lambda p: recebido.update(p) or {})

    mod.processar_fila_tardia({})

    assert recebido['_prazo'] == pytest.approx(1000.0 + mod.PRAZO_POR_DISPARO_S)


def test_cota_estourada_continua_parando_na_hora(fila_em_disco, sem_carona,
                                                 monkeypatch):
    fila_em_disco(5)

    def _cota(_payload):
        raise RuntimeError('APIError: [429]: Quota exceeded for quota metric')

    vistos = _processador(monkeypatch, _cota)

    r = mod.processar_fila_tardia({})

    assert len(vistos) == 1
    assert r['parou_por'] == 'cota_do_google'
    assert r['pendentes'] == 5                      # o que falhou continua na fila
    assert r['processados'][0]['tentativas'] == 1


@pytest.mark.parametrize('valor', ['0', '-3', 'abc', '9999'])
def test_prazo_fora_de_faixa_no_ambiente_cai_no_padrao(monkeypatch, valor):
    """Zero não pode virar 'nunca faz nada'; um número enorme não pode trazer
    de volta a linha presa por minutos."""
    monkeypatch.setenv('BAIXABRADESCO_PRAZO_CRON_S', valor)
    assert mod._prazo_do_disparo({}) == mod.PRAZO_POR_DISPARO_S


def test_o_ambiente_ajusta_o_prazo_sem_publicar(monkeypatch):
    monkeypatch.setenv('BAIXABRADESCO_PRAZO_CRON_S', '40')
    assert mod._prazo_do_disparo({}) == 40


# =====================================================================
# a fila de falhas também respeita o prazo
# =====================================================================

def test_drenagem_com_prazo_vencido_nao_toca_em_nada(monkeypatch, relogio):
    chamadas = []
    monkeypatch.setattr(fila_mod, 'reprocessar_fila', lambda p: chamadas.append(p) or {})

    r = fila_mod.drenar_por_etapa(mod.LIMITES_CRON, {'_prazo': relogio.agora - 1})

    assert chamadas == []
    assert r['parou_por_prazo_na_etapa'] == 'omie'
    assert r['zapi'] == {'adiado_para_o_proximo_disparo': True}


def test_reprocessar_fila_para_no_prazo_e_deixa_o_resto_pendente(monkeypatch, relogio):
    itens = [{'_row_number': n, 'Tentativas': '0', 'Etapa': 'omie'} for n in range(2, 12)]
    monkeypatch.setattr(fila_mod, 'get_gc', lambda: object())
    monkeypatch.setattr(fila_mod, 'ensure_fila_sheet', lambda gc: object())
    monkeypatch.setattr(fila_mod, 'listar_pendentes', lambda *a, **k: list(itens))
    gravadas = []
    monkeypatch.setattr(fila_mod, '_update_row',
                        lambda ws, row, status, *a, **k: gravadas.append((row, status)))

    def _omie(item, payload):
        relogio.agora += 10
        return {'ok': True}

    monkeypatch.setattr(fila_mod, '_retry_omie', _omie)

    r = fila_mod.reprocessar_fila({'limite': 10, 'etapas': ['omie'],
                                   '_prazo': relogio.agora + 25})

    assert r['interrompido'] == 'prazo_do_disparo'
    assert r['pendentes_processados'] == 3
    assert [g[0] for g in gravadas] == [2, 3, 4]   # as outras 7 linhas intactas


def test_reprocessar_fila_sem_prazo_continua_como_era(monkeypatch):
    """As rotas de drenagem à mão não passam `_prazo`: nada muda para elas."""
    itens = [{'_row_number': n, 'Tentativas': '0', 'Etapa': 'omie'} for n in range(2, 7)]
    monkeypatch.setattr(fila_mod, 'get_gc', lambda: object())
    monkeypatch.setattr(fila_mod, 'ensure_fila_sheet', lambda gc: object())
    monkeypatch.setattr(fila_mod, 'listar_pendentes', lambda *a, **k: list(itens))
    monkeypatch.setattr(fila_mod, '_update_row', lambda *a, **k: None)
    monkeypatch.setattr(fila_mod, '_retry_omie', lambda item, payload: {'ok': True})

    r = fila_mod.reprocessar_fila({'limite': 5, 'etapas': ['omie']})

    assert r['pendentes_processados'] == 5
    assert r['interrompido'] == ''


# =====================================================================
# segundo plano: a rota responde na hora, um disparo por vez
# =====================================================================

@pytest.fixture
def disparo_preso(monkeypatch):
    """`processar_fila_tardia` que só termina quando o teste solta."""
    soltar = threading.Event()
    comecou = threading.Event()
    rodadas = []

    def _processar(payload):
        rodadas.append(payload)
        comecou.set()
        assert soltar.wait(5), 'o teste não soltou o disparo'
        return {'ok': True, 'em_portugues': 'feito', 'segundos': 0.1, 'pendentes': 0}

    monkeypatch.setattr(mod, 'processar_fila_tardia', _processar)
    monkeypatch.setattr(mod, '_listar', lambda: ['a.json', 'b.json'])
    monkeypatch.setattr(mod, '_estado', {'rodando_desde': None, 'ultima': None})
    monkeypatch.setattr(mod, '_trava', threading.Lock())
    yield soltar, comecou, rodadas
    soltar.set()


def _esperar_soltar(trava, segundos=5):
    assert trava.acquire(timeout=segundos), 'a trava não foi solta'
    trava.release()


def test_a_rota_responde_sem_esperar_o_trabalho(disparo_preso):
    soltar, comecou, rodadas = disparo_preso

    r = mod.disparar_em_segundo_plano({'segredo': 'x'})

    assert r['disparado'] is True
    assert r['pendentes'] == 2
    assert comecou.wait(5)
    assert not soltar.is_set()          # respondeu com o trabalho ainda rodando
    soltar.set()
    _esperar_soltar(mod._trava)
    assert mod._estado['ultima']['resultado']['em_portugues'] == 'feito'


def test_dois_disparos_ao_mesmo_tempo_nao_processam_em_dobro(disparo_preso):
    soltar, comecou, rodadas = disparo_preso

    primeiro = mod.disparar_em_segundo_plano({})
    assert comecou.wait(5)
    segundo = mod.disparar_em_segundo_plano({})
    agora = mod.processar_agora({})

    assert primeiro['disparado'] is True
    assert segundo['disparado'] is False and segundo['ja_em_andamento'] is True
    assert segundo['rodando_desde']
    assert agora['ja_em_andamento'] is True
    soltar.set()
    _esperar_soltar(mod._trava)
    assert len(rodadas) == 1


def test_o_disparo_seguinte_mostra_o_resultado_do_anterior(disparo_preso):
    soltar, comecou, rodadas = disparo_preso
    soltar.set()

    mod.disparar_em_segundo_plano({})
    _esperar_soltar(mod._trava)
    r = mod.disparar_em_segundo_plano({})
    _esperar_soltar(mod._trava)

    assert r['ultima_execucao']['resultado']['em_portugues'] == 'feito'
    assert r['ultima_execucao']['terminou_em']


def test_disparo_que_explode_solta_a_trava(monkeypatch):
    """Uma trava presa pararia a fila até o serviço reiniciar."""
    monkeypatch.setattr(mod, '_estado', {'rodando_desde': None, 'ultima': None})
    monkeypatch.setattr(mod, '_trava', threading.Lock())
    monkeypatch.setattr(mod, '_listar', lambda: [])
    monkeypatch.setattr(mod, 'processar_fila_tardia',
                        lambda p: (_ for _ in ()).throw(RuntimeError('planilha fora')))

    mod.disparar_em_segundo_plano({})
    _esperar_soltar(mod._trava)

    assert mod._estado['rodando_desde'] is None
    assert 'planilha fora' in mod._estado['ultima']['resultado']['erro']
    assert mod.disparar_em_segundo_plano({})['disparado'] is True
    _esperar_soltar(mod._trava)


def test_a_rota_usa_o_segundo_plano_e_esperar_roda_na_hora(monkeypatch):
    from flask import Flask
    from app.apps.baixabradesco import routes

    monkeypatch.setattr(routes, '_authorized', lambda p: True)
    monkeypatch.setattr(routes, 'disparar_em_segundo_plano',
                        lambda p: {'ok': True, 'disparado': True})
    monkeypatch.setattr(routes, 'processar_agora',
                        lambda p: {'ok': True, 'processados': []})
    app = Flask(__name__)
    app.register_blueprint(routes.bp, url_prefix='/api/baixabradesco')
    cliente = app.test_client()

    r1 = cliente.post('/api/baixabradesco/processar-fila-tardia', json={})
    r2 = cliente.post('/api/baixabradesco/processar-fila-tardia', json={'esperar': True})

    assert r1.get_json() == {'ok': True, 'disparado': True}
    assert r2.get_json() == {'ok': True, 'processados': []}
