# -*- coding: utf-8 -*-
"""A aba enxuta primeiro; a SPsBD inteira só como plano B.

Decisão do dono em 14/11/2026, depois de eu explicar por que o robô baixava as
52 mil linhas da SPsBD a cada comprovante:

> *"Vamos para o caminho C. A probabilidade do registro que a aplicação procura
> estar em SPsAgendar é perto de 100%. Todavia nela não ficam todas as
> informações. Criei a aba 'baixabradesco'. Nela sim estão os mesmos registros
> de SPsAgendar só que com as mesmas informações de SPsBD. Então, primeiro
> procure nela. Se não achar, só então você vai pra SPsBD."*

Era a causa de fundo do travamento: `/api/baixabradesco/executar` levava 162 s
nos registros de 13/11/2026, e com quatro linhas de atendimento isso derrubava
quem estava usando a tela. Dezenas de linhas no lugar de 52 mil.

O que este arquivo trava, porque errar em qualquer direção custa:

1. **O caso comum não lê a SPsBD.** Se voltar a ler, voltou o problema.
2. **O caso raro continua funcionando.** Comprovante cuja SP não está na aba
   enxuta ainda tem de ser encontrado — só mais devagar.
3. **Aba ausente não para a baixa.** Se alguém renomear ou apagar a aba, o robô
   volta ao comportamento antigo. Baixa devagar vale mais que baixa parada.
4. **Reenvio não é barrado por engano.** A decisão "a baixa anterior ficou pela
   metade?" olha as SPs que faltam pagar; se a SP não está na aba enxuta, não dá
   para concluir que está tudo certo — tem de conferir na SPsBD antes de barrar.
"""
import pytest

from app.apps.baixabradesco import sheets


# Mesmas colunas da SPsBD (A..AK). O cabeçalho é o que o leitor usa.
CABECALHO = [
    'ID', 'Solicitação', 'Vencim.', 'Nome do Credor', 'CPF/CNPJ',
    'Descrição da Despesa', 'Valor Total', 'Centro de Custo', 'Tipo de Despesa',
    'Tipo de Pagamento', 'Responsável pelo Registro', 'Dt. Autorização',
    'Responsável Autorização', 'Status Aut.', 'Status Pgt', 'Código Integração',
    'Anexo Link', 'Card Link', 'Anexo', 'Card', 'Status Aut. Símbolo',
    'Status Pgt. Simbolo', 'Pesquisa', 'Data do Pagamento', 'Info de Pgt',
    'Parcela', 'Nº da NF', 'Agendado', 'Linha', 'Nº do Pedido', 'Anuente',
    'Status Anuencia', 'Comprovante', 'Validação', 'Código de Barras',
    'ID Pipefy Contrato', 'Conta Pagamento',
]


def _linha(sp_id, valor='1.000,00', status_pgt='Pagar', agendado='agendado'):
    linha = [''] * len(CABECALHO)
    linha[0] = sp_id                  # A  = ID
    linha[6] = valor                  # G  = Valor Total
    linha[14] = status_pgt            # O  = Status Pgt
    linha[27] = agendado              # AB = Agendado
    return linha


class _Aba:
    def __init__(self, linhas, nome=''):
        self.linhas = linhas
        self.nome = nome
        self.leituras = []

    def get(self, faixa):
        self.leituras.append(faixa)
        return [CABECALHO] + self.linhas if self.linhas else []


class _Planilha:
    def __init__(self, abas):
        self.abas = abas
        self.pedidas = []

    def worksheet(self, nome):
        self.pedidas.append(nome)
        if nome not in self.abas:
            raise Exception(f'aba {nome} não existe')
        return self.abas[nome]


class _Google:
    def __init__(self, planilha):
        self.planilha = planilha

    def open_by_key(self, key):
        return self.planilha


# =====================================================================
# 1. o leitor da aba enxuta
# =====================================================================

def test_le_as_mesmas_colunas_da_spsbd(monkeypatch):
    """A aba espelha a SPsBD em colunas — mesmos índices, mesmo leitor."""
    aba = _Aba([_linha('1234567')])
    planilha = _Planilha({sheets.ABA_ENXUTA: aba})
    monkeypatch.setattr(sheets, 'get_gc', lambda: _Google(planilha))

    valores = sheets.load_baixabradesco_values()

    assert aba.leituras == ['A:AK']
    assert planilha.pedidas == [sheets.ABA_ENXUTA]
    assert valores[0] == CABECALHO


def test_aba_ausente_devolve_vazio_em_vez_de_derrubar(monkeypatch):
    """Aba renomeada ou apagada não pode parar a baixa."""
    planilha = _Planilha({})            # a aba enxuta não existe
    monkeypatch.setattr(sheets, 'get_gc', lambda: _Google(planilha))

    assert sheets.load_baixabradesco_values() == []


def test_aba_vazia_devolve_vazio(monkeypatch):
    planilha = _Planilha({sheets.ABA_ENXUTA: _Aba([])})
    monkeypatch.setattr(sheets, 'get_gc', lambda: _Google(planilha))

    assert sheets.load_baixabradesco_values() == []


def test_o_filtro_operacional_funciona_igual_na_aba_enxuta(monkeypatch):
    """O mesmo filtro de sempre: Status Pgt=Pagar e Agendado reconhecido."""
    valores = [CABECALHO,
               _linha('aberta'),
               _linha('ja_paga', status_pgt='Pago'),
               _linha('sem_agendamento', agendado=''),
               _linha('falha', agendado='falhaagendar')]

    index = sheets.load_spsbd_operacional(values=valores)

    assert sorted(index) == ['aberta', 'falha']


# =====================================================================
# 2. o caso comum não toca na SPsBD
# =====================================================================

def test_a_aba_enxuta_resolvendo_a_spsbd_NAO_e_lida(monkeypatch):
    from app.apps.baixabradesco import core

    lidas = []
    monkeypatch.setattr(core, 'load_baixabradesco_values',
                        lambda gc=None: [CABECALHO, _linha('1234567')])
    monkeypatch.setattr(core, 'load_spsbd_values',
                        lambda gc=None: lidas.append('spsbd') or [CABECALHO])
    monkeypatch.setattr(core, 'load_spsbd_omie_pendente',
                        lambda gc=None, values=None: lidas.append('omie_pendente') or {})

    index = core.load_spsbd_operacional(values=core.load_baixabradesco_values())

    assert '1234567' in index
    assert lidas == []


def test_o_retorno_diz_de_onde_veio_a_lista():
    """Para dar para MEDIR, em vez de supor."""
    import inspect
    fonte = inspect.getsource(core_fonte())
    assert "'fonte_das_sps'" in fonte
    assert "'aba_enxuta'" in fonte
    assert "'spsbd_completa'" in fonte


def core_fonte():
    from app.apps.baixabradesco import core
    return core


# =====================================================================
# 3. o plano B continua existindo
# =====================================================================

def test_o_plano_B_le_a_spsbd_uma_unica_vez():
    """Duas páginas sem SP não podem custar duas leituras de 52 mil linhas."""
    import inspect
    from app.apps.baixabradesco import core
    fonte = inspect.getsource(core.processar_baixabradesco)
    # a trava de "uma vez só" e o motivo estão no código, não na memória de quem leu
    assert "estado_spsbd['completa']" in fonte
    assert 'load_spsbd_values' in fonte


def test_o_plano_B_e_acionado_quando_nao_acha():
    import inspect
    from app.apps.baixabradesco import core
    fonte = inspect.getsource(core.processar_baixabradesco)
    assert "match.status == 'nao_localizado' and not estado_spsbd['completa']" in fonte
    # e tenta casar DE NOVO depois de completar — senão a leitura foi em vão
    assert fonte.count('match = match_receipt(rec, sps_index, sps_agendar)') >= 2


def test_reenvio_nao_e_barrado_so_porque_a_SP_nao_esta_na_aba_enxuta():
    """A SP pode simplesmente não estar ali; isso não prova que a baixa acabou."""
    import inspect
    from app.apps.baixabradesco import core
    fonte = inspect.getsource(core.processar_baixabradesco)
    i_check = fonte.index('sp_registrada and sp_registrada not in sps_index')
    i_decide = fonte.index('baixa_incompleta = bool(sp_registrada)')
    assert i_check < i_decide        # confere na SPsBD ANTES de decidir


def test_aba_enxuta_vazia_cai_no_comportamento_antigo():
    import inspect
    from app.apps.baixabradesco import core
    fonte = inspect.getsource(core.processar_baixabradesco)
    assert 'if not leu_da_aba_enxuta and not modo_teste:' in fonte


# =====================================================================
# 4. a coluna AC: a linha do registro na SPsBD
# =====================================================================
# Informação do dono, 14/11/2026: *"a coluna AC de baixabradesco é a linha onde
# está o registro em SPsBD"*. Ela economiza a SEGUNDA leitura gigante — a da
# gravação, que varria a coluna A inteira (52 mil valores) só para achar a linha
# onde escrever.
#
# ⚠️ Mas a aba é um ESPELHO, e espelho pode estar velho. Gravar numa linha
# errada marcaria a SP ERRADA como paga, e ninguém perceberia olhando a tela.
# Por isso a linha é tratada como PISTA, confirmada lendo uma célula.

def _linha_com_ac(sp_id, linha_spsbd, valor='1.000,00'):
    linha = _linha(sp_id, valor=valor)
    linha[28] = str(linha_spsbd)      # AC
    return linha


def test_a_linha_da_spsbd_vem_da_coluna_AC():
    valores = [CABECALHO, _linha_com_ac('1234567', 48321)]

    index = sheets.load_spsbd_operacional(values=valores,
                                          linha_vem_da_coluna_ac=True)

    assert index['1234567'].row_number == 48321


def test_sem_a_coluna_AC_fica_sem_numero_e_nao_inventa():
    """Número chutado seria pior que número nenhum: grava na SP errada."""
    valores = [CABECALHO, _linha('1234567')]          # AC vazia

    index = sheets.load_spsbd_operacional(values=valores,
                                          linha_vem_da_coluna_ac=True)

    assert index['1234567'].row_number == 0


def test_coluna_AC_com_lixo_ou_cabecalho_e_ignorada():
    for ruim in ('abc', '0', '1', '', '-5'):
        valores = [CABECALHO, _linha_com_ac('1234567', ruim)]
        index = sheets.load_spsbd_operacional(values=valores,
                                              linha_vem_da_coluna_ac=True)
        assert index['1234567'].row_number == 0, ruim


def test_lendo_a_SPsBD_a_linha_continua_sendo_a_posicao_real():
    """Na SPsBD, a linha é a própria posição — não a coluna AC."""
    valores = [CABECALHO, _linha_com_ac('1234567', 99999)]

    index = sheets.load_spsbd_operacional(values=valores)   # sem o atalho

    assert index['1234567'].row_number == 2


# --- a gravação usa a pista, mas confere ---------------------------------

class _AbaGravacao:
    def __init__(self, id_na_linha, total_linhas=3):
        self.id_na_linha = id_na_linha          # {linha: id}
        self.total = total_linhas
        self.celulas_lidas = []
        self.varreduras = 0
        self.gravado = []

    def get(self, faixa):
        self.celulas_lidas.append(faixa)
        linha = int(faixa[1:])
        return [[self.id_na_linha.get(linha, '')]]

    def batch_get(self, faixas):
        # A varredura é a leitura de COLUNA inteira ('A2:A'). A conferência
        # pós-gravação também usa batch_get, mas numa célula ('O48321') — contar
        # as duas como a mesma coisa era o defeito deste dublê.
        if any(f.endswith(':A') or f.endswith(':A1000000') for f in faixas):
            self.varreduras += 1
            maior = max(self.id_na_linha) if self.id_na_linha else 1
            return [[[self.id_na_linha.get(n, '')] for n in range(2, maior + 1)]]
        # conferência de célula: devolve o que foi gravado ali
        saida = []
        for f in faixas:
            valor = ''
            for item in self.gravado:
                if item['range'] == f:
                    valor = item['values'][0][0]
            saida.append([[valor]])
        return saida

    def batch_update(self, data, value_input_option=None):
        self.gravado.extend(data)


def _planilha_de_gravacao(monkeypatch, aba):
    monkeypatch.setattr(sheets, 'get_gc', lambda: _Google(_Planilha({'SPsBD': aba})))


def test_a_gravacao_confere_a_linha_e_nao_varre_a_coluna(monkeypatch):
    aba = _AbaGravacao({48321: '1234567'})
    _planilha_de_gravacao(monkeypatch, aba)

    r = sheets.execute_spsbd_updates([{
        'sheet_id': 'x', 'aba': 'SPsBD', 'linha': 48321,
        'filtros': {'A': '=1234567'}, 'updates': {'O': 'Pago'},
    }])

    assert r['ok'] is True
    assert aba.celulas_lidas[0] == 'A48321'      # uma célula
    assert aba.varreduras == 0                   # nenhuma varredura
    assert aba.gravado[0]['range'] == 'O48321'


def test_linha_errada_no_espelho_NAO_grava_na_SP_errada(monkeypatch):
    """O caso que justifica a conferência: o espelho envelheceu."""
    aba = _AbaGravacao({48321: 'OUTRA_SP', 7: '1234567'})
    _planilha_de_gravacao(monkeypatch, aba)

    r = sheets.execute_spsbd_updates([{
        'sheet_id': 'x', 'aba': 'SPsBD', 'linha': 48321,
        'filtros': {'A': '=1234567'}, 'updates': {'O': 'Pago'},
    }])

    assert aba.varreduras == 1                   # caiu na varredura de sempre
    assert r['ok'] is True
    assert all(d['range'].endswith('7') for d in aba.gravado)   # linha 7, a certa


def test_sem_pista_de_linha_a_gravacao_funciona_como_antes(monkeypatch):
    aba = _AbaGravacao({7: '1234567'})
    _planilha_de_gravacao(monkeypatch, aba)

    r = sheets.execute_spsbd_updates([{
        'sheet_id': 'x', 'aba': 'SPsBD', 'linha': 0,
        'filtros': {'A': '=1234567'}, 'updates': {'O': 'Pago'},
    }])

    assert aba.celulas_lidas == []
    assert aba.varreduras == 1
    assert r['ok'] is True


def test_falha_ao_conferir_a_celula_cai_na_varredura(monkeypatch):
    class _AbaQueFalhaNaCelula(_AbaGravacao):
        def get(self, faixa):
            raise RuntimeError('Google recusou')

    aba = _AbaQueFalhaNaCelula({7: '1234567'})
    _planilha_de_gravacao(monkeypatch, aba)

    r = sheets.execute_spsbd_updates([{
        'sheet_id': 'x', 'aba': 'SPsBD', 'linha': 48321,
        'filtros': {'A': '=1234567'}, 'updates': {'O': 'Pago'},
    }])

    assert aba.varreduras == 1
    assert r['ok'] is True
