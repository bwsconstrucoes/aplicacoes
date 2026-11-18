# -*- coding: utf-8 -*-
"""Comprovante de duas folhas: a segunda não é comprovante — 18/11/2026.

Pedido do dono, com o arquivo na mão: *"alguns comprovantes como esse saem em
duas páginas e o sistema tenta a segunda página. Gostaria que isso fosse
detectado pra não tentar baixar."*

O caso é o boleto pago pelo Sicredi. A folha 2 tem 106 caracteres e são só os
telefones do banco: *"SAC 0800 … Ouvidoria 0800 … Atendimento aos deficientes
auditivos …"*. Nenhum valor, nenhuma data, nenhuma SP.

O estrago era duplo, e o segundo é o caro:

1. Virava uma linha "NÃO localizado" no relatório, em TODO comprovante desses.
2. E, por ser "não localizada", mandava o robô ler as **52 mil linhas da
   SPsBD** procurando uma SP paga com Omie pendente. Uma folha de rodapé
   disparava a leitura mais cara do sistema.

⚠️ O risco de uma regra assim é jogar fora página que ERA comprovante. Por isso
ela exige a ausência de TUDO que identifica um pagamento (valor, data, SP,
autenticação, código de barras, conta, chave PIX, documento de quem recebeu) e
AINDA por cima que a página seja curta. Página cheia de texto que o leitor não
entendeu continua indo ao relatório como "não baixou" — ali pode haver dinheiro.

Os dados do comprovante abaixo são FICTÍCIOS. O arquivo que o dono mandou tinha
CNPJ, nome e conta de empresas de verdade; o formato do texto foi mantido, o
conteúdo não.
"""
import inspect
from pathlib import Path

import pytest

from app.apps.baixabradesco import core
from app.apps.baixabradesco.parser_sicredi import is_sicredi, parse_sicredi_text


EXEMPLOS = Path(__file__).parent / 'exemplos_baixabradesco'

# As duas folhas do comprovante que o dono mandou, no formato que a aplicação
# realmente lê (pdfplumber, "rótulo: valor"). Dados trocados por fictícios; o
# rodapé é o original, porque são só telefones públicos do banco.
FOLHA_1_SICREDI = (EXEMPLOS / 'sicredi_boleto_folha1.txt').read_text(encoding='utf-8')
RODAPE_SICREDI = (EXEMPLOS / 'sicredi_boleto_folha2.txt').read_text(encoding='utf-8')


def _ler(texto, pagina=1):
    from app.apps.baixabradesco.parser_bradesco import parse_bradesco_text
    ler = parse_sicredi_text if is_sicredi(texto) else parse_bradesco_text
    return ler(filename='c.pdf', page=pagina, text=texto,
               drive_link='', fingerprint=f'fp:{pagina}')


# =====================================================================
# 1. A decisão: o que é rodapé e o que não é
# =====================================================================

def test_o_rodape_do_sicredi_e_reconhecido():
    rec = _ler(RODAPE_SICREDI, pagina=2)
    assert core._pagina_sem_pagamento(rec, RODAPE_SICREDI) is True


def test_a_folha_do_comprovante_NAO_e_confundida_com_rodape():
    rec = _ler(FOLHA_1_SICREDI)
    assert rec.valor_pago == '1.000,00'
    assert rec.id_pipefy == '9999991'
    assert core._pagina_sem_pagamento(rec, FOLHA_1_SICREDI) is False


@pytest.mark.parametrize('campo, valor', [
    ('valor_pago', '1.000,00'),
    ('data_pagamento', '18/11/2026'),
    ('id_pipefy', '1234567'),
    ('identificador', 'ABC123'),
    ('codigo_barras', '23790000000000000'),
    ('conta_origem', '50024-0'),
    ('chave_pix_destino', 'fulano@exemplo.com'),
    ('documento_recebedor', '11.111.111/0001-11'),
])
def test_qualquer_pista_de_pagamento_salva_a_pagina(campo, valor):
    """Oito campos, e basta UM: é o que impede jogar fora um pagamento."""
    rec = _ler(RODAPE_SICREDI, pagina=2)
    setattr(rec, campo, valor)
    assert core._pagina_sem_pagamento(rec, RODAPE_SICREDI) is False


def test_pagina_cheia_de_texto_que_o_leitor_NAO_entendeu_continua_aparecendo():
    """A trava mais importante: aqui pode haver pagamento de verdade. Ela tem
    de chegar ao relatório como "não baixou", e não ser descartada."""
    texto = 'palavra ' * 200          # 1.600 caracteres, nada reconhecido
    rec = _ler(texto, pagina=2)
    assert rec.valor_pago == ''
    assert core._pagina_sem_pagamento(rec, texto) is False


def test_o_teto_de_tamanho_e_declarado():
    assert core.TEXTO_MINIMO_DE_COMPROVANTE >= 200
    assert len(' '.join(RODAPE_SICREDI.split())) < core.TEXTO_MINIMO_DE_COMPROVANTE


# =====================================================================
# 2. O lote inteiro: a folha 2 não tenta nada e não custa nada
# =====================================================================

CABECALHO = ['ID', 'Solicitação', 'Vencim.', 'Nome do Credor', 'CPF/CNPJ',
             'Descrição da Despesa', 'Valor Total', 'Centro de Custo',
             'Tipo de Despesa', 'Tipo de Pagamento', 'Responsável pelo Registro',
             'Dt. Autorização', 'Responsável Autorização', 'Status Aut.',
             'Status Pgt', 'Código Integração', 'Anexo Link', 'Card Link',
             'Anexo', 'Card', 'Status Aut. Símbolo', 'Status Pgt. Simbolo',
             'Pesquisa', 'Data do Pagamento', 'Info de Pgt', 'Parcela',
             'Nº da NF', 'Agendado', 'Linha', 'Nº do Pedido', 'Anuente',
             'Status Anuencia', 'Comprovante', 'Validação', 'Código de Barras',
             'ID Pipefy Contrato', 'Conta Pagamento']


def _linha_da_sp(sp_id='9999991', valor='1.000,00'):
    linha = [''] * len(CABECALHO)
    linha[0] = sp_id
    linha[6] = valor
    linha[14] = 'Pagar'
    linha[27] = 'agendado'
    return linha


@pytest.fixture
def lote(monkeypatch):
    """O lote de duas folhas, sem encostar em nada de fora."""
    leituras = []

    monkeypatch.setattr(core, 'get_gc', lambda: object())
    monkeypatch.setattr(core, 'load_spsagendar', lambda gc=None: [])
    monkeypatch.setattr(core, 'load_base_bancos', lambda gc=None: [])
    monkeypatch.setattr(core, 'load_fingerprints_processados', lambda gc=None: {})
    monkeypatch.setattr(core, 'load_baixabradesco_values',
                        lambda gc=None: [CABECALHO, _linha_da_sp()])
    monkeypatch.setattr(core, 'load_spsbd_values',
                        lambda gc=None: leituras.append('spsbd_52_mil') or [CABECALHO])
    monkeypatch.setattr(core, 'load_spsbd_omie_pendente',
                        lambda gc=None, values=None: leituras.append('omie_pendente') or {})
    monkeypatch.setattr(core, 'load_attachment_bytes', lambda att: b'%PDF-1.4')
    monkeypatch.setattr(core, 'extract_pdf_pages',
                        lambda b: [(1, FOLHA_1_SICREDI), (2, RODAPE_SICREDI)])
    monkeypatch.setattr(core, '_drenar_fila', lambda payload: {'ok': True})
    monkeypatch.setattr(core, 'enviar_aviso', lambda r, p: {'ok': None})
    monkeypatch.setattr(core, 'enviar_relatorio', lambda r, p: {'ok': None})
    return leituras


def _pedido():
    return {
        'modo_teste': False,
        'attachments': [{'filename': 'boleto_sicredi.pdf', 'base64': 'JVBERi0x'}],
        'opcoes': {'executar_omie': False, 'atualizar_spsbd': False,
                   'atualizar_pipefy': False, 'enviar_whatsapp': False,
                   'salvar_comprovante': False},
    }


def test_a_folha_2_nao_vira_comprovante(lote):
    r = core._processar_lote(_pedido())

    assert r['resumo']['paginas_processadas'] == 1, 'a folha de rodapé virou plano'
    assert r['resumo']['paginas_sem_pagamento'] == 1
    assert r['paginas_sem_pagamento'][0]['pagina'] == 2
    assert 'rodapé' in r['paginas_sem_pagamento'][0]['motivo']


def test_a_folha_2_NAO_dispara_a_leitura_das_52_mil_linhas(lote):
    """Era o custo escondido: toda folha de rodapé pagava a leitura mais cara
    que este sistema tem."""
    core._processar_lote(_pedido())
    assert lote == [], f'a folha de rodapé ainda custa leitura: {lote}'


def test_a_folha_1_continua_sendo_localizada(lote):
    r = core._processar_lote(_pedido())
    assert r['resumo']['localizados'] == 1
    assert r['planos'][0]['match']['id'] == '9999991'


def test_a_decisao_vem_ANTES_do_plano_B():
    """Se a ordem inverter, o custo volta sem ninguém notar."""
    fonte = inspect.getsource(core._processar_lote)
    # `rindex` de propósito: a primeira aparição de `_carregar_omie_pendente()`
    # é a DEFINIÇÃO da função aninhada, não a chamada.
    assert fonte.index('_pagina_sem_pagamento(') < fonte.rindex('_carregar_omie_pendente()')


# =====================================================================
# 3. Página sem texto nenhum deixou de sair em silêncio
# =====================================================================

def test_pagina_sem_texto_aparece_no_resumo(lote, monkeypatch):
    """Comprovante FOTOGRAFADO não tem texto, e o robô não lê imagem. Antes
    desaparecia sem uma palavra — o pagamento não baixava e ninguém sabia."""
    monkeypatch.setattr(core, 'extract_pdf_pages',
                        lambda b: [(1, FOLHA_1_SICREDI), (2, '')])

    r = core._processar_lote(_pedido())

    assert r['resumo']['paginas_sem_pagamento'] == 1
    motivo = r['paginas_sem_pagamento'][0]['motivo']
    assert 'sem texto' in motivo and 'imagem' in motivo


# =====================================================================
# 4. O relatório conta isso sem virar barulho
# =====================================================================

def test_o_relatorio_resume_os_rodapes_numa_linha_so():
    """Trinta boletos do Sicredi seriam trinta linhas de nada."""
    from app.apps.baixabradesco import avisos
    resultado = {
        'planos': [],
        'duplicados': [],
        'recusados': [],
        'paginas_sem_pagamento': [
            {'arquivo': 'c.pdf', 'pagina': 2, 'motivo': 'rodapé ou continuação'},
            {'arquivo': 'd.pdf', 'pagina': 2, 'motivo': 'rodapé ou continuação'},
        ],
        'resumo': {},
    }
    texto = '\n'.join(avisos.montar_relatorio(resultado))
    assert texto.count('rodapé') == 1
    assert '2 página(s) de rodapé' in texto


def test_o_relatorio_destaca_a_pagina_escaneada():
    """Essa não é rotina: é comprovante que não vai baixar nunca sem ação."""
    from app.apps.baixabradesco import avisos
    resultado = {
        'planos': [], 'duplicados': [], 'recusados': [],
        'paginas_sem_pagamento': [
            {'arquivo': 'c.pdf', 'pagina': 1,
             'motivo': 'Página sem texto nenhum — provavelmente foto'},
        ],
        'resumo': {},
    }
    texto = '\n'.join(avisos.montar_relatorio(resultado))
    assert 'sem texto nenhum' in texto
    assert 'PDF do banco' in texto
