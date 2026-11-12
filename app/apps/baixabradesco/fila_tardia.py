# -*- coding: utf-8 -*-
"""Fila tardia em disco (/tmp) para payloads que falharam na carga do Sheets.

Quando a quota do Google (429) estoura na carga inicial das bases, o payload
é gravado em /tmp/baixabradesco_fila_tardia/ e a resposta ao Make é 200 com
adiado=True — o cenário NÃO é interrompido. O reprocessamento é disparado
pelo cron-job.org via POST /api/baixabradesco/processar-fila-tardia.

Obs: no plano pago do Render o /tmp persiste entre requests (mesma instância).
Um deploy/restart limpa a fila — perda aceitável, pois o comprovante pode ser
reenviado pelo Make.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from datetime import datetime, timedelta

from .utils import as_string

FILA_DIR = '/tmp/baixabradesco_fila_tardia'
MAX_TENTATIVAS = 10

# ── Por que estes três limites existem ───────────────────────────────────────
#
# Diagnóstico do dono em 12/11/2026, e ele estava certo:
#
#   *"A gente manda um comprovante, aí manda de novo, aí não acontece, aí manda
#   de novo, fica na fila tardia, aí aquilo vai comendo processamento. A gente
#   tem quatro threads e essa baixa normalmente leva muito tempo: tá ocupando as
#   linhas todas, aí chegou outra requisição e acaba bloqueando. E tá tentando
#   fazer baixa de coisa que nem precisaria mais."*
#
# Eram três defeitos somados, e cada um multiplicava o outro:
#
# 1. **O laço não tinha limite.** Um disparo reprocessava TODOS os adiados numa
#    requisição só, e cada um custa a carga inteira da SPsBD (A:AK, ~52 mil
#    linhas). Dez adiados = dez cargas pesadas numa requisição.
# 2. **Nada impedia dois disparos ao mesmo tempo.** O cron dispara de 5 em 5
#    minutos; se a passada demora mais que isso, a seguinte começa com a
#    anterior ainda rodando, lendo os MESMOS arquivos e refazendo o MESMO
#    trabalho. Com 4 threads, duas ou três passadas sobrepostas tomam o serviço
#    inteiro — é exatamente o travamento que ele sentiu.
# 3. **O mesmo comprovante entrava várias vezes.** Reenviar durante a crise de
#    cota criava um arquivo novo a cada envio, e todos eram processados.
#
# A trava contra duplicidade (a impressão digital do comprovante) sempre
# funcionou — mas ela só decide DEPOIS da carga pesada. Ou seja: o trabalho
# inútil era pago antes de ser reconhecido como inútil.

# Quantos adiados uma passada processa. Baixo de propósito: o cron dispara de 5
# em 5 minutos, então 2 por passada são ~24 por hora, e a requisição termina
# rápido em vez de segurar uma das quatro linhas do serviço.
LIMITE_POR_DISPARO = 2

# Marca de "tem passada rodando". Sem isso, disparos se sobrepõem.
LOCK_NOME = '_em_execucao.lock'

# Depois deste tempo a marca é considerada abandonada (o serviço reiniciou no
# meio, por exemplo) e a passada seguinte assume. Tem de ser maior que uma
# passada normal e menor que a paciência de quem espera.
LOCK_VALIDADE_MIN = 15

# Adiado mais velho que isto quase certamente já foi reenviado ou resolvido na
# mão. Insistir nele é o "fazer de novo uma coisa que já foi feita".
IDADE_MAXIMA_DIAS = 2


def _garantir_dir():
    os.makedirs(FILA_DIR, exist_ok=True)


def _listar():
    _garantir_dir()
    return sorted(a for a in os.listdir(FILA_DIR) if a.endswith('.json'))


def _impressao_do_pedido(payload: dict) -> str:
    """Impressão digital do PEDIDO, calculada só com o que já está na mão.

    Serve para reconhecer o mesmo comprovante reenviado **antes** de gastar
    qualquer leitura de planilha. A impressão digital que já existia (a do
    comprovante, na SPsBD) resolve a duplicidade, mas só decide depois da carga
    pesada — e é justamente a carga que estava travando o serviço.
    """
    anexos = payload.get('attachments') or payload.get('comprovantes') or []
    if isinstance(anexos, dict):
        anexos = [anexos]
    partes = []
    for a in anexos:
        if not isinstance(a, dict):
            partes.append(as_string(a)[:200])
            continue
        conteudo = as_string(a.get('base64') or a.get('url') or '')
        partes.append('|'.join([
            as_string(a.get('filename') or a.get('nome') or ''),
            str(len(conteudo)),
            hashlib.sha256(conteudo.encode('utf-8', 'ignore')).hexdigest()[:32],
        ]))
    if not partes:
        return ''
    return hashlib.sha256('\n'.join(sorted(partes)).encode('utf-8')).hexdigest()[:32]


def _ja_esta_na_fila(impressao: str) -> str:
    """Nome do arquivo já enfileirado com a mesma impressão, ou vazio."""
    if not impressao:
        return ''
    for nome in _listar():
        try:
            with open(os.path.join(FILA_DIR, nome), encoding='utf-8') as f:
                item = json.load(f)
        except Exception:
            continue
        if as_string(item.get('impressao')) == impressao:
            return nome
    return ''


def _caminho_lock() -> str:
    return os.path.join(FILA_DIR, LOCK_NOME)


def _tomar_lock() -> bool:
    """Marca que há uma passada rodando. False quando já há outra viva.

    Sem isto, o disparo de 5 em 5 minutos começa com o anterior ainda rodando:
    os dois leem os mesmos arquivos e refazem o mesmo trabalho, e com quatro
    linhas de atendimento duas ou três passadas sobrepostas tomam o serviço.
    """
    _garantir_dir()
    caminho = _caminho_lock()
    try:
        idade = time.time() - os.path.getmtime(caminho)
        if idade < LOCK_VALIDADE_MIN * 60:
            return False
    except FileNotFoundError:
        pass
    except Exception:
        return False
    try:
        with open(caminho, 'w', encoding='utf-8') as f:
            f.write(time.strftime('%d/%m/%Y %H:%M:%S'))
        return True
    except Exception:
        # Não conseguiu marcar: deixa passar, porque travar tudo é pior.
        return True


def _soltar_lock() -> None:
    try:
        os.remove(_caminho_lock())
    except Exception:
        pass


def _velho_demais(item: dict) -> bool:
    criado = as_string(item.get('criado_em'))
    for fmt in ('%d/%m/%Y %H:%M:%S', '%d/%m/%Y %H:%M'):
        try:
            nascido = datetime.strptime(criado, fmt)
        except Exception:
            continue
        return (datetime.now() - nascido) > timedelta(days=IDADE_MAXIMA_DIAS)
    return False


def adiar_payload(payload: dict, erro: str) -> dict:
    """Grava o payload em disco e retorna resposta 200-ok para o Make."""
    _garantir_dir()

    # Reenviar o mesmo comprovante durante a crise de cota criava um arquivo
    # novo a cada envio, e TODOS eram processados depois. Agora o segundo envio
    # reconhece o primeiro e não entra de novo.
    impressao = _impressao_do_pedido(payload)
    repetido = _ja_esta_na_fila(impressao)
    if repetido:
        return {
            'ok': True,
            'app': 'baixabradesco',
            'adiado': True,
            'ja_estava_na_fila': True,
            'arquivo': repetido,
            'pendentes': len(_listar()),
            'motivo': 'Este comprovante já está na fila de adiados; não foi'
                      ' enfileirado de novo.',
        }

    item = {
        'payload': payload,
        'impressao': impressao,
        'erro_original': (erro or '')[:500],
        'tentativas': 0,
        'criado_em': time.strftime('%d/%m/%Y %H:%M:%S'),
    }
    nome = f'{int(time.time())}_{uuid.uuid4().hex[:8]}.json'
    with open(os.path.join(FILA_DIR, nome), 'w', encoding='utf-8') as f:
        json.dump(item, f, ensure_ascii=False)
    return {
        'ok': True,
        'app': 'baixabradesco',
        'adiado': True,
        'arquivo': nome,
        'pendentes': len(_listar()),
        'motivo': 'Quota do Google Sheets excedida; payload enfileirado para reprocessamento automático.',
    }


# Quanto o cron drena por disparo. Ele roda de 5 em 5 minutos: 15 por etapa dá
# ~180 itens por hora sem encostar na cota do Google, que é compartilhada com o
# ERP e o painel. A ORDEM mora em `fila.ORDEM_ETAPAS`, num lugar só, porque os
# dois caminhos automáticos já divergiram uma vez.
LIMITES_CRON = {'omie': 15, 'sheets': 15, 'pipefy': 15, 'zapi': 10}

# ── Mutirão único: zerar o que ficou para trás ────────────────────────────────
#
# Pedido do dono em 08/10/2026, vendo a fila com 2.213 pendências cuja mais
# antiga era de 18/06: *"só preciso que rode as coisas desse mês em diante. O que
# tá pra trás, poderia zerar."* A razão é dele e é boa: faz conciliação bancária
# diária, então o que ficou para trás já foi resolvido na mão — a pendência é de
# registro, não de dinheiro.
#
# ⚠️ A data é FIXA de propósito, não "o mês corrente". Ele autorizou zerar o que
# estava para trás *naquele dia*; uma regra que andasse com o calendário ficaria
# dispensando pendência nova todo mês primeiro, o que ele não pediu e seria a
# forma mais silenciosa possível de perder trabalho.
#
# É um mutirão que se encerra sozinho: depois que as linhas antigas estão
# marcadas, nenhuma casa com o critério e a passada fica de graça.
ZERAR_ANTES_DE = '01/10/2026'
ZERAR_POR_DISPARO = 500


def _zerar_atraso_uma_vez(payload: dict | None = None) -> dict:
    """Dispensa, aos poucos, as pendências anteriores ao corte autorizado.

    Em blocos por disparo para não tomar a cota do Google de uma vez — ela é
    por minuto e é compartilhada com o ERP, o painel e o Análise de SPs.
    """
    from .fila import zerar_fila_antiga

    corte = (os.getenv('BAIXABRADESCO_ZERAR_ANTES_DE', '') or ZERAR_ANTES_DE).strip()
    if not corte:
        return {'desligado': True}
    pedido = dict(payload or {})
    pedido.update({'antes_de': corte, 'limite': ZERAR_POR_DISPARO})
    try:
        r = zerar_fila_antiga(pedido)
        saida = {
            'antes_de': r.get('antes_de'),
            'dispensadas': r.get('dispensadas'),
            'por_etapa': r.get('por_etapa'),
            'erro': r.get('erro'),
        }
        # Um mutirão precisa saber dizer que acabou, senão alguém fica olhando
        # número sem saber o que esperar — e a varredura segue custando uma
        # leitura da faixa de controle a cada cinco minutos, de graça.
        if r.get('ok') and not r.get('encontradas'):
            saida['concluido'] = True
            saida['em_portugues'] = (
                'Mutirão concluído: não há mais pendência anterior a '
                + as_string(r.get('antes_de'))
                + '. Pode esvaziar BAIXABRADESCO_ZERAR_ANTES_DE no Render para'
                  ' a varredura parar de rodar à toa.')
        return saida
    except Exception as e:
        return {'erro': str(e)[:200]}


def _dispensar_avisos_vencidos(payload: dict | None = None) -> dict:
    """Tira da fila o aviso que já não serve. Vale só para aviso."""
    from .fila import dispensar_avisos_vencidos
    try:
        r = dispensar_avisos_vencidos(dict(payload or {}))
        return {'dispensadas': r.get('dispensadas'), 'erro': r.get('erro')}
    except Exception as e:
        return {'erro': str(e)[:200]}


def _drenar_a_fila(payload: dict | None = None) -> dict:
    from .fila import drenar_por_etapa
    return drenar_por_etapa(LIMITES_CRON, payload)


def _descrever_item(nome: str) -> dict:
    """Resumo SEGURO de um item da fila tardia.

    ⚠️ Nunca devolve o `payload`. Ele vem do Make e carrega, dentro dele, a
    chave e o segredo da API do Omie e os tokens do Z-API — é assim que essas
    credenciais chegam hoje. Uma rota de diagnóstico que devolvesse o payload
    cru estaria publicando as credenciais da empresa para quem tivesse o
    endereço.
    """
    caminho = os.path.join(FILA_DIR, nome)
    try:
        with open(caminho, encoding='utf-8') as f:
            item = json.load(f)
    except Exception as e:
        return {'arquivo': nome, 'ilegivel': True, 'erro': str(e)[:200]}

    payload = item.get('payload') or {}
    anexos = payload.get('attachments') or payload.get('comprovantes') or []
    if isinstance(anexos, dict):
        anexos = [anexos]
    nomes = []
    for a in anexos:
        if isinstance(a, dict):
            nome_arq = as_string(a.get('filename') or a.get('nome') or '')
            if nome_arq:
                nomes.append(nome_arq[:80])

    try:
        tamanho = os.path.getsize(caminho)
    except Exception:
        tamanho = 0

    return {
        'arquivo': nome,
        'criado_em': as_string(item.get('criado_em')),
        'tentativas': item.get('tentativas') or 0,
        'erro_original': as_string(item.get('erro_original'))[:300],
        'comprovantes': nomes or ['(sem nome de arquivo no pedido)'],
        'quantidade_comprovantes': len(anexos),
        'tamanho_bytes': tamanho,
    }


def resumo_fila_tardia() -> dict:
    """Mostra o que está na fila tardia. Não processa e não apaga nada."""
    nomes = _listar()
    itens = [_descrever_item(n) for n in nomes]
    frase = (f'{len(itens)} comprovante(s) adiado(s) esperando na fila tardia.'
             ' Ela fica em arquivo dentro do servidor, e uma publicação ou'
             ' reinício do serviço APAGA tudo que estiver aqui.'
             if itens else
             'A fila tardia está vazia — nenhum comprovante adiado esperando.')
    if itens:
        frase += (' O mais antigo é de ' + as_string(itens[0].get('criado_em'))
                  + '.')
    return {
        'ok': True,
        'app': 'baixabradesco',
        'acao': 'resumo_fila_tardia',
        'pasta': FILA_DIR,
        'pendentes': len(itens),
        'itens': itens,
        'em_portugues': frase,
    }


def limpar_fila_tardia(payload: dict | None = None) -> dict:
    """Descarta os comprovantes adiados. Exige confirmação explícita.

    ⚠️ Isto JOGA FORA comprovante que ainda não foi processado — ou seja,
    pagamento que ainda não foi baixado. Por isso duas coisas:

    1. **`confirmar: true` é obrigatório.** Sem ele a rota só mostra o que
       seria descartado. Um "limpar" que roda sem confirmação é só uma forma
       elaborada de perder trabalho.
    2. **O que sai vai no relatório**, item por item, para ficar registro de
       quais comprovantes precisam ser reenviados pelo Make.
    """
    payload = payload or {}
    itens = [_descrever_item(n) for n in _listar()]

    if not payload.get('confirmar'):
        return {
            'ok': True,
            'app': 'baixabradesco',
            'acao': 'limpar_fila_tardia',
            'apagou': 0,
            'seria_apagado': len(itens),
            'itens': itens,
            'em_portugues': (
                f'{len(itens)} comprovante(s) SERIAM descartados — nada foi'
                ' apagado ainda. Para apagar de verdade, repita o pedido com'
                ' confirmar: true. Cada um desses comprovantes volta a ser'
                ' baixado só se for reenviado pelo Make.'
                if itens else
                'A fila tardia está vazia — não há nada a limpar.'),
        }

    apagados, falhas = [], []
    for item in itens:
        nome = item['arquivo']
        try:
            os.remove(os.path.join(FILA_DIR, nome))
            apagados.append(item)
        except Exception as e:
            falhas.append({'arquivo': nome, 'erro': str(e)[:200]})

    return {
        'ok': True,
        'app': 'baixabradesco',
        'acao': 'limpar_fila_tardia',
        'apagou': len(apagados),
        'itens_descartados': apagados,
        'falhas': falhas,
        'pendentes_agora': len(_listar()),
        'em_portugues': (
            f'{len(apagados)} comprovante(s) descartados da fila tardia.'
            ' Eles NÃO foram baixados: cada um volta a ser processado só se for'
            ' reenviado pelo Make. A lista deles está no campo'
            ' itens_descartados — guarde se precisar reenviar.'
            if apagados else
            'Nada foi apagado: a fila tardia já estava vazia.'),
    }


def processar_fila_tardia(payload: dict | None = None) -> dict:
    """Reprocessa os payloads adiados, um a um, em ordem de chegada.

    - Sucesso: remove o arquivo.
    - Falha comum: incrementa tentativas (descarta após MAX_TENTATIVAS).
    - Falha por quota (429): para o loop imediatamente — o resto fica para o
      próximo disparo do cron, evitando queimar a janela de quota seguinte.
    """
    from .core import processar_baixabradesco

    payload = payload or {}
    resultados = []
    descartados_por_idade = []

    # ⚠️ Uma passada por vez. O cron dispara de 5 em 5 minutos; sem isto, a
    # passada seguinte começa com a anterior ainda rodando e as duas refazem o
    # mesmo trabalho — com quatro linhas de atendimento, é o que derruba o
    # serviço para quem está usando a tela.
    if not _tomar_lock():
        return {
            'ok': True,
            'app': 'baixabradesco',
            'acao': 'processar_fila_tardia',
            'pulado': 'ja_ha_uma_passada_rodando',
            'pendentes': len(_listar()),
            'em_portugues': ('Já havia uma passada em andamento, então esta não'
                             ' começou. É assim de propósito: duas ao mesmo'
                             ' tempo refazem o mesmo trabalho e ocupam as'
                             ' linhas de atendimento do serviço.'),
        }

    try:
        return _processar_adiados(payload, resultados, descartados_por_idade)
    finally:
        _soltar_lock()


def _processar_adiados(payload, resultados, descartados_por_idade) -> dict:
    from .core import processar_baixabradesco

    limite = int(payload.get('limite_adiados') or LIMITE_POR_DISPARO)
    feitos = 0

    for nome in _listar():
        if feitos >= limite:
            break
        caminho = os.path.join(FILA_DIR, nome)
        try:
            with open(caminho, encoding='utf-8') as f:
                item = json.load(f)
        except Exception:
            os.remove(caminho)
            continue

        # Adiado de dois dias atrás quase certamente já foi reenviado ou
        # resolvido na mão. Insistir nele é refazer o que já foi feito — e
        # custa a carga inteira da planilha para descobrir isso.
        if _velho_demais(item):
            try:
                os.remove(caminho)
            except Exception:
                pass
            descartados_por_idade.append({
                'arquivo': nome,
                'criado_em': as_string(item.get('criado_em')),
            })
            continue

        feitos += 1
        try:
            r = processar_baixabradesco(item.get('payload') or {})
            resultados.append({'arquivo': nome, 'ok': True, 'resumo': r.get('resumo')})
            os.remove(caminho)
        except Exception as e:
            msg = str(e)
            item['tentativas'] = int(item.get('tentativas') or 0) + 1
            item['ultimo_erro'] = msg[:500]
            if item['tentativas'] >= MAX_TENTATIVAS:
                os.remove(caminho)
                resultados.append({'arquivo': nome, 'ok': False, 'descartado': True, 'erro': msg[:200]})
            else:
                with open(caminho, 'w', encoding='utf-8') as f:
                    json.dump(item, f, ensure_ascii=False)
                resultados.append({'arquivo': nome, 'ok': False, 'tentativas': item['tentativas'], 'erro': msg[:200]})
            if '429' in msg or 'Quota exceeded' in msg or 'RESOURCE_EXHAUSTED' in msg:
                break  # quota estourada de novo — tenta no próximo cron

    pendentes = len(_listar())
    return {
        'ok': True,
        'app': 'baixabradesco',
        'processados': resultados,
        'descartados_por_idade': descartados_por_idade,
        'limite_por_disparo': limite,
        'pendentes': pendentes,
        'em_portugues': (
            f'{len(resultados)} adiado(s) processados nesta passada'
            + (f', {len(descartados_por_idade)} descartados por serem antigos'
               if descartados_por_idade else '')
            + f'. Ainda esperando: {pendentes}.'),
        # O cron já roda de 5 em 5 minutos e já está autenticado: a fila de
        # falhas pega carona nele. Antes ela só andava se alguém chamasse a rota
        # à mão — e, pelos números de 08/10/2026 (2.269 linhas, nenhuma
        # concluída, a mais antiga de 18/06/2026), nunca ninguém chamou.
        # A ORDEM importa: zerar primeiro, drenar depois. Senão a drenagem
        # gastaria a passada inteira nas linhas antigas que vão ser dispensadas
        # dois segundos mais tarde.
        'atraso_zerado': _zerar_atraso_uma_vez(payload),
        # Aviso que passou do prazo nunca mais vai ser enviado: deixá-lo
        # PENDENTE era um limbo que travava a contagem em 121 pendências que
        # jamais andariam (números do dono em 08/10/2026).
        'avisos_vencidos': _dispensar_avisos_vencidos(payload),
        'fila_de_falhas': _drenar_a_fila(payload),
    }