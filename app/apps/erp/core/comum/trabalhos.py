# ============================================================================
# ERP — core/comum/trabalhos.py
# QUEM faz o trabalho pesado. A fila em si é o `tarefas.py`; aqui ficam os
# executores, e só eles.
#
# POR QUE SEPARADO DA FILA
#
# A fila não pode conhecer o ERP. Se ela importasse a importação do Pipefy, a
# emissão de nota e a leitura de documento, um defeito em qualquer um desses
# módulos derrubaria a fila inteira no import — e, como a fila sobe junto com o
# serviço, derrubaria os catorze módulos. Aqui os executores são registrados de
# fora, e a fila só sabe o nome deles.
#
# REGRA DE OURO DE UM EXECUTOR
#
# Ele recebe a sessão, os parâmetros que foram gravados na fila e a função de
# andamento. Devolve um dicionário — que é o que a pessoa vai ler quando o
# trabalho terminar. Se levantar erro, a fila cuida de tentar de novo.
#
# Os parâmetros vêm do BANCO, não da memória: o trabalho pode rodar depois de o
# serviço ter reiniciado. Por isso são números e textos simples, nunca objetos.
# ============================================================================
from __future__ import annotations

import logging
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.apps.erp.core.comum import tarefas
from app.apps.erp.db.models.cadastros import Usuario

logger = logging.getLogger(__name__)

# Teto de cards por importação, o mesmo que a tela já anunciava.
MAX_CARDS = 100


def _usuario(s: Session, parametros: dict[str, Any]) -> Optional[Usuario]:
    uid = parametros.get("usuario_id")
    return s.get(Usuario, int(uid)) if uid else None


# ---------------------------------------------------------------------------
# Importar cards do Pipefy
# ---------------------------------------------------------------------------
def importar_pipefy(s: Session, parametros: dict[str, Any], andamento) -> dict[str, Any]:
    """Cem cards, cada um com consulta ao Pipefy e anexos para baixar.

    Era o caso mais claro de trabalho que não cabe num clique: enquanto rodava,
    segurava uma das quatro linhas de atendimento do serviço e todo mundo
    sentia o sistema pesado, sem saber por quê.
    """
    from app.apps.erp.core.importadores.pipefy_cards import buscar_cards, importar_cards

    ids = [str(i) for i in (parametros.get("ids") or [])]
    if not ids:
        raise ValueError("Nenhum card para importar.")

    andamento(0, len(ids), f"Buscando {len(ids)} card(s) no Pipefy…")
    cards = buscar_cards(ids)
    if not cards:
        raise ValueError("Nenhum card encontrado com esses IDs no Pipefy.")

    rel = importar_cards(
        s, cards, _usuario(s, parametros),
        categoria_padrao_id=parametros.get("categoria_padrao_id"),
        obra_padrao_id=parametros.get("obra_padrao_id"),
        criar_fornecedor=bool(parametros.get("criar_fornecedor", True)),
        baixar_anexos=bool(parametros.get("baixar_anexos", True)),
        andamento=andamento)
    s.commit()

    andamento(len(ids), len(ids), "Terminado.")
    return {
        "relatorio": rel,
        "resumo": (f"{len(rel.get('importados', []))} importado(s), "
                   f"{len(rel.get('ja_existiam', []))} já existia(m), "
                   f"{len(rel.get('pendencias', []))} precisa(m) de decisão."),
    }


# ---------------------------------------------------------------------------
# Sincronizar a agenda
# ---------------------------------------------------------------------------
def sincronizar_agenda(s: Session, parametros: dict[str, Any], andamento) -> dict[str, Any]:
    """Recalcula os avisos deduzidos (reajuste, certidão, locação, contrato,
    certificado). Percorre obras, contratos e certificados — cresce com a
    empresa, e a tela da agenda é justamente onde ninguém quer esperar."""
    from app.apps.erp.core.agenda import service as svc

    andamento(0, 1, "Recalculando os avisos…")
    resultado = svc.sincronizar(s, usuario=_usuario(s, parametros))
    s.commit()
    andamento(1, 1, "Terminado.")
    return {"resultado": resultado}


# ---------------------------------------------------------------------------
# Emitir a nota fiscal de serviço
# ---------------------------------------------------------------------------
def emitir_nota(s: Session, parametros: dict[str, Any], andamento) -> dict[str, Any]:
    """Declara a medição à prefeitura e guarda o que ela devolve.

    Está na fila porque entre enviar e a prefeitura responder passam-se de
    segundos a dois minutos — tempo em que o clique seguraria uma das quatro
    linhas de atendimento do serviço, e o sistema inteiro ficaria pesado por
    causa de uma nota.

    ⚠️ NÃO É REPETÍVEL À TOA. Se a fila tentasse de novo sozinha, poderia
    declarar a mesma medição duas vezes na prefeitura. Por isso a emissão
    conta como UMA tentativa: quando falha, o número fica queimado com o
    motivo, e refazer é decisão de gente.
    """
    from app.apps.erp.core.notas_emitidas import automatica

    titulo_id = int(parametros.get("titulo_id") or 0)
    if not titulo_id:
        raise ValueError("Sem o título, não há o que emitir.")
    return automatica.emitir(
        s, titulo_id,
        valor=parametros.get("valor"),
        observacao=str(parametros.get("observacao") or ""),
        usuario=_usuario(s, parametros), andamento=andamento)


def registrar_todos() -> None:
    """Liga os executores à fila. Chamado uma vez, no import das rotas."""
    tarefas.registrar("importar_pipefy", importar_pipefy)
    tarefas.registrar("sincronizar_agenda", sincronizar_agenda)
    tarefas.registrar("emitir_nota", emitir_nota)
    # Emitir duas vezes cria duas notas de verdade na prefeitura.
    tarefas.registrar_sem_repeticao("emitir_nota")
