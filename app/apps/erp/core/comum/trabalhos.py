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
from datetime import datetime, timezone
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


# ---------------------------------------------------------------------------
# Deixar o acervo antigo legível
# ---------------------------------------------------------------------------
# Documento arquivado antes de 12/09/2026 pode não ter o texto de dentro
# guardado: até então só a leitura por IA o trazia, e só das seis primeiras
# páginas. Sem o texto, o documento não aparece na busca por palavra e não dá
# para perguntar sobre ele.
#
# Está na fila, e não num clique, por dois motivos: o acervo tem milhares de
# documentos, e cada um precisa ter os bytes buscados (banco ou Google Drive)
# antes de extrair. Não gasta IA nenhuma — é a camada de texto do próprio PDF.
LOTE_DE_TEXTOS = 200


def texto_dos_documentos(s: Session, parametros: dict[str, Any], andamento) -> dict[str, Any]:
    """Extrai e guarda o texto dos documentos que ainda não têm."""
    from sqlalchemy import func, or_, select

    from app.apps.erp.core.arquivo import texto as svc_texto
    from app.apps.erp.db.models.financeiro import Documento

    limite = int(parametros.get("limite") or LOTE_DE_TEXTOS)
    alvos = s.scalars(
        select(Documento)
        .where(or_(Documento.texto.is_(None), Documento.texto == ""))
        .order_by(Documento.id.desc())
        .limit(max(1, limite))).all()

    lidos = vazios = 0
    for i, d in enumerate(alvos, start=1):
        andamento(i, len(alvos), f"Lendo {d.nome_padronizado}")
        try:
            if svc_texto.garantir_texto(s, d):
                lidos += 1
            else:
                vazios += 1
        except Exception as e:
            # Um documento ilegível não pode parar o lote inteiro: o valor
            # está nos outros mil que vão ficar legíveis.
            vazios += 1
            logger.warning("ERP/trabalhos: documento %s não deu texto (%s)",
                           d.id, e)
        if i % 25 == 0:
            s.commit()
    s.commit()

    faltam = s.scalar(
        select(func.count()).select_from(Documento)
        .where(or_(Documento.texto.is_(None), Documento.texto == ""))) or 0
    return {
        "lidos": lidos, "sem_texto": vazios, "faltam": int(faltam),
        "recado": (
            f"{lidos} documento(s) ficaram legíveis para perguntas. "
            f"{vazios} não têm texto por dentro (são imagem ou digitalização). "
            + (f"Ainda faltam {faltam} — rode de novo." if faltam else
               "Não falta nenhum.")),
    }


# ---------------------------------------------------------------------------
# Equalizar o cadastro de fornecedores pela Receita
# ---------------------------------------------------------------------------
# Pedido do dono em 18/09/2026: *"faz a pesquisa desses CNPJs todos e já
# readequa com a nomenclatura certa"*.
#
# É TAREFA DE FUNDO por aritmética, não por preferência: a consulta pública
# aceita 3 CNPJs por minuto, e são ~1.700 fornecedores. Um botão que trava a
# tela por dez horas não é um botão. Aqui ela roda em lote, guarda onde parou e
# pode ser rodada de novo amanhã de onde ficou.
#
# E ela NUNCA sobrescreve calada: preenche o que está vazio e RELATA o que
# discorda. Trocar a razão social de um fornecedor é decisão de gente olhando
# os dois nomes lado a lado.
LOTE_DA_RECEITA = 120


def equalizar_fornecedores(s: Session, parametros: dict[str, Any],
                           andamento) -> dict[str, Any]:
    """Consulta a Receita para cada fornecedor e ajeita o que dá."""
    import re
    import time

    from sqlalchemy import select

    from app.apps.erp.core.cadastros import receita
    from app.apps.erp.db.models.cadastros import Fornecedor

    limite = int(parametros.get("limite") or LOTE_DA_RECEITA)
    so_pendentes = parametros.get("todos") is not True

    alvos = []
    for f in s.scalars(select(Fornecedor).order_by(Fornecedor.id)).all():
        if not f.ativo:
            continue
        # CPF não tem cadastro de CNPJ para consultar. Pular aqui é o que
        # impede o lote de gastar as 3 consultas por minuto com quem não tem
        # resposta possível.
        if len(re.sub(r"\D", "", f.cnpj_cpf or "")) != 14:
            continue
        if so_pendentes and (f.situacao_rfb or "").strip():
            continue                 # já consultado antes
        alvos.append(f)
        if len(alvos) >= limite:
            break

    preenchidos = divergentes = baixadas = nao_achados = 0
    fora_do_ar = ""
    divergencias: list[dict[str, Any]] = []
    for i, f in enumerate(alvos, start=1):
        andamento(i, len(alvos), f"Consultando {f.razao_social[:40]}")
        try:
            dados = receita.consultar(f.cnpj_cpf)
        except receita.ReceitaIndisponivel as erro:
            # Rede fora não é erro de dado: para o lote e diz onde parou, para
            # a próxima rodada continuar daqui.
            fora_do_ar = str(erro)
            logger.warning("ERP/trabalhos: Receita indisponível (%s)", erro)
            break
        if dados is None:
            nao_achados += 1
            f.situacao_rfb = "NAO ENCONTRADO"
            f.situacao_rfb_em = datetime.now(timezone.utc)
            continue

        r = receita.comparar(f, dados)
        for campo, valor in r["preencher"].items():
            setattr(f, campo, valor)
        if r["preencher"]:
            preenchidos += 1
        if r["diverge"]:
            divergentes += 1
            divergencias.append({"fornecedor_id": f.id, "cnpj": f.cnpj_cpf,
                                 "no_erp": f.razao_social, **r["diverge"]})
        f.situacao_rfb = r["situacao"] or "ATIVA"
        f.situacao_rfb_em = datetime.now(timezone.utc)
        if r["baixada"]:
            baixadas += 1
        if i % 10 == 0:
            s.commit()
        if i < len(alvos):
            time.sleep(receita.ESPERA_ENTRE_CONSULTAS)
    s.commit()

    faltam = sum(1 for f in s.scalars(select(Fornecedor)).all()
                 if f.ativo and not (f.situacao_rfb or "").strip())
    return {
        "consultados": len(alvos), "preenchidos": preenchidos,
        "divergentes": divergentes, "baixadas": baixadas,
        "nao_encontrados": nao_achados, "faltam": faltam,
        # Só as 50 primeiras: a lista serve para a pessoa começar a conferir,
        # não para virar um relatório que ninguém lê até o fim.
        "divergencias": divergencias[:50],
        "recado": (
            (f"A consulta parou: {fora_do_ar}. " if fora_do_ar else "")
            + f"{len(alvos)} fornecedor(es) consultados. "
            + f"{preenchidos} tiveram endereço ou CNAE preenchidos. "
            + (f"{divergentes} têm o nome DIFERENTE do da Receita — confira e "
               f"decida, o sistema não troca sozinho. " if divergentes else "")
            + (f"{baixadas} não estão ATIVAS na Receita. " if baixadas else "")
            + (f"Ainda faltam {faltam} — rode de novo." if faltam
               else "Não falta nenhum.")),
    }


def registrar_todos() -> None:
    """Liga os executores à fila. Chamado uma vez, no import das rotas."""
    tarefas.registrar("importar_pipefy", importar_pipefy)
    tarefas.registrar("sincronizar_agenda", sincronizar_agenda)
    tarefas.registrar("emitir_nota", emitir_nota)
    tarefas.registrar("texto_dos_documentos", texto_dos_documentos)
    tarefas.registrar("equalizar_fornecedores", equalizar_fornecedores)
    # Emitir duas vezes cria duas notas de verdade na prefeitura.
    tarefas.registrar_sem_repeticao("emitir_nota")
