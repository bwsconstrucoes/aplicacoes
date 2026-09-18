# ============================================================================
# ERP — core/acompanhamento/processos.py
# A gestão burocrática da obra: aditivo, apostilamento, licença, protocolo.
#
# PEDIDO DO DONO, 17/09/2026, aprovado em 18/09:
#
#   "Uma espécie de gerenciamento da parte documental, burocrática, de
#   acompanhamento da obra — não da execução (…) acompanhar o andamento,
#   alimentar do andamento: 'ó, eu liguei aqui e falei que fulano está em tal
#   setor' (…) me lembrando um pouco do sistema SEI, MAS EU NÃO QUERO UMA
#   COISA TRAVADA (…) imagina que uma pessoa saiu de férias: quem for fazer
#   esse acompanhamento precisaria bater o olho e ver o que está pendente."
#
# O desenho inteiro está em `app/apps/erp/ACOMPANHAMENTO.md`. Três coisas
# mandam neste arquivo, e é por elas que ele é curto:
#
# 1. LANÇAR ANDAMENTO É UMA FRASE. Nada obrigatório além do texto. Se custar
#    mais, ninguém lança, e acompanhamento desatualizado é pior que nenhum.
# 2. NADA TRAVA NADA. Não há transição proibida entre situações — o órgão não
#    segue ordem, e obrigar ordem faria a pessoa mentir para o sistema.
# 3. "PARADO" É CONCLUSÃO, NÃO CAMPO. Sai dos dias desde o último andamento.
#    Campo de situação que depende de alguém lembrar de mexer estará errado.
# ============================================================================
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import (
    Empresa, Obra, Processo, ProcessoAndamento, SituacaoProcesso, Usuario,
)

# Os tipos de assunto, com o nome que a BWS usa e quantos dias sem andamento
# fazem aquilo virar "parado".
#
# O prazo é POR TIPO porque a realidade é por tipo: licença ambiental dorme
# semanas sem que isso signifique nada, e aditivo de prazo parado dez dias já
# é problema. Um prazo único acenderia a luz nos dois lugares errados.
TIPOS: dict[str, dict[str, Any]] = {
    "ADITIVO_PRAZO":   {"rotulo": "Aditivo de prazo", "parado_em": 10},
    "ADITIVO_VALOR":   {"rotulo": "Aditivo de valor", "parado_em": 10},
    "APOSTILAMENTO":   {"rotulo": "Apostilamento (reajuste)", "parado_em": 15},
    "LICENCA":         {"rotulo": "Licença ou autorização", "parado_em": 30},
    "CERTIDAO":        {"rotulo": "Certidão", "parado_em": 15},
    "CNO":             {"rotulo": "Matrícula CNO", "parado_em": 20},
    "ART":             {"rotulo": "ART / RRT", "parado_em": 15},
    "SEGURO":          {"rotulo": "Seguro-garantia", "parado_em": 15},
    "PROTOCOLO":       {"rotulo": "Protocolo de medição", "parado_em": 15},
    "OUTRO":           {"rotulo": "Outro assunto", "parado_em": 20},
}

SITUACOES: dict[str, str] = {
    "RASCUNHO":    "Em preparo",
    "PROTOCOLADO": "Protocolado",
    "EM_ANALISE":  "Em análise",
    "EXIGENCIA":   "Exigência a responder",
    "DEFERIDO":    "Deferido / publicado",
    "INDEFERIDO":  "Indeferido",
    "ARQUIVADO":   "Arquivado",
}

# Situações em que o processo deixou de ser trabalho: não entram na lista de
# pendências, não ficam "parados" e não cobram previsão.
ENCERRADAS = {"DEFERIDO", "INDEFERIDO", "ARQUIVADO"}


def _hoje() -> date:
    return date.today()


def _texto(valor: Any, campo: str, *, obrigatorio: bool = False,
           maximo: int = 400) -> Optional[str]:
    t = (valor or "").strip() if isinstance(valor, str) else ""
    if not t:
        if obrigatorio:
            raise ErroValidacao(f"{campo} é obrigatório.")
        return None
    return t[:maximo]


def _data(valor: Any, campo: str) -> Optional[date]:
    if valor in (None, "", "null"):
        return None
    if isinstance(valor, date):
        return valor
    try:
        return date.fromisoformat(str(valor)[:10])
    except ValueError:
        raise ErroValidacao(f"{campo} não é uma data válida.")


def proximo_numero(s: Session) -> str:
    """AC-000042 — curto o bastante para citar ao telefone e em ofício."""
    numeros = []
    for p in s.scalars(select(Processo)).all():
        bruto = getattr(p, "numero", "") or ""
        sufixo = bruto.split("-", 1)[1] if bruto.startswith("AC-") else ""
        if sufixo.isdigit():
            numeros.append(int(sufixo))
    return f"AC-{(max(numeros) + 1) if numeros else 1:06d}"


# ---------------------------------------------------------------------------
# Abrir
# ---------------------------------------------------------------------------
def criar(s: Session, dados: dict[str, Any], usuario: Optional[Usuario]) -> Processo:
    """Abre o processo. Obrigatórios: assunto, tipo e UM dono (obra ou empresa).

    Nada mais. Órgão, responsável, prazo e protocolo entram conforme a coisa
    anda — exigir tudo na abertura faz a pessoa deixar para abrir depois, e
    depois é nunca.
    """
    assunto = _texto(dados.get("assunto"), "O assunto", obrigatorio=True)
    tipo = (dados.get("tipo") or "OUTRO").strip().upper()
    if tipo not in TIPOS:
        raise ErroValidacao(f"Tipo de processo desconhecido: {tipo}.")

    obra_id = dados.get("obra_id") or None
    empresa_id = dados.get("empresa_id") or None
    if bool(obra_id) == bool(empresa_id):
        raise ErroValidacao("O processo é de UMA obra ou da empresa — escolha um "
                            "dos dois.")
    if obra_id and s.get(Obra, int(obra_id)) is None:
        raise ErroValidacao("Obra não encontrada.")
    if empresa_id and s.get(Empresa, int(empresa_id)) is None:
        raise ErroValidacao("Empresa não encontrada.")

    responsavel_id = dados.get("responsavel_id") or (usuario.id if usuario else None)
    processo = Processo(
        numero=proximo_numero(s),
        assunto=assunto,
        tipo=tipo,
        obra_id=int(obra_id) if obra_id else None,
        empresa_id=int(empresa_id) if empresa_id else None,
        orgao=_texto(dados.get("orgao"), "O órgão"),
        responsavel_id=int(responsavel_id) if responsavel_id else None,
        situacao="RASCUNHO",
        onde_esta=_texto(dados.get("onde_esta"), "Onde está"),
        protocolo=_texto(dados.get("protocolo"), "O protocolo", maximo=80),
        protocolado_em=_data(dados.get("protocolado_em"), "A data do protocolo"),
        previsao=_data(dados.get("previsao"), "A previsão"),
        documento_id=dados.get("documento_id") or None,
        criado_por=usuario.id if usuario else None)
    s.add(processo)
    s.flush()
    registrar_evento(s, "processo", processo.id, "PROCESSO_ABERTO", {
        "numero": processo.numero, "assunto": assunto, "tipo": tipo,
        "obra_id": processo.obra_id, "empresa_id": processo.empresa_id},
        usuario.id if usuario else None)
    return processo


# ---------------------------------------------------------------------------
# O andamento — o coração do módulo
# ---------------------------------------------------------------------------
def lancar_andamento(s: Session, processo_id: int, dados: dict[str, Any],
                     usuario: Optional[Usuario]) -> ProcessoAndamento:
    """Uma frase, e pronto. Data e autor entram sozinhos.

    Os três opcionais (onde está, previsão, situação) mudam o processo na
    MESMA frase — a pessoa que ligou para o órgão descobre as três coisas de
    uma vez, e abrir outro formulário para cada uma é o jeito de nenhuma ser
    registrada.
    """
    processo = s.get(Processo, processo_id)
    if processo is None:
        raise ErroValidacao("Processo não encontrado.")

    texto = _texto(dados.get("texto"), "O andamento", obrigatorio=True, maximo=2000)
    onde = _texto(dados.get("onde_esta"), "Onde está")
    previsao = _data(dados.get("previsao"), "A previsão")
    situacao = (dados.get("situacao") or "").strip().upper() or None
    if situacao and situacao not in SITUACOES:
        raise ErroValidacao(f"Situação desconhecida: {situacao}.")

    andamento = ProcessoAndamento(
        processo_id=processo.id, texto=texto, onde_esta=onde,
        previsao=previsao, situacao=situacao,
        anexo_id=dados.get("anexo_id") or None,
        por_id=usuario.id if usuario else None)
    s.add(andamento)

    if onde:
        processo.onde_esta = onde
    if previsao:
        processo.previsao = previsao
    if situacao:
        _mudar_situacao(processo, situacao)
    processo.atualizado_em = datetime.now(timezone.utc)
    s.flush()
    registrar_evento(s, "processo", processo.id, "PROCESSO_ANDAMENTO", {
        "numero": processo.numero, "texto": texto[:200],
        "onde_esta": onde, "situacao": situacao,
        "previsao": previsao.isoformat() if previsao else None},
        usuario.id if usuario else None)
    return andamento


def _mudar_situacao(processo: Processo, situacao: str) -> None:
    """Sem transição proibida: o órgão não segue ordem, e obrigar ordem faria a
    pessoa mentir para o sistema. Encerrar carimba a data; reabrir a apaga."""
    processo.situacao = situacao
    if situacao in ENCERRADAS:
        processo.encerrado_em = processo.encerrado_em or datetime.now(timezone.utc)
    else:
        processo.encerrado_em = None


def atualizar(s: Session, processo_id: int, dados: dict[str, Any],
              usuario: Optional[Usuario]) -> Processo:
    """Os campos do cabeçalho: assunto, órgão, responsável, protocolo, previsão."""
    processo = s.get(Processo, processo_id)
    if processo is None:
        raise ErroValidacao("Processo não encontrado.")

    mudou: dict[str, Any] = {}
    if "assunto" in dados:
        processo.assunto = _texto(dados["assunto"], "O assunto", obrigatorio=True)
        mudou["assunto"] = processo.assunto
    if "tipo" in dados and dados["tipo"]:
        tipo = str(dados["tipo"]).strip().upper()
        if tipo not in TIPOS:
            raise ErroValidacao(f"Tipo de processo desconhecido: {tipo}.")
        processo.tipo = tipo
        mudou["tipo"] = tipo
    for campo, rotulo in (("orgao", "O órgão"), ("onde_esta", "Onde está")):
        if campo in dados:
            setattr(processo, campo, _texto(dados[campo], rotulo))
            mudou[campo] = getattr(processo, campo)
    if "protocolo" in dados:
        processo.protocolo = _texto(dados["protocolo"], "O protocolo", maximo=80)
        mudou["protocolo"] = processo.protocolo
    for campo, rotulo in (("protocolado_em", "A data do protocolo"),
                          ("previsao", "A previsão")):
        if campo in dados:
            setattr(processo, campo, _data(dados[campo], rotulo))
            mudou[campo] = str(getattr(processo, campo) or "")
    if "responsavel_id" in dados:
        novo = dados["responsavel_id"] or None
        if novo and s.get(Usuario, int(novo)) is None:
            raise ErroValidacao("Operador não encontrado.")
        processo.responsavel_id = int(novo) if novo else None
        mudou["responsavel_id"] = processo.responsavel_id
    if "situacao" in dados and dados["situacao"]:
        situacao = str(dados["situacao"]).strip().upper()
        if situacao not in SITUACOES:
            raise ErroValidacao(f"Situação desconhecida: {situacao}.")
        _mudar_situacao(processo, situacao)
        mudou["situacao"] = situacao

    processo.atualizado_em = datetime.now(timezone.utc)
    s.flush()
    if mudou:
        registrar_evento(s, "processo", processo.id, "PROCESSO_ALTERADO",
                         {"numero": processo.numero, **mudou},
                         usuario.id if usuario else None)
    return processo


def assumir(s: Session, processo_ids: list[int], novo_responsavel_id: int,
            usuario: Optional[Usuario]) -> int:
    """Passa vários processos para uma pessoa de uma vez.

    É o botão do caso das férias: quem entra no lugar de quem saiu não deveria
    abrir um por um. A troca fica registrada em cada processo.
    """
    if s.get(Usuario, int(novo_responsavel_id)) is None:
        raise ErroValidacao("Operador não encontrado.")
    trocados = 0
    for pid in processo_ids or []:
        processo = s.get(Processo, int(pid))
        if processo is None:
            continue
        antes = processo.responsavel_id
        if antes == int(novo_responsavel_id):
            continue
        processo.responsavel_id = int(novo_responsavel_id)
        processo.atualizado_em = datetime.now(timezone.utc)
        registrar_evento(s, "processo", processo.id, "PROCESSO_ASSUMIDO",
                         {"numero": processo.numero, "de": antes,
                          "para": int(novo_responsavel_id)},
                         usuario.id if usuario else None)
        trocados += 1
    s.flush()
    return trocados


# ---------------------------------------------------------------------------
# "O que está pendente" — a tela que resolve o caso das férias
# ---------------------------------------------------------------------------
# A ordem NÃO é por data de criação: é por quem está mais perto de virar
# problema. Quem assume o assunto de outro precisa que a primeira linha da tela
# seja a que mais dói, e não a mais antiga.
URGENCIAS = [
    ("EXIGENCIA",  "exigência a responder", 0),
    ("ATRASADO",   "passou da previsão",    1),
    ("PARADO",     "parado",                2),
    ("PROXIMO",    "previsão chegando",     3),
    ("EM_DIA",     "em dia",                4),
]
URGENCIA_ORDEM = {chave: ordem for chave, _, ordem in URGENCIAS}
URGENCIA_ROTULO = {chave: rotulo for chave, rotulo, _ in URGENCIAS}

# Quantos dias antes da previsão a linha já acende. Uma semana é o que dá para
# ligar para o órgão antes da data e ainda ter o que fazer.
DIAS_DE_AVISO = 7


def dias_parado(processo: Processo, ultimo: Optional[date], hoje: date) -> int:
    """Dias desde o último andamento — ou desde a abertura, se não houve nenhum.

    Processo aberto e nunca tocado é o caso que mais interessa, então a
    abertura conta como a última movimentação.
    """
    partida = ultimo or (processo.criado_em.date()
                         if getattr(processo, "criado_em", None) else hoje)
    return max(0, (hoje - partida).days)


def urgencia(processo: Processo, ultimo_andamento: Optional[date],
             hoje: Optional[date] = None) -> dict[str, Any]:
    """Como esta linha entra na fila, e por quê — dito em português.

    O "por quê" viaja junto com a chave de propósito: a tela mostra o motivo, e
    urgência sem motivo obriga quem lê a abrir o processo para descobrir, que é
    exatamente o trabalho que esta tela existe para evitar.
    """
    hoje = hoje or _hoje()
    parado = dias_parado(processo, ultimo_andamento, hoje)
    teto = TIPOS.get(processo.tipo, TIPOS["OUTRO"])["parado_em"]

    if processo.situacao in ENCERRADAS:
        return {"chave": "EM_DIA", "rotulo": SITUACOES.get(processo.situacao, ""),
                "motivo": "", "dias_parado": parado, "ordem": 9}
    if processo.situacao == "EXIGENCIA":
        return {"chave": "EXIGENCIA", "rotulo": URGENCIA_ROTULO["EXIGENCIA"],
                "motivo": "o órgão pediu alguma coisa e ainda não foi respondido",
                "dias_parado": parado, "ordem": URGENCIA_ORDEM["EXIGENCIA"]}
    if processo.previsao and processo.previsao < hoje:
        atraso = (hoje - processo.previsao).days
        return {"chave": "ATRASADO", "rotulo": URGENCIA_ROTULO["ATRASADO"],
                "motivo": f"prometeram para {processo.previsao.strftime('%d/%m')} "
                          f"e já faz {atraso} dia(s)",
                "dias_parado": parado, "ordem": URGENCIA_ORDEM["ATRASADO"]}
    if parado >= teto:
        return {"chave": "PARADO", "rotulo": URGENCIA_ROTULO["PARADO"],
                "motivo": f"{parado} dias sem andamento "
                          f"(para {TIPOS.get(processo.tipo, TIPOS['OUTRO'])['rotulo'].lower()} "
                          f"o normal é até {teto})",
                "dias_parado": parado, "ordem": URGENCIA_ORDEM["PARADO"]}
    if processo.previsao and (processo.previsao - hoje).days <= DIAS_DE_AVISO:
        faltam = (processo.previsao - hoje).days
        return {"chave": "PROXIMO", "rotulo": URGENCIA_ROTULO["PROXIMO"],
                "motivo": f"previsto para {processo.previsao.strftime('%d/%m')} "
                          f"({faltam} dia(s))",
                "dias_parado": parado, "ordem": URGENCIA_ORDEM["PROXIMO"]}
    return {"chave": "EM_DIA", "rotulo": URGENCIA_ROTULO["EM_DIA"], "motivo": "",
            "dias_parado": parado, "ordem": URGENCIA_ORDEM["EM_DIA"]}


# ---------------------------------------------------------------------------
# A lista
# ---------------------------------------------------------------------------
def _ultimo_andamento_por_processo(s: Session, ids: list[int]) -> dict[int, date]:
    """Quando cada processo andou pela última vez.

    Uma consulta para todos, e não uma por linha: a tela mostra dezenas de
    processos, e uma ida ao banco por linha é o que faz uma tela boa ficar
    lenta o bastante para ninguém abrir.
    """
    if not ids:
        return {}
    ultimos: dict[int, date] = {}
    for a in s.scalars(select(ProcessoAndamento)).all():
        if a.processo_id not in ids or a.em is None:
            continue
        quando = a.em.date() if hasattr(a.em, "date") else a.em
        if a.processo_id not in ultimos or quando > ultimos[a.processo_id]:
            ultimos[a.processo_id] = quando
    return ultimos


def listar(s: Session, *, obras_permitidas: Optional[list[int]] = None,
           obra_id: Optional[int] = None, responsavel_id: Optional[int] = None,
           tipo: Optional[str] = None, incluir_encerrados: bool = False,
           hoje: Optional[date] = None) -> dict[str, Any]:
    """Os processos, ordenados por quem está mais perto de virar problema.

    `obras_permitidas=None` significa "esta pessoa enxerga tudo". Lista vazia
    significa "nenhuma obra designada" — e aí ela não vê nenhum processo, que é
    o padrão NEGAR do ERP, não efeito colateral.

    Processo da EMPRESA (certidão, alvará da sede) não é de obra nenhuma: só
    aparece para quem enxerga a base inteira.
    """
    hoje = hoje or _hoje()
    todos = list(s.scalars(select(Processo)).all())

    if obras_permitidas is not None:
        alcance = set(obras_permitidas)
        todos = [p for p in todos if p.obra_id is not None and p.obra_id in alcance]
    if obra_id:
        todos = [p for p in todos if p.obra_id == int(obra_id)]
    if responsavel_id:
        todos = [p for p in todos if p.responsavel_id == int(responsavel_id)]
    if tipo:
        todos = [p for p in todos if p.tipo == tipo.strip().upper()]
    if not incluir_encerrados:
        todos = [p for p in todos if p.situacao not in ENCERRADAS]

    ultimos = _ultimo_andamento_por_processo(s, [p.id for p in todos])
    linhas = []
    for p in todos:
        u = urgencia(p, ultimos.get(p.id), hoje)
        linhas.append({
            "id": p.id, "numero": p.numero, "assunto": p.assunto,
            "tipo": p.tipo,
            "tipo_rotulo": TIPOS.get(p.tipo, TIPOS["OUTRO"])["rotulo"],
            "obra_id": p.obra_id, "empresa_id": p.empresa_id,
            "orgao": p.orgao, "onde_esta": p.onde_esta,
            "protocolo": p.protocolo,
            "responsavel_id": p.responsavel_id,
            "situacao": p.situacao,
            "situacao_rotulo": SITUACOES.get(p.situacao, p.situacao),
            "previsao": p.previsao.isoformat() if p.previsao else None,
            "urgencia": u["chave"], "urgencia_rotulo": u["rotulo"],
            "motivo": u["motivo"], "dias_parado": u["dias_parado"]})

    linhas.sort(key=lambda l: (URGENCIA_ORDEM.get(l["urgencia"], 9),
                               -l["dias_parado"], l["numero"]))
    resumo = {chave: sum(1 for l in linhas if l["urgencia"] == chave)
              for chave, _, _ in URGENCIAS}
    return {"processos": linhas, "resumo": resumo,
            "pendentes": sum(v for k, v in resumo.items() if k != "EM_DIA")}


def detalhe(s: Session, processo_id: int) -> dict[str, Any]:
    """O processo e o histórico dele, do mais novo para o mais antigo."""
    p = s.get(Processo, processo_id)
    if p is None:
        raise ErroValidacao("Processo não encontrado.")
    # Desempata pelo id: dois andamentos lançados no mesmo segundo (acontece
    # quando alguém cola duas frases seguidas) sairiam em ordem aleatória, e um
    # histórico fora de ordem é pior que um histórico curto.
    andamentos = sorted(
        [a for a in s.scalars(select(ProcessoAndamento)).all()
         if a.processo_id == p.id],
        key=lambda a: (a.em or datetime.now(timezone.utc), a.id or 0),
        reverse=True)
    ultimo = andamentos[0].em.date() if andamentos and andamentos[0].em else None
    u = urgencia(p, ultimo)
    return {
        "id": p.id, "numero": p.numero, "assunto": p.assunto, "tipo": p.tipo,
        "tipo_rotulo": TIPOS.get(p.tipo, TIPOS["OUTRO"])["rotulo"],
        "obra_id": p.obra_id, "empresa_id": p.empresa_id, "orgao": p.orgao,
        "responsavel_id": p.responsavel_id, "situacao": p.situacao,
        "situacao_rotulo": SITUACOES.get(p.situacao, p.situacao),
        "onde_esta": p.onde_esta, "protocolo": p.protocolo,
        "protocolado_em": p.protocolado_em.isoformat() if p.protocolado_em else None,
        "previsao": p.previsao.isoformat() if p.previsao else None,
        "documento_id": p.documento_id,
        "urgencia": u["chave"], "urgencia_rotulo": u["rotulo"],
        "motivo": u["motivo"], "dias_parado": u["dias_parado"],
        "andamentos": [{
            "id": a.id, "texto": a.texto, "onde_esta": a.onde_esta,
            "previsao": a.previsao.isoformat() if a.previsao else None,
            "situacao": a.situacao,
            "situacao_rotulo": SITUACOES.get(a.situacao or "", ""),
            "anexo_id": a.anexo_id, "por_id": a.por_id,
            "em": a.em.strftime("%d/%m/%Y %H:%M") if a.em else ""}
            for a in andamentos]}
