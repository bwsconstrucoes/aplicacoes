# -*- coding: utf-8 -*-
"""
O PORTAL DA OBRA — o "portal de solicitações" do Pipefy, dentro do ERP.

O dono, em 05/11/2026:

> *"lembrando que a gente tem que ter um portal, né, de acesso. Semelhante ao
> que a gente tem no Pipe (…) porque tem o acesso ao ERP mesmo, mas aí o
> administrativo de obra não vai ter esse mesmo acesso. Ele precisa só entrar e
> por exemplo, preencher alguns dados, fazer uma requisição e encaminhar por
> lá. (…) atualmente a gente já tem um acesso via ponto. (…) Aí como é que fica
> o permissionamento do ERP e esse acesso ao portal e esse acesso ao ponto? Já
> começa a complicar um pouquinho."*

**O PORTAL JÁ EXISTE, E É O APLICATIVO DO PONTO** (`/ponto/app`). Ele tem login
de gente de obra (CPF + PIN), papéis próprios e recorte por obra. Criar um
segundo portal com um terceiro login seria a quarta porta da mesma casa — e
quem usa é a mesma pessoa, no mesmo celular, no mesmo dia.

TRÊS MUNDOS DE ACESSO, e cada um responde uma pergunta diferente:

| Mundo | Quem entra | Como | O que decide |
|---|---|---|---|
| **ERP** | quem opera o escritório | e-mail e senha, sessão | perfil → ações → seções (`core/auth/`) |
| **Portal / app do ponto** | gente de obra | CPF + PIN | papéis do ponto (`ponto/core/papeis.py`) + a obra onde ESTÁ |
| **Aparelho** | o celular da obra | token do dispositivo | bate ponto por todos, sem login |

**O QUE ESTE ARQUIVO FAZ:** a ponte do segundo mundo para dentro do ERP. Ele NÃO
cria login nenhum: lê a sessão que o aplicativo do ponto já abriu, e com ela
descobre quem é a pessoa e quais obras ela alcança.

**POR QUE ISSO É SEGURO:** os dois módulos rodam no MESMO processo e dividem o
mesmo cookie de sessão. Quem não passou pelo CPF+PIN do ponto não tem a chave
na sessão, e o portal responde "entre pelo aplicativo". Nenhuma permissão do
ERP é concedida por aqui: o portal não vê título, não vê salário de ninguém e
não abre tela do ERP — ele faz TRÊS coisas, e só.

**AS TRÊS COISAS:**
  1. **abrir requisição** ao DP (os tipos que a obra pode abrir);
  2. **acompanhar** o que ela mesma abriu, e o que o DP respondeu;
  3. **devolver documento** — a rescisão assinada, por exemplo. *"essa devolução
     não pode ser via WhatsApp. Tem que ser via sistema."*

**O WHATSAPP AVISA E COBRA; O ARQUIVO VOLTA PELO PORTAL.** Foto de documento em
conversa de WhatsApp não é arquivo: é foto perdida num celular, sem tipo, sem
dono e sem prazo.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroValidacao, registrar_evento,
)
from app.apps.erp.db.models.cadastros import Colaborador

logger = logging.getLogger(__name__)

# OS TIPOS QUE A OBRA PODE ABRIR pelo portal. É um subconjunto de propósito:
# desconto em folha, por exemplo, é do DP — a obra não lança desconto no
# salário de ninguém.
TIPOS_DO_PORTAL = (
    "PRE_CADASTRO", "DESLIGAMENTO", "READMISSAO", "ADMISSAO_PRESTADOR",
    "ALTERACAO_CARGO", "ATUALIZACAO_DADOS", "DEPENDENTE", "AJUDA_DE_CUSTO",
    "VALE_TRANSPORTE", "DIVERSOS",
)


class SemAcesso(Exception):
    """Não há sessão do aplicativo do ponto. Não é erro: é "entre primeiro"."""


def id_na_sessao() -> int:
    """Quem está logado no aplicativo do ponto, SEM TOCAR NO BANCO.

    Existe separada de `quem_esta` por um motivo prático: a recusa de quem não
    entrou não pode custar uma conexão de banco. Rota pública que abre conexão
    antes de conferir quem é vira porta de esgotar o pool com requisição
    anônima — e, de quebra, não dava para testar a recusa sem banco nenhum.
    """
    try:
        from flask import session

        from app.apps.ponto import auth as ponto_auth
    except Exception as e:                              # noqa: BLE001
        logger.warning("ERP/portal: o módulo do ponto não carregou — %s", e)
        raise SemAcesso("O aplicativo do ponto está fora do ar.") from e
    cid = session.get(ponto_auth.SESSAO_COLABORADOR)
    if not cid:
        raise SemAcesso("Entre pelo aplicativo do ponto para usar o portal.")
    return int(cid)


def quem_esta(s: Session) -> dict[str, Any]:
    """Quem está usando o portal, pela sessão do aplicativo do ponto.

    Levanta `SemAcesso` quando não há sessão — e a rota responde 401 com a
    frase "entre pelo aplicativo do ponto", nunca um erro técnico.

    A IMPORTAÇÃO É PREGUIÇOSA E PROTEGIDA de propósito: se o módulo do ponto
    quebrar ao carregar, o ERP continua subindo e o portal responde que está
    fora — em vez de derrubar os catorze blueprints junto.
    """
    c = s.get(Colaborador, id_na_sessao())
    if c is None or (c.situacao or "").upper() != "ATIVO":
        raise SemAcesso("Este acesso não está mais ativo.")
    return {"colaborador_id": c.id, "nome": c.nome, "cpf": c.cpf,
            "obra_id": c.obra_id, "pessoa": c}


def obras_que_alcanca(s: Session, pessoa: Colaborador) -> list[int]:
    """As obras que esta pessoa alcança — a MESMA regra do aplicativo do ponto.

    Não reimplemento a regra: chamo a do ponto. Duas regras de alcance que
    divergem é o pior dos dois mundos — a pessoa vê no ponto o que não vê no
    portal, e ninguém entende por quê.

    Se o ponto não responder, sobra a obra do cadastro — que é o mínimo
    honesto, e nunca mais que isso.
    """
    minimo = [pessoa.obra_id] if pessoa.obra_id else []
    try:
        from app.apps.ponto import db as ponto_db
        from app.apps.ponto.core import papeis
    except Exception:                                   # noqa: BLE001
        return minimo
    try:
        with ponto_db.conexao() as conn:
            if not papeis.disponivel(conn):
                return minimo
            # A REGRA DO PONTO PARA QUEM ESTÁ NO COMPUTADOR, que é o caso aqui:
            # o portal abre no navegador e não manda localização, então o
            # alcance é a obra do cadastro MAIS as obras em que a pessoa bateu
            # ponto nos últimos dias. Está escrito assim em `ponto/core/
            # papeis.py` — e a decisão é dele, de 08/10/2026: *"acho que pode
            # afrouxar mais (…) se precisar lançar algo fora do horário,
            # deixa."*
            recentes = papeis.obras_recentes(conn, pessoa.id)
            return sorted({int(o) for o in (recentes or [])} | set(minimo))
    except Exception as e:                              # noqa: BLE001
        logger.warning("ERP/portal: não consegui o alcance do ponto — %s", e)
        return minimo


def e_administrativo_de_obra(s: Session, pessoa: Colaborador) -> bool:
    """Esta pessoa pode pedir POR OUTRAS?

    É a MESMA marcação do aplicativo do ponto (`colaborador_config.
    administrativo_obra`) — não invento uma segunda. Duas marcações para a mesma
    autoridade divergiriam no primeiro mês, e aí a pessoa pede no ponto e não
    pede no portal sem ninguém entender por quê.
    """
    try:
        from app.apps.ponto import db as ponto_db
        from app.apps.ponto.core import cadastros, papeis
        with ponto_db.conexao() as conn:
            if not papeis.disponivel(conn):
                return False
            dela = cadastros.colaborador_por_id(conn, pessoa.id)
            return bool(papeis.e_administrativo(conn, dela))
    except Exception as e:                              # noqa: BLE001
        logger.warning("ERP/portal: não consegui o papel do ponto — %s", e)
        return False


def _pode_pela_pessoa(s: Session, eu: dict[str, Any],
                      colaborador_id: Optional[int]) -> None:
    """Posso abrir pedido POR esta pessoa?

    Por mim mesmo, sempre. Por outro, só se ele estiver numa obra que eu
    alcanço. Fora disso responde "não encontrado", nunca "sem permissão": os
    números são sequenciais, e confirmar a existência de alguém permitiria
    varrer o cadastro de fora.
    """
    if not colaborador_id or int(colaborador_id) == eu["colaborador_id"]:
        return                      # por mim mesmo, sempre
    if not e_administrativo_de_obra(s, eu["pessoa"]):
        # Não é "sem permissão": para quem não é administrativo, a pessoa do
        # outro simplesmente não existe. É a mesma escolha do resto do ERP.
        raise ErroNaoEncontrado("Pessoa não encontrada.")
    outro = s.get(Colaborador, int(colaborador_id))
    if outro is None:
        raise ErroNaoEncontrado("Pessoa não encontrada.")
    alcance = set(obras_que_alcanca(s, eu["pessoa"]))
    if outro.obra_id is None or int(outro.obra_id) not in alcance:
        raise ErroNaoEncontrado("Pessoa não encontrada.")


# ---------------------------------------------------------------------------
# 1. ABRIR REQUISIÇÃO
# ---------------------------------------------------------------------------
def tipos(s: Session) -> list[dict[str, Any]]:
    from app.apps.erp.core import pessoal_solicitacoes as svc_sp
    return [{
        "chave": k, "nome": svc_sp.TIPOS[k]["nome"],
        "explicacao": svc_sp.TIPOS[k]["explicacao"],
        "exige_pessoa": svc_sp.TIPOS[k]["exige_pessoa"],
        "campos": list(svc_sp.TIPOS[k]["campos"]),
    } for k in TIPOS_DO_PORTAL if k in svc_sp.TIPOS]


def listas_fechadas(s: Session | None = None) -> dict[str, list[dict[str, str]]]:
    """As caixinhas do portal, com a chave sendo O NOME DO CAMPO do formulário.

    SEM ISTO O CAMPO VIRA TEXTO LIVRE, e aí a obra digita "acordo", "mandado
    embora" e "pediu pra sair" para a mesma coisa — e o DP passa a traduzir à
    mão o que o sistema já sabe.

    A fonte é o `DA_LISTA` do próprio cadastro, não uma cópia: lista que mudar
    lá muda aqui sozinha. A chave é o nome do campo (`categoria_transporte`),
    nunca o nome da lista (`categoria`) — errar isso faz o campo voltar a ser
    texto livre sem ninguém perceber, porque a tela não reclama.
    """
    from app.apps.erp.core.cadastros import pessoas as svc

    ROTULOS = {
        "sexo": svc.SEXO_ROTULOS, "estado_civil": svc.ESTADO_CIVIL_ROTULOS,
        "escolaridade": svc.ESCOLARIDADE_ROTULOS,
        "vinculo": svc.VINCULO_ROTULOS, "tipo_contrato": svc.TIPO_CONTRATO_ROTULOS,
        "categoria_alimentacao": svc.CATEGORIA_ALIMENTACAO_ROTULOS,
        "categoria_transporte": svc.CATEGORIA_TRANSPORTE_ROTULOS,
        "meio_transporte": svc.MEIO_TRANSPORTE_ROTULOS,
    }
    # FORA DE USO NÃO APARECE para quem está abrindo pedido novo. O cadastro
    # antigo que já tem "cartão" continua lendo — só não se escolhe de novo.
    FORA_DE_USO = {"categoria_transporte": set(svc.CATEGORIAS_TRANSPORTE_FORA_DE_USO)}

    def par(campo, chaves, rotulos):
        fora = FORA_DE_USO.get(campo, set())
        return [{"valor": c, "rotulo": rotulos.get(c, c)}
                for c in chaves if c not in fora]

    listas = {campo: par(campo, chaves, ROTULOS.get(campo, {}))
              for campo, chaves in svc.DA_LISTA.items()}
    # Estes dois são campos do PEDIDO, não do cadastro, então não estão no
    # `DA_LISTA` — mas a lista é a mesma do cadastro.
    listas["forma_desligamento"] = par(
        "forma_desligamento", svc.FORMAS_DESLIGAMENTO,
        svc.FORMA_DESLIGAMENTO_ROTULOS)
    listas["parentesco"] = par("parentesco", svc.PARENTESCOS,
                               svc.PARENTESCO_ROTULOS)
    listas["pix_tipo"] = [{"valor": v, "rotulo": r} for v, r in (
        ("CPF", "CPF"), ("CNPJ", "CNPJ"), ("EMAIL", "E-mail"),
        ("TELEFONE", "Telefone"), ("ALEATORIA", "Chave aleatória"))]

    # Cargo e convenção vêm do banco: são cadastro, mudam sem código.
    if s is not None:
        listas["cargo_id"] = [
            {"valor": str(l["id"]), "rotulo": l["nome"]}
            for l in s.execute(text(
                "SELECT id, nome FROM cargos WHERE ativo IS NOT FALSE"
                " ORDER BY nome")).mappings().all()]
        listas["convencao_id"] = [
            {"valor": str(l["id"]), "rotulo": l["nome"]}
            for l in s.execute(text(
                "SELECT id, nome FROM convencoes WHERE ativo IS NOT FALSE"
                " ORDER BY nome")).mappings().all()]
    return listas


def abrir(s: Session, dados: dict[str, Any]) -> dict[str, Any]:
    """Abre a requisição em nome de quem está no portal.

    Reusa `pessoal_solicitacoes.abrir` — a MESMA função da tela do ERP, com as
    mesmas críticas. Fila com duas portas e duas regras seria fila com dois
    comportamentos, e o DP descobriria a diferença no pior dia.
    """
    from app.apps.erp.core import pessoal_solicitacoes as svc_sp

    eu = quem_esta(s)
    tipo = str(dados.get("tipo") or "").strip().upper()
    if tipo not in TIPOS_DO_PORTAL:
        raise ErroValidacao(
            "Este tipo de pedido não é aberto pela obra — fale com o DP.")
    _pode_pela_pessoa(s, eu, dados.get("colaborador_id"))

    r = svc_sp.abrir(s, {**dados, "tipo": tipo,
                         "requerente": eu["nome"]}, usuario=None)
    # QUEM ABRIU É COLABORADOR, não usuário do ERP (migração 091). Sem esta
    # coluna, o pedido do portal ficaria sem autor — e autor é o que permite a
    # pessoa acompanhar o que ela mesma abriu.
    s.execute(text("""UPDATE dp_solicitacoes
                         SET solicitante_colaborador_id = :c
                       WHERE id = :s"""),
              {"c": eu["colaborador_id"], "s": r["id"]})
    registrar_evento(s, "colaborador", eu["colaborador_id"],
                     "PEDIDO_PELO_PORTAL",
                     {"numero": r.get("numero"), "tipo": tipo}, None)
    s.flush()
    return {**r, "aberto_por": eu["nome"]}


# ---------------------------------------------------------------------------
# 2. ACOMPANHAR
# ---------------------------------------------------------------------------
def meus_pedidos(s: Session) -> list[dict[str, Any]]:
    """O que EU abri, e em que pé está. Sem salário, sem dado de ninguém."""
    eu = quem_esta(s)
    linhas = s.execute(text("""
        SELECT sol.id, sol.numero, sol.tipo, sol.situacao, sol.criado_em,
               sol.resposta, c.nome AS pessoa
          FROM dp_solicitacoes sol
          LEFT JOIN colaboradores c ON c.id = sol.colaborador_id
         WHERE sol.solicitante_colaborador_id = :eu
         ORDER BY sol.id DESC LIMIT 100"""),
        {"eu": eu["colaborador_id"]}).mappings().all()
    from app.apps.erp.core import pessoal_solicitacoes as svc_sp
    return [{
        "numero": l["numero"],
        "tipo": svc_sp.TIPOS.get(l["tipo"], {}).get("nome", l["tipo"]),
        # O RÓTULO EM PORTUGUÊS, NÃO O CÓDIGO. Quem olha está na obra, no
        # celular: "EM_ANALISE" na tela é defeito, não detalhe. E o rótulo é o
        # MESMO que o DP vê na tela dele — a mesma palavra nos dois lados.
        "situacao": svc_sp.SITUACAO_ROTULOS.get(l["situacao"], l["situacao"]),
        "pessoa": l["pessoa"] or "",
        "quando": str(l["criado_em"]), "resposta": l["resposta"] or "",
    } for l in linhas]


# ---------------------------------------------------------------------------
# 3. A DEVOLUÇÃO DE DOCUMENTO — o fluxo que ele desenhou em voz alta
#
# > *"digamos que eu precise que a obra me mande um determinado documento.
# > Imagina aí, a rescisão chegou, contabilidade fez tal, a gente encaminhou
# > para a obra, para a obra coletar a assinatura das pessoas (…) eu preciso que
# > aquela documentação seja devolvida. E essa devolução não pode ser via
# > WhatsApp. Tem que ser via sistema."*
#
# Então: o DP abre a pendência, o WhatsApp AVISA e COBRA, e o arquivo volta por
# aqui — com tipo, com dono e com prazo.
# ---------------------------------------------------------------------------
def minhas_devolucoes(s: Session) -> list[dict[str, Any]]:
    """O que a obra tem de devolver. Aparece para quem alcança a obra."""
    eu = quem_esta(s)
    obras = obras_que_alcanca(s, eu["pessoa"])
    if not obras:
        return []
    linhas = s.execute(text("""
        SELECT d.id, d.numero, d.o_que, d.documento_esperado, d.prazo,
               d.situacao, d.cobrancas, d.documento_enviado_id,
               c.nome AS pessoa, o.codigo AS obra,
               t.nome AS documento_nome
          FROM dp_devolucoes d
          LEFT JOIN colaboradores c ON c.id = d.colaborador_id
          LEFT JOIN obras o ON o.id = d.obra_id
          LEFT JOIN documento_tipos t ON t.codigo = d.documento_esperado
         WHERE d.obra_id = ANY(:obras) AND d.situacao IN ('ABERTA', 'COBRADA')
         ORDER BY (d.prazo IS NULL), d.prazo, d.id"""),
        {"obras": obras}).mappings().all()
    hoje = date.today()
    return [{
        "id": l["id"], "numero": l["numero"], "o_que": l["o_que"],
        "pessoa": l["pessoa"] or "", "obra": l["obra"] or "",
        "documento": l["documento_nome"] or l["documento_esperado"],
        "prazo": str(l["prazo"]) if l["prazo"] else None,
        "atrasada": bool(l["prazo"] and l["prazo"] < hoje),
        "cobrancas": l["cobrancas"],
        "tem_anexo_para_baixar": bool(l["documento_enviado_id"]),
    } for l in linhas]


def devolver(s: Session, devolucao_id: int, conteudo: bytes,
             nome_arquivo: str) -> dict[str, Any]:
    """A obra devolve o documento assinado. Arquiva no Drive e fecha a pendência.

    O TIPO vem da pendência, não do nome do arquivo: "foto.jpg" não diz nada, e
    quem abriu a pendência sabia exatamente o que estava esperando. É isso que
    faz o documento ser ACHÁVEL depois.
    """
    from app.apps.erp.core.arquivo import service as svc_arq

    eu = quem_esta(s)
    linha = s.execute(text("""
        SELECT id, numero, obra_id, colaborador_id, documento_esperado, situacao
          FROM dp_devolucoes WHERE id = :d"""),
        {"d": devolucao_id}).mappings().first()
    if linha is None:
        raise ErroNaoEncontrado("Pendência não encontrada.")
    if linha["obra_id"] not in set(obras_que_alcanca(s, eu["pessoa"])):
        raise ErroNaoEncontrado("Pendência não encontrada.")
    if linha["situacao"] not in ("ABERTA", "COBRADA"):
        raise ErroValidacao(
            f"A pendência {linha['numero']} já está "
            f"{linha['situacao'].lower()}.")
    if not conteudo:
        raise ErroValidacao("Escolha o arquivo do documento.")

    d = svc_arq.arquivar(
        s, conteudo, nome_arquivo or "documento",
        tipo_codigo=linha["documento_esperado"],
        colaborador_id=linha["colaborador_id"],
        referencia=f"{linha['numero']} — devolvido pela obra",
        origem="PORTAL",
        observacao=f"devolvido por {eu['nome']} pelo portal",
        usuario=None)
    s.execute(text("""
        UPDATE dp_devolucoes
           SET situacao = 'DEVOLVIDA', documento_id = :doc, devolvido_em = now(),
               devolvido_por_colaborador = :quem
         WHERE id = :d"""),
        {"doc": d.id, "quem": eu["colaborador_id"], "d": devolucao_id})
    registrar_evento(s, "colaborador", linha["colaborador_id"] or 0,
                     "DOCUMENTO_DEVOLVIDO_PELO_PORTAL",
                     {"devolucao": linha["numero"], "por": eu["nome"],
                      "documento_id": d.id}, None)
    s.flush()
    return {"devolvida": True, "numero": linha["numero"], "documento_id": d.id}


# ---------------------------------------------------------------------------
# O LADO DO DP: abrir a pendência e cobrar
# ---------------------------------------------------------------------------
def proximo_numero_devolucao(s: Session) -> str:
    n = s.execute(text("SELECT nextval('dp_devolucao_numero_seq')")).scalar()
    return f"DV-{int(n):05d}"


def abrir_devolucao(s: Session, dados: dict[str, Any],
                    usuario: Optional[Any] = None) -> dict[str, Any]:
    """O DP manda algo para a obra fazer e esperar de volta.

    Nasce com PRAZO sempre: pendência sem prazo não é cobrável, e pendência que
    ninguém cobra é a mesma coisa que não existir.
    """
    colaborador_id = int(dados["colaborador_id"]) if dados.get("colaborador_id") else None
    c = s.get(Colaborador, colaborador_id) if colaborador_id else None
    if c is None:
        raise ErroValidacao("Escolha a pessoa de quem é o documento.")
    obra_id = int(dados["obra_id"]) if dados.get("obra_id") else c.obra_id
    if not obra_id:
        raise ErroValidacao(
            "Sem obra não há para quem mandar — informe a obra que vai devolver.")
    o_que = " ".join(str(dados.get("o_que") or "").split())
    if len(o_que) < 5:
        raise ErroValidacao("Escreva o que a obra tem de fazer e devolver.")
    tipo = str(dados.get("documento_esperado") or "").strip().upper()
    if not tipo:
        raise ErroValidacao(
            "Diga qual documento tem de voltar — é o que faz a devolução ser "
            "conferível e achável depois.")
    dias = int(dados.get("dias") or 5)
    numero = proximo_numero_devolucao(s)
    linha = s.execute(text("""
        INSERT INTO dp_devolucoes (numero, pedido_id, colaborador_id, obra_id,
                                   o_que, documento_enviado_id,
                                   documento_esperado, prazo, aberto_por)
        VALUES (:n, :ped, :c, :o, :q, :env, :tipo, :prazo, :u)
        RETURNING id"""),
        {"n": numero,
         "ped": int(dados["pedido_id"]) if dados.get("pedido_id") else None,
         "c": c.id, "o": obra_id, "q": o_que,
         "env": dados.get("documento_enviado_id"), "tipo": tipo,
         "prazo": date.today() + timedelta(days=dias),
         "u": usuario.id if usuario else None}).one()
    registrar_evento(s, "colaborador", c.id, "DEVOLUCAO_PEDIDA",
                     {"numero": numero, "o_que": o_que, "obra_id": obra_id},
                     usuario.id if usuario else None)
    s.flush()
    return {"id": linha.id, "numero": numero}


def devolucoes_abertas(s: Session, *, atrasadas: bool = False) -> list[dict[str, Any]]:
    onde = ["d.situacao IN ('ABERTA', 'COBRADA')"]
    if atrasadas:
        onde.append("d.prazo < CURRENT_DATE")
    linhas = s.execute(text(f"""
        SELECT d.id, d.numero, d.o_que, d.prazo, d.situacao, d.cobrancas,
               d.documento_esperado, c.nome AS pessoa, c.telefone,
               o.codigo AS obra, o.id AS obra_id
          FROM dp_devolucoes d
          LEFT JOIN colaboradores c ON c.id = d.colaborador_id
          LEFT JOIN obras o ON o.id = d.obra_id
         WHERE {' AND '.join(onde)}
         ORDER BY (d.prazo IS NULL), d.prazo, d.id""")).mappings().all()
    hoje = date.today()
    return [{**{k: (str(v) if k == "prazo" and v is not None else v)
                for k, v in dict(l).items()},
             "atrasada": bool(l["prazo"] and l["prazo"] < hoje),
             "dias_atraso": ((hoje - l["prazo"]).days
                             if l["prazo"] and l["prazo"] < hoje else 0)}
            for l in linhas]


def cobrar_devolucao(s: Session, devolucao_id: int,
                     usuario: Optional[Any] = None, *,
                     pelo_agente: bool = False) -> dict[str, Any]:
    """Cobra a obra por WhatsApp. O ARQUIVO volta pelo portal, não pela conversa.

    Decisão dele sobre os canais (05/11/2026): *"a interação entre os
    requisitores de informação vai ser feita via WhatsApp, que é o pessoal
    administrativo de obra"*, mas *"essa devolução não pode ser via WhatsApp.
    Tem que ser via sistema."* Então a mensagem AVISA e manda o endereço do
    portal; ela nunca pede foto.
    """
    linha = s.execute(text("""
        SELECT d.id, d.numero, d.o_que, d.prazo, d.cobrancas, d.situacao,
               c.nome AS pessoa, c.telefone AS telefone_pessoa, o.codigo AS obra
          FROM dp_devolucoes d
          LEFT JOIN colaboradores c ON c.id = d.colaborador_id
          LEFT JOIN obras o ON o.id = d.obra_id
         WHERE d.id = :d"""), {"d": devolucao_id}).mappings().first()
    if linha is None:
        raise ErroNaoEncontrado("Pendência não encontrada.")
    if linha["situacao"] not in ("ABERTA", "COBRADA"):
        raise ErroValidacao(f"A pendência {linha['numero']} já está "
                            f"{linha['situacao'].lower()}.")
    telefones = _telefones_da_obra(s, devolucao_id)
    if not telefones:
        raise ErroValidacao(
            "Não achei telefone de ninguém da obra para avisar. Cadastre o "
            "telefone do administrativo de obra.")
    texto_ = (
        f"*{linha['numero']} — documento a devolver*\n\n"
        f"{linha['o_que']}\n\n"
        f"Pessoa: {linha['pessoa'] or '—'}\n"
        f"Obra: {linha['obra'] or '—'}\n"
        f"Prazo: {linha['prazo'].strftime('%d/%m/%Y') if linha['prazo'] else '—'}\n\n"
        "Envie o documento assinado pelo *portal* (aplicativo do ponto › "
        "Pedidos ao DP › A devolver). Não mande por aqui: no portal ele fica "
        "arquivado no cadastro da pessoa, com tipo e data.")
    enviados = 0
    for tel in telefones:
        try:
            from app.apps.notificador import notificar
            r = notificar(telefone=tel, mensagem=texto_,
                          finalidade="erp.dp_devolucao")
            enviados += 1 if any((v or {}).get("ok") for v in r.values()) else 0
        except Exception as e:                          # noqa: BLE001
            logger.warning("ERP/portal: não avisei %s — %s", tel, e)
    s.execute(text("""UPDATE dp_devolucoes
                         SET situacao = 'COBRADA', cobrancas = cobrancas + 1,
                             cobrado_em = now() WHERE id = :d"""),
              {"d": devolucao_id})
    s.flush()
    return {"avisados": enviados, "telefones": len(telefones)}


def _telefones_da_obra(s: Session, devolucao_id: int) -> list[str]:
    """Quem avisar na obra: o administrativo de obra dela, pela marcação do
    ponto. Sem ninguém marcado, a própria pessoa do documento."""
    linha = s.execute(text("""
        SELECT d.obra_id, c.telefone
          FROM dp_devolucoes d LEFT JOIN colaboradores c ON c.id = d.colaborador_id
         WHERE d.id = :d"""), {"d": devolucao_id}).mappings().first()
    if linha is None:
        return []
    alvos: list[str] = []
    try:
        from app.apps.ponto import db as ponto_db
        with ponto_db.conexao() as conn:
            for l in conn.execute(text("""
                SELECT col.telefone FROM colaboradores col
                  JOIN ponto.colaborador_config cfg
                    ON cfg.colaborador_id = col.id
                 WHERE cfg.administrativo_obra = TRUE
                   AND col.obra_id = :o AND col.situacao = 'ATIVO'
                   AND col.telefone IS NOT NULL"""),
                {"o": linha["obra_id"]}).mappings().all():
                alvos.append(l["telefone"])
    except Exception as e:                              # noqa: BLE001
        logger.warning("ERP/portal: não achei o administrativo da obra — %s", e)
    if not alvos and linha["telefone"]:
        alvos.append(linha["telefone"])
    return [t for t in dict.fromkeys(alvos) if t]


def pessoas_ao_alcance(s: Session) -> list[dict[str, Any]]:
    """Por quem esta pessoa pode abrir pedido.

    Para o administrativo de obra, a gente das obras que ele alcança. Para
    qualquer outro, só ele mesmo — e isso não é restrição por desconfiança: é
    que pedir pelo colega sem responder pela obra dele não existe no processo.
    """
    eu = quem_esta(s)
    if not e_administrativo_de_obra(s, eu["pessoa"]):
        return [{"id": eu["colaborador_id"], "nome": eu["nome"]}]
    obras = obras_que_alcanca(s, eu["pessoa"])
    if not obras:
        return [{"id": eu["colaborador_id"], "nome": eu["nome"]}]
    linhas = s.execute(text("""
        SELECT id, nome FROM colaboradores
         WHERE obra_id = ANY(:obras) AND situacao = 'ATIVO'
         ORDER BY nome LIMIT 500"""), {"obras": obras}).mappings().all()
    return [{"id": l["id"], "nome": l["nome"]} for l in linhas]


def obras_para_escolher(s: Session) -> list[dict[str, Any]]:
    """As obras que aparecem na caixinha do pedido — só as que ela alcança."""
    eu = quem_esta(s)
    obras = obras_que_alcanca(s, eu["pessoa"])
    if not obras:
        return []
    linhas = s.execute(text("""
        SELECT id, codigo, nome FROM obras WHERE id = ANY(:obras)
         ORDER BY codigo"""), {"obras": obras}).mappings().all()
    return [{"id": l["id"], "codigo": l["codigo"], "nome": l["nome"]} for l in linhas]


# ---------------------------------------------------------------------------
# UM CADASTRO SÓ: as marcações do PONTO dentro da ficha do ERP
#
# O dono, em 06/11/2026: *"tem cadastro da pessoa, tem cadastro no ponto. Tem a
# tela de visualizar pessoas num lado, no outro. Não deveria ser assim, né? A
# gente tem que falar a mesma linguagem nos dois lados, afinal é a mesma
# empresa, é o mesmo ERP."*
#
# Então a ficha do ERP passa a LER e ESCREVER as marcações do ponto — chamando
# as funções do próprio ponto, nunca a tabela dele por fora. Se a regra do
# ponto mudar, muda num lugar só; e se o módulo do ponto não estiver no ar, a
# ficha diz isso em vez de mostrar marcação errada.
# ---------------------------------------------------------------------------
def marcacoes_do_ponto(s: Session, colaborador_id: int) -> dict[str, Any]:
    """O que o PONTO sabe desta pessoa, para a ficha mostrar junto.

    Nunca levanta: ponto fora do ar devolve `disponivel: False` e a ficha mostra
    o aviso. Travar a ficha da pessoa porque o ponto não respondeu seria trocar
    um problema pequeno por um grande.
    """
    try:
        from app.apps.ponto import db as ponto_db
        from app.apps.ponto.core import acesso, cadastros, papeis
    except Exception as e:                              # noqa: BLE001
        return {"disponivel": False, "motivo": f"o módulo do ponto não carregou: {e}"}
    try:
        with ponto_db.conexao() as conn:
            if not papeis.disponivel(conn):
                return {"disponivel": False,
                        "motivo": "as atualizações do ponto ainda não foram aplicadas"}
            pessoa = cadastros.colaborador_por_id(conn, colaborador_id)
            if not pessoa:
                return {"disponivel": False,
                        "motivo": "esta pessoa ainda não aparece no ponto"}
            saida = {
                "disponivel": True,
                "bate_ponto": bool(pessoa.get("ativo_no_ponto")),
                "entra_no_app": bool(papeis.entra_no_app(conn, pessoa)),
                "administrativo_obra": bool(papeis.e_administrativo(conn, pessoa)),
                "pede_no_celular": bool(papeis.pede_pelo_proprio_celular(conn, pessoa)),
                "responsavel_pelo_ponto": bool(
                    papeis.responsavel_por_ponto_da_obra(conn, pessoa)),
            }
            try:
                d = acesso.diagnostico(conn, colaborador_id)
                saida["acesso"] = {
                    "telefone": d.get("telefone", ""),
                    "origem_do_telefone": d.get("origem", ""),
                    "tem_pin": bool(d.get("tem_pin")),
                    "impede": list(d.get("impede") or []),
                }
            except Exception:                           # noqa: BLE001
                saida["acesso"] = None
            return saida
    except Exception as e:                              # noqa: BLE001
        logger.warning("ERP/ficha: não li as marcações do ponto — %s", e)
        return {"disponivel": False, "motivo": f"o ponto não respondeu: {e}"}


def definir_marcacoes_do_ponto(s: Session, colaborador_id: int,
                               dados: dict[str, Any]) -> dict[str, Any]:
    """Grava as marcações do ponto a partir da ficha do ERP.

    Chama `papeis.definir` do próprio ponto — a regra, a validação e a tabela
    são dele. O ERP só passa o que a pessoa marcou na tela.
    """
    from app.apps.erp.core.comum.auditoria import registrar_evento

    try:
        from app.apps.ponto import db as ponto_db
        from app.apps.ponto.core import papeis
    except Exception as e:                              # noqa: BLE001
        raise ErroValidacao(f"O módulo do ponto não está no ar: {e}") from e

    def tri(chave: str):
        """None quando a tela não mandou o campo — é o que faz `definir` manter
        o valor que já estava, em vez de zerar o que ninguém mexeu."""
        if chave not in dados:
            return None
        return bool(dados[chave])

    with ponto_db.conexao() as conn:
        try:
            papeis.definir(conn, colaborador_id,
                           pede_no_celular=tri("pede_no_celular"),
                           administrativo_obra=tri("administrativo_obra"))
        except Exception as e:                          # noqa: BLE001
            raise ErroValidacao(str(e)) from e
    registrar_evento(s, "colaborador", colaborador_id, "MARCACOES_DO_PONTO",
                     {k: dados.get(k) for k in
                      ("pede_no_celular", "administrativo_obra")}, None)
    s.flush()
    return marcacoes_do_ponto(s, colaborador_id)
