# -*- coding: utf-8 -*-
"""
O AGENTE DE DP — as sete rotinas, cada uma com liga/desliga e parâmetros.

**A metodologia inteira está em `AGENTE_DP.md`, nesta pasta. Ler lá primeiro:**
este arquivo é a execução dela, não a explicação.

O resumo do desenho, para quem abre o arquivo:

> *"A gente precisa poder ativar e desativar isso aí, que a gente não vai
> começar com ele ativo. A gente precisa definir esses parâmetros: o que é que
> analisa, o que é que não analisa, o que é esperado da contabilidade, qual é o
> tipo de documento."* (dono, 05/11/2026)

**TRÊS MODOS, e o do meio é o que torna isto seguro:**

  · `DESLIGADA` — não roda. É como TODAS nascem.
  · `SUGERE`    — analisa e escreve o que FARIA, para alguém aprovar com um
                  clique. É o modo de ganhar confiança: liga, olha uma semana,
                  e só então promove.
  · `FAZ`       — executa.

**O QUE O AGENTE NUNCA FAZ** (a lista inteira está na §5 do `AGENTE_DP.md`, e
está no código porque regra que só vive em documento não é regra):

  1. não desliga, não admite, não muda salário nem cargo por conta própria;
  2. não manda e-mail para fora sem um pedido registrado antes, com autor;
  3. não conclui procedimento com divergência;
  4. não sobrescreve o que uma pessoa digitou — preenche o vazio;
  5. não inventa: o que ele não leu com certeza fica em branco e marcado;
  6. não age fora do que está ligado;
  7. tudo o que ele faz fica na trilha com a marca `pelo_agente`.

**QUATRO DAS SETE ROTINAS NÃO USAM IA NENHUMA.** Triagem, despacho, cobrança e
vigia são regra de negócio escrita. Isso é de propósito: o agente começa útil e
burro, e fica inteligente depois — em vez de nascer inteligente e errado.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.apps.erp.core import contabilidade as svc_ct
from app.apps.erp.core.cadastros import pessoas as svc_pessoas
from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import Colaborador, Usuario

logger = logging.getLogger(__name__)

MODOS = ("DESLIGADA", "SUGERE", "FAZ")

# ---------------------------------------------------------------------------
# AS SETE ROTINAS, com o que cada uma analisa e o que NÃO analisa.
#
# O "nao_analisa" não é enfeite de documentação: é o limite escrito, e é o que
# impede a próxima pessoa (ou o próximo chat) de ampliar o agente para dentro de
# decisão que é de gente.
# ---------------------------------------------------------------------------
ROTINAS: dict[str, dict[str, Any]] = {
    "triagem": {
        "nome": "Triagem da requisição que chega da obra",
        "quando": "quando uma solicitação é aberta na fila do DP",
        "analisa": "campos obrigatórios do tipo, CPF válido, pessoa existe, e "
                   "coerência (desligar quem já saiu, férias de quem tem menos "
                   "de 12 meses, salário abaixo do piso, obra que não é a de "
                   "quem pediu)",
        "nao_analisa": "mérito. Se vale a pena contratar, se o salário é justo, "
                       "se o desligamento é certo — isso é decisão de gente",
        "usa_ia": False,
        "padrao": {"meses_para_ferias": 12},
    },
    "leitura": {
        "nome": "Leitura dos documentos para preencher o cadastro",
        "quando": "quando um documento é anexado ao cadastro de alguém",
        "analisa": "RG, CNH, CPF, CTPS e comprovante de endereço, extraindo os "
                   "campos que estão dentro deles",
        "nao_analisa": "cargo, obra, empresa, vínculo, tipo de contrato e "
                       "salário — nenhum está em documento, e são justamente "
                       "os campos que o formulário precisa ter",
        "usa_ia": True,
        "padrao": {"so_preenche_vazio": True},
    },
    "despacho": {
        "nome": "Despachante da contabilidade",
        "quando": "quando o cadastro entra na etapa Na contabilidade",
        "analisa": "se o pedido tem tudo o que a contabilidade precisa para "
                   "fazer o ato",
        "nao_analisa": "se o ato deve ser feito — quem decide admitir, demitir "
                       "ou alterar é gente",
        "usa_ia": False,
        "padrao": {},
    },
    "cobranca": {
        "nome": "Cobrador de prazo",
        "quando": "todo dia, uma vez",
        "analisa": "cada pedido aberto contra o prazo do template",
        "nao_analisa": "pedido que já teve retorno e espera conferência — a "
                       "bola está com a gente, e cobrar quem já respondeu "
                       "queima a relação",
        "usa_ia": False,
        "padrao": {"nao_cobrar_fim_de_semana": True},
    },
    "caixa": {
        "nome": "Leitura da caixa de entrada",
        "quando": "a cada rodada do agente",
        "analisa": "as mensagens não lidas da caixa: casa cada uma com o "
                   "pedido que a originou (pelo In-Reply-To, pelo número no "
                   "assunto ou pelo CPF), baixa os anexos e arquiva no Drive "
                   "com o tipo que o pedido esperava",
        "nao_analisa": "o CONTEÚDO do documento — isso é a rotina de "
                       "conferência. E não apaga mensagem, não responde "
                       "sozinho e não arquiva anexo em pedido que não casou "
                       "com certeza: anexar a rescisão no pedido de férias é "
                       "pior que não anexar",
        "usa_ia": False,
        "padrao": {"teto_por_rodada": 40},
    },
    "conferencia": {
        "nome": "Conferente do retorno da contabilidade",
        "quando": "quando um documento é anexado a um pedido",
        "analisa": "se o documento é o esperado, e se nome, CPF, cargo, salário "
                   "e datas batem com o cadastro",
        "nao_analisa": "nada além dos campos do template — e documento que não "
                       "dá para ler fica para conferência à mão, nunca chutado",
        "usa_ia": True,
        "padrao": {"concluir_sozinho": False},
    },
    "vigia": {
        "nome": "Vigia das pendências",
        "quando": "todo dia",
        "analisa": "cadastro parado além do prazo, papel que falta para "
                   "admitir, ASO e NR vencendo, admissão sem ficha de volta, "
                   "desligamento sem documentação rescisória, ativo sem obra",
        "nao_analisa": "não cobra ninguém — quem cobra é o cobrador, e só o "
                       "que tem pedido aberto",
        "usa_ia": False,
        "padrao": {"dias_parado": 3},
    },
    "relatorio": {
        "nome": "Relator",
        "quando": "uma vez por dia para o DP, uma por semana para o dono",
        "analisa": "o que entrou, o que saiu, o que está atrasado e com quem "
                   "está a bola",
        "nao_analisa": "nada, porque é só consulta: ele não muda registro "
                       "nenhum nem fala com ninguém de fora",
        "usa_ia": False,
        "padrao": {},
    },
}


# ---------------------------------------------------------------------------
# Liga, desliga e parâmetros
# ---------------------------------------------------------------------------
def configuracao(s: Session) -> list[dict[str, Any]]:
    """O estado de cada rotina, do jeito que a tela precisa mostrar."""
    linhas = {r["chave"]: r for r in s.execute(text(
        "SELECT chave, modo, parametros, rodou_em FROM dp_agente_rotinas"
    )).mappings().all()}
    saida = []
    for chave, meta in ROTINAS.items():
        linha = linhas.get(chave) or {}
        params = dict(meta["padrao"])
        params.update(linha.get("parametros") or {})
        saida.append({
            "chave": chave, "nome": meta["nome"], "quando": meta["quando"],
            "analisa": meta["analisa"], "nao_analisa": meta["nao_analisa"],
            "usa_ia": meta["usa_ia"],
            "modo": (linha.get("modo") or "DESLIGADA"),
            "parametros": params,
            "rodou_em": str(linha["rodou_em"]) if linha.get("rodou_em") else None,
        })
    return saida


def modo(s: Session, chave: str) -> str:
    v = s.execute(text("SELECT modo FROM dp_agente_rotinas WHERE chave = :c"),
                  {"c": chave}).scalar()
    return (v or "DESLIGADA").upper()


def parametros(s: Session, chave: str) -> dict[str, Any]:
    base = dict(ROTINAS.get(chave, {}).get("padrao") or {})
    bruto = s.execute(text("SELECT parametros FROM dp_agente_rotinas "
                           "WHERE chave = :c"), {"c": chave}).scalar()
    base.update(bruto or {})
    return base


def definir(s: Session, chave: str, *, modo_novo: str,
            params: Optional[dict[str, Any]] = None,
            usuario: Optional[Usuario] = None) -> dict[str, Any]:
    if chave not in ROTINAS:
        raise ErroValidacao("Rotina desconhecida.")
    m = (modo_novo or "").strip().upper()
    if m not in MODOS:
        raise ErroValidacao("Modo deve ser DESLIGADA, SUGERE ou FAZ.")
    atual = parametros(s, chave)
    atual.update(params or {})
    s.execute(text("""
        INSERT INTO dp_agente_rotinas (chave, modo, parametros, alterado_por,
                                       alterado_em)
        VALUES (:c, :m, CAST(:p AS jsonb), :u, now())
        ON CONFLICT (chave) DO UPDATE
           SET modo = :m, parametros = CAST(:p AS jsonb),
               alterado_por = :u, alterado_em = now()"""),
        {"c": chave, "m": m, "p": json.dumps(atual, ensure_ascii=False),
         "u": usuario.id if usuario else None})
    registrar_evento(s, "agente_dp", 0, "ROTINA_ALTERADA",
                     {"rotina": chave, "modo": m, "parametros": atual},
                     usuario.id if usuario else None)
    s.flush()
    return {"chave": chave, "modo": m, "parametros": atual}


# ---------------------------------------------------------------------------
# AS SUGESTÕES — o modo SUGERE
# ---------------------------------------------------------------------------
def sugerir(s: Session, *, rotina: str, alvo_tipo: str, alvo_id: int,
            acao: str, explicacao: str,
            dados: Optional[dict[str, Any]] = None) -> Optional[int]:
    """Escreve o que o agente FARIA. Devolve None quando já existe igual aberta.

    O índice único parcial do banco garante que a rotina diária não vire um
    monte de linhas iguais — mas quem chama não precisa saber disso, e por isso
    o conflito aqui é silencioso.
    """
    linha = s.execute(text("""
        INSERT INTO dp_agente_sugestoes (rotina, alvo_tipo, alvo_id, acao,
                                         explicacao, dados)
        VALUES (:r, :t, :i, :a, :e, CAST(:d AS jsonb))
        ON CONFLICT DO NOTHING
        RETURNING id"""),
        {"r": rotina, "t": alvo_tipo, "i": alvo_id, "a": acao,
         "e": explicacao,
         "d": json.dumps(dados or {}, ensure_ascii=False, default=str)}
    ).first()
    s.flush()
    return linha.id if linha else None


def sugestoes(s: Session, *, rotina: str = "",
              situacao: str = "ABERTA") -> list[dict[str, Any]]:
    onde = ["g.situacao = :sit"]
    par: dict[str, Any] = {"sit": situacao.upper()}
    if rotina:
        onde.append("g.rotina = :r")
        par["r"] = rotina
    linhas = s.execute(text(f"""
        SELECT g.*, u.nome AS decidido_nome
          FROM dp_agente_sugestoes g
          LEFT JOIN usuarios u ON u.id = g.decidido_por
         WHERE {' AND '.join(onde)}
         ORDER BY g.criado_em DESC LIMIT 300"""), par).mappings().all()
    return [{**{k: (str(v) if k in ("criado_em", "decidido_em") and v is not None
                    else v) for k, v in dict(l).items()},
             "rotina_nome": ROTINAS.get(l["rotina"], {}).get("nome", l["rotina"])}
            for l in linhas]


def aplicar_sugestao(s: Session, sugestao_id: int,
                     usuario: Optional[Usuario]) -> dict[str, Any]:
    """Alguém aprovou: faz o que a sugestão dizia.

    O que ela faz depende da ação, e a lista é FECHADA de propósito — ação que
    não está aqui não é aplicável, e aprovar não pode virar "executa qualquer
    coisa que estava escrita no banco".
    """
    g = s.execute(text("SELECT * FROM dp_agente_sugestoes WHERE id = :id"),
                  {"id": sugestao_id}).mappings().first()
    if g is None:
        from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado
        raise ErroNaoEncontrado("Sugestão não encontrada.")
    if g["situacao"] != "ABERTA":
        raise ErroValidacao(f"Esta sugestão já está {g['situacao'].lower()}.")
    acao, dados = g["acao"], (g["dados"] or {})
    feito: dict[str, Any] = {}

    if acao == "cobrar_pedido":
        feito = svc_ct.cobrar(s, int(g["alvo_id"]), usuario, pelo_agente=True)
    elif acao == "cobrar_devolucao":
        from app.apps.erp.core import portal as svc_portal
        feito = svc_portal.cobrar_devolucao(s, int(g["alvo_id"]), usuario,
                                            pelo_agente=True)
    elif acao == "abrir_pedido":
        feito = svc_ct.abrir(s, {"template": dados.get("template"),
                                 "colaborador_id": g["alvo_id"],
                                 "dados": dados.get("dados") or {}},
                             usuario, pelo_agente=True)
    elif acao == "preencher_cadastro":
        from app.apps.erp.core.arquivo import preenchimento
        feito = preenchimento.aplicar_no_colaborador(
            s, int(g["alvo_id"]), dados.get("campos") or {},
            tipo_codigo=dados.get("tipo_codigo") or "",
            documento_id=dados.get("documento_id"), usuario=usuario)
    elif acao == "concluir_pedido":
        feito = svc_ct.concluir(s, int(g["alvo_id"]), usuario,
                                motivo="retorno conferido pelo agente",
                                pelo_agente=True)
    else:
        raise ErroValidacao(
            f"A ação “{acao}” é só um aviso — não há o que aplicar por aqui.")

    s.execute(text("""UPDATE dp_agente_sugestoes
                         SET situacao = 'APLICADA', decidido_por = :u,
                             decidido_em = now() WHERE id = :id"""),
              {"u": usuario.id if usuario else None, "id": sugestao_id})
    s.flush()
    return {"aplicada": True, "resultado": feito}


def recusar_sugestao(s: Session, sugestao_id: int, usuario: Optional[Usuario],
                     *, motivo: str = "") -> dict[str, Any]:
    s.execute(text("""UPDATE dp_agente_sugestoes
                         SET situacao = 'RECUSADA', decidido_por = :u,
                             decidido_em = now(), motivo = :m
                       WHERE id = :id AND situacao = 'ABERTA'"""),
              {"u": usuario.id if usuario else None,
               "m": (motivo or "").strip() or None, "id": sugestao_id})
    s.flush()
    return {"recusada": True}


# ===========================================================================
# R1 · TRIAGEM — determinística, sem IA
# ===========================================================================
def triagem(s: Session, solicitacao_id: int) -> dict[str, Any]:
    """Analisa uma solicitação que chegou da obra. NÃO MUDA NADA.

    Devolve `{"falta": [...], "incoerencias": [...], "resumo": "..."}`. A
    diferença entre as duas listas importa: `falta` é dado que não veio e
    volta para quem pediu; `incoerencia` é pedido impossível ou suspeito, e
    quem decide é o DP.
    """
    from app.apps.erp.core import pessoal_solicitacoes as svc_sp

    p = svc_sp.ler(s, solicitacao_id)
    regra = svc_sp.TIPOS.get(p["tipo"], {})
    par = parametros(s, "triagem")
    dados = p.get("dados") or {}
    falta, incoerencias = [], []

    # 1. Os campos obrigatórios DAQUELE tipo.
    for campo in regra.get("campos", ()):
        if campo in ("observacao", "observacoes", "descricao", "motivo"):
            continue            # texto livre nunca é obrigatório
        if dados.get(campo) in (None, ""):
            falta.append(campo.replace("_", " "))

    c = s.get(Colaborador, p["colaborador_id"]) if p.get("colaborador_id") else None

    # 2. A coerência. Cada item aqui é um pedido que o DP ia descobrir
    #    impossível só ao abrir — e aí volta para a obra por WhatsApp, e o
    #    pedido anda um dia por pergunta.
    if p["tipo"] == "DESLIGAMENTO" and c is not None:
        if (c.situacao or "").upper() == "DESLIGADO" or c.demissao:
            incoerencias.append(
                f"{c.nome} já está desligado"
                + (f" desde {c.demissao:%d/%m/%Y}" if c.demissao else "")
                + " — este pedido não tem o que desligar")
    if p["tipo"] == "READMISSAO" and c is not None:
        if (c.situacao or "").upper() == "ATIVO" and not c.demissao:
            incoerencias.append(f"{c.nome} está ATIVO — não há o que readmitir")
    if p["tipo"] == "ALTERACAO_CARGO" and dados.get("cargo_id"):
        from app.apps.erp.db.models.cadastros import Cargo
        if s.get(Cargo, int(dados["cargo_id"])) is None:
            incoerencias.append("o cargo pedido não existe no cadastro")
    # O CAMPO DO SALÁRIO SE CHAMA `salario` na fila do DP (não `salario_novo`) —
    # e isto custou um teste vermelho: a peneira de `abrir` descarta o que não
    # está na lista do tipo, então um nome errado aqui olharia um campo que
    # nunca existe e a conferência passaria a vida calada.
    novo_salario = dados.get("salario") or dados.get("salario_novo")
    if c is not None and novo_salario:
        abaixo = _abaixo_do_piso(s, c, novo_salario)
        if abaixo:
            incoerencias.append(abaixo)
    # FÉRIAS NÃO É TIPO DA FILA DO DP: ela entra pelo ponto (afastamento) e, do
    # lado formal, pelo pedido à contabilidade. A conferência do período
    # aquisitivo vive lá, em `contabilidade.avisos_do_pedido` — aqui ela seria
    # código morto, e código morto mente para quem lê.

    resumo = ("nada a apontar" if not falta and not incoerencias else
              "; ".join(([f"falta: {', '.join(falta)}"] if falta else [])
                        + incoerencias))
    return {"falta": falta, "incoerencias": incoerencias, "resumo": resumo,
            "tipo": p["tipo"], "numero": p.get("numero", "")}


def _abaixo_do_piso(s: Session, c: Colaborador, salario: Any) -> str:
    """O salário pedido fura o piso da convenção da pessoa?

    É a conferência que mais vale a pena automatizar: ninguém sabe de cabeça o
    piso de 62 cargos em 13 convenções, e pagar abaixo do piso é passivo.
    """
    from app.apps.erp.core.cadastros import cargos as svc_cargos
    if not (c.cargo_id and c.convencao_id):
        return ""
    try:
        pedido = svc_pessoas._dec(salario, "salário")
    except ErroValidacao:
        return ""
    if pedido is None:
        return ""
    cc = svc_cargos.cruzamento(s, c.cargo_id, c.convencao_id, criar=False)
    piso = svc_cargos.piso_em(s, cc.id, date.today()) if cc else None
    if piso and pedido < piso:
        return (f"o salário pedido (R$ {pedido}) está ABAIXO do piso da "
                f"convenção (R$ {piso}) para este cargo")
    return ""


# ===========================================================================
# R8 · CAIXA DE ENTRADA — o conserto do ponto cego
#
# O dono: *"Que agente é esse que depende de uma intervenção humana? Aí não é
# agente. (…) o ERP precisa estar fazendo a leitura das caixas de entrada,
# baixando anexo de forma completamente autônoma para saber o que fazer."*
#
# A mecânica está em `core/comum/email_entrada.py`. Aqui fica só o liga/desliga
# e a diferença entre SUGERE e FAZ:
#   · SUGERE → lê a caixa SEM GRAVAR e mostra o que faria, mensagem por
#              mensagem. Nenhum pedido é tocado, nenhum anexo é arquivado, e a
#              mensagem continua não lida no servidor;
#   · FAZ    → arquiva, entrega o retorno ao pedido e marca como lida.
# ===========================================================================
def rodar_caixa(s: Session, usuario: Optional[Usuario] = None) -> dict[str, Any]:
    from app.apps.erp.core.comum import email_entrada

    m = modo(s, "caixa")
    if m == "DESLIGADA":
        return {"modo": m, "olhou": 0}
    try:
        r = email_entrada.rodada(s, usuario, gravar=(m == "FAZ"))
    except (ErroValidacao, email_entrada.ErroDeLeitura) as e:
        # Caixa fora do ar NÃO derruba a rodada do agente: a cobrança e o vigia
        # continuam valendo, e o motivo fica escrito para a tela mostrar.
        logger.warning("ERP/agente: a caixa de entrada não respondeu — %s", e)
        return {"modo": m, "olhou": 0, "erro": str(e)}
    _carimbar(s, "caixa")
    # MENSAGEM SEM DONO VIRA SUGESTÃO, para alguém resolver à mão: "não achei de
    # quem é" é informação; silêncio não é.
    for mens in r.get("mensagens", []):
        if mens["situacao"] == "SEM_DONO":
            sugerir(s, rotina="caixa", alvo_tipo="email",
                    alvo_id=abs(hash(mens["assunto"])) % 2_000_000_000,
                    acao="email_sem_dono",
                    explicacao=(f"chegou um e-mail de {mens['de']} "
                                f"(“{mens['assunto']}”) e eu não achei a que "
                                f"pedido ele responde: {mens['motivo']}"),
                    dados={"anexos": mens["anexos"]})
    return {"modo": m, **r}


# ===========================================================================
# R4 · COBRANÇA — a rotina diária, determinística
# ===========================================================================
def rodar_cobranca(s: Session, usuario: Optional[Usuario] = None,
                   *, hoje: Optional[date] = None) -> dict[str, Any]:
    """Varre os pedidos vencidos e cobra (ou sugere a cobrança).

    NÃO COBRA NO FIM DE SEMANA quando o parâmetro manda — cobrar no sábado é
    ruído para quem só lê na segunda, e queima a cobrança da segunda.
    """
    m = modo(s, "cobranca")
    if m == "DESLIGADA":
        return {"modo": m, "olhou": 0, "cobrou": 0, "sugeriu": 0}
    par = parametros(s, "cobranca")
    dia = hoje or date.today()
    if par.get("nao_cobrar_fim_de_semana", True) and dia.weekday() >= 5:
        return {"modo": m, "olhou": 0, "cobrou": 0, "sugeriu": 0,
                "pulou": "fim de semana"}

    vencidos = svc_ct.listar(s, atrasados=True)
    cobrou = sugeriu = 0
    for p in vencidos:
        regra = svc_ct.TEMPLATES.get(p["template"], {})
        texto = (f"o pedido {p['numero']} ({p['template_nome']}) de "
                 f"{p['colaborador_nome']} venceu há {p['dias_atraso']} dia(s) "
                 f"e ainda não teve retorno")
        if p["cobrancas"] >= regra.get("cobrancas", 3):
            texto += " — as cobranças acabaram; é hora de escalar"
        if m == "FAZ":
            try:
                r = svc_ct.cobrar(s, p["id"], usuario, pelo_agente=True)
                cobrou += 1 if r.get("cobrou") else 0
            except ErroValidacao as e:
                logger.warning("ERP/agente: não cobrei o pedido %s — %s",
                               p["numero"], e)
        else:
            if sugerir(s, rotina="cobranca", alvo_tipo="dp_pedido",
                       alvo_id=p["id"], acao="cobrar_pedido",
                       explicacao=texto,
                       dados={"numero": p["numero"],
                              "dias_atraso": p["dias_atraso"]}):
                sugeriu += 1
    # AS DEVOLUÇÕES DA OBRA entram na mesma rotina, por canal diferente: a
    # contabilidade é cobrada por e-mail, a obra por WhatsApp. É a divisão que
    # ele definiu, e ter uma rotina só evita dois cobradores com dois critérios
    # de prazo.
    from app.apps.erp.core import portal as svc_portal
    devolucoes = svc_portal.devolucoes_abertas(s, atrasadas=True)
    for d in devolucoes:
        texto = (f"a devolução {d['numero']} ({d['o_que']}) da obra "
                 f"{d['obra'] or '—'} venceu há {d['dias_atraso']} dia(s)")
        if m == "FAZ":
            try:
                svc_portal.cobrar_devolucao(s, d["id"], usuario, pelo_agente=True)
                cobrou += 1
            except ErroValidacao as e:
                logger.warning("ERP/agente: não cobrei a devolução %s — %s",
                               d["numero"], e)
        elif sugerir(s, rotina="cobranca", alvo_tipo="dp_devolucao",
                     alvo_id=d["id"], acao="cobrar_devolucao",
                     explicacao=texto, dados={"numero": d["numero"]}):
            sugeriu += 1
    _carimbar(s, "cobranca")
    return {"modo": m, "olhou": len(vencidos) + len(devolucoes),
            "cobrou": cobrou, "sugeriu": sugeriu}


# ===========================================================================
# R6 · VIGIA — a lista do que envelheceu. Não cobra ninguém.
# ===========================================================================
def vigia(s: Session, *, hoje: Optional[date] = None) -> list[dict[str, Any]]:
    """O que está envelhecendo no cadastro. CONSULTA — não muda nada.

    Roda com a rotina desligada também, porque é leitura: desligar o vigia
    desliga o AVISO automático, não a capacidade de olhar.
    """
    dia = hoje or date.today()
    par = parametros(s, "vigia")
    limite = int(par.get("dias_parado") or 3)
    achados: list[dict[str, Any]] = []

    for p in svc_pessoas.fila_do_cadastro(s):
        if (p.get("dias_parado") or 0) >= limite:
            achados.append({
                "tipo": "cadastro_parado", "colaborador_id": p["id"],
                "quem": p["nome"],
                "o_que": (f"parado em “{p['etapa_rotulo']}” há "
                          f"{p['dias_parado']} dias"),
                "o_que_fazer": p["o_que_fazer"]})
        if p.get("falta"):
            achados.append({
                "tipo": "falta_dado", "colaborador_id": p["id"],
                "quem": p["nome"],
                "o_que": "falta para admitir: " + ", ".join(p["falta"]),
                "o_que_fazer": "completar o cadastro"})

    # Admissão de CTPS sem a ficha de registro de volta: o cadastro está
    # concluído aqui dentro e não existe no eSocial. É o pior dos silêncios.
    for linha in s.execute(text("""
        SELECT c.id, c.nome, c.admissao
          FROM colaboradores c
         WHERE c.vinculo = 'CTPS' AND c.situacao = 'ATIVO'
           AND c.demissao IS NULL AND c.admissao IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM documentos d
                            WHERE d.colaborador_id = c.id
                              AND d.tipo_codigo = 'FICHA-REGISTRO')
           AND NOT EXISTS (SELECT 1 FROM dp_pedidos p
                            WHERE p.colaborador_id = c.id
                              AND p.template = 'ASSINATURA_CTPS'
                              AND p.situacao <> 'CANCELADO')
         LIMIT 200""")).mappings().all():
        achados.append({
            "tipo": "admissao_sem_registro", "colaborador_id": linha["id"],
            "quem": linha["nome"],
            "o_que": (f"admitido em {linha['admissao']:%d/%m/%Y} e sem ficha de "
                      "registro nem pedido à contabilidade"),
            "o_que_fazer": "abrir o pedido de assinatura de CTPS"})

    # Desligamento sem a documentação rescisória de volta.
    for linha in s.execute(text("""
        SELECT c.id, c.nome, c.demissao
          FROM colaboradores c
         WHERE c.demissao IS NOT NULL AND c.demissao >= :limite
           AND NOT EXISTS (SELECT 1 FROM documentos d
                            WHERE d.colaborador_id = c.id
                              AND d.tipo_codigo = 'RESCISAO')
         LIMIT 200"""), {"limite": date(dia.year - 1, dia.month, 1)}
    ).mappings().all():
        achados.append({
            "tipo": "desligamento_sem_documento", "colaborador_id": linha["id"],
            "quem": linha["nome"],
            "o_que": (f"saiu em {linha['demissao']:%d/%m/%Y} e não há "
                      "documentação rescisória arquivada"),
            "o_que_fazer": "cobrar a documentação rescisória"})

    # Devolução que a obra não mandou: o documento está num envelope em cima de
    # uma mesa, e o processo parado por causa disso.
    from app.apps.erp.core import portal as svc_portal
    for d in svc_portal.devolucoes_abertas(s, atrasadas=True):
        achados.append({
            "tipo": "devolucao_atrasada",
            "colaborador_id": d.get("colaborador_id") or 0,
            "quem": d.get("pessoa") or d["numero"],
            "o_que": (f"{d['numero']} ({d['o_que']}) está com a obra "
                      f"{d.get('obra') or '—'} há {d['dias_atraso']} dia(s) "
                      "além do prazo"),
            "o_que_fazer": "cobrar a obra pelo WhatsApp"})

    # Pessoa ativa sem obra: não entra em rateio nem em lista de obra nenhuma.
    for linha in s.execute(text("""
        SELECT id, nome FROM colaboradores
         WHERE situacao = 'ATIVO' AND demissao IS NULL AND obra_id IS NULL
         LIMIT 200""")).mappings().all():
        achados.append({
            "tipo": "ativo_sem_obra", "colaborador_id": linha["id"],
            "quem": linha["nome"],
            "o_que": "ativo e sem obra — pode ser do escritório, ou esquecimento",
            "o_que_fazer": "conferir a obra do cadastro"})
    return achados


# ===========================================================================
# R7 · RELATOR
# ===========================================================================
def relatorio(s: Session, *, dias: int = 1) -> dict[str, Any]:
    """O resumo em português. Cada linha diz o que fazer, não um número solto."""
    pedidos = svc_ct.listar(s, situacao="ABERTOS")
    atrasados = [p for p in pedidos if p["atrasado"]]
    divergentes = [p for p in pedidos if p["situacao"] == "DIVERGENTE"]
    fila = svc_pessoas.fila_do_cadastro(s)
    pendencias = vigia(s)
    linhas = []
    if atrasados:
        linhas.append(
            f"{len(atrasados)} pedido(s) à contabilidade atrasado(s): "
            + "; ".join(f"{p['numero']} ({p['colaborador_nome']}, "
                        f"{p['dias_atraso']}d)" for p in atrasados[:10]))
    if divergentes:
        linhas.append(
            f"{len(divergentes)} retorno(s) com dado diferente do cadastro — "
            "ninguém pode concluir até alguém olhar: "
            + "; ".join(p["numero"] for p in divergentes[:10]))
    if fila:
        linhas.append(f"{len(fila)} cadastro(s) parado(s) em alguma etapa")
    if pendencias:
        linhas.append(f"{len(pendencias)} pendência(s) no cadastro "
                      "(papel que falta, admissão sem registro, ativo sem obra)")
    if not linhas:
        linhas.append("nada atrasado e nada parado.")
    return {"linhas": linhas, "atrasados": atrasados[:50],
            "divergentes": divergentes[:50], "fila": fila[:50],
            "pendencias": pendencias[:100]}


# ===========================================================================
# O BOTÃO (e, mais tarde, a rotina diária)
# ===========================================================================
def rodar(s: Session, usuario: Optional[Usuario] = None) -> dict[str, Any]:
    """Roda o que está ligado. Nada ligado, nada feito — e diz isso.

    Hoje é chamado por um botão na tela. Quando virar rotina diária, é esta
    mesma função: ela já é segura para rodar duas vezes (a sugestão não
    duplica, a cobrança respeita o teto e o prazo).
    """
    feito: dict[str, Any] = {}
    # A CAIXA VEM PRIMEIRO, de propósito: o retorno que chegou hoje tem de ser
    # casado ANTES de a cobrança olhar os prazos — senão o agente cobra um
    # pedido que já foi respondido, e cobrar quem já respondeu queima a
    # relação com a contabilidade.
    feito["caixa"] = rodar_caixa(s, usuario)
    feito["cobranca"] = rodar_cobranca(s, usuario)
    if modo(s, "vigia") != "DESLIGADA":
        achados = vigia(s)
        quantos = 0
        for a in achados:
            if sugerir(s, rotina="vigia", alvo_tipo="colaborador",
                       alvo_id=a["colaborador_id"], acao=a["tipo"],
                       explicacao=f"{a['quem']}: {a['o_que']}",
                       dados={"o_que_fazer": a["o_que_fazer"]}):
                quantos += 1
        _carimbar(s, "vigia")
        feito["vigia"] = {"achou": len(achados), "avisou": quantos}
    if modo(s, "relatorio") != "DESLIGADA":
        feito["relatorio"] = relatorio(s)
        _carimbar(s, "relatorio")
    ligadas = [c for c in ROTINAS if modo(s, c) != "DESLIGADA"]
    feito["rotinas_ligadas"] = ligadas
    if not ligadas:
        feito["aviso"] = ("Nenhuma rotina do agente está ligada — ele não fez "
                          "nada, e é assim que ele nasce.")
    return feito


def _carimbar(s: Session, chave: str) -> None:
    s.execute(text("UPDATE dp_agente_rotinas SET rodou_em = now() "
                   "WHERE chave = :c"), {"c": chave})
