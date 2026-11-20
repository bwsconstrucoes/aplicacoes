# -*- coding: utf-8 -*-
"""
O PROCESSO DO DESLIGAMENTO — o pedido só se encerra quando a última coisa
acontece.

⚑ PEDIDO DELE, 19/11/2026, com as palavras dele:

> *"É feita a solicitação, o DP analisa, o DP encaminha o pedido para
> contabilidade, para serem feitos registros e etc. A contabilidade retorna a
> documentação rescisória, aí o DP vai encaminhar para quem solicitou, essa
> pessoa que solicitou vai apresentar para o colaborador a documentação, colher
> assinaturas e devolver para o DP. E o DP, além disso, ainda gera o lançamento
> financeiro, que vai ser pago, e pago não só o lançamento financeiro da
> rescisão, como também tem uma segunda obrigação, que é a multa rescisória.
> Vai ser feito esses dois lançamentos pagos, e só então a solicitação se
> encerra. (…) a gente tem que reestruturar todo esse comando para ele
> funcionar do início ao fim e nada dessas ações se perderem."*

**O QUE ACONTECIA ANTES:** "Atender" gravava as datas no cadastro e FECHAVA o
pedido na mesma hora. O resto da cadeia vivia num checklist que ninguém era
obrigado a marcar — e o pedido dizia "atendido" com a rescisão por pagar.

**O QUE MUDA:** atender ABRE o processo (situação "Em andamento"). Cada passo
abaixo tem dono e tem prova. O pedido só se encerra com todos cumpridos — e o
servidor recusa encerrar antes, não a tela.

⚑ **O SISTEMA PROCURA A PROVA ANTES DE PEDIR A MARCAÇÃO.** O pedido à
contabilidade, a volta da documentação e o pagamento dos dois lançamentos são
LIDOS dos registros que já existem; ninguém precisa marcar "paguei" — basta o
lançamento estar pago. Só o que acontece fora do sistema (mandar a
documentação para a obra, receber de volta assinada) é marcado à mão. Passo
marcado à mão é passo que alguém esquece; passo lido é passo que não se perde.

⚑ **O CHECKLIST CONTINUA EXISTINDO, e é outra coisa.** O checklist é a lista de
tarefas do DP, configurável na tela (migração 112) — imprimir a folha de ponto,
montar o malote. Os passos daqui são o PORTÃO do encerramento: o que tem de ter
acontecido para a empresa estar quite com a pessoa.

⚑ **VALE PARA QUEM TEM CARTEIRA.** O desligamento de prestador não tem
contabilidade, rescisão nem multa — continua fechando no atendimento, como era.
"""
from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import Usuario

logger = logging.getLogger(__name__)

# A situação do pedido enquanto o processo corre. Não é "Em análise": a análise
# acabou, o DP já registrou a saída — o que falta é a cadeia que vem depois.
EM_ANDAMENTO = "EM_ANDAMENTO"

# ---------------------------------------------------------------------------
# OS PASSOS, na ordem em que acontecem
#
# `como`:
#   · "lido"    — o sistema enxerga sozinho; marcar à mão só com motivo (feito
#                 por fora do sistema), para nada travar;
#   · "marcar"  — acontece fora do sistema; o DP marca;
#   · "titulo"  — o DP liga o lançamento (pelo número da SP); cumpre-se quando
#                 o lançamento estiver PAGO;
#   · "decidir" — a confirmação da continuidade (só existe quando o pedido
#                 disse que a pessoa continua).
# ---------------------------------------------------------------------------
PASSOS: tuple[dict[str, Any], ...] = (
    {"codigo": "REGISTRADO", "como": "lido", "quem": "DP",
     "rotulo": "O DP analisou e registrou a saída"},
    {"codigo": "CONTINUIDADE", "como": "decidir", "quem": "DP",
     "rotulo": "Confirmar se a pessoa continua como prestador",
     "so_se_continua": True},
    # ⚑ O AVISO ASSINADO VEM ANTES DA CONTABILIDADE (20/11/2026). Ele: *"Eu
    # dou o aviso da pessoa, solicito o desligamento (…) a colaboradora vai
    # assinar e dispara para o DP. O DP verifica e manda para frente, para a
    # contabilidade. Já com o aviso assinado, datado."* E a ordem: o DP
    # analisa ANTES de a obra apresentar o aviso — nasce no atendimento
    # (`termos.py`). Encerramento de experiência não tem aviso: dispensado.
    {"codigo": "AVISO", "como": "lido", "quem": "obra colhe, DP confere",
     "rotulo": "Aviso prévio assinado pelo colaborador"},
    {"codigo": "CONTABILIDADE", "como": "lido", "quem": "DP",
     "rotulo": "Rescisão pedida à contabilidade"},
    {"codigo": "DOCUMENTACAO", "como": "lido", "quem": "Contabilidade",
     "rotulo": "A contabilidade devolveu a documentação rescisória"},
    # ⚑ A IDA E A VOLTA DA ASSINATURA SÃO PELO SISTEMA (19/11/2026). Ele: *"o
    # arquivo precisa ser cobrado pelo Agente e recebido. Imagino que o ideal é
    # enviar via solicitação DP, o agente analisa e o DP valida o recebimento
    # da documentação."* A ida é uma DEVOLUÇÃO para a obra (a mesma pendência
    # do portal, com prazo, que o agente já cobra por WhatsApp); a volta chega
    # pelo portal, o agente confere o arquivo, e o DP valida.
    {"codigo": "ENCAMINHADA", "como": "lido", "quem": "DP",
     "rotulo": "A documentação foi para a obra colher a assinatura"},
    {"codigo": "ASSINADA", "como": "lido", "quem": "obra, e o DP valida",
     "rotulo": "A documentação voltou assinada e o DP validou"},
    {"codigo": "RESCISAO_PAGA", "como": "titulo", "quem": "DP e Financeiro",
     "rotulo": "Rescisão (TRCT) lançada e paga"},
    {"codigo": "MULTA_PAGA", "como": "titulo", "quem": "DP e Financeiro",
     "rotulo": "Multa rescisória do FGTS lançada e paga"},
)
CODIGOS = tuple(p["codigo"] for p in PASSOS)

# ⚑ A MULTA DE 40% DO FGTS SÓ EXISTE NA DEMISSÃO SEM JUSTA CAUSA. No pedido de
# demissão e no fim do contrato de experiência (no prazo) não há multa — então
# o passo nasce dispensado, dizendo por quê. Se o DP ligar um lançamento a ele
# mesmo assim, o lançamento vale: há caso (rescisão antecipada da experiência)
# que a forma escolhida não descreve.
SEM_MULTA = {
    "PEDIDO_COLABORADOR": "pedido de demissão não gera multa do FGTS",
    "FIM_EXPERIENCIA": "fim do contrato de experiência no prazo não gera "
                       "multa do FGTS",
}

# Situações de lançamento que não contam: o passo volta a pedir um lançamento.
_TITULO_MORTO = ("CANCELADO", "ESTORNADO")


def _tem_tabela(s: Session) -> bool:
    """A migração 114 pode não ter sido aplicada ainda: o código sobe para o
    Render antes de alguém apertar o botão. Sem a tabela, o processo mostra os
    passos lidos e recusa as marcações com recado — não derruba a tela."""
    return s.execute(text(
        "SELECT 1 FROM information_schema.tables "
        " WHERE table_schema = 'public' "
        "   AND table_name = 'dp_desligamento_passos'")).first() is not None


# ---------------------------------------------------------------------------
# Quando o processo se aplica
# ---------------------------------------------------------------------------
def e_processo(tipo: str, vinculo: Optional[str]) -> bool:
    """Desligamento de quem tem carteira. Prestador fecha no atendimento."""
    return tipo == "DESLIGAMENTO" and (vinculo or "CTPS").upper() == "CTPS"


def continua(dados: dict[str, Any]) -> bool:
    """O pedido disse que a pessoa continua como prestador?"""
    return str((dados or {}).get("continua_prestador") or "") == "1"


def inicio_padrao_como_prestador(dados: dict[str, Any]) -> Optional[date]:
    """O dia seguinte ao último dia trabalhado — ou à saída, sem ele.

    ⚑ É O ÚLTIMO DIA, E NÃO A SAÍDA, porque no aviso indenizado a data de
    saída é PROJETADA (último dia + o aviso), e a pessoa que continua começa a
    trabalhar como prestador no dia seguinte ao último dia de carteira — não um
    mês depois. A tela mostra a data e o DP pode trocá-la.
    """
    base = dados.get("ultimo_dia") or dados.get("data_saida")
    if not base:
        return None
    try:
        return date.fromisoformat(str(base)[:10]) + timedelta(days=1)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Ler os passos
# ---------------------------------------------------------------------------
def _marcas(s: Session, solicitacao_id: int) -> dict[str, dict[str, Any]]:
    if not _tem_tabela(s):
        return {}
    linhas = s.execute(text("""
        SELECT m.passo, m.situacao, m.titulo_id, m.observacao, m.detalhe,
               m.feito_em, u.nome AS por,
               t.numero_sp, t.status::text AS titulo_status,
               t.valor_liquido
          FROM dp_desligamento_passos m
          LEFT JOIN usuarios u ON u.id = m.feito_por
          LEFT JOIN titulos t ON t.id = m.titulo_id
         WHERE m.solicitacao_id = :s"""), {"s": solicitacao_id}).mappings().all()
    return {l["passo"]: dict(l) for l in linhas}


def _pedido_a_contabilidade(s: Session, sol: dict[str, Any]
                            ) -> Optional[dict[str, Any]]:
    """O pedido de rescisão deste desligamento.

    Ligado pelo número da solicitação quando nasceu do processo; e, para o que
    o DP abriu pela ficha da pessoa, o pedido de desligamento dela aberto
    depois desta solicitação. Os dois caminhos existem na tela, e exigir o
    primeiro faria o processo travar para quem usou o segundo.
    """
    if not sol.get("colaborador_id"):
        return None
    linha = s.execute(text("""
        SELECT id, numero, situacao, prazo, respondido_em
          FROM dp_pedidos
         WHERE template = 'DESLIGAMENTO' AND situacao <> 'CANCELADO'
           AND (solicitacao_id = :sid
                OR (solicitacao_id IS NULL AND colaborador_id = :c
                    AND criado_em >= :desde))
         ORDER BY (solicitacao_id = :sid) DESC NULLS LAST, criado_em DESC
         LIMIT 1"""),
        {"sid": sol["id"], "c": sol["colaborador_id"],
         "desde": sol["criado_em"]}).mappings().first()
    return dict(linha) if linha else None


def _devolucao(s: Session, sol: dict[str, Any],
               pedido: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """A pendência que levou a documentação para a obra assinar.

    Ligada ao pedido de rescisão (quando nasceu daqui) ou, sem ele, a última
    devolução de documentação rescisória desta pessoa aberta depois do pedido
    de desligamento. Cancelada não conta.
    """
    if not sol.get("colaborador_id"):
        return None
    linha = s.execute(text("""
        SELECT d.id, d.numero, d.situacao, d.prazo, d.cobrancas,
               d.documento_id, d.documento_enviado_id, d.devolvido_em,
               o.codigo AS obra, doc.anexo_id
          FROM dp_devolucoes d
          LEFT JOIN obras o ON o.id = d.obra_id
          LEFT JOIN documentos doc ON doc.id = d.documento_id
         WHERE d.situacao <> 'CANCELADA'
           AND ((:ped IS NOT NULL AND d.pedido_id = :ped)
                OR (d.colaborador_id = :c AND d.documento_esperado = :tipo
                    AND d.criado_em >= :desde))
         ORDER BY d.criado_em DESC, d.id DESC LIMIT 1"""),
        {"ped": pedido["id"] if pedido else None, "c": sol["colaborador_id"],
         "tipo": DOC_ASSINADA, "desde": sol["criado_em"]}).mappings().first()
    return dict(linha) if linha else None


# O tipo do documento que volta assinado (migração 114).
DOC_ASSINADA = "RESCISAO-ASSINADA"


def analisar_devolucao(s: Session, dev: dict[str, Any]) -> list[dict[str, Any]]:
    """O que o agente confere no arquivo que voltou da obra — SEM IA.

    ⚑ O LIMITE, DITO SEM RODEIO: o agente NÃO enxerga assinatura. Ele confere
    o que dá para conferir com o que o sistema já tem — o texto do PDF (tirado
    na hora do arquivamento, sem custo) e o tamanho — e diz quando não deu
    para ler. Quem confirma que está assinado é o DP, olhando o arquivo; por
    isso o passo só se cumpre com a validação dele.

    ⚑ CUSTO: zero idas à internet. O texto e o tamanho já estão no banco.
    """
    if not dev.get("documento_id"):
        return [{"ok": False, "texto": "o arquivo não está ligado à pendência"}]
    doc = s.execute(text("""
        SELECT d.texto, d.colaborador_id, a.tamanho_bytes, a.mime_type,
               c.nome, c.cpf
          FROM documentos d
          JOIN anexos a ON a.id = d.anexo_id
          LEFT JOIN colaboradores c ON c.id = d.colaborador_id
         WHERE d.id = :i"""), {"i": dev["documento_id"]}).mappings().first()
    if doc is None:
        return [{"ok": False, "texto": "o arquivo não foi achado no Arquivo"}]
    achados: list[dict[str, Any]] = [
        {"ok": True, "texto": "o arquivo chegou pelo portal e foi guardado no "
                              "cadastro da pessoa"}]
    if (doc["tamanho_bytes"] or 0) < 2000:
        achados.append({"ok": False, "texto": "o arquivo é muito pequeno — "
                                              "pode estar vazio ou cortado"})
    texto_ = " ".join(str(doc["texto"] or "").split())
    if not texto_:
        achados.append({"ok": None, "texto": "é imagem ou escaneado: não deu "
                                             "para ler o conteúdo — confira à mão"})
    else:
        import re as _re
        digitos = _re.sub(r"\D", "", texto_)
        cpf = _re.sub(r"\D", "", str(doc["cpf"] or ""))
        if cpf and cpf in digitos:
            achados.append({"ok": True, "texto": "o CPF da pessoa está no documento"})
        elif cpf:
            achados.append({"ok": False, "texto": "o CPF da pessoa NÃO aparece no "
                                                  "documento — confira se é dela"})
        if "rescis" not in texto_.lower():
            achados.append({"ok": None, "texto": "não achei a palavra "
                                                 "“rescisão” no texto"})
    if dev.get("documento_enviado_id"):
        foi = s.execute(text("""
            SELECT a.tamanho_bytes FROM documentos d
              JOIN anexos a ON a.id = d.anexo_id WHERE d.id = :i"""),
            {"i": dev["documento_enviado_id"]}).scalar()
        if foi and doc["tamanho_bytes"] and doc["tamanho_bytes"] == foi:
            achados.append({"ok": False, "texto": "o arquivo tem exatamente o "
                            "tamanho do que foi enviado — pode ter voltado SEM "
                            "assinatura"})
    achados.append({"ok": None, "texto": "a assinatura o agente não enxerga: "
                                         "abra o arquivo e confira"})
    return achados


# ---------------------------------------------------------------------------
# O AVISO PRÉVIO PARA A PESSOA ASSINAR (20/11/2026)
# ---------------------------------------------------------------------------
TIPO_DO_TERMO = {"AVISO_TRABALHADO": "AVISO-PREVIO",
                 "AVISO_INDENIZADO": "AVISO-PREVIO",
                 "PEDIDO_COLABORADOR": "PEDIDO-DEMISSAO"}


def precisa_de_aviso(forma: str) -> bool:
    return str(forma or "").upper() in TIPO_DO_TERMO


def _passo_do_aviso(s: Session, sol: dict[str, Any], forma: str,
                    item: dict[str, Any]) -> None:
    from app.apps.erp.core import termos
    if not precisa_de_aviso(forma):
        item["estado"] = "NAO_SE_APLICA"
        item["detalhe"] = ("encerramento do contrato de experiência no prazo "
                           "não tem aviso prévio" if forma == "FIM_EXPERIENCIA"
                           else "esta forma não tem aviso para assinar")
        return
    if forma == "PEDIDO_COLABORADOR":
        item["rotulo"] = "Pedido de demissão assinado pelo colaborador"
    t = termos.do_pedido(s, int(sol["id"]))
    if sol.get("situacao") not in (EM_ANDAMENTO, "ATENDIDA"):
        item["detalhe"] = "o DP analisa e atende; aí o documento vai para a obra"
        return
    if t is None:
        item["detalhe"] = ("gere o documento para a obra colher a assinatura"
                           if termos.tem_tabela(s) else
                           "falta aplicar a atualização do banco (117)")
        item["gerar_termo"] = True
        return
    item["termo"] = {"id": t["id"], "numero": t["numero"],
                     "situacao": t["situacao"], "forma": t.get("forma"),
                     "anexo_id": t.get("anexo_id"),
                     "prazo": str(t["prazo"]) if t.get("prazo") else None}
    if termos.concluido(t):
        item["estado"] = "FEITO"
        item["detalhe"] = (f"{t['numero']}: "
                           + termos.FORMA_ROTULOS.get(t.get("forma") or "", "assinado")
                           + (f" em {t['assinado_em']:%d/%m/%Y}" if t.get("assinado_em") else ""))
    elif t["situacao"] == "ABERTO":
        item["estado"] = "AGUARDANDO"
        prazo = t.get("prazo")
        item["detalhe"] = (f"{t['numero']} com a obra {t.get('obra') or ''} para "
                           "colher a assinatura"
                           + (f", prazo {prazo:%d/%m/%Y}" if prazo else "")
                           + (f" — {t['motivo_recusa']}" if t.get("motivo_recusa") else ""))
        item["atrasado"] = bool(prazo and prazo < date.today())
    else:
        item["detalhe"] = (f"{t['numero']} voltou "
                           + termos.FORMA_ROTULOS.get(t.get("forma") or "", "")
                           + " — abra e valide")
        item["termo_a_validar"] = True


def texto_do_aviso(s: Session, sol: dict[str, Any]) -> tuple[str, str, str]:
    """(tipo, título, texto) do documento que a pessoa assina, montado com o
    que o DP aprovou. Linguagem simples e as datas por extenso de número —
    é para ser lido na obra, no celular."""
    from app.apps.erp.core import dp_analise
    d = sol.get("dados") or {}
    forma = str(d.get("forma_desligamento") or "").upper()
    c = s.execute(text("""
        SELECT c.nome, c.cpf, c.admissao, g.nome AS cargo,
               COALESCE(e.razao_social, e.nome_fantasia) AS empresa, e.cnpj
          FROM colaboradores c
          LEFT JOIN cargos g ON g.id = c.cargo_id
          LEFT JOIN empresas e ON e.id = c.empresa_id
         WHERE c.id = :i"""), {"i": sol["colaborador_id"]}).mappings().first()
    aviso = dp_analise._data(d.get("aviso_previo")) or date.today()
    cumpre = str(d.get("cumpre_aviso") or "1") not in ("0",)
    esp = dp_analise.datas_do_aviso(c["admissao"], aviso, forma, cumpre)
    ultimo = dp_analise._data(d.get("ultimo_dia")) or esp["ultimo_dia"]
    br = dp_analise._br
    cpf = "".join(ch for ch in str(c["cpf"] or "") if ch.isdigit())
    cpf_f = f"{cpf[0:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:11]}" if len(cpf) == 11 else cpf
    quem = (f"{c['nome']}, CPF {cpf_f}, {c['cargo'] or 'colaborador(a)'}, "
            f"admitido(a) em {br(c['admissao'])}")
    empresa = f"{c['empresa'] or 'A empresa'}" + (f" (CNPJ {c['cnpj']})" if c["cnpj"] else "")
    if forma == "PEDIDO_COLABORADOR":
        titulo = "Pedido de demissão"
        texto_ = (f"Eu, {quem}, comunico a {c['empresa'] or 'empresa'} o meu pedido de demissão "
                  f"nesta data, {br(aviso)}.\n\n"
                  + (f"Cumprirei o aviso prévio de 30 dias, trabalhando até "
                     f"{br(ultimo)}." if cumpre else
                     "Peço a dispensa do cumprimento do aviso prévio. Sei que, "
                     "se a empresa não dispensar, os dias não trabalhados podem "
                     "ser descontados (CLT art. 487 §2º)."))
        return "PEDIDO-DEMISSAO", titulo, texto_
    titulo = "Aviso prévio do empregador"
    if forma == "AVISO_INDENIZADO":
        texto_ = (f"{empresa} comunica a {quem} que o seu contrato de trabalho "
                  f"está sendo encerrado, sem justa causa, nesta data, "
                  f"{br(aviso)}.\n\nO aviso prévio de {esp['dias']} dias será "
                  f"INDENIZADO (pago sem trabalho). O último dia de trabalho é "
                  f"{br(ultimo)}. A data de saída, com a projeção do aviso, é "
                  f"{br(esp['data_saida'])}.\n\nAs verbas rescisórias serão "
                  "pagas em até 10 dias (CLT art. 477 §6º).")
    else:
        reducao = str(d.get("reducao_jornada") or "").upper()
        como = ("com a jornada reduzida em 2 horas por dia"
                if reducao == "DUAS_HORAS" else
                "com 7 dias corridos de folga no fim do aviso"
                if reducao == "SETE_DIAS" else
                "com a redução de jornada da lei (2 horas por dia ou 7 dias "
                "corridos no fim), à sua escolha")
        texto_ = (f"{empresa} comunica a {quem} que o seu contrato de trabalho "
                  f"será encerrado, sem justa causa, e lhe concede nesta data, "
                  f"{br(aviso)}, o aviso prévio.\n\nO aviso será TRABALHADO "
                  f"por 30 dias, até {br(ultimo)}, {como} (CLT art. 488).")
        if esp["indenizados"]:
            texto_ += (f" Os outros {esp['indenizados']} dias do aviso "
                       "proporcional (Lei 12.506/2011) serão indenizados; a "
                       f"data de saída, com a projeção, é {br(esp['data_saida'])}.")
    return "AVISO-PREVIO", titulo, texto_


def gerar_termo(s: Session, sol: dict[str, Any], usuario: Optional[Usuario],
                dias: int = 2) -> Optional[dict[str, Any]]:
    """Gera o aviso (ou o pedido de demissão) para a obra colher a assinatura.
    Chamado no atendimento; o DP também pode gerar de novo pela tela."""
    from app.apps.erp.core import termos
    from app.apps.erp.core.pessoal_solicitacoes import _evento
    forma = str((sol.get("dados") or {}).get("forma_desligamento") or "").upper()
    if not precisa_de_aviso(forma) or not sol.get("colaborador_id"):
        return None
    if not termos.tem_tabela(s):
        return None
    vivo = termos.do_pedido(s, int(sol["id"]))
    if vivo is not None and vivo["situacao"] != "CANCELADO":
        if vivo["situacao"] == "ABERTO":
            termos.cancelar(s, vivo["id"], "gerado de novo pelo DP", usuario)
        else:
            raise ErroValidacao(f"{vivo['numero']} já voltou assinado.")
    tipo, titulo, texto_ = texto_do_aviso(s, sol)
    r = termos.abrir(s, colaborador_id=int(sol["colaborador_id"]), tipo=tipo,
                     titulo=titulo, texto_=texto_, solicitacao_id=int(sol["id"]),
                     obra_id=sol.get("obra_id"), dias=dias, usuario=usuario)
    _evento(s, sol["id"], "AVISO_PARA_ASSINAR", {"numero": r["numero"],
                                                 "titulo": titulo}, usuario)
    return r


def aviso_pendente(s: Session, solicitacao_id: int) -> str:
    """O motivo de a rescisão ainda não poder ir à contabilidade, ou ""."""
    sol = s.execute(text("SELECT id, tipo, situacao, colaborador_id, dados, "
                         "criado_em, numero FROM dp_solicitacoes WHERE id = :i"),
                    {"i": solicitacao_id}).mappings().first()
    if sol is None or sol["tipo"] != "DESLIGAMENTO":
        return ""
    forma = str((sol["dados"] or {}).get("forma_desligamento") or "").upper()
    if not precisa_de_aviso(forma):
        return ""
    marcas = _marcas(s, solicitacao_id)
    m = marcas.get("AVISO")
    if m and m["situacao"] in ("FEITO", "NAO_SE_APLICA"):
        return ""
    from app.apps.erp.core import termos
    if not termos.tem_tabela(s):
        return ""
    if termos.concluido(termos.do_pedido(s, solicitacao_id)):
        return ""
    return ("O aviso prévio ainda não voltou assinado pelo colaborador (ou o DP "
            "ainda não validou). A rescisão vai à contabilidade com o aviso "
            "assinado — ou marque o passo como feito por fora do sistema.")


def montar_rescisao(s: Session, sol: dict[str, Any],
                    usuario: Optional[Usuario]) -> Optional[dict[str, Any]]:
    """O pedido de rescisão à contabilidade, montado com o que o DP aprovou —
    o mesmo que o botão da tela monta. Não monta duas vezes."""
    from app.apps.erp.core import contabilidade as svc_ct
    if _pedido_a_contabilidade(s, sol) is not None:
        return None
    d = sol.get("dados") or {}
    return svc_ct.abrir(s, {
        "template": "DESLIGAMENTO", "colaborador_id": sol["colaborador_id"],
        "solicitacao_id": sol["id"],
        "dados": {"forma_desligamento": d.get("forma_desligamento"),
                  "data_saida": d.get("data_saida"),
                  "faltas": d.get("faltas_a_descontar"),
                  "continua_prestador": d.get("continua_prestador"),
                  "observacao": d.get("observacao")}}, usuario)


def aviso_assinado(s: Session, solicitacao_id: int) -> str:
    """A linha que vai no e-mail da rescisão: qual aviso, quando, como."""
    from app.apps.erp.core import termos
    t = termos.do_pedido(s, solicitacao_id)
    if not termos.concluido(t):
        return ""
    quando = t.get("assinado_em")
    return (f"{t['numero']}, {termos.FORMA_ROTULOS.get(t.get('forma') or '', '')}"
            + (f", em {quando:%d/%m/%Y}" if quando else ""))


def passos(s: Session, sol: dict[str, Any]) -> dict[str, Any]:
    """Os passos deste desligamento, cada um com estado e o que fazer.

    `sol` é o pedido como `pessoal_solicitacoes.ler` monta (precisa de `id`,
    `colaborador_id`, `situacao`, `dados`, `criado_em`).

    Estados: FEITO · NAO_SE_APLICA · PENDENTE · AGUARDANDO (a bola está com
    outra pessoa, e o sistema está olhando).
    """
    dados = sol.get("dados") or {}
    marcas = _marcas(s, int(sol["id"]))
    registrado = sol.get("situacao") in (EM_ANDAMENTO, "ATENDIDA")
    pedido = _pedido_a_contabilidade(s, sol)
    dev = _devolucao(s, sol, pedido)
    forma = str(dados.get("forma_desligamento") or "")
    hoje = date.today()
    saida = []

    for p in PASSOS:
        # O passo aparece também depois de um "não continua" — senão a
        # decisão sumiria da lista justamente de quem mudou de ideia.
        if p.get("so_se_continua") and not (
                continua(dados) or dados.get("continuidade_revista")):
            continue
        m = marcas.get(p["codigo"])
        item = {"codigo": p["codigo"], "rotulo": p["rotulo"], "quem": p["quem"],
                "como": p["como"], "estado": "PENDENTE", "detalhe": "",
                "por": (m or {}).get("por") or "",
                "em": (m["feito_em"].isoformat() if m and m.get("feito_em")
                       else None),
                "observacao": (m or {}).get("observacao") or ""}
        cod = p["codigo"]

        # Marca explícita do DP vale por cima do que o sistema lê — inclusive
        # "não se aplica", que só existe com motivo escrito.
        if (m and m["situacao"] in ("FEITO", "NAO_SE_APLICA")
                and cod != "CONTINUIDADE"
                and not (cod == "ASSINADA" and (m.get("detalhe") or {}).get("validada"))):
            item["estado"] = m["situacao"]
            item["detalhe"] = ("marcado à mão" if m["situacao"] == "FEITO"
                               else "dispensado")
            saida.append(item)
            continue

        if cod == "REGISTRADO":
            item["estado"] = "FEITO" if registrado else "PENDENTE"
            item["detalhe"] = ("datas da saída gravadas" if registrado
                               else "atender o pedido grava as datas")

        elif cod == "CONTINUIDADE":
            inicio = inicio_padrao_como_prestador(dados)
            item["inicio_padrao"] = inicio.isoformat() if inicio else None
            from app.apps.erp.core.cadastros import pessoas as svc_pessoas
            item["tipos_contrato"] = [
                {"valor": c, "rotulo": svc_pessoas.TIPO_CONTRATO_ROTULOS[c]}
                for c in svc_pessoas.TIPOS_CONTRATO_PRESTADOR]
            if m and m["situacao"] == "FEITO":
                item["estado"] = "FEITO"
                decidiu = (m.get("detalhe") or {}).get("continua")
                item["detalhe"] = ("confirmado: passou a prestador"
                                   if decidiu else
                                   "confirmado: NÃO continua — desligado")
            elif not registrado:
                item["detalhe"] = "depois de registrar a saída"
            elif inicio and hoje < inicio:
                item["estado"] = "AGUARDANDO"
                item["detalhe"] = (f"confirmar a partir de {inicio:%d/%m/%Y}, o "
                                   "dia seguinte ao último dia de carteira — "
                                   "até lá a pessoa segue de carteira")
            else:
                atraso = (hoje - inicio).days if inicio else 0
                item["detalhe"] = (
                    "o último dia de carteira já passou: confirme se a pessoa "
                    "continua — enquanto não confirmar, ela segue ativa como "
                    "CTPS" + (f" (há {atraso} dia(s))" if atraso else ""))
                item["atrasado"] = atraso > 0

        elif cod == "AVISO":
            _passo_do_aviso(s, sol, forma, item)

        elif cod == "CONTABILIDADE":
            if pedido is None:
                item["detalhe"] = "abrir o pedido de rescisão à contabilidade"
            elif pedido["situacao"] == "RASCUNHO":
                item["detalhe"] = f"{pedido['numero']} montado e ainda não enviado"
            else:
                item["estado"] = "FEITO"
                item["detalhe"] = f"{pedido['numero']} enviado"
            if pedido:
                item["pedido"] = {"id": pedido["id"], "numero": pedido["numero"],
                                  "situacao": pedido["situacao"]}

        elif cod == "DOCUMENTACAO":
            if pedido is None or pedido["situacao"] == "RASCUNHO":
                item["detalhe"] = "depois do pedido à contabilidade"
            elif pedido["situacao"] in ("RESPONDIDO", "CONCLUIDO"):
                item["estado"] = "FEITO"
                item["detalhe"] = f"voltou no {pedido['numero']}"
            elif pedido["situacao"] == "DIVERGENTE":
                item["detalhe"] = (f"voltou no {pedido['numero']} com dado "
                                   "diferente do cadastro — resolva no pedido")
            else:
                item["estado"] = "AGUARDANDO"
                prazo = pedido.get("prazo")
                item["detalhe"] = ("esperando a contabilidade"
                                   + (f", prazo {prazo:%d/%m/%Y}" if prazo else ""))
                item["atrasado"] = bool(prazo and prazo < hoje)

        elif cod == "ENCAMINHADA":
            if dev is None:
                item["detalhe"] = ("mande para a obra assinar — vai junto a "
                                   "documentação que a contabilidade devolveu")
            else:
                item["estado"] = "FEITO"
                item["detalhe"] = (f"{dev['numero']} aberto para a obra "
                                   f"{dev.get('obra') or ''}".strip())
            item["devolucao"] = dev and {
                "id": dev["id"], "numero": dev["numero"],
                "situacao": dev["situacao"], "cobrancas": dev["cobrancas"],
                "prazo": dev["prazo"].isoformat() if dev.get("prazo") else None}

        elif cod == "ASSINADA":
            if m and m["situacao"] == "FEITO":
                item["estado"] = "FEITO"
                item["detalhe"] = "validada pelo DP"
            elif dev is None:
                item["detalhe"] = "depois de mandar para a obra"
            elif dev["situacao"] in ("ABERTA", "COBRADA"):
                prazo = dev.get("prazo")
                item["estado"] = "AGUARDANDO"
                item["detalhe"] = (
                    f"com a obra ({dev['numero']})"
                    + (f", prazo {prazo:%d/%m/%Y}" if prazo else "")
                    + (f", cobrada {dev['cobrancas']} vez(es)"
                       if dev["cobrancas"] else "")
                    + " — o agente cobra a obra quando o prazo vence")
                item["atrasado"] = bool(prazo and prazo < hoje)
            elif dev["situacao"] == "DEVOLVIDA":
                item["detalhe"] = ("chegou pelo portal — confira a análise e "
                                   "valide o recebimento")
                item["a_validar"] = True
                item["analise"] = analisar_devolucao(s, dev)
                item["documento_id"] = dev.get("documento_id")
                item["anexo_id"] = dev.get("anexo_id")

        elif cod in ("RESCISAO_PAGA", "MULTA_PAGA"):
            if m and m["situacao"] == "LANCADO" and m.get("titulo_id"):
                st = (m.get("titulo_status") or "").upper()
                item["titulo"] = {"id": m["titulo_id"],
                                  "numero_sp": m.get("numero_sp") or "",
                                  "status": st,
                                  "valor": (float(m["valor_liquido"])
                                            if m.get("valor_liquido") is not None
                                            else None)}
                if st == "PAGO":
                    item["estado"] = "FEITO"
                    item["detalhe"] = f"{m.get('numero_sp')} pago"
                elif st in _TITULO_MORTO:
                    item["detalhe"] = (f"{m.get('numero_sp')} foi "
                                       f"{st.lower()} — ligue o lançamento novo")
                else:
                    item["estado"] = "AGUARDANDO"
                    item["detalhe"] = (f"{m.get('numero_sp')} lançado, esperando "
                                       "o pagamento")
            elif cod == "MULTA_PAGA" and forma in SEM_MULTA:
                item["estado"] = "NAO_SE_APLICA"
                item["detalhe"] = SEM_MULTA[forma]
            else:
                item["detalhe"] = "lance a SP e informe o número aqui"
        saida.append(item)

    cumpridos = sum(1 for x in saida if x["estado"] in ("FEITO", "NAO_SE_APLICA"))
    proximo = next((x for x in saida
                    if x["estado"] not in ("FEITO", "NAO_SE_APLICA")), None)
    return {"passos": saida, "cumpridos": cumpridos, "total": len(saida),
            "pode_encerrar": registrado and proximo is None,
            "proximo": proximo["rotulo"] if proximo else "",
            "marcacoes_disponiveis": _tem_tabela(s)}


# ---------------------------------------------------------------------------
# Escrever
# ---------------------------------------------------------------------------
def _exigir_em_andamento(sol: dict[str, Any]) -> None:
    if sol.get("situacao") != EM_ANDAMENTO:
        raise ErroValidacao(
            f"{sol.get('numero')} não está em andamento — "
            + ("atenda o pedido primeiro." if sol.get("situacao") in
               ("ABERTA", "EM_ANALISE") else "o processo já foi encerrado."))


def _exigir_tabela(s: Session) -> None:
    if not _tem_tabela(s):
        raise ErroValidacao(
            "Falta aplicar a atualização do banco (migração 114) — peça em "
            "Configurações › Aplicar atualizações do banco.")


def _gravar(s: Session, solicitacao_id: int, passo: str, situacao: str,
            usuario: Optional[Usuario], *, titulo_id: Optional[int] = None,
            observacao: str = "", detalhe: Optional[dict] = None) -> None:
    s.execute(text("""
        INSERT INTO dp_desligamento_passos
               (solicitacao_id, passo, situacao, titulo_id, observacao,
                detalhe, feito_por, feito_em)
        VALUES (:s, :p, :sit, :t, :o, CAST(:d AS jsonb), :u, now())
        ON CONFLICT (solicitacao_id, passo) DO UPDATE
           SET situacao = EXCLUDED.situacao, titulo_id = EXCLUDED.titulo_id,
               observacao = EXCLUDED.observacao, detalhe = EXCLUDED.detalhe,
               feito_por = EXCLUDED.feito_por, feito_em = now()"""),
        {"s": solicitacao_id, "p": passo, "sit": situacao, "t": titulo_id,
         "o": (observacao or "").strip()[:2000] or None,
         "d": json.dumps(detalhe or {}, ensure_ascii=False, default=str),
         "u": usuario.id if usuario else None})


def marcar(s: Session, sol: dict[str, Any], dados: dict[str, Any],
           usuario: Optional[Usuario]) -> dict[str, Any]:
    """Marca um passo: feito, dispensado, desfeito, ou o lançamento ligado.

    `dados`: `passo`, `acao` ∈ {feito, nao_se_aplica, desfazer, lancamento},
    `observacao` e, para `lancamento`, `numero_sp`.

    ⚑ PASSO LIDO PELO SISTEMA SÓ SE MARCA À MÃO COM MOTIVO. É a porta para o
    que foi feito por fora (o e-mail mandado da caixa pessoal, o pagamento em
    dinheiro) — sem ela o processo travaria; com ela sem motivo, a prova
    perderia o sentido.
    """
    from app.apps.erp.core.pessoal_solicitacoes import _evento

    _exigir_tabela(s)
    _exigir_em_andamento(sol)
    passo = str(dados.get("passo") or "").strip().upper()
    meta = next((p for p in PASSOS if p["codigo"] == passo), None)
    if meta is None or passo in ("REGISTRADO", "CONTINUIDADE"):
        raise ErroValidacao("Este passo não se marca por aqui.")
    acao = str(dados.get("acao") or "").strip().lower()
    obs = str(dados.get("observacao") or "").strip()

    if acao == "desfazer":
        s.execute(text("DELETE FROM dp_desligamento_passos "
                       " WHERE solicitacao_id = :s AND passo = :p"),
                  {"s": sol["id"], "p": passo})
        _evento(s, sol["id"], "PASSO_DESFEITO", {"passo": meta["rotulo"]}, usuario)
        return {"passo": passo, "estado": "desfeito"}

    if acao == "nao_se_aplica":
        if len(obs) < 5:
            raise ErroValidacao(
                "Diga por que este passo não se aplica — é a pergunta que "
                "alguém vai fazer quando a rescisão for questionada.")
        _gravar(s, sol["id"], passo, "NAO_SE_APLICA", usuario, observacao=obs)
        _evento(s, sol["id"], "PASSO_DISPENSADO",
                {"passo": meta["rotulo"], "motivo": obs}, usuario)
        return {"passo": passo, "estado": "NAO_SE_APLICA"}

    if acao == "lancamento":
        if meta["como"] != "titulo":
            raise ErroValidacao("Este passo não tem lançamento financeiro.")
        numero = str(dados.get("numero_sp") or "").strip()
        if not numero:
            raise ErroValidacao("Informe o número da SP do lançamento.")
        t = s.execute(text("""
            SELECT id, numero_sp, status::text AS status, colaborador_id
              FROM titulos WHERE upper(numero_sp) = upper(:n)"""),
            {"n": numero}).mappings().first()
        if t is None:
            raise ErroValidacao(
                f"Não achei o lançamento {numero}. Confira o número da SP.")
        if t["status"] in _TITULO_MORTO:
            raise ErroValidacao(
                f"O lançamento {t['numero_sp']} está {t['status'].lower()}.")
        # O mesmo lançamento não paga dois passos: a rescisão e a multa são
        # duas obrigações, e ligar a mesma SP às duas faria o processo dizer
        # que pagou as duas com um pagamento só.
        outro = s.execute(text("""
            SELECT passo FROM dp_desligamento_passos
             WHERE titulo_id = :t AND NOT (solicitacao_id = :s AND passo = :p)
             LIMIT 1"""), {"t": t["id"], "s": sol["id"], "p": passo}).first()
        if outro is not None:
            raise ErroValidacao(
                f"O lançamento {t['numero_sp']} já está ligado a outro passo de "
                "desligamento. Rescisão e multa são dois lançamentos.")
        _gravar(s, sol["id"], passo, "LANCADO", usuario, titulo_id=t["id"],
                observacao=obs, detalhe={"numero_sp": t["numero_sp"]})
        _evento(s, sol["id"], "LANCAMENTO_LIGADO",
                {"passo": meta["rotulo"], "numero_sp": t["numero_sp"]}, usuario)
        aviso = ""
        if t["colaborador_id"] and sol.get("colaborador_id") and \
                int(t["colaborador_id"]) != int(sol["colaborador_id"]):
            aviso = (f"Atenção: o lançamento {t['numero_sp']} está em nome de "
                     "outra pessoa.")
        return {"passo": passo, "estado": "LANCADO", "aviso": aviso}

    if acao == "feito":
        if meta["como"] in ("lido", "titulo") and len(obs) < 5:
            raise ErroValidacao(
                "Este passo o sistema confere sozinho. Para marcar à mão, "
                "escreva o que foi feito por fora do sistema.")
        _gravar(s, sol["id"], passo, "FEITO", usuario, observacao=obs)
        _evento(s, sol["id"], "PASSO_FEITO",
                {"passo": meta["rotulo"], "observacao": obs}, usuario)
        return {"passo": passo, "estado": "FEITO"}

    raise ErroValidacao("Ação desconhecida para o passo.")


def mandar_para_assinar(s: Session, sol: dict[str, Any], dados: dict[str, Any],
                        usuario: Optional[Usuario]) -> dict[str, Any]:
    """Abre a pendência para a obra: a documentação vai, e tem de voltar
    assinada pelo portal. O agente cobra quando o prazo vence (rotina de
    cobrança, que já cobra as devoluções atrasadas por WhatsApp).
    """
    from app.apps.erp.core import portal as svc_portal
    from app.apps.erp.core.pessoal_solicitacoes import _evento

    _exigir_em_andamento(sol)
    pedido = _pedido_a_contabilidade(s, sol)
    if _devolucao(s, sol, pedido) is not None:
        raise ErroValidacao("A documentação já foi mandada para a obra.")
    enviado = None
    if pedido:
        enviado = s.execute(text("SELECT documento_id FROM dp_pedidos WHERE id = :i"),
                            {"i": pedido["id"]}).scalar()
    if not enviado and not str(dados.get("sem_arquivo") or ""):
        raise ErroValidacao(
            "A documentação rescisória ainda não voltou da contabilidade com o "
            "arquivo. Espere o retorno — ou confirme que vai mandar o papel por "
            "outro caminho.")
    r = svc_portal.abrir_devolucao(s, {
        "colaborador_id": sol["colaborador_id"],
        "obra_id": sol.get("obra_id"),
        "pedido_id": pedido["id"] if pedido else None,
        "documento_enviado_id": enviado,
        "documento_esperado": DOC_ASSINADA,
        "dias": dados.get("dias") or 5,
        "o_que": (str(dados.get("o_que") or "").strip()
                  or "Imprimir a documentação rescisória, apresentar ao "
                     "colaborador, colher as assinaturas e devolver pelo "
                     "portal.")}, usuario)
    _evento(s, sol["id"], "MANDADA_PARA_ASSINAR",
            {"devolucao": r["numero"], "com_arquivo": bool(enviado)}, usuario)
    return r


def validar_assinada(s: Session, sol: dict[str, Any], dados: dict[str, Any],
                     usuario: Optional[Usuario]) -> dict[str, Any]:
    """O DP valida — ou recusa — a documentação que voltou da obra.

    Recusar devolve a pendência para a obra, com o motivo e prazo novo: o
    arquivo errado continua guardado no cadastro (é história), mas não fecha
    o passo.
    """
    from app.apps.erp.core.pessoal_solicitacoes import _evento

    _exigir_tabela(s)
    _exigir_em_andamento(sol)
    dev = _devolucao(s, sol, _pedido_a_contabilidade(s, sol))
    if dev is None or dev["situacao"] != "DEVOLVIDA":
        raise ErroValidacao("A documentação assinada ainda não voltou da obra.")
    acao = str(dados.get("acao") or "").strip().lower()
    if acao == "validar":
        _gravar(s, sol["id"], "ASSINADA", "FEITO", usuario,
                observacao=str(dados.get("observacao") or ""),
                detalhe={"validada": True, "devolucao": dev["numero"],
                         "documento_id": dev["documento_id"]})
        _evento(s, sol["id"], "ASSINADA_VALIDADA",
                {"devolucao": dev["numero"]}, usuario)
        return {"validada": True}
    if acao == "recusar":
        motivo = str(dados.get("motivo") or "").strip()
        if len(motivo) < 5:
            raise ErroValidacao("Diga o que está errado — a obra precisa saber "
                                "o que corrigir.")
        s.execute(text("""
            UPDATE dp_devolucoes
               SET situacao = 'ABERTA', documento_id = NULL, devolvido_em = NULL,
                   prazo = :p,
                   o_que = o_que || ' — DEVOLVIDO PELO DP: ' || :m
             WHERE id = :d"""),
            {"p": date.today() + timedelta(days=int(dados.get("dias") or 3)),
             "m": motivo[:500], "d": dev["id"]})
        _evento(s, sol["id"], "ASSINADA_RECUSADA",
                {"devolucao": dev["numero"], "motivo": motivo}, usuario)
        return {"validada": False}
    raise ErroValidacao("Diga se valida ou recusa.")


def confirmar_continuidade(s: Session, sol: dict[str, Any],
                           dados: dict[str, Any],
                           usuario: Optional[Usuario]) -> dict[str, Any]:
    """O DP confirma se a pessoa continua como prestador.

    · "continua"   → o cadastro passa a prestador (`pessoas.passar_a_prestador`);
    · "não"        → desliga como qualquer desligamento (`pessoas.desligar`),
                     com a data de saída que o pedido já tinha.

    ⚑ DÁ PARA DIZER "NÃO" A QUALQUER MOMENTO, e "SIM" só depois do último dia.
    Ele: *"o cara pode dizer que não, ele vai continuar como prestador, mas de
    repente no meio do caminho decidiu-se que ele não continua mais."* Mudar
    para "não" antes da saída é só voltar ao desligamento comum. Já "sim" muda
    o vínculo, e não se muda o vínculo de quem ainda está de carteira.
    """
    from app.apps.erp.core.cadastros import pessoas as svc_pessoas
    from app.apps.erp.core.pessoal_solicitacoes import _evento

    _exigir_tabela(s)
    _exigir_em_andamento(sol)
    pedido = sol.get("dados") or {}
    if not continua(pedido):
        raise ErroValidacao("Este desligamento não disse que a pessoa continua.")
    ja = s.execute(text("""
        SELECT 1 FROM dp_desligamento_passos
         WHERE solicitacao_id = :s AND passo = 'CONTINUIDADE'"""),
        {"s": sol["id"]}).first()
    if ja is not None:
        raise ErroValidacao("A continuidade já foi confirmada neste pedido.")
    cid = sol.get("colaborador_id")
    if not cid:
        raise ErroValidacao("O pedido não tem pessoa ligada.")
    resposta = str(dados.get("continua") or "").strip()
    if resposta not in ("1", "0"):
        raise ErroValidacao("Diga se a pessoa continua ou não.")

    if resposta == "1":
        padrao = inicio_padrao_como_prestador(pedido)
        inicio = dados.get("inicio") or (padrao.isoformat() if padrao else None)
        svc_pessoas.passar_a_prestador(s, int(cid), {
            "data_saida": pedido.get("data_saida"),
            "forma_desligamento": pedido.get("forma_desligamento"),
            "inicio": inicio,
            "tipo_contrato": dados.get("tipo_contrato"),
            "valor_diaria": dados.get("valor_diaria"),
            "valor_mes": dados.get("valor_mes")}, usuario)
        detalhe = {"continua": True, "inicio": str(inicio),
                   "tipo_contrato": dados.get("tipo_contrato")}
        _evento(s, sol["id"], "CONTINUA_COMO_PRESTADOR", detalhe, usuario)
    else:
        motivo = str(dados.get("observacao") or "").strip()
        svc_pessoas.desligar(s, int(cid), {
            "data_saida": pedido.get("data_saida"),
            "forma_desligamento": pedido.get("forma_desligamento"),
            "aviso_previo": pedido.get("aviso_previo"),
            "ultimo_dia": pedido.get("ultimo_dia"),
            "motivo": motivo or "confirmado pelo DP: não continua"}, usuario)
        detalhe = {"continua": False, "motivo": motivo}
        _evento(s, sol["id"], "NAO_CONTINUA", detalhe, usuario)
        # O pedido passa a dizer o que vale, para quem o ler depois — e para
        # as listas que ainda procuram "continua = 1".
        s.execute(text("""
            UPDATE dp_solicitacoes SET dados = dados || CAST(:j AS jsonb)
             WHERE id = :s"""),
            {"s": sol["id"], "j": json.dumps({"continua_prestador": "0",
                                              "continuidade_revista": "1"})})
    _gravar(s, sol["id"], "CONTINUIDADE", "FEITO", usuario,
            observacao=str(dados.get("observacao") or ""), detalhe=detalhe)
    s.flush()
    return detalhe


def encerrar(s: Session, sol: dict[str, Any],
             usuario: Optional[Usuario]) -> None:
    """Fecha o pedido — só com todos os passos cumpridos.

    ⚑ A TRAVA É AQUI, no servidor. A tela esconde o botão enquanto falta algo,
    mas quem decide é esta conferência.
    """
    from app.apps.erp.core.pessoal_solicitacoes import _evento

    _exigir_em_andamento(sol)
    from app.apps.erp.core import contabil_processo
    if contabil_processo.extras_abertos(s, int(sol["id"])):
        raise ErroValidacao("Há uma correção ou um cancelamento esperando a "
                            "ciência da contabilidade neste desligamento.")
    estado = passos(s, sol)
    if not estado["pode_encerrar"]:
        falta = [p["rotulo"] for p in estado["passos"]
                 if p["estado"] not in ("FEITO", "NAO_SE_APLICA")]
        raise ErroValidacao(
            "O desligamento ainda não acabou. Falta: " + "; ".join(falta) + ".")
    s.execute(text("""
        UPDATE dp_solicitacoes SET situacao = 'ATENDIDA', atualizado_em = now()
         WHERE id = :s"""), {"s": sol["id"]})
    _evento(s, sol["id"], "PROCESSO_ENCERRADO",
            {"passos": [{"passo": p["rotulo"], "estado": p["estado"],
                         "detalhe": p["detalhe"]} for p in estado["passos"]]},
            usuario)
    if sol.get("colaborador_id"):
        registrar_evento(s, "colaborador", int(sol["colaborador_id"]),
                         "DESLIGAMENTO_ENCERRADO", {"numero": sol.get("numero")},
                         usuario.id if usuario else None)


# ---------------------------------------------------------------------------
# A fila do que está em andamento — para a tela e para o vigia
# ---------------------------------------------------------------------------
def em_andamento(s: Session, *, limite: int = 200) -> list[dict[str, Any]]:
    """Os desligamentos em andamento, cada um com o próximo passo.

    ⚑ CUSTO: uma consulta para a lista e, por pedido, duas pequenas (as marcas
    e o pedido à contabilidade). Com o volume do DP (dezenas por mês, não
    milhares) é o preço de não manter um "estado" gravado que divergiria do
    que o sistema enxerga.
    """
    linhas = s.execute(text("""
        SELECT d.id, d.numero, d.situacao, d.colaborador_id, d.dados,
               d.criado_em, d.atendido_em, COALESCE(c.nome, d.nome_informado) AS pessoa,
               o.codigo AS obra
          FROM dp_solicitacoes d
          LEFT JOIN colaboradores c ON c.id = d.colaborador_id
          LEFT JOIN obras o ON o.id = d.obra_id
         WHERE d.tipo = 'DESLIGAMENTO' AND d.situacao = :sit
         ORDER BY d.atendido_em NULLS LAST, d.id
         LIMIT :lim"""), {"sit": EM_ANDAMENTO, "lim": limite}).mappings().all()
    saida = []
    for l in linhas:
        estado = passos(s, dict(l))
        atrasado = any(p.get("atrasado") for p in estado["passos"])
        continuidade = next((p for p in estado["passos"]
                             if p["codigo"] == "CONTINUIDADE"), None)
        a_validar = any(p.get("a_validar") for p in estado["passos"])
        saida.append({
            "id": l["id"], "numero": l["numero"], "pessoa": l["pessoa"] or "",
            "colaborador_id": l["colaborador_id"], "obra": l["obra"] or "",
            "cumpridos": estado["cumpridos"], "total": estado["total"],
            "proximo": estado["proximo"], "pode_encerrar": estado["pode_encerrar"],
            "atrasado": atrasado,
            "assinada_a_validar": a_validar,
            "continuidade_a_confirmar": bool(
                continuidade and continuidade["estado"] == "PENDENTE"),
            "dias": ((date.today() - l["atendido_em"].date()).days
                     if l["atendido_em"] else None)})
    return saida
