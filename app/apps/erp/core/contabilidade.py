# -*- coding: utf-8 -*-
"""
OS PEDIDOS FORMAIS À CONTABILIDADE — com prazo, cobrança e retorno esperado.

Desenho inteiro em `AGENTE_DP.md`. O que este arquivo resolve, nas palavras
dele (05/11/2026):

> *"quem é carteira assinada, CTPS, a gente precisa fazer contato com a
> contabilidade para ela fazer o registro formal no eSocial. No Pipe a gente tem
> uns modelos de informações, de e-mails, que a gente dispara a partir dele (…)
> a gente solicita férias, a gente solicita rescisão, a gente solicita admissão,
> e isso a gente dispara através de e-mails para eles lá, e a gente precisa
> fazer isso também dentro do ERP."*

**O QUE MUDA EM RELAÇÃO AO PIPEFY, e é só isto, mas é tudo:** lá o pedido era um
e-mail que saía e desaparecia. Aqui o pedido é um REGISTRO com três coisas que o
e-mail não tem:

  1. **o que se espera de volta** — o tipo de documento, pelo nome do catálogo;
  2. **até quando** — o prazo em dias úteis, e as cobranças depois dele;
  3. **se o que voltou está certo** — a lista de campos que têm de bater com o
     cadastro.

Sem as três, "pedir" é mandar e esquecer, e foi isso que custou os documentos que
aparecem faltando no dia do pagamento.

**O TEXTO DO E-MAIL É GUARDADO, não recalculado.** O template muda com o tempo;
o pedido de março tem de continuar mostrando o que foi mandado em março. Isto
não é zelo de arquivo: é o que permite discutir com a contabilidade o que foi
pedido, em vez de discutir o que o sistema hoje acha que pediria.
"""
from __future__ import annotations

import logging
import re
from datetime import date, timedelta
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.apps.erp.core.cadastros import pessoas as svc_pessoas
from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import Colaborador, Usuario

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# OS TEMPLATES — os que ele listou, com as palavras dele
#
# `exige`: o que o CADASTRO precisa ter para o pedido poder sair. Pedido de
#   admissão sem PIS volta para o DP em vez de sair pela metade, porque o e-mail
#   incompleto custa dois dias de ida e volta com a contabilidade.
# `espera`: o documento que fecha o pedido (código do catálogo do Arquivo).
# `prazo`: dias ÚTEIS até a primeira cobrança.
# `conferir`: os campos que têm de bater no documento que voltar. É a lista que
#   a rotina de conferência usa, e a que impede ficha de registro com salário
#   diferente do combinado passar batida.
# ---------------------------------------------------------------------------
TEMPLATES: dict[str, dict[str, Any]] = {
    "ASSINATURA_CTPS": {
        "nome": "Assinatura de CTPS (admissão)",
        "explicacao": "o registro formal no eSocial de quem foi admitido",
        # ⚑ Sem o PIS desde 19/11/2026: o eSocial é feito com o CPF (ele).
        "exige": ("nome", "cpf", "data_nascimento", "cargo", "salario",
                  "admissao", "empresa"),
        "espera": "FICHA-REGISTRO",
        "prazo": 2, "cobrancas": 3, "intervalo": 3,
        "conferir": ("nome", "cpf", "cargo", "salario", "admissao"),
        "etapa_ao_concluir": "CONCLUIDO",
    },
    "ALTERACAO_CARGO": {
        "nome": "Alteração de cargo e salário",
        "explicacao": "mudança de função, de salário ou os dois",
        "exige": ("nome", "cpf", "cargo", "salario"),
        "espera": "ALTERACAO-CONTRATO",
        "prazo": 3, "cobrancas": 3, "intervalo": 3,
        "conferir": ("nome", "cpf", "cargo", "salario"),
        "etapa_ao_concluir": "",
    },
    "FERIAS": {
        "nome": "Solicitação de férias",
        "explicacao": "o período de férias, para a contabilidade emitir o aviso",
        "exige": ("nome", "cpf", "admissao"),
        "espera": "AVISO-FERIAS",
        "prazo": 3, "cobrancas": 3, "intervalo": 3,
        "conferir": ("nome", "cpf"),
        "etapa_ao_concluir": "",
    },
    # ⚑ 20/11/2026: alterar e cancelar férias também passam por ela — é ela
    # quem refaz (ou desfaz) o aviso e o registro no eSocial.
    "ALTERACAO_FERIAS": {
        "nome": "Alteração de férias",
        "explicacao": "nova data ou novos dias para férias já comunicadas",
        "exige": ("nome", "cpf"),
        "espera": "AVISO-FERIAS",
        "prazo": 2, "cobrancas": 3, "intervalo": 2,
        "conferir": ("nome", "cpf"),
        "etapa_ao_concluir": "",
    },
    "CANCELAMENTO_FERIAS": {
        "nome": "Cancelamento de férias",
        "explicacao": "desfazer férias já comunicadas",
        "exige": ("nome", "cpf"),
        "espera": "AVISO-FERIAS",
        "prazo": 1, "cobrancas": 3, "intervalo": 1,
        "conferir": ("nome", "cpf"),
        "etapa_ao_concluir": "",
    },
    # ⚑ 20/11/2026: o afastamento e o retorno vão à contabilidade (eSocial).
    "AFASTAMENTO": {
        "nome": "Afastamento",
        "explicacao": "INSS, acidente de trabalho ou maternidade, para o eSocial",
        "exige": ("nome", "cpf"),
        "espera": "ALTERACAO-CONTRATO",
        # Acidente de trabalho: a CAT tem prazo de 1 dia útil.
        "prazo": 1, "cobrancas": 3, "intervalo": 1,
        "conferir": ("nome", "cpf"),
        "etapa_ao_concluir": "",
    },
    "TRANSFERENCIA_OBRA": {
        "nome": "Transferência de obra",
        "explicacao": "a pessoa mudou de obra — muda a lotação das guias de FGTS e tributos",
        "exige": ("nome", "cpf"),
        "espera": "ALTERACAO-CONTRATO",
        "prazo": 3, "cobrancas": 2, "intervalo": 3,
        "conferir": ("nome", "cpf"),
        "etapa_ao_concluir": "",
    },
    "RETORNO_AFASTAMENTO": {
        "nome": "Retorno de afastamento",
        "explicacao": "a volta ao trabalho, para o eSocial",
        "exige": ("nome", "cpf"),
        "espera": "ALTERACAO-CONTRATO",
        "prazo": 2, "cobrancas": 2, "intervalo": 2,
        "conferir": ("nome", "cpf"),
        "etapa_ao_concluir": "",
    },
    "DESCONTO_FOLHA": {
        "nome": "Desconto em folha",
        "explicacao": "o desconto autorizado pelo colaborador, na folha",
        "exige": ("nome", "cpf"),
        "espera": "ALTERACAO-CONTRATO",
        "prazo": 3, "cobrancas": 2, "intervalo": 3,
        "conferir": ("nome", "cpf"),
        "etapa_ao_concluir": "",
    },
    # ⚑ A CORREÇÃO E O CANCELAMENTO DO QUE JÁ FOI (20/11/2026, noite) — ver
    # `contabil_processo.comunicar_correcao`. Ele: *"Preciso comunicar a
    # correção, preciso ter confirmar que há a ciência daquela alteração."*
    "RETIFICACAO": {
        "nome": "Correção de informação já enviada",
        "explicacao": "o que mudou num pedido que a contabilidade já recebeu",
        "exige": ("nome", "cpf"),
        "espera": "ALTERACAO-CONTRATO",
        "prazo": 1, "cobrancas": 3, "intervalo": 1,
        "conferir": ("nome", "cpf"),
        "etapa_ao_concluir": "",
    },
    "CANCELAMENTO_SOLICITACAO": {
        "nome": "Cancelamento de pedido já enviado",
        "explicacao": "desfazer um pedido que a contabilidade já recebeu",
        "exige": ("nome", "cpf"),
        "espera": "ALTERACAO-CONTRATO",
        "prazo": 1, "cobrancas": 3, "intervalo": 1,
        "conferir": ("nome", "cpf"),
        "etapa_ao_concluir": "",
    },
    "DEPENDENTE": {
        "nome": "Cadastro de dependente",
        "explicacao": "incluir dependente para salário-família e imposto de renda",
        "exige": ("nome", "cpf"),
        "espera": "ALTERACAO-CONTRATO",
        "prazo": 3, "cobrancas": 2, "intervalo": 5,
        "conferir": ("nome", "cpf"),
        "etapa_ao_concluir": "",
    },
    "CONJUGE": {
        "nome": "Cadastro de cônjuge",
        "explicacao": "incluir o cônjuge no cadastro da pessoa",
        "exige": ("nome", "cpf"),
        "espera": "ALTERACAO-CONTRATO",
        "prazo": 3, "cobrancas": 2, "intervalo": 5,
        "conferir": ("nome", "cpf"),
        "etapa_ao_concluir": "",
    },
    "DESLIGAMENTO": {
        "nome": "Desligamento de colaborador",
        "explicacao": "a rescisão, e a documentação rescisória de volta",
        # A DOCUMENTAÇÃO RESCISÓRIA NÃO É UM TEMPLATE SEPARADO, e é ele quem
        # disse por quê: *"tem solicitação de documentação rescisória, mas isso
        # aqui fica dentro do processo de demissão."* Ela é o RETORNO esperado
        # deste pedido — e por isso o desligamento só fecha quando ela chega.
        "exige": ("nome", "cpf", "admissao", "ultimo_dia", "forma_desligamento"),
        "espera": "RESCISAO",
        "prazo": 5, "cobrancas": 3, "intervalo": 3,
        "conferir": ("nome", "cpf", "admissao", "data_saida"),
        "etapa_ao_concluir": "DESLIGADO",
    },
    # ⚑ A SOLICITAÇÃO DIVERSA, EM TEXTO (26/11/2026). Ele: *"é algo que é
    # muito raro de acontecer. Permitir que a solicitação diversa
    # opcionalmente seja encaminhada à contabilidade com a demanda, textual
    # mesmo (…) E o DP edita o cadastro da pessoa manualmente. E essa opção
    # serve para outras possibilidades também que venham a surgir."* (O caso
    # que puxou: reajuste só de salário, que não tem tipo próprio.) O e-mail
    # leva o texto do pedido; o retorno é a resposta da contabilidade.
    "DIVERSOS": {
        "nome": "Solicitação diversa",
        "explicacao": "o texto do pedido, para o que não tem modelo próprio",
        "exige": ("nome", "cpf"),
        # O que volta é o documento do que foi feito (no reajuste, a alteração
        # contratual) — o mesmo das correções e cancelamentos, que também são
        # texto. O DP pode concluir à mão se a resposta for só o e-mail.
        "espera": "ALTERACAO-CONTRATO",
        "prazo": 3, "cobrancas": 3, "intervalo": 3,
        "conferir": ("nome", "cpf"),
        "etapa_ao_concluir": "",
    },
    "CANCELAMENTO_DESLIGAMENTO": {
        "nome": "Cancelamento de desligamento",
        "explicacao": "desfazer um desligamento já comunicado",
        "exige": ("nome", "cpf"),
        "espera": "ALTERACAO-CONTRATO",
        # PRAZO CURTO DE PROPÓSITO: cancelamento que chega depois do
        # processamento da rescisão não cancela nada — virou acerto. Um dia.
        "prazo": 1, "cobrancas": 3, "intervalo": 1,
        "conferir": ("nome", "cpf"),
        "etapa_ao_concluir": "",
    },
}

SITUACOES = ("RASCUNHO", "ENVIADO", "COBRADO", "RESPONDIDO", "DIVERGENTE",
             "CONCLUIDO", "CANCELADO")
# O NOME QUE A PESSOA LÊ (23/11/2026). Ele: *"como o que foi mandado, se nem
# foi mandado ainda (…) E aí a situação rascunho. Não fica estranho?"*
SITUACAO_ROTULOS = {
    "RASCUNHO": "Ainda não enviado", "ENVIADO": "Enviado — esperando a contabilidade",
    "COBRADO": "Cobrado — esperando a contabilidade",
    "RESPONDIDO": "A contabilidade respondeu", "DIVERGENTE": "Voltou diferente do cadastro",
    "CONCLUIDO": "Concluído", "CANCELADO": "Cancelado",
}
# Em que situações o pedido ainda espera algo de FORA.
ESPERANDO = ("ENVIADO", "COBRADO")
ABERTAS = ("RASCUNHO", "ENVIADO", "COBRADO", "RESPONDIDO", "DIVERGENTE")


def proximo_numero(s: Session) -> str:
    n = s.execute(text("SELECT nextval('dp_pedido_numero_seq')")).scalar()
    return f"CT-{int(n):05d}"


def dias_uteis_depois(inicio: date, dias: int) -> date:
    """Prazo em dias ÚTEIS. Contabilidade não responde no sábado, e prazo que
    vence no fim de semana gera cobrança na segunda de manhã por um atraso que
    não existiu."""
    d, faltam = inicio, max(int(dias or 0), 0)
    while faltam > 0:
        d += timedelta(days=1)
        if d.weekday() < 5:
            faltam -= 1
    return d


# ---------------------------------------------------------------------------
# O QUE FALTA NO CADASTRO PARA O PEDIDO PODER SAIR
# ---------------------------------------------------------------------------
_ROTULOS_EXIGIDOS = {
    "nome": "nome completo", "cpf": "CPF", "data_nascimento": "data de nascimento",
    "pis": "nº do PIS", "cargo": "cargo", "salario": "salário",
    "admissao": "data de admissão", "empresa": "empresa de registro",
    "ultimo_dia": "último dia trabalhado", "forma_desligamento": "forma do desligamento",
}


def carteira_que_saiu(s: Session, c: Colaborador):
    """O vínculo de CARTEIRA que fechou, quando a pessoa saiu dela e continua
    como prestador — ou None.

    ⚑ ACHADO DIRIGINDO A TELA (19/11/2026): confirmada a continuidade, o
    cadastro passa a dizer o que vale HOJE — prestador, sem último dia, com a
    admissão do prestador. E a rescisão pedida DEPOIS disso (o caso comum: a
    contabilidade é chamada depois do último dia) saía com a admissão errada,
    e nem saía: faltava o "último dia". Para a rescisão, o que vale é a
    carteira que fechou, e ela está no histórico de vínculos.
    """
    from app.apps.erp.db.models.cadastros import ColaboradorVinculo
    from sqlalchemy import select as _select
    if (c.vinculo or "CTPS").upper() == "CTPS":
        return None      # ainda de carteira, ou desligamento comum: o cadastro diz tudo
    return s.scalars(_select(ColaboradorVinculo).where(
        ColaboradorVinculo.colaborador_id == c.id,
        ColaboradorVinculo.vinculo == "CTPS",
        ColaboradorVinculo.demissao.is_not(None)).order_by(
        ColaboradorVinculo.demissao.desc())).first()


def o_que_falta_para_pedir(s: Session, template: str,
                           c: Colaborador,
                           pedido: Optional[dict[str, Any]] = None) -> list[str]:
    """PURA o bastante para a tela avisar ANTES de alguém apertar enviar."""
    regra = TEMPLATES.get(template)
    if regra is None:
        raise ErroValidacao("Tipo de pedido desconhecido.")
    falta = []
    saiu = carteira_que_saiu(s, c) if template == "DESLIGAMENTO" else None
    for campo in regra["exige"]:
        if saiu is not None and campo in ("admissao", "ultimo_dia"):
            if not (getattr(saiu, campo, None) or saiu.demissao):
                falta.append(_ROTULOS_EXIGIDOS.get(campo, campo))
            continue
        if campo == "cargo":
            tem = c.cargo_id
        elif campo == "empresa":
            tem = c.empresa_id or c.cnpj_admissao
        elif campo == "forma_desligamento":
            v = svc_pessoas.vinculo_aberto(s, c.id)
            # ⚑ 23/11/2026: a FORMA QUE O PEDIDO TRAZ também vale. Quem foi
            # importado sem histórico de vínculo não tem onde a forma ficar
            # gravada, e a rescisão travava com "falta a forma do
            # desligamento" — com a forma escrita na própria solicitação.
            tem = ((v.forma_desligamento if v else None) or _ultima_forma(s, c.id)
                   or str((pedido or {}).get("forma_desligamento") or "").strip())
        else:
            tem = getattr(c, campo, None)
        if not tem:
            falta.append(_ROTULOS_EXIGIDOS.get(campo, campo))
    return falta


def avisos_do_pedido(s: Session, template: str, c: Colaborador) -> list[str]:
    """O que é estranho, mas NÃO impede o pedido.

    A diferença entre isto e `o_que_falta_para_pedir` é a que importa: falta é
    dado que o outro lado precisa e não tem, e trava; aviso é coisa que merece
    um olhar e pode estar certa. Férias antes de doze meses existe (coletivas,
    proporcional), e travar aqui impediria o caso legítimo.
    """
    avisos: list[str] = []
    if template == "FERIAS":
        v = svc_pessoas.vinculo_aberto(s, c.id)
        inicio = (v.admissao if v else None) or c.admissao
        if inicio:
            dias = (date.today() - inicio).days
            if dias < 365:
                avisos.append(
                    f"{c.nome} tem {dias} dias de vínculo — menos que o período "
                    "aquisitivo de 12 meses. Confira se são férias coletivas ou "
                    "proporcionais.")
    if template == "ASSINATURA_CTPS" and c.admissao:
        atraso = (date.today() - c.admissao).days
        if atraso > 5:
            avisos.append(
                f"a admissão foi há {atraso} dias e o registro só está sendo "
                "pedido agora — o eSocial tem prazo.")
    return avisos


def _ultima_forma(s: Session, colaborador_id: int) -> Optional[str]:
    return s.execute(text("""
        SELECT forma_desligamento FROM colaborador_vinculos
         WHERE colaborador_id = :c AND forma_desligamento IS NOT NULL
         ORDER BY COALESCE(demissao, admissao) DESC LIMIT 1"""),
        {"c": colaborador_id}).scalar()


# ---------------------------------------------------------------------------
# O TEXTO DO E-MAIL — montado do cadastro, guardado como foi mandado
# ---------------------------------------------------------------------------
def _linha(rotulo: str, valor: Any) -> str:
    return f"{rotulo}: {valor}" if valor not in (None, "") else ""


def montar_mensagem(s: Session, template: str, c: Colaborador,
                    dados: Optional[dict[str, Any]] = None,
                    *, numero: str = "") -> tuple[str, str]:
    """(assunto, corpo). Em português, com os dados que a contabilidade precisa.

    O corpo é texto simples de propósito: ele vai ser lido, respondido e às
    vezes colado num sistema do outro lado. HTML bonito atrapalha as três.
    """
    from app.apps.erp.db.models.cadastros import Cargo, Convencao, Empresa, Obra

    regra = TEMPLATES[template]
    d = dados or {}
    cargo = s.get(Cargo, c.cargo_id) if c.cargo_id else None
    conv = s.get(Convencao, c.convencao_id) if c.convencao_id else None
    obra = s.get(Obra, c.obra_id) if c.obra_id else None
    empresa = s.get(Empresa, c.empresa_id) if c.empresa_id else None

    def dia(v) -> str:
        return v.strftime("%d/%m/%Y") if v else ""

    # Na rescisão de quem saiu da carteira e continua como prestador, a
    # admissão, o salário e as datas são os da CARTEIRA (ver `carteira_que_saiu`).
    saiu = carteira_que_saiu(s, c) if template == "DESLIGAMENTO" else None
    admissao = saiu.admissao if saiu is not None else c.admissao
    salario = saiu.salario if saiu is not None else c.salario
    aviso = (saiu.aviso_previo if saiu is not None else None) or c.aviso_previo
    ultimo = (saiu.ultimo_dia if saiu is not None else None) or c.ultimo_dia
    saida = (saiu.demissao if saiu is not None else None) or c.demissao

    cabeca = [
        _linha("Nome", c.nome),
        _linha("CPF", c.cpf),
        _linha("Data de nascimento", dia(c.data_nascimento)),
        _linha("PIS/NIS", c.pis),
        _linha("CTPS", f"{c.ctps_numero or ''} série {c.ctps_serie or ''}".strip()),
        _linha("Empresa de registro", empresa.razao_social if empresa else c.cnpj_admissao),
        _linha("Obra", f"{obra.codigo} — {obra.nome}" if obra else ""),
        _linha("Cargo", cargo.nome if cargo else ""),
        _linha("Convenção", conv.nome if conv else ""),
        _linha("Salário", f"R$ {salario:,.2f}".replace(",", "X").replace(".", ",")
               .replace("X", ".") if salario is not None else ""),
        _linha("Admissão", dia(admissao)),
    ]
    miolo: list[str] = []
    if template == "ASSINATURA_CTPS":
        miolo = [
            "Solicitamos o registro formal da admissão abaixo no eSocial.",
            "", _linha("Tipo de contrato", svc_pessoas.TIPO_CONTRATO_ROTULOS.get(
                c.tipo_contrato or "", c.tipo_contrato or "")),
            _linha("Contrato de experiência", c.contrato_experiencia),
        ]
    elif template == "ALTERACAO_CARGO":
        miolo = ["Solicitamos a alteração de contrato abaixo.", "",
                 _linha("Cargo novo", d.get("cargo_novo") or (cargo.nome if cargo else "")),
                 _linha("Convenção", d.get("convencao_nova") or (conv.nome if conv else "")),
                 _linha("Salário novo", d.get("salario_novo")),
                 _linha("A partir de", d.get("vigencia")),
                 _linha("Motivo", d.get("motivo"))]
    elif template == "FERIAS":
        miolo = ["Solicitamos a emissão do aviso e do recibo de férias.", "",
                 _linha("Início", d.get("inicio")),
                 _linha("Dias", d.get("dias")),
                 _linha("Abono pecuniário", d.get("abono")),
                 _linha("Adiantamento do 13º", d.get("adiantamento_13"))]
    elif template == "DEPENDENTE":
        miolo = ["Solicitamos a inclusão do dependente abaixo.", "",
                 _linha("Nome do dependente", d.get("dependente_nome")),
                 _linha("Nascimento", d.get("dependente_nascimento")),
                 _linha("CPF", d.get("dependente_cpf")),
                 _linha("Parentesco", d.get("parentesco")),
                 _linha("Salário-família", d.get("salario_familia")),
                 _linha("Dependente de IR", d.get("dependente_ir"))]
    elif template == "CONJUGE":
        miolo = ["Solicitamos a inclusão do cônjuge abaixo.", "",
                 _linha("Nome", d.get("conjuge_nome")),
                 _linha("Nascimento", d.get("conjuge_nascimento")),
                 _linha("CPF", d.get("conjuge_cpf"))]
    elif template == "DESLIGAMENTO":
        miolo = ["Solicitamos a rescisão do contrato de trabalho abaixo, e o "
                 "envio da documentação rescisória.", "",
                 _linha("Forma do desligamento",
                        svc_pessoas.FORMA_DESLIGAMENTO_ROTULOS.get(
                            d.get("forma_desligamento") or "",
                            d.get("forma_desligamento") or "")),
                 _linha("Aviso prévio", dia(aviso)),
                 _linha("Último dia trabalhado", dia(ultimo)),
                 # ⚑ QUEM CONTINUA COMO PRESTADOR não tem data de saída no
                 # cadastro (ela cortaria o ponto — ver
                 # `pessoas.agendar_saida_da_carteira`): a data da rescisão
                 # vem do pedido de desligamento.
                 _linha("Data de saída", dia(saida) or dia(
                     svc_pessoas._data(d.get("data_saida"), "data de saída"))),
                 _linha("Continua trabalhando como prestador de serviço",
                        "sim" if (str(d.get("continua_prestador") or "") == "1"
                                  or saiu is not None) else ""),
                 _linha("Faltas a descontar", d.get("faltas"))]
    elif template == "CANCELAMENTO_DESLIGAMENTO":
        miolo = ["Solicitamos o CANCELAMENTO do desligamento comunicado "
                 "anteriormente para o colaborador abaixo. Ele CONTINUA "
                 "trabalhando: nenhuma rescisão deve ser processada.", "",
                 _linha("Desligamento comunicado em", d.get("comunicado_em")),
                 _linha("Motivo do cancelamento", d.get("motivo")),
                 "", "Por favor, confirme a ciência respondendo este e-mail."]
    # ⚑ 20/11/2026 (noite): os modelos que faltavam texto — sem isto a
    # alteração e o cancelamento de férias saíam só com os dados da pessoa,
    # sem dizer o que se pedia.
    elif template == "ALTERACAO_FERIAS":
        miolo = ["Solicitamos a ALTERAÇÃO das férias do colaborador abaixo.", "",
                 _linha("Férias como estavam (início)", d.get("inicio_anterior")),
                 _linha("Dias como estavam", d.get("dias_anteriores")),
                 _linha("Novo início", d.get("inicio")),
                 _linha("Novos dias", d.get("dias")),
                 _linha("Abono pecuniário", d.get("abono")),
                 _linha("Adiantamento do 13º", d.get("adiantamento_13")),
                 _linha("Motivo", d.get("motivo")),
                 "", "Por favor, devolva o aviso de férias refeito."]
    elif template == "CANCELAMENTO_FERIAS":
        miolo = ["Solicitamos o CANCELAMENTO das férias do colaborador abaixo.", "",
                 _linha("Início das férias canceladas", d.get("inicio_anterior")),
                 _linha("Dias", d.get("dias_anteriores")),
                 _linha("Motivo", d.get("motivo")),
                 "", "Por favor, confirme a ciência respondendo este e-mail."]
    elif template == "AFASTAMENTO":
        miolo = ["Comunicamos o AFASTAMENTO do colaborador abaixo, para as "
                 "providências no eSocial e na folha.", "",
                 _linha("Natureza", d.get("natureza")),
                 _linha("Início", d.get("inicio")),
                 _linha("Previsão de retorno", d.get("previsao")),
                 _linha("O que aconteceu", d.get("motivo"))]
        if d.get("acidente"):
            miolo += ["", "ACIDENTE DE TRABALHO: por favor, emitam a CAT — o "
                          "prazo é de 1 dia útil (Lei 8.213/91, art. 22)."]
        miolo += ["", "Por favor, confirmem o recebimento e o que for registrado."]
    elif template == "TRANSFERENCIA_OBRA":
        miolo = ["Comunicamos a TRANSFERÊNCIA DE OBRA do colaborador abaixo — "
                 "muda a lotação para as guias de FGTS e tributos.", "",
                 _linha("Obra de origem", d.get("obra_de")),
                 _linha("Obra de destino", d.get("obra_para")),
                 _linha("A partir de", d.get("a_partir_de")),
                 _linha("Observação", d.get("motivo")),
                 "", "Por favor, confirmem o recebimento e a alteração."]
    elif template == "RETORNO_AFASTAMENTO":
        miolo = ["Comunicamos o RETORNO ao trabalho do colaborador abaixo.", "",
                 _linha("Voltou em", d.get("retorno")),
                 _linha("Observação", d.get("motivo")),
                 "", "Por favor, confirmem o recebimento."]
    elif template == "DESCONTO_FOLHA":
        miolo = ["Solicitamos o lançamento do desconto em folha abaixo. A "
                 "autorização assinada pelo colaborador segue anexa.", "",
                 _linha("Valor total", d.get("valor_total")),
                 _linha("Parcelas", d.get("parcelas")),
                 _linha("Valor da parcela", d.get("valor_parcela")),
                 _linha("A partir da competência", d.get("primeira_competencia")),
                 _linha("Referente a", d.get("motivo"))]
    elif template == "RETIFICACAO":
        miolo = ["Pedimos a CORREÇÃO de uma informação já enviada a vocês.", "",
                 _linha("Pedido corrigido", d.get("pedido_original")),
                 _linha("O que mudou (de → para)", d.get("o_que_mudou")),
                 "", "Por favor, confirme a ciência e o que for refeito "
                     "respondendo este e-mail."]
    elif template == "DIVERSOS":
        miolo = ["Solicitamos o que segue:", "", str(d.get("descricao") or ""),
                 "", "Por favor, confirme o que for feito respondendo este e-mail."]
    elif template == "CANCELAMENTO_SOLICITACAO":
        miolo = ["Pedimos o CANCELAMENTO de um pedido já enviado a vocês: "
                 "nada dele deve ser processado.", "",
                 _linha("Pedido cancelado", d.get("pedido_original")),
                 _linha("Motivo", d.get("motivo")),
                 "", "Por favor, confirme a ciência respondendo este e-mail."]
    if template == "DESLIGAMENTO" and d.get("aviso_assinado"):
        miolo.append(_linha("Aviso prévio assinado", d.get("aviso_assinado")
                            + " (segue anexo)"))

    corpo = "\n".join([x for x in miolo if x is not None]
                      + ["", "--- Dados do colaborador ---"]
                      + [x for x in cabeca if x]
                      + ["", _linha("Observação", d.get("observacao"))]
                      + ["", "Mensagem gerada pelo ERP da BWS Construções."])
    # O NÚMERO VAI NO ASSUNTO, ENTRE COLCHETES, e isto não é enfeite: é por ele
    # que a resposta da contabilidade é casada com o pedido quando o cliente de
    # e-mail dela não devolve o In-Reply-To. Formato fixo `[SDP-000001]` (ou
    # `[CT-00001]`, sem solicitação), porque quem vai procurar é uma expressão
    # regular, não uma pessoa.
    assunto = f"[{numero}] {regra['nome']} — {c.nome} ({c.cpf})"
    return assunto, "\n".join([l for l in corpo.split("\n")]).replace("\n\n\n", "\n\n")


# ---------------------------------------------------------------------------
# O CICLO DO PEDIDO: abrir → enviar → cobrar → receber → conferir → concluir
# ---------------------------------------------------------------------------
def _evento(s: Session, pedido_id: int, tipo: str, texto_: str = "",
            dados: Optional[dict] = None, usuario: Optional[Usuario] = None,
            pelo_agente: bool = False) -> None:
    import json
    s.execute(text("""
        INSERT INTO dp_pedido_eventos (pedido_id, tipo, texto, dados,
                                       usuario_id, pelo_agente)
        VALUES (:p, :t, :x, CAST(:d AS jsonb), :u, :a)"""),
        {"p": pedido_id, "t": tipo, "x": texto_ or None,
         "d": json.dumps(dados or {}, ensure_ascii=False, default=str),
         "u": usuario.id if usuario else None, "a": pelo_agente})


# ⚑ UM NÚMERO SÓ (26/11/2026). Ele: *"a tela única e número único, sim (…) como
# é que ficariam os que já saíram da outra forma (…) você tem que manter o
# casamento."* O e-mail que nasce de uma solicitação leva no assunto o número
# DELA — `[SDP-000003]`, e `[SDP-000003/2]` para o segundo e-mail da mesma
# solicitação (uma retificação, um cancelamento). O CT continua existindo por
# dentro (é a chave do pedido), e os e-mails que já saíram com `[CT-00012]`
# continuam casando: a caixa de entrada procura as duas marcas.
MARCA_NO_ASSUNTO = re.compile(r"\[((?:SDP|CT)-\d{4,6}(?:/\d{1,2})?)\]")


def marca_para_o_email(s: Session, solicitacao_id: Optional[int], numero_ct: str) -> str:
    """A marca do assunto de um e-mail NOVO: o número da solicitação (com /2, /3
    a partir do segundo e-mail dela) ou, sem solicitação, o CT."""
    if not solicitacao_id:
        return numero_ct
    sdp = s.execute(text("SELECT numero FROM dp_solicitacoes WHERE id = :i"),
                    {"i": solicitacao_id}).scalar()
    if not sdp:
        return numero_ct
    ja = int(s.execute(text("SELECT count(*) FROM dp_pedidos WHERE solicitacao_id = :i"),
                       {"i": solicitacao_id}).scalar() or 0)
    return sdp if ja == 0 else f"{sdp}/{ja + 1}"


def marca_do_pedido(p: dict[str, Any]) -> str:
    """A marca que saiu no assunto deste pedido (SDP… ou o CT dos antigos)."""
    m = MARCA_NO_ASSUNTO.search(p.get("assunto") or "")
    return m.group(1) if m else (p.get("numero") or "")


def abrir(s: Session, dados: dict[str, Any], usuario: Optional[Usuario],
          *, pelo_agente: bool = False) -> dict[str, Any]:
    """Cria o pedido em RASCUNHO. Não manda nada — enviar é outro passo.

    Separar abrir de enviar não é burocracia: é o que permite o DP montar o
    pedido, ver o texto que vai sair e corrigir antes de a contabilidade ler.
    """
    import json

    template = str(dados.get("template") or "").strip().upper()
    if template not in TEMPLATES:
        raise ErroValidacao(f"Tipo de pedido desconhecido: {template or '(vazio)'}")
    regra = TEMPLATES[template]
    cid = int(dados["colaborador_id"]) if dados.get("colaborador_id") else None
    c = s.get(Colaborador, cid) if cid else None
    if c is None:
        raise ErroValidacao("Escolha a pessoa do pedido.")

    # ⚑ A RESCISÃO VAI COM O AVISO ASSINADO (20/11/2026). Ele: *"O DP
    # verifica e manda para frente, para a contabilidade. Já com o aviso
    # assinado, datado."*
    if template == "DESLIGAMENTO" and dados.get("solicitacao_id"):
        from app.apps.erp.core import desligamento_processo as svc_desl
        motivo_ = svc_desl.aviso_pendente(s, int(dados["solicitacao_id"]))
        if motivo_:
            raise ErroValidacao(motivo_)
        assinado = svc_desl.aviso_assinado(s, int(dados["solicitacao_id"]))
        if assinado:
            dados = {**dados, "dados": {**(dados.get("dados") or {}),
                                        "aviso_assinado": assinado}}
    falta = o_que_falta_para_pedir(s, template, c, dados.get("dados") or {})
    if falta:
        # RECUSA ANTES DE SAIR PELA METADE. É a economia da rotina R3 do
        # `AGENTE_DP.md`: e-mail incompleto custa dois dias de ida e volta.
        raise ErroValidacao(
            f"Para pedir “{regra['nome']}” falta no cadastro de {c.nome}: "
            + ", ".join(falta) + ".")

    pedido = {k: v for k, v in (dados.get("dados") or {}).items()
              if v not in (None, "")}
    numero = proximo_numero(s)
    marca = marca_para_o_email(
        s, int(dados["solicitacao_id"]) if dados.get("solicitacao_id") else None, numero)
    assunto, corpo = montar_mensagem(s, template, c, pedido, numero=marca)
    linha = s.execute(text("""
        INSERT INTO dp_pedidos (numero, template, situacao, colaborador_id,
                                solicitacao_id, empresa_id, assunto, corpo,
                                dados, documento_esperado, aberto_por)
        VALUES (:n, :t, 'RASCUNHO', :c, :sol, :emp, :a, :co,
                CAST(:d AS jsonb), :esp, :u)
        RETURNING id, numero"""),
        {"n": numero, "t": template, "c": c.id,
         "sol": int(dados["solicitacao_id"]) if dados.get("solicitacao_id") else None,
         "emp": c.empresa_id, "a": assunto, "co": corpo,
         "d": json.dumps(pedido, ensure_ascii=False, default=str),
         "esp": regra["espera"], "u": usuario.id if usuario else None}).one()
    _evento(s, linha.id, "ABERTO", f"pedido {regra['nome']} criado",
            {"template": template}, usuario, pelo_agente)
    registrar_evento(s, "dp_pedido", linha.id, "ABERTO",
                     {"numero": numero, "template": template,
                      "colaborador": c.nome}, usuario.id if usuario else None)
    avisos = avisos_do_pedido(s, template, c)
    for a in avisos:
        _evento(s, linha.id, "AVISO", a, {}, usuario, pelo_agente)
    # ⚑ O QUADRO ACOMPANHA O PEDIDO (19/11/2026): pedido o registro da
    # carteira, o cartão sai da Inicial e vai para "Contabilidade" — a etapa
    # mostra onde a pessoa está sem ninguém ter de arrastar.
    if template == "ASSINATURA_CTPS" and (c.etapa or "").upper() == "CADASTRO_INICIAL":
        try:
            svc_pessoas.mover_etapa(s, c.id, "NA_CONTABILIDADE", usuario,
                                    automatico=True,
                                    motivo=f"registro da carteira pedido ({numero})")
        except ErroValidacao as e:
            _evento(s, linha.id, "AVISO", f"a etapa não mudou: {e}", {}, usuario,
                    pelo_agente)
    s.flush()
    return {"id": linha.id, "numero": linha.numero, "assunto": assunto,
            "corpo": corpo, "avisos": avisos}


def _documentos_assinados(s: Session, p: dict[str, Any]) -> list[dict[str, Any]]:
    """O que a pessoa assinou nesta solicitação (o aviso prévio, a autorização
    de desconto) vai anexo ao e-mail. Uma leitura do arquivo por documento —
    no Drive, uma ida ao Google cada; costuma ser um só."""
    sid = p.get("solicitacao_id")
    if not sid:
        return []
    try:
        from app.apps.erp.core import termos
        from app.apps.erp.core.documentos.armazenamento import conteudo_de, obter
        if not termos.tem_tabela(s):
            return []
        saida = []
        for l in s.execute(text("""
            SELECT t.numero, t.tipo, d.anexo_id FROM dp_termos t
              JOIN documentos d ON d.id = t.documento_id
             WHERE t.solicitacao_id = :s AND t.situacao IN ('ASSINADO', 'RECUSADO')
             ORDER BY t.id"""), {"s": sid}).mappings().all():
            a = obter(s, int(l["anexo_id"]))
            saida.append({"nome": f"{l['tipo'].lower()}_{l['numero']}.pdf"
                          if (a.mime_type or "").endswith("pdf") else a.nome_arquivo,
                          "mime": a.mime_type or "application/pdf",
                          "conteudo": conteudo_de(s, a)})
        return saida
    except Exception:  # noqa: BLE001 — sem o anexo o e-mail sai e o corpo avisa
        logger.exception("ERP/contabilidade: não anexei o documento assinado")
        return []


# ---------------------------------------------------------------------------
# O RASCUNHO NA TELA DE ATENDIMENTO (23/11/2026)
#
# Ele: *"Eu clico [montar], aí eu vou para outra tela ainda de contabilidade
# para poder enviar (…) A definição é para ser aqui, na mesma tela de
# atendimento (…) se for um e-mail (…) é para eu ler o e-mail, ver se eu
# concordo, para poder enviar (…) permitir até anexar alguma coisa."*
#
# O rascunho se lê, se corrige, ganha anexo e sai — dali mesmo. Depois de
# enviado o texto não muda mais: é o que foi mandado.
# ---------------------------------------------------------------------------
def _exigir_rascunho(s: Session, pedido_id: int) -> dict[str, Any]:
    p = ler(s, pedido_id)
    if p["situacao"] != "RASCUNHO":
        raise ErroValidacao(f"O pedido {p['numero']} já foi enviado — o texto "
                            "fica como foi mandado.")
    return p


def editar_rascunho(s: Session, pedido_id: int, *, assunto: str, corpo: str,
                    usuario: Optional[Usuario]) -> dict[str, Any]:
    p = _exigir_rascunho(s, pedido_id)
    assunto, corpo = str(assunto or "").strip(), str(corpo or "").strip()
    if not assunto or not corpo:
        raise ErroValidacao("O assunto e o texto do e-mail não podem ficar vazios.")
    if assunto == (p["assunto"] or "") and corpo == (p["corpo"] or ""):
        return {"mudou": False}
    s.execute(text("UPDATE dp_pedidos SET assunto = :a, corpo = :c WHERE id = :i"),
              {"a": assunto, "c": corpo, "i": pedido_id})
    _evento(s, pedido_id, "EDITADO", "o DP corrigiu o texto antes de enviar",
            usuario=usuario)
    return {"mudou": True}


def anexos_extras(p: dict[str, Any]) -> list[dict[str, Any]]:
    return list(((p.get("dados") or {}).get("anexos_extras")) or [])


def anexar_ao_rascunho(s: Session, pedido_id: int, *, conteudo: bytes,
                       nome_arquivo: str, usuario: Optional[Usuario]) -> dict[str, Any]:
    """Um arquivo a mais no e-mail. Fica também no cadastro da pessoa."""
    import json
    from app.apps.erp.core.arquivo import service as svc_arq
    p = _exigir_rascunho(s, pedido_id)
    if not conteudo:
        raise ErroValidacao("Escolha o arquivo.")
    if len(conteudo) > 15 * 1024 * 1024:
        raise ErroValidacao("Arquivo acima de 15 MB — o e-mail não sairia.")
    doc = svc_arq.arquivar(
        s, conteudo, nome_arquivo or "anexo", tipo_codigo="DOC-PESSOA-OUTRO",
        colaborador_id=p.get("colaborador_id"),
        referencia=f"{p['numero']} — anexo do pedido à contabilidade",
        observacao=f"anexado ao pedido {p['numero']}", usuario=usuario)
    extras = anexos_extras(p) + [{"documento_id": doc.id,
                                  "nome": nome_arquivo or "anexo"}]
    s.execute(text("""UPDATE dp_pedidos SET dados = dados || CAST(:d AS jsonb)
                       WHERE id = :i"""),
              {"d": json.dumps({"anexos_extras": extras}), "i": pedido_id})
    _evento(s, pedido_id, "ANEXO", f"anexado: {nome_arquivo}", usuario=usuario)
    return {"anexos_extras": extras}


def tirar_anexo(s: Session, pedido_id: int, documento_id: int,
                usuario: Optional[Usuario]) -> dict[str, Any]:
    """Tira do e-mail; o arquivo continua no cadastro da pessoa."""
    import json
    p = _exigir_rascunho(s, pedido_id)
    extras = [x for x in anexos_extras(p) if int(x["documento_id"]) != int(documento_id)]
    s.execute(text("""UPDATE dp_pedidos SET dados = dados || CAST(:d AS jsonb)
                       WHERE id = :i"""),
              {"d": json.dumps({"anexos_extras": extras}), "i": pedido_id})
    return {"anexos_extras": extras}


def anexos_que_vao(s: Session, p: dict[str, Any]) -> list[str]:
    """Os NOMES do que vai junto — para a tela mostrar antes de enviar, sem
    baixar arquivo nenhum."""
    nomes = []
    from app.apps.erp.core import termos
    if p.get("solicitacao_id") and termos.tem_tabela(s):
        for l in s.execute(text("""
            SELECT t.numero, t.tipo FROM dp_termos t
             WHERE t.solicitacao_id = :s AND t.situacao IN ('ASSINADO', 'RECUSADO')
               AND t.documento_id IS NOT NULL ORDER BY t.id"""),
                {"s": p["solicitacao_id"]}).mappings().all():
            nomes.append(f"{l['tipo'].lower()}_{l['numero']}.pdf (assinado pela pessoa)")
    return nomes + [x["nome"] for x in anexos_extras(p)]


def _anexos_extras_para_envio(s: Session, p: dict[str, Any]) -> list[dict[str, Any]]:
    from app.apps.erp.core.documentos.armazenamento import conteudo_de, obter
    saida = []
    for x in anexos_extras(p):
        doc = s.execute(text("SELECT anexo_id FROM documentos WHERE id = :i"),
                        {"i": int(x["documento_id"])}).scalar()
        if not doc:
            continue
        a = obter(s, int(doc))
        saida.append({"nome": x.get("nome") or a.nome_arquivo,
                      "mime": a.mime_type or "application/octet-stream",
                      "conteudo": conteudo_de(s, a)})
    return saida


def enviar(s: Session, pedido_id: int, usuario: Optional[Usuario], *,
           para: Optional[list[str]] = None,
           pelo_agente: bool = False, _reenviar: bool = False) -> dict[str, Any]:
    """Manda o e-mail pela conta da empresa e marca o prazo.

    NUNCA LEVANTA POR FALHA DE ENVIO — o módulo de e-mail devolve o registro com
    o motivo, e o pedido fica em RASCUNHO com o erro escrito. Pedido que
    "sumiu" por servidor fora do ar seria pior que pedido que não saiu.
    """
    from app.apps.erp.core.comum import email as svc_email
    from app.apps.erp.db.models.cadastros import Empresa

    p = ler(s, pedido_id)
    reenvio = p["situacao"] in ("ENVIADO", "COBRADO", "RESPONDIDO", "DIVERGENTE")
    if p["situacao"] != "RASCUNHO" and not (reenvio and _reenviar):
        raise ErroValidacao(
            f"O pedido {p['numero']} já foi enviado (está "
            f"{p['situacao'].lower()}).")
    destinos = [e.strip() for e in (para or p["para"] or endereco_padrao(s)) if e.strip()]
    if not destinos:
        raise ErroValidacao(
            "Não há e-mail da contabilidade cadastrado. Preencha em "
            "Pessoal › Contabilidade, no bloco “Para onde vão os pedidos”, "
            "antes de enviar.")
    # ⚑ O PEDIDO SAI PELA CONTA DO DP (18/11/2026), não mais pela conta da
    # empresa. Ele, ao perguntar como isto estava sendo tratado:
    #
    #   *"O que eu vou encaminhar a partir do DP é uma coisa, é um e-mail. O
    #    que eu vou encaminhar a partir do suprimento é outro e-mail. (…) o DP,
    #    por enquanto, eu quero um e-mail geral."*
    #
    # Antes disto, o pedido à contabilidade e a COTAÇÃO AO FORNECEDOR saíam do
    # mesmo endereço (a conta da empresa) e a resposta dos dois voltava para a
    # mesma caixa — e é a caixa do DP que o agente lê, para casar resposta com
    # pedido. Resposta de fornecedor caindo lá era ruído garantido.
    #
    # ⚑ A VOLTA PARA A CONTA DA EMPRESA FICA, e é o que impede isto de parar o
    # DP: enquanto a conta do DP não estiver configurada, o pedido continua
    # saindo como saía. Sem essa volta, publicar isto deixaria o DP sem
    # conseguir enviar até alguém preencher uma tela nova — e ninguém avisaria.
    from app.apps.erp.core.comum import email_sistema
    empresa = email_sistema.conta(s, "DP")
    if empresa is None:
        empresa = (s.get(Empresa, p["empresa_id"]) if p["empresa_id"]
                   else svc_email.conta_de_envio(s, None))
    if empresa is None:
        raise ErroValidacao(
            "Não há conta de e-mail para enviar o pedido. Configure o e-mail "
            "do DP em Pessoal › Contabilidade, ou a conta da empresa no "
            "cadastro de Empresas.")

    regra = TEMPLATES[p["template"]]
    registro = svc_email.enviar(
        s, empresa=empresa, para=destinos, assunto=p["assunto"],
        corpo=p["corpo"], entidade_tipo="dp_pedido", entidade_id=pedido_id,
        usuario=usuario, destinatario_tipo="contabilidade",
        anexos=_documentos_assinados(s, p) + _anexos_extras_para_envio(s, p),
        guardar_nos_enviados=True)
    deu = (registro.situacao == "ENVIADO")
    copia = getattr(registro, "copia_nos_enviados", None)
    # ⚑ DE QUAL CONTA SAIU (23/11/2026). Ele: *"Que caixa da empresa é essa?
    # (…) No sistema não ficou claro."* A trilha passa a dizer o remetente: a
    # conta do DP, ou a da empresa quando a do DP não está configurada.
    from email.utils import parseaddr as _parse
    de = _parse(svc_email.remetente_de(empresa))[1] or "?"
    if deu and _reenviar:
        _evento(s, pedido_id, "REENVIADO",
                f"o mesmo e-mail enviado de novo, de {de} para {', '.join(destinos)}"
                + (f" ({copia})" if copia else ""),
                {"para": destinos, "de": de}, usuario, pelo_agente)
        s.flush()
        return {"enviado": True, "erro": None, "copia_nos_enviados": copia,
                "prazo": p.get("prazo")}
    if deu:
        prazo = dias_uteis_depois(date.today(), regra["prazo"])
        s.execute(text("""
            UPDATE dp_pedidos SET situacao = 'ENVIADO', para = :para,
                   prazo = :prazo, message_id = COALESCE(:mid, message_id),
                   atualizado_em = now() WHERE id = :id"""),
            {"para": destinos, "prazo": prazo, "id": pedido_id,
             "mid": registro.message_id})
        _evento(s, pedido_id, "ENVIADO",
                f"e-mail enviado de {de} para {', '.join(destinos)}; "
                f"retorno esperado até {prazo:%d/%m/%Y}"
                + (f" ({copia})" if copia else ""),
                {"para": destinos, "de": de, "prazo": str(prazo)}, usuario, pelo_agente)
    else:
        _evento(s, pedido_id, "FALHA_ENVIO", registro.erro or "não saiu",
                {"para": destinos}, usuario, pelo_agente)
    s.flush()
    return {"enviado": deu, "erro": registro.erro, "copia_nos_enviados": copia,
            "prazo": None if not deu else str(dias_uteis_depois(
                date.today(), regra["prazo"]))}


def reenviar(s: Session, pedido_id: int, usuario: Optional[Usuario]) -> dict[str, Any]:
    """O MESMO e-mail, de novo (23/11/2026). Ele: *"se de repente eu precisar
    reenviar o e-mail, eu não tenho essa opção."* Mesmo texto, mesmos anexos,
    mesmos destinatários; o prazo de volta não muda — reenviar não é cobrar."""
    p = ler(s, pedido_id)
    if p["situacao"] not in ("ENVIADO", "COBRADO", "RESPONDIDO", "DIVERGENTE"):
        raise ErroValidacao(
            f"O pedido {p['numero']} está {p['situacao_rotulo'].lower()} — "
            + ("use Enviar." if p["situacao"] == "RASCUNHO" else "não há o que reenviar."))
    return enviar(s, pedido_id, usuario, _reenviar=True)


# A chave do parâmetro, na tabela `parametros` que o ERP já usa para tudo que é
# configuração de tela (pasta do Drive, etc.) — nada de envvar nova.
CHAVE_EMAILS = "contabilidade_emails"


def endereco_padrao(s: Session) -> list[str]:
    """O e-mail da contabilidade, de Configurações. LISTA, porque contabilidade
    costuma ter mais de uma pessoa no assunto, e mandar só para uma faz o
    pedido parar nas férias dela."""
    from app.apps.erp.db.models.cadastros import Parametro
    linha = s.get(Parametro, CHAVE_EMAILS)
    bruto = (linha.valor if linha is not None else "") or ""
    return [e.strip() for e in bruto.replace(";", ",").split(",") if e.strip()]


# Um e-mail de verdade, sem inventar RFC: tem arroba, tem ponto depois do
# arroba, e não tem espaço. Serve para pegar o engano de digitação, que é o
# que acontece de verdade — "contabilidade@escritorio" e "fulano@gmail,com".
_PARECE_EMAIL = re.compile(r"^[^@\s,;]+@[^@\s,;]+\.[A-Za-z]{2,}$")


def definir_enderecos(s: Session, texto: str,
                      usuario: Optional[Usuario] = None) -> list[str]:
    """Grava o e-mail da contabilidade, de Configurações.

    ACEITA MAIS DE UM, separado por vírgula ou ponto e vírgula: contabilidade
    costuma ter mais de uma pessoa no assunto, e mandar só para uma faz o
    pedido parar nas férias dela.

    Confere o feitio de cada endereço ANTES de gravar. Endereço torto só
    apareceria semanas depois, num pedido que a contabilidade nunca recebeu e
    que o ERP ficou cobrando de ninguém.
    """
    from app.apps.erp.db.models.cadastros import Parametro

    brutos = [e.strip() for e in (texto or "").replace(";", ",").split(",")]
    enderecos: list[str] = []
    for e in brutos:
        if not e:
            continue
        if not _PARECE_EMAIL.match(e):
            raise ErroValidacao(
                f"“{e}” não parece um e-mail. Confira: é o endereço para onde "
                f"o ERP manda os pedidos à contabilidade.")
        if e.lower() not in [x.lower() for x in enderecos]:
            enderecos.append(e)

    antes = endereco_padrao(s)
    valor = ", ".join(enderecos)
    linha = s.get(Parametro, CHAVE_EMAILS)
    if linha is None:
        s.add(Parametro(chave=CHAVE_EMAILS, valor=valor))
    else:
        linha.valor = valor
    # QUEM MUDOU FICA REGISTRADO: trocar este endereço desvia para onde vai
    # TODO pedido novo, e um desvio silencioso seria descoberto tarde.
    #
    # `entidade_id` é NÚMERO na tabela de eventos, e parâmetro é identificado
    # por texto — então a chave vai no detalhe e o id fica 0. Tentar passar a
    # chave no lugar do número explode no banco; foi o que o teste pegou.
    registrar_evento(s, "parametro", 0, "ALTERADO",
                     {"chave": CHAVE_EMAILS, "antes": antes,
                      "depois": enderecos},
                     usuario.id if usuario else None)
    s.flush()
    return enderecos


def cobrar(s: Session, pedido_id: int, usuario: Optional[Usuario], *,
           pelo_agente: bool = False) -> dict[str, Any]:
    """Cobra o retorno. Conta a cobrança e respeita o teto do template.

    O teto existe porque cobrar para sempre é ruído, e ruído faz a
    contabilidade parar de ler. Quando acaba, ESCALA — para de cobrar e avisa,
    que é diferente de desistir em silêncio.
    """
    from app.apps.erp.core.comum import email as svc_email
    from app.apps.erp.db.models.cadastros import Empresa

    p = ler(s, pedido_id)
    if p["situacao"] not in ESPERANDO:
        raise ErroValidacao(
            f"O pedido {p['numero']} não está esperando retorno "
            f"(está {p['situacao'].lower()}).")
    regra = TEMPLATES[p["template"]]
    if p["cobrancas"] >= regra["cobrancas"]:
        s.execute(text("UPDATE dp_pedidos SET escalado_em = now(), "
                       "atualizado_em = now() WHERE id = :id"), {"id": pedido_id})
        _evento(s, pedido_id, "ESCALADO",
                f"{p['cobrancas']} cobranças sem retorno — parei de cobrar e "
                "avisei quem decide", {}, usuario, pelo_agente)
        s.flush()
        return {"cobrou": False, "escalou": True}

    # A COBRANÇA SAI PELA MESMA CONTA DO ENVIO (a do DP, com volta para a da
    # empresa). Cobrar de um endereço diferente do que mandou o pedido quebra a
    # thread do outro lado — e quebra também o casamento da resposta aqui, que
    # é feito pelo `In-Reply-To`.
    from app.apps.erp.core.comum import email_sistema
    empresa = email_sistema.conta(s, "DP")
    if empresa is None:
        empresa = (s.get(Empresa, p["empresa_id"]) if p["empresa_id"]
                   else svc_email.conta_de_envio(s, None))
    if empresa is None:
        raise ErroValidacao(
            "Não há conta de e-mail para cobrar. Configure o e-mail do DP em "
            "Pessoal › Contabilidade.")
    vez = p["cobrancas"] + 1
    corpo = (
        f"Prezados,\n\n"
        f"Reiteramos o pedido {p['marca']} — {regra['nome']} — enviado "
        f"em {p['criado_em'][:10].split('-')[::-1] and ''}"
        f"{p['criado_em'][8:10]}/{p['criado_em'][5:7]}/{p['criado_em'][0:4]}, "
        f"cujo prazo era {p['prazo'] or '—'}.\n\n"
        f"Ainda não recebemos o retorno esperado "
        f"({_rotulo_documento(s, regra['espera'])}).\n\n"
        f"Segue abaixo o pedido original.\n\n"
        f"{'=' * 50}\n{p['corpo']}")
    registro = svc_email.enviar(
        s, empresa=empresa, para=p["para"] or endereco_padrao(s),
        assunto=f"[{vez}ª cobrança] {p['assunto']}", corpo=corpo,
        entidade_tipo="dp_pedido", entidade_id=pedido_id, usuario=usuario,
        destinatario_tipo="contabilidade")
    if registro.situacao != "ENVIADO":
        _evento(s, pedido_id, "FALHA_ENVIO",
                f"cobrança não saiu: {registro.erro}", {}, usuario, pelo_agente)
        s.flush()
        return {"cobrou": False, "erro": registro.erro}
    proximo = dias_uteis_depois(date.today(), regra["intervalo"])
    s.execute(text("""
        UPDATE dp_pedidos SET situacao = 'COBRADO', cobrancas = cobrancas + 1,
               cobrado_em = now(), prazo = :prazo, atualizado_em = now()
         WHERE id = :id"""), {"prazo": proximo, "id": pedido_id})
    _evento(s, pedido_id, "COBRADO", f"{vez}ª cobrança enviada",
            {"vez": vez, "proximo_prazo": str(proximo)}, usuario, pelo_agente)
    s.flush()
    return {"cobrou": True, "vez": vez, "proximo_prazo": str(proximo)}


def _rotulo_documento(s: Session, codigo: str) -> str:
    nome = s.execute(text("SELECT nome FROM documento_tipos WHERE codigo = :c"),
                     {"c": codigo}).scalar()
    return nome or codigo


# ---------------------------------------------------------------------------
# O RETORNO: receber, CONFERIR e concluir
#
# É o *"finalize aquele procedimento"* dele — e a parte que ninguém faz hoje. A
# conferência compara o documento que voltou com o cadastro, campo por campo:
# ficha de registro com salário diferente do combinado é um problema que
# aparece meses depois, no eSocial ou na rescisão, e custa caro.
# ---------------------------------------------------------------------------
def _comparar(s: Session, p: dict[str, Any], c: Colaborador,
              lido: dict[str, Any]) -> list[dict[str, str]]:
    """O de/para do que não bate. Campo que o documento não trouxe NÃO é
    divergência — é ausência, e acusar ausência faria toda conferência falhar."""
    from app.apps.erp.db.models.cadastros import Cargo

    regra = TEMPLATES[p["template"]]
    cargo = s.get(Cargo, c.cargo_id) if c.cargo_id else None
    saiu = carteira_que_saiu(s, c) if p["template"] == "DESLIGAMENTO" else None

    def no_erp(campo: str) -> str:
        if campo == "cargo":
            return cargo.nome if cargo else ""
        if campo == "salario":
            return f"{c.salario:.2f}" if c.salario is not None else ""
        if campo in ("admissao", "data_saida"):
            v = c.admissao if campo == "admissao" else c.demissao
            if p["template"] == "DESLIGAMENTO" and saiu is not None:
                v = saiu.admissao if campo == "admissao" else saiu.demissao
            if v is None and campo == "data_saida":
                # Quem continua como prestador: a data está no pedido.
                return str((p.get("dados") or {}).get("data_saida") or "")
            return v.isoformat() if v else ""
        return str(getattr(c, campo, "") or "")

    def comparavel(campo: str, valor: Any) -> str:
        import re as _re
        t = str(valor or "").strip()
        if campo in ("cpf",):
            return _re.sub(r"\D", "", t)
        if campo == "salario":
            d = _re.sub(r"[^\d]", "", t)
            return d.lstrip("0")
        if campo in ("admissao", "data_saida"):
            try:
                return str(svc_pessoas._data(t, campo) or "")
            except ErroValidacao:
                return t
        return svc_pessoas._sem_acento(t)

    fora = []
    for campo in regra["conferir"]:
        veio = lido.get(campo)
        if veio in (None, ""):
            continue
        aqui = no_erp(campo)
        if comparavel(campo, veio) != comparavel(campo, aqui):
            fora.append({"campo": campo,
                         "no_erp": aqui or "—",
                         "no_documento": str(veio)})
    return fora


def receber(s: Session, pedido_id: int, usuario: Optional[Usuario], *,
            documento_id: Optional[int] = None,
            lido: Optional[dict[str, Any]] = None,
            pelo_agente: bool = False) -> dict[str, Any]:
    """A contabilidade respondeu. Confere o que voltou e decide.

    Três saídas, e nenhuma delas é "concluir mesmo assim":
      · bate tudo        → RESPONDIDO, pronto para concluir;
      · não bate         → DIVERGENTE, com o de/para campo por campo;
      · nada para ler    → RESPONDIDO sem conferência, e a tela diz isso.

    `lido` é o que se extraiu do documento. Quando vem vazio, o pedido fica
    RESPONDIDO **sem conferência** — e a tela diz "confira à mão", em vez de
    fingir que conferiu.
    """
    import json

    p = ler(s, pedido_id)
    if p["situacao"] in ("CONCLUIDO", "CANCELADO"):
        raise ErroValidacao(f"O pedido {p['numero']} já está "
                            f"{p['situacao'].lower()}.")
    c = s.get(Colaborador, p["colaborador_id"])
    if c is None:
        raise ErroValidacao("Colaborador do pedido não encontrado.")
    fora = _comparar(s, p, c, lido or {})
    situacao = "DIVERGENTE" if fora else "RESPONDIDO"
    s.execute(text("""
        UPDATE dp_pedidos SET situacao = :sit, documento_id = :doc,
               respondido_em = now(), divergencias = CAST(:div AS jsonb),
               atualizado_em = now() WHERE id = :id"""),
        {"sit": situacao, "doc": documento_id,
         "div": json.dumps(fora, ensure_ascii=False), "id": pedido_id})
    # ⚑ 26/11/2026: "os dados batem" só quando os dados foram LIDOS do
    # documento. Sem leitura (o retorno pela caixa de entrada), a frase era
    # falsa — dizia que conferiu sem ter conferido.
    _evento(s, pedido_id, situacao,
            ("o documento voltou — o conteúdo ainda não foi conferido pelo sistema"
             if not lido else
             "o documento voltou e os dados batem com o cadastro" if not fora
             else "o documento voltou com " + str(len(fora))
             + " dado(s) diferente(s) do cadastro"),
            {"conferido": bool(lido), "divergencias": fora}, usuario, pelo_agente)
    s.flush()
    return {"situacao": situacao, "divergencias": fora,
            "conferido": bool(lido)}


def receber_arquivo(s: Session, pedido_id: int, *, conteudo: bytes, nome_arquivo: str,
                    usuario: Optional[Usuario]) -> dict[str, Any]:
    """O retorno que chegou POR FORA da caixa de entrada (26/11/2026).

    Mesmo caminho do anexo que a caixa lê: arquiva na pessoa com o tipo que o
    pedido espera e passa pela conferência do agente — que lê, compara e, se
    bate e o tipo permite, conclui. Com a conferência desligada, só registra o
    retorno, e a tela pede a conferência à mão.
    """
    from app.apps.erp.core import conferencia_retorno
    from app.apps.erp.core.arquivo import service as svc_arq

    p = ler(s, pedido_id)
    if p["situacao"] in ("CONCLUIDO", "CANCELADO", "RASCUNHO"):
        raise ErroValidacao(f"O pedido {p['numero']} está "
                            f"{(p.get('situacao_rotulo') or p['situacao']).lower()} — "
                            "não espera retorno.")
    doc = svc_arq.arquivar(
        s, conteudo, nome_arquivo,
        tipo_codigo=p.get("documento_esperado") or "DOC-PESSOA-OUTRO",
        colaborador_id=p["colaborador_id"],
        referencia=f"{p['numero']} — retorno da contabilidade",
        origem="TELA", observacao="anexado pelo DP no pedido", usuario=usuario)
    return conferencia_retorno.conferir_e_talvez_concluir(
        s, pedido_id, doc.id, conteudo, nome_arquivo, usuario)


def concluir(s: Session, pedido_id: int, usuario: Optional[Usuario], *,
             motivo: str = "", pelo_agente: bool = False) -> dict[str, Any]:
    """Fecha o pedido e move a etapa do cadastro, quando o template manda.

    **Pedido DIVERGENTE não fecha por aqui** — e isto é regra, não zelo: fechar
    com divergência é carimbar como certo um documento que não bate, e é
    exatamente o que ninguém pega hoje. Para fechar, ou o cadastro é corrigido
    e o retorno reconferido, ou alguém marca a divergência como aceita com o
    motivo escrito.
    """
    p = ler(s, pedido_id)
    if p["situacao"] == "CONCLUIDO":
        return {"ja_estava": True}
    if p["situacao"] == "DIVERGENTE":
        raise ErroValidacao(
            f"O pedido {p['numero']} está com divergência entre o documento e o "
            "cadastro. Corrija o cadastro e confira de novo, ou aceite a "
            "divergência com o motivo — concluir assim carimbaria como certo um "
            "documento que não bate.")
    if p["situacao"] not in ("RESPONDIDO", "ENVIADO", "COBRADO"):
        raise ErroValidacao(f"O pedido {p['numero']} está "
                            f"{p['situacao'].lower()} e não pode ser concluído.")
    regra = TEMPLATES[p["template"]]
    s.execute(text("""
        UPDATE dp_pedidos SET situacao = 'CONCLUIDO', concluido_em = now(),
               concluido_por = :u, atualizado_em = now() WHERE id = :id"""),
        {"u": usuario.id if usuario else None, "id": pedido_id})
    _evento(s, pedido_id, "CONCLUIDO", motivo or "procedimento finalizado",
            {}, usuario, pelo_agente)

    etapa = regra.get("etapa_ao_concluir") or ""
    moveu = ""
    if etapa == "DESLIGADO":
        # ⚑ A RESCISÃO VOLTAR NÃO DESLIGA QUEM AINDA NÃO SAIU (19/11/2026).
        # Quem decide a saída é a DATA (`pessoas.ja_saiu`), e a virada do dia
        # cumpre. Mover para "Desligado" aqui tiraria do ponto quem cumpre
        # aviso prévio — e quem saiu da carteira e CONTINUA como prestador,
        # que nem data de saída tem no cadastro.
        c = s.get(Colaborador, p["colaborador_id"])
        if c is None or not svc_pessoas.ja_saiu(c.demissao):
            _evento(s, pedido_id, "AVISO",
                    "pedido concluído; o cadastro não foi para Desligado porque "
                    "a data de saída ainda não passou (ou a pessoa continua "
                    "como prestador)", {}, usuario, pelo_agente)
            etapa = ""
    if etapa:
        c = s.get(Colaborador, p["colaborador_id"])
        if c is not None and (c.etapa or "").upper() != etapa:
            try:
                svc_pessoas.mover_etapa(
                    s, c.id, etapa, usuario, automatico=True,
                    motivo=f"retorno da contabilidade no pedido {p['numero']}")
                moveu = etapa
            except ErroValidacao as e:
                # O caminho da etapa não permitir o salto NÃO desfaz a conclusão
                # do pedido: o documento chegou, e isso é verdade
                # independentemente de onde o cadastro está. Fica dito na trilha.
                _evento(s, pedido_id, "AVISO",
                        f"pedido concluído, mas a etapa não mudou: {e}",
                        {}, usuario, pelo_agente)
    registrar_evento(s, "dp_pedido", pedido_id, "CONCLUIDO",
                     {"numero": p["numero"], "etapa": moveu},
                     usuario.id if usuario else None)
    # ⚑ A SOLICITAÇÃO QUE ESPERAVA POR ESTE RETORNO FECHA (20/11/2026) —
    # alteração de cargo, dependente, férias, cancelamento de desligamento.
    from app.apps.erp.core import contabil_processo
    contabil_processo.fechar_pelo_pedido(s, p, usuario)
    s.flush()
    return {"concluido": True, "etapa": moveu}


def aceitar_divergencia(s: Session, pedido_id: int, usuario: Optional[Usuario],
                        *, motivo: str) -> dict[str, Any]:
    """Alguém olhou a divergência e decidiu que está certa assim.

    Exige motivo com conteúdo: "ok" não explica nada a quem ler isso em um ano,
    e esta é justamente a decisão que alguém vai querer entender depois.
    """
    if len((motivo or "").strip()) < 5:
        raise ErroValidacao("Escreva por que a divergência está aceita.")
    p = ler(s, pedido_id)
    if p["situacao"] != "DIVERGENTE":
        raise ErroValidacao(f"O pedido {p['numero']} não está com divergência.")
    s.execute(text("UPDATE dp_pedidos SET situacao = 'RESPONDIDO', "
                   "atualizado_em = now() WHERE id = :id"), {"id": pedido_id})
    _evento(s, pedido_id, "DIVERGENCIA_ACEITA", motivo.strip(),
            {"divergencias": p["divergencias"]}, usuario)
    s.flush()
    return {"situacao": "RESPONDIDO"}


def cancelar(s: Session, pedido_id: int, usuario: Optional[Usuario], *,
             motivo: str) -> dict[str, Any]:
    if len((motivo or "").strip()) < 5:
        raise ErroValidacao("Escreva o motivo do cancelamento.")
    p = ler(s, pedido_id)
    if p["situacao"] in ("CONCLUIDO", "CANCELADO"):
        raise ErroValidacao(f"O pedido {p['numero']} já está "
                            f"{p['situacao'].lower()}.")
    s.execute(text("UPDATE dp_pedidos SET situacao = 'CANCELADO', "
                   "atualizado_em = now() WHERE id = :id"), {"id": pedido_id})
    _evento(s, pedido_id, "CANCELADO", motivo.strip(), {}, usuario)
    s.flush()
    return {"situacao": "CANCELADO"}


# ---------------------------------------------------------------------------
# Leitura
# ---------------------------------------------------------------------------
def ler(s: Session, pedido_id: int) -> dict[str, Any]:
    linha = s.execute(text("""
        SELECT p.*, c.nome AS colaborador_nome, c.cpf AS colaborador_cpf
          FROM dp_pedidos p
          LEFT JOIN colaboradores c ON c.id = p.colaborador_id
         WHERE p.id = :id"""), {"id": pedido_id}).mappings().first()
    if linha is None:
        from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado
        raise ErroNaoEncontrado("Pedido não encontrado.")
    d = dict(linha)
    regra = TEMPLATES.get(d["template"], {})
    d["template_nome"] = regra.get("nome", d["template"])
    # ⚑ NÚMERO ÚNICO (26/11/2026): o número que a contabilidade vê no assunto —
    # o da solicitação (SDP-000003) para o que nasceu depois de 26/11, o CT
    # para os antigos. É o que a tela mostra.
    d["marca"] = marca_do_pedido(d)
    d["documento_esperado_nome"] = _rotulo_documento(
        s, d.get("documento_esperado") or "")
    d["criado_em"] = str(d["criado_em"])
    d["prazo"] = str(d["prazo"]) if d.get("prazo") else None
    d["atrasado"] = bool(d.get("prazo") and d["situacao"] in ESPERANDO
                         and str(d["prazo"]) < date.today().isoformat())
    # A TRILHA, em ordem estável: em Postgres `now()` é o início da transação, e
    # dois eventos do mesmo pedido têm timestamp idêntico. Sem o `id` no
    # desempate, o histórico mostra "concluído" antes de "enviado".
    d["eventos"] = [{
        "tipo": e["tipo"], "texto": e["texto"] or "",
        "quando": str(e["criado_em"]), "quem": e["quem"] or "",
        "pelo_agente": e["pelo_agente"],
    } for e in s.execute(text("""
        SELECT e.tipo, e.texto, e.criado_em, e.pelo_agente, u.nome AS quem
          FROM dp_pedido_eventos e
          LEFT JOIN usuarios u ON u.id = e.usuario_id
         WHERE e.pedido_id = :id ORDER BY e.criado_em, e.id"""),
        {"id": pedido_id}).mappings().all()]
    # O que vai junto no e-mail (23/11/2026): a tela mostra antes de enviar.
    d["anexos"] = anexos_que_vao(s, d)
    d["anexos_extras"] = anexos_extras(d)
    d["situacao_rotulo"] = SITUACAO_ROTULOS.get(d["situacao"], d["situacao"])
    d["para_que_vai"] = list(d.get("para") or []) or list(endereco_padrao(s) or [])
    d.update(o_que_voltou(s, pedido_id, d.get("numero") or ""))
    return d


def o_que_voltou(s: Session, pedido_id: int, numero: str) -> dict[str, Any]:
    """⚑ O QUE A CONTABILIDADE RESPONDEU, À VISTA NO PEDIDO (26/11/2026). O dono:
    *"a gente clica (…) não vê o que é que a contabilidade respondeu, não vê o
    que é que ela anexou. Então meio que perde o sentido eu ter que de novo lá
    na caixa de entrada de e-mail, buscar a informação."* As respostas lidas
    pela caixa (com o texto, desde a migração 129) e os documentos que vieram
    nelas — que já ficam no Arquivo da pessoa."""
    from app.apps.erp.core.comum.email_entrada import _tem_coluna_corpo
    corpo = "corpo" if _tem_coluna_corpo(s) else "NULL AS corpo"
    respostas = [dict(r) for r in s.execute(text(f"""
        SELECT de, assunto, recebido_em, processado_em, situacao, anexos, {corpo}
          FROM email_entrada_mensagens WHERE pedido_id = :p
         ORDER BY processado_em DESC NULLS LAST, id DESC LIMIT 20"""),
        {"p": pedido_id}).mappings().all()]
    for r in respostas:
        for k in ("recebido_em", "processado_em"):
            r[k] = r[k].isoformat() if r.get(k) else None
    docs = [dict(r) for r in s.execute(text("""
        SELECT d.id, d.anexo_id, d.nome_padronizado AS nome, t.nome AS tipo
          FROM documentos d LEFT JOIN documento_tipos t ON t.codigo = d.tipo_codigo
         WHERE d.referencia LIKE :ref ORDER BY d.id"""),
        {"ref": f"{numero} — retorno%"}).mappings().all()] if numero else []
    return {"respostas": respostas, "documentos_que_voltaram": docs}


def listar(s: Session, *, situacao: str = "", atrasados: bool = False,
           colaborador_id: Optional[int] = None,
           obra_ids: Optional[list[int]] = None) -> list[dict[str, Any]]:
    """Os pedidos, recortados por obra para quem é preso a obra.

    O administrativo de obra acompanha o pedido que nasceu da requisição DELE —
    e é isso que acaba com o "e aí, saiu?" por WhatsApp.
    """
    onde = ["1 = 1"]
    par: dict[str, Any] = {}
    if situacao:
        if situacao == "ABERTOS":
            onde.append("p.situacao = ANY(:abertas)")
            par["abertas"] = list(ABERTAS)
        else:
            onde.append("p.situacao = :sit")
            par["sit"] = situacao.upper()
    if atrasados:
        onde.append("p.prazo < CURRENT_DATE AND p.situacao = ANY(:esperando)")
        par["esperando"] = list(ESPERANDO)
    if colaborador_id:
        onde.append("p.colaborador_id = :cid")
        par["cid"] = colaborador_id
    if obra_ids is not None:
        if not obra_ids:
            return []
        onde.append("c.obra_id = ANY(:obras)")
        par["obras"] = list(obra_ids)
    linhas = s.execute(text(f"""
        SELECT p.id, p.numero, p.template, p.situacao, p.prazo, p.cobrancas,
               COALESCE(substring(p.assunto from '\\[((SDP|CT)-[0-9]{{4,6}}(/[0-9]{{1,2}})?)\\]'),
                        p.numero) AS marca,
               p.criado_em, p.respondido_em, p.concluido_em,
               p.documento_esperado, p.divergencias,
               c.nome AS colaborador_nome, c.cpf AS colaborador_cpf,
               o.codigo AS obra
          FROM dp_pedidos p
          LEFT JOIN colaboradores c ON c.id = p.colaborador_id
          LEFT JOIN obras o ON o.id = c.obra_id
         WHERE {' AND '.join(onde)}
         ORDER BY (p.prazo IS NULL), p.prazo, p.id DESC
         LIMIT 500"""), par).mappings().all()
    hoje = date.today().isoformat()
    return [{
        **{k: (str(v) if k in ("prazo", "criado_em", "respondido_em",
                               "concluido_em") and v is not None else v)
           for k, v in dict(l).items()},
        "template_nome": TEMPLATES.get(l["template"], {}).get("nome", l["template"]),
        "atrasado": bool(l["prazo"] and l["situacao"] in ESPERANDO
                         and str(l["prazo"]) < hoje),
        "dias_atraso": ((date.today() - l["prazo"]).days
                        if l["prazo"] and str(l["prazo"]) < hoje else 0),
    } for l in linhas]


def templates_para_tela() -> list[dict[str, Any]]:
    return [{
        "chave": k, "nome": v["nome"], "explicacao": v["explicacao"],
        "espera": v["espera"], "prazo": v["prazo"],
        "cobrancas": v["cobrancas"], "intervalo": v["intervalo"],
        "conferir": list(v["conferir"]), "exige": list(v["exige"]),
    } for k, v in TEMPLATES.items()]
