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
        "exige": ("nome", "cpf", "data_nascimento", "pis", "cargo", "salario",
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


def o_que_falta_para_pedir(s: Session, template: str,
                           c: Colaborador) -> list[str]:
    """PURA o bastante para a tela avisar ANTES de alguém apertar enviar."""
    regra = TEMPLATES.get(template)
    if regra is None:
        raise ErroValidacao("Tipo de pedido desconhecido.")
    falta = []
    for campo in regra["exige"]:
        if campo == "cargo":
            tem = c.cargo_id or c.funcao_id
        elif campo == "empresa":
            tem = c.empresa_id or c.cnpj_admissao
        elif campo == "forma_desligamento":
            v = svc_pessoas.vinculo_aberto(s, c.id)
            tem = (v.forma_desligamento if v else None) or _ultima_forma(s, c.id)
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
        _linha("Salário", f"R$ {c.salario:,.2f}".replace(",", "X").replace(".", ",")
               .replace("X", ".") if c.salario is not None else ""),
        _linha("Admissão", dia(c.admissao)),
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
                 _linha("Aviso prévio", dia(c.aviso_previo)),
                 _linha("Último dia trabalhado", dia(c.ultimo_dia)),
                 _linha("Data de saída", dia(c.demissao)),
                 _linha("Faltas a descontar", d.get("faltas"))]
    elif template == "CANCELAMENTO_DESLIGAMENTO":
        miolo = ["Solicitamos o CANCELAMENTO do desligamento comunicado "
                 "anteriormente para o colaborador abaixo.", "",
                 _linha("Desligamento comunicado em", d.get("comunicado_em")),
                 _linha("Motivo do cancelamento", d.get("motivo"))]

    corpo = "\n".join([x for x in miolo if x is not None]
                      + ["", "--- Dados do colaborador ---"]
                      + [x for x in cabeca if x]
                      + ["", _linha("Observação", d.get("observacao"))]
                      + ["", "Mensagem gerada pelo ERP da BWS Construções."])
    # O NÚMERO VAI NO ASSUNTO, ENTRE COLCHETES, e isto não é enfeite: é por ele
    # que a resposta da contabilidade é casada com o pedido quando o cliente de
    # e-mail dela não devolve o In-Reply-To. Formato fixo `[CT-00001]`, porque
    # quem vai procurar é uma expressão regular, não uma pessoa.
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

    falta = o_que_falta_para_pedir(s, template, c)
    if falta:
        # RECUSA ANTES DE SAIR PELA METADE. É a economia da rotina R3 do
        # `AGENTE_DP.md`: e-mail incompleto custa dois dias de ida e volta.
        raise ErroValidacao(
            f"Para pedir “{regra['nome']}” falta no cadastro de {c.nome}: "
            + ", ".join(falta) + ".")

    pedido = {k: v for k, v in (dados.get("dados") or {}).items()
              if v not in (None, "")}
    numero = proximo_numero(s)
    assunto, corpo = montar_mensagem(s, template, c, pedido, numero=numero)
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
    s.flush()
    return {"id": linha.id, "numero": linha.numero, "assunto": assunto,
            "corpo": corpo, "avisos": avisos}


def enviar(s: Session, pedido_id: int, usuario: Optional[Usuario], *,
           para: Optional[list[str]] = None,
           pelo_agente: bool = False) -> dict[str, Any]:
    """Manda o e-mail pela conta da empresa e marca o prazo.

    NUNCA LEVANTA POR FALHA DE ENVIO — o módulo de e-mail devolve o registro com
    o motivo, e o pedido fica em RASCUNHO com o erro escrito. Pedido que
    "sumiu" por servidor fora do ar seria pior que pedido que não saiu.
    """
    from app.apps.erp.core.comum import email as svc_email
    from app.apps.erp.db.models.cadastros import Empresa

    p = ler(s, pedido_id)
    if p["situacao"] not in ("RASCUNHO",):
        raise ErroValidacao(
            f"O pedido {p['numero']} já foi enviado (está "
            f"{p['situacao'].lower()}).")
    destinos = [e.strip() for e in (para or p["para"] or endereco_padrao(s)) if e.strip()]
    if not destinos:
        raise ErroValidacao(
            "Não há e-mail da contabilidade cadastrado. Preencha em "
            "Pessoal › Contabilidade, no bloco “Para onde vão os pedidos”, "
            "antes de enviar.")
    empresa = (s.get(Empresa, p["empresa_id"]) if p["empresa_id"]
               else svc_email.conta_de_envio(s, None))
    if empresa is None:
        raise ErroValidacao(
            "Nenhuma empresa com conta de e-mail configurada para enviar.")

    regra = TEMPLATES[p["template"]]
    registro = svc_email.enviar(
        s, empresa=empresa, para=destinos, assunto=p["assunto"],
        corpo=p["corpo"], entidade_tipo="dp_pedido", entidade_id=pedido_id,
        usuario=usuario, destinatario_tipo="contabilidade")
    deu = (registro.situacao == "ENVIADO")
    if deu:
        prazo = dias_uteis_depois(date.today(), regra["prazo"])
        s.execute(text("""
            UPDATE dp_pedidos SET situacao = 'ENVIADO', para = :para,
                   prazo = :prazo, message_id = COALESCE(:mid, message_id),
                   atualizado_em = now() WHERE id = :id"""),
            {"para": destinos, "prazo": prazo, "id": pedido_id,
             "mid": registro.message_id})
        _evento(s, pedido_id, "ENVIADO",
                f"e-mail enviado para {', '.join(destinos)}; "
                f"retorno esperado até {prazo:%d/%m/%Y}",
                {"para": destinos, "prazo": str(prazo)}, usuario, pelo_agente)
    else:
        _evento(s, pedido_id, "FALHA_ENVIO", registro.erro or "não saiu",
                {"para": destinos}, usuario, pelo_agente)
    s.flush()
    return {"enviado": deu, "erro": registro.erro,
            "prazo": None if not deu else str(dias_uteis_depois(
                date.today(), regra["prazo"]))}


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

    empresa = (s.get(Empresa, p["empresa_id"]) if p["empresa_id"]
               else svc_email.conta_de_envio(s, None))
    if empresa is None:
        raise ErroValidacao("Nenhuma empresa com conta de e-mail configurada.")
    vez = p["cobrancas"] + 1
    corpo = (
        f"Prezados,\n\n"
        f"Reiteramos o pedido {p['numero']} — {regra['nome']} — enviado "
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

    def no_erp(campo: str) -> str:
        if campo == "cargo":
            return cargo.nome if cargo else ""
        if campo == "salario":
            return f"{c.salario:.2f}" if c.salario is not None else ""
        if campo in ("admissao", "data_saida"):
            v = c.admissao if campo == "admissao" else c.demissao
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
    _evento(s, pedido_id, situacao,
            ("o documento voltou e os dados batem com o cadastro" if not fora
             else "o documento voltou com " + str(len(fora))
             + " dado(s) diferente(s) do cadastro"),
            {"conferido": bool(lido), "divergencias": fora}, usuario, pelo_agente)
    s.flush()
    return {"situacao": situacao, "divergencias": fora,
            "conferido": bool(lido)}


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
    return d


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
