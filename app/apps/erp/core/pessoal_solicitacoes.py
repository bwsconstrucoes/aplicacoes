# -*- coding: utf-8 -*-
"""
A FILA DE SOLICITAÇÕES DO DP — o segundo pipe do Pipefy, dentro do ERP.

O pipe "SOLICITAÇÕES DP" tem 80.992 cards desde 2021 e 28 tipos de pedido. O
ponto do ERP já absorveu os de PONTO (atestado, declaração, licença, férias,
ajuste de batida, compensação, folga do banco); o que sobra é o ADMINISTRATIVO,
e é o que vive aqui.

DECISÃO DO DONO, 05/11/2026: *"todos os pedidos vão para o DP, e quem tiver
perfil do DP que vai aprovar."* **Uma etapa só** — diferente do ponto, onde
algumas coisas passam pelo encarregado. Não há cadeia para configurar, e isso é
simplificação de verdade: no pipe a autorização era um checklist chamado "você
não possui autorização para esta solicitação".

ABRIR é de quem está na obra (`solicitar_dp`); ATENDER é do DP (`atender_dp`).
A divisão vem do próprio pipe, que perguntava *"requisição solicitada por um
terceiro?"* porque o encarregado abre pedido para a equipe dele.

O QUE CADA TIPO FAZ AO SER ATENDIDO, e esta é a diferença que importa: no Pipefy
atender era mover o card e alguém ia mexer no cadastro à mão (ou num cenário do
Make). Aqui o atendimento **executa**: readmissão readmite, desligamento
desliga, alteração de cargo altera. O pedido e o efeito deixam de poder divergir.
"""
from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from app.apps.erp.core.cadastros import pessoas as svc_pessoas
from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import Colaborador, Usuario

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# OS TIPOS — os do pipe que sobraram para o administrativo
#
# `executa` diz o que o atendimento faz no cadastro. `campos` é o que o
# formulário pede, e vira o JSONB do pedido.
# ---------------------------------------------------------------------------
TIPOS: dict[str, dict[str, Any]] = {
    "DESLIGAMENTO": {
        "nome": "Desligamento de colaborador",
        "explicacao": "fecha o vínculo e tira o acesso ao ponto",
        "exige_pessoa": True, "executa": "desligar",
        "campos": ("data_saida", "aviso_previo", "ultimo_dia", "forma_desligamento",
                   "faltas_a_descontar", "observacao"),
        # Os ONZE itens do checklist do pipe, na ordem em que o DP faz.
        "checklist": (
            "Solicitação de aviso prévio ou encerramento de contrato",
            "Lançamento de faltas",
            "Recebimento da documentação rescisória",
            "Lançar a despesa do TRCT",
            "Lançar a SP da GRRF",
            "Impressão do TRCT",
            "Impressão da folha de ponto",
            "Malote físico ou digital",
            "Pagamento do TRCT",
            "Desativação no sistema de pagamento (após o pagamento)",
            "Desativação no sistema de ponto",
        ),
    },
    "READMISSAO": {
        "nome": "Readmissão de colaborador",
        "explicacao": "traz a pessoa de volta sem refazer o cadastro",
        "exige_pessoa": True, "executa": "readmitir",
        "campos": ("admissao", "vinculo", "tipo_contrato", "cargo_id", "convencao_id",
                   "obra_id", "salario", "matricula", "observacao"),
        "checklist": ("Atualizar a data de admissão", "Conferir cargo e convenção",
                      "Conferir a obra", "Atualizar a matrícula",
                      "Reativar no sistema de pagamento"),
    },
    "ADMISSAO_PRESTADOR": {
        "nome": "Prestador que passa a ter carteira",
        "explicacao": "troca o vínculo sem apagar o período de prestador",
        "exige_pessoa": True, "executa": "trocar_vinculo",
        "campos": ("admissao", "vinculo", "tipo_contrato", "cargo_id", "salario",
                   "observacao"),
        "checklist": ("Conferir a documentação de admissão",
                      "Registrar na carteira", "Cadastro no sistema de pagamento"),
    },
    "ALTERACAO_CARGO": {
        "nome": "Alteração de cargo ou salário",
        "explicacao": "com o de→para e o motivo gravados",
        "exige_pessoa": True, "executa": "alteracao",
        "campos": ("vigencia", "cargo_id", "salario", "motivo", "justificativa"),
        "checklist": ("Conferir o piso da convenção", "Registrar a alteração",
                      "Atualizar no sistema de pagamento"),
    },
    "ATUALIZACAO_DADOS": {
        "nome": "Atualização de dados cadastrais",
        "explicacao": "telefone, chave Pix, endereço, nome",
        "exige_pessoa": True, "executa": "atualizar_cadastro",
        "campos": ("o_que_muda", "telefone", "email", "pix_tipo", "pix_chave",
                   "cep", "logradouro", "numero_endereco", "bairro", "municipio", "uf"),
        "checklist": ("Conferir o documento que comprova", "Atualizar o cadastro",
                      "Atualizar no sistema de pagamento"),
    },
    "DEPENDENTE": {
        "nome": "Cadastro de dependente",
        "explicacao": "para salário-família e IR",
        "exige_pessoa": True, "executa": "dependente",
        "campos": ("nome", "parentesco", "data_nascimento", "cpf",
                   "salario_familia", "dependente_ir", "observacao"),
        "checklist": ("Receber a certidão", "Cadastrar o dependente",
                      "Solicitar o salário-família"),
    },
    "AJUDA_DE_CUSTO": {
        "nome": "Ajuda de custo",
        "explicacao": "alimentação, transporte e observação",
        "exige_pessoa": True, "executa": "beneficios",
        "campos": ("categoria_alimentacao", "aux_alimentacao", "categoria_transporte",
                   "aux_transporte", "obra_id", "observacao"),
        "checklist": ("Conferir o valor com a obra", "Atualizar o cadastro"),
    },
    "VALE_TRANSPORTE": {
        "nome": "Vale-transporte",
        "explicacao": "conceder, trocar a modalidade ou cancelar",
        "exige_pessoa": True, "executa": "beneficios",
        "campos": ("conceder", "categoria_transporte", "aux_transporte",
                   "meio_transporte", "observacao"),
        "checklist": ("Cadastro no sistema de transporte",
                      "Encaminhamento do cartão"),
    },
    "DESCONTO_EM_FOLHA": {
        "nome": "Desconto em folha",
        "explicacao": "valor total e número de parcelas",
        "exige_pessoa": True, "executa": None,
        "campos": ("valor_total", "parcelas", "primeira_competencia", "observacao"),
        "checklist": ("Conferir a autorização do colaborador",
                      "Lançar no sistema de pagamento"),
    },
    "SEGUNDA_VIA_CARTAO": {
        "nome": "2ª via de cartão",
        "explicacao": "pagamento ou vale-transporte, com custo descontado",
        "exige_pessoa": True, "executa": None,
        "campos": ("qual_cartao", "observacao"),
        "checklist": ("Solicitação do cartão", "Encaminhamento do cartão",
                      "Lançar o desconto do custo"),
    },
    "PRE_CADASTRO": {
        "nome": "Pré-cadastro de colaborador",
        "explicacao": "a obra manda o nome e o CPF; o DP completa",
        "exige_pessoa": False, "executa": "criar_cadastro",
        "campos": ("nome", "cpf", "obra_id", "cargo_id", "vinculo", "data_inicio",
                   "telefone", "observacao"),
        "checklist": ("Receber a documentação", "Completar o cadastro",
                      "Conferir o que falta para admitir"),
    },
    "DOCUMENTACAO": {
        "nome": "Documentação (admissão ou rescisão)",
        "explicacao": "cobrar e guardar o que falta",
        "exige_pessoa": True, "executa": None,
        "campos": ("qual_documentacao", "observacao"),
        "checklist": ("Receber os documentos", "Arquivar no cadastro da pessoa"),
    },
    "DIVERSOS": {
        "nome": "Outra solicitação",
        "explicacao": "o que não cabe nos tipos acima",
        "exige_pessoa": False, "executa": None,
        "campos": ("descricao",),
        "checklist": (),
    },
}

SITUACOES = ("ABERTA", "EM_ANALISE", "ATENDIDA", "RECUSADA", "CANCELADA")
ABERTAS = ("ABERTA", "EM_ANALISE")


def tipos_para_tela() -> list[dict[str, Any]]:
    return [{"valor": c, "rotulo": d["nome"], "explicacao": d["explicacao"],
             "exige_pessoa": d["exige_pessoa"], "campos": list(d["campos"])}
            for c, d in TIPOS.items()]


def _dec(v: Any, campo: str) -> Optional[Decimal]:
    texto = str(v or "").strip().replace("R$", "").strip()
    if not texto:
        return None
    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    try:
        return Decimal(texto).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        raise ErroValidacao(f"Valor inválido em {campo}: {v!r}")


def _data(v: Any, campo: str) -> Optional[date]:
    from datetime import datetime
    texto = str(v or "").strip()
    if not texto:
        return None
    for formato in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(texto, formato).date()
        except ValueError:
            continue
    raise ErroValidacao(f"Data inválida em {campo}: {v!r}")


def proximo_numero(s: Session) -> str:
    """SDP-000001, como o "Nº SDP" do pipe. Numeração humana, não o id."""
    n = s.execute(text("SELECT nextval('dp_solicitacao_numero_seq')")).scalar()
    return f"SDP-{int(n):06d}"


# ---------------------------------------------------------------------------
# Abrir
# ---------------------------------------------------------------------------
def abrir(s: Session, dados: dict[str, Any], usuario: Usuario) -> dict[str, Any]:
    tipo = str(dados.get("tipo") or "").strip().upper()
    if tipo not in TIPOS:
        raise ErroValidacao(f"Tipo de solicitação desconhecido: {tipo or '(vazio)'}")
    regra = TIPOS[tipo]
    colaborador_id = int(dados["colaborador_id"]) if dados.get("colaborador_id") else None
    if regra["exige_pessoa"] and not colaborador_id:
        raise ErroValidacao(f"{regra['nome']}: escolha a pessoa.")
    pessoa = s.get(Colaborador, colaborador_id) if colaborador_id else None
    if colaborador_id and pessoa is None:
        raise ErroValidacao("Colaborador não encontrado.")

    # O PEDIDO SÓ GUARDA OS CAMPOS DO TIPO. Sem esta peneira, o formulário de um
    # tipo deixaria lixo do outro no JSONB — e foi assim que o pipe terminou com
    # 106 campos e metade vazia em cada card.
    pedido = {c: dados.get(c) for c in regra["campos"] if dados.get(c) not in (None, "")}

    # Duas conferências que evitam fila com pedido impossível. O resto é do
    # atendimento: conferir tudo na abertura empurraria o trabalho do DP para a
    # obra, que é o contrário do que a fila existe para fazer.
    if tipo == "DESLIGAMENTO" and not pedido.get("data_saida"):
        raise ErroValidacao("Informe a data de saída.")
    if tipo == "PRE_CADASTRO":
        from app.apps.erp.core.cadastros.validadores import cpf_valido, somente_digitos
        cpf = somente_digitos(pedido.get("cpf") or "")
        if not cpf_valido(cpf):
            raise ErroValidacao("CPF inválido.")
        ja = svc_pessoas.conferir_cpf(s, cpf)
        if ja["situacao"] in ("ativo", "desligado"):
            raise ErroValidacao(ja["aviso"])
        pedido["cpf"] = cpf

    numero = proximo_numero(s)
    linha = s.execute(text("""
        INSERT INTO dp_solicitacoes
               (numero, tipo, situacao, colaborador_id, nome_informado, cpf_informado,
                obra_id, solicitante_id, requerente, dados, data_referencia, valor)
        VALUES (:numero, :tipo, 'ABERTA', :colaborador_id, :nome, :cpf, :obra_id,
                :solicitante, :requerente, CAST(:dados AS jsonb), :ref, :valor)
        RETURNING id"""), {
        "numero": numero, "tipo": tipo, "colaborador_id": colaborador_id,
        "nome": (pessoa.nome if pessoa else (pedido.get("nome") or None)),
        "cpf": (pessoa.cpf if pessoa else (pedido.get("cpf") or None)),
        "obra_id": (int(dados["obra_id"]) if dados.get("obra_id")
                    else (pessoa.obra_id if pessoa else None)),
        "solicitante": usuario.id if usuario else None,
        "requerente": (str(dados.get("requerente") or "").strip() or None),
        "dados": __import__("json").dumps(pedido, ensure_ascii=False, default=str),
        "ref": _data(pedido.get("data_saida") or pedido.get("admissao")
                     or pedido.get("vigencia") or pedido.get("data_nascimento"), "data"),
        "valor": _dec(pedido.get("valor_total") or pedido.get("salario")
                      or pedido.get("aux_transporte"), "valor"),
    })
    solicitacao_id = int(linha.scalar())
    for i, item in enumerate(regra["checklist"]):
        s.execute(text("""INSERT INTO dp_solicitacao_itens (solicitacao_id, ordem, descricao)
                          VALUES (:s, :o, :d)"""),
                  {"s": solicitacao_id, "o": i, "d": item})
    _evento(s, solicitacao_id, "ABERTA", {"tipo": tipo, "pedido": pedido}, usuario)
    if colaborador_id:
        registrar_evento(s, "colaborador", colaborador_id, "SOLICITACAO_DP_ABERTA",
                         {"numero": numero, "tipo": tipo},
                         usuario.id if usuario else None)
    logger.info("ERP/DP: solicitação %s aberta (%s) por %s", numero, tipo,
                usuario.nome if usuario else "?")
    return ler(s, solicitacao_id)


def _evento(s: Session, solicitacao_id: int, acao: str, detalhe: dict,
            usuario: Optional[Usuario]) -> None:
    s.execute(text("""INSERT INTO dp_solicitacao_eventos
                             (solicitacao_id, acao, detalhe, usuario_id)
                      VALUES (:s, :a, CAST(:d AS jsonb), :u)"""),
              {"s": solicitacao_id, "a": acao,
               "d": __import__("json").dumps(detalhe, ensure_ascii=False, default=str),
               "u": usuario.id if usuario else None})


# ---------------------------------------------------------------------------
# Atender — e EXECUTAR
# ---------------------------------------------------------------------------
def atender(s: Session, solicitacao_id: int, dados: dict[str, Any],
            usuario: Usuario) -> dict[str, Any]:
    """Atende o pedido E faz o que ele pede, na mesma transação.

    É a diferença que importa em relação ao Pipefy: lá, atender era mover o card
    — e alguém, depois, ia mexer no cadastro à mão ou por um cenário do Make. O
    pedido dizia uma coisa e o cadastro ficava outra, e ninguém sabia qual valia.

    Aqui, ou as duas coisas acontecem, ou nenhuma: o efeito no cadastro usa as
    MESMAS funções da tela (`core/cadastros/pessoas.py`), com as mesmas
    conferências. Pedido que não pode ser executado é RECUSADO com o motivo, não
    atendido pela metade.
    """
    p = ler(s, solicitacao_id)
    if p["situacao"] not in ABERTAS:
        raise ErroValidacao(
            f"{p['numero']} já está {p['situacao_rotulo'].lower()} — não dá para atender de novo.")
    regra = TIPOS[p["tipo"]]
    # O que vale é o pedido, com o que o DP corrigiu em cima. Corrigir é normal:
    # a obra pede "salário 2200" e o DP acerta no piso da convenção.
    pedido = {**p["dados"], **{k: v for k, v in (dados.get("dados") or {}).items()
                               if v not in (None, "")}}
    efeito: dict[str, Any] = {}
    executa = regra["executa"]
    if executa and p["colaborador_id"]:
        alvo = p["colaborador_id"]
        if executa == "desligar":
            svc_pessoas.desligar(s, alvo, pedido, usuario)
        elif executa == "readmitir":
            svc_pessoas.readmitir(s, alvo, pedido, usuario)
        elif executa == "trocar_vinculo":
            svc_pessoas.trocar_vinculo(s, alvo, {**pedido, "vinculo": pedido.get("vinculo") or "CTPS"}, usuario)
        elif executa == "alteracao":
            svc_pessoas.registrar_alteracao(s, alvo, pedido, usuario)
        elif executa == "dependente":
            svc_pessoas.salvar_dependente(s, alvo, pedido, usuario)
        elif executa in ("atualizar_cadastro", "beneficios"):
            c = s.get(Colaborador, alvo)
            svc_pessoas.salvar(s, {**pedido, "cpf": c.cpf, "nome": c.nome}, usuario)
        efeito["executou"] = executa
    elif executa == "criar_cadastro":
        c = svc_pessoas.salvar(s, {**pedido, "admissao": pedido.get("data_inicio")}, usuario)
        s.execute(text("UPDATE dp_solicitacoes SET colaborador_id = :c WHERE id = :s"),
                  {"c": c.id, "s": solicitacao_id})
        efeito["colaborador_id"] = c.id
    s.execute(text("""UPDATE dp_solicitacoes
                         SET situacao = 'ATENDIDA', atendido_por = :u,
                             atendido_em = now(), resposta = :r, atualizado_em = now(),
                             dados = CAST(:d AS jsonb)
                       WHERE id = :s"""),
              {"u": usuario.id if usuario else None, "s": solicitacao_id,
               "r": (str(dados.get("resposta") or "").strip() or None),
               "d": __import__("json").dumps(pedido, ensure_ascii=False, default=str)})
    _evento(s, solicitacao_id, "ATENDIDA",
            {"resposta": dados.get("resposta"), **efeito}, usuario)
    logger.info("ERP/DP: solicitação %s atendida por %s (%s)", p["numero"],
                usuario.nome if usuario else "?", efeito.get("executou") or "sem efeito")
    return ler(s, solicitacao_id)


def recusar(s: Session, solicitacao_id: int, dados: dict[str, Any],
            usuario: Usuario) -> dict[str, Any]:
    p = ler(s, solicitacao_id)
    if p["situacao"] not in ABERTAS:
        raise ErroValidacao(f"{p['numero']} já está {p['situacao_rotulo'].lower()}.")
    motivo = str(dados.get("resposta") or dados.get("motivo") or "").strip()
    if len(motivo) < 5:
        # O pipe tinha o campo "Por quê?" obrigatório na recusa, e com razão:
        # quem pediu precisa saber o que corrigir para pedir de novo.
        raise ErroValidacao("Diga por que o pedido foi recusado — quem pediu precisa saber.")
    s.execute(text("""UPDATE dp_solicitacoes
                         SET situacao = 'RECUSADA', atendido_por = :u, atendido_em = now(),
                             resposta = :r, atualizado_em = now()
                       WHERE id = :s"""),
              {"u": usuario.id if usuario else None, "s": solicitacao_id, "r": motivo})
    _evento(s, solicitacao_id, "RECUSADA", {"motivo": motivo}, usuario)
    return ler(s, solicitacao_id)


def cancelar(s: Session, solicitacao_id: int, dados: dict[str, Any],
             usuario: Usuario) -> dict[str, Any]:
    """Quem abriu cancela o próprio pedido.

    No pipe, cancelar era ABRIR OUTRO CARD ("Cancelar Solicitação de
    Desligamento"). Aqui é ação sobre o próprio pedido — mas ele não desaparece:
    pedido apagado deixa "e o que aconteceu com aquele desligamento?" sem
    resposta.
    """
    p = ler(s, solicitacao_id)
    if p["situacao"] not in ABERTAS:
        raise ErroValidacao(
            f"{p['numero']} já está {p['situacao_rotulo'].lower()} — não dá para cancelar.")
    motivo = str(dados.get("motivo") or "").strip()
    if len(motivo) < 5:
        raise ErroValidacao("Diga por que está cancelando.")
    s.execute(text("""UPDATE dp_solicitacoes
                         SET situacao = 'CANCELADA', cancelado_por = :u,
                             cancelado_em = now(), motivo_cancelamento = :m,
                             atualizado_em = now()
                       WHERE id = :s"""),
              {"u": usuario.id if usuario else None, "s": solicitacao_id, "m": motivo})
    _evento(s, solicitacao_id, "CANCELADA", {"motivo": motivo}, usuario)
    return ler(s, solicitacao_id)


def marcar_item(s: Session, solicitacao_id: int, item_id: int, feito: bool,
                usuario: Usuario) -> dict[str, Any]:
    linha = s.execute(text("""SELECT descricao FROM dp_solicitacao_itens
                               WHERE id = :i AND solicitacao_id = :s"""),
                      {"i": item_id, "s": solicitacao_id}).first()
    if linha is None:
        raise ErroValidacao("Item do checklist não encontrado.")
    s.execute(text("""UPDATE dp_solicitacao_itens
                         SET feito = :f, feito_por = :u,
                             feito_em = CASE WHEN :f THEN now() ELSE NULL END
                       WHERE id = :i"""),
              {"f": bool(feito), "u": usuario.id if usuario else None, "i": item_id})
    s.execute(text("""UPDATE dp_solicitacoes SET situacao = 'EM_ANALISE',
                             atualizado_em = now()
                       WHERE id = :s AND situacao = 'ABERTA'"""), {"s": solicitacao_id})
    _evento(s, solicitacao_id, "ITEM_FEITO" if feito else "ITEM_DESMARCADO",
            {"item": linha[0]}, usuario)
    return ler(s, solicitacao_id)


# ---------------------------------------------------------------------------
# Ler
# ---------------------------------------------------------------------------
SITUACAO_ROTULOS = {"ABERTA": "Aberta", "EM_ANALISE": "Em análise",
                    "ATENDIDA": "Atendida", "RECUSADA": "Recusada",
                    "CANCELADA": "Cancelada"}


def ler(s: Session, solicitacao_id: int) -> dict[str, Any]:
    """O pedido inteiro: o que foi pedido, o checklist e a trilha.

    ⚠️ A TRILHA ORDENA POR `criado_em, id`, e o `id` não é enfeite: em Postgres
    `now()` é o **início da transação**, então dois eventos gravados no mesmo
    pedido (abrir e atender, num teste; marcar dois itens, na vida real) têm
    timestamp IDÊNTICO — e aí a ordem fica indefinida. O histórico mostrava
    "atendida" antes de "aberta". Pego por teste.
    """
    linha = s.execute(text("""
        SELECT d.*, c.nome AS pessoa, c.cpf AS pessoa_cpf, o.codigo AS obra,
               u.nome AS solicitante, a.nome AS atendente
          FROM dp_solicitacoes d
          LEFT JOIN colaboradores c ON c.id = d.colaborador_id
          LEFT JOIN obras o ON o.id = d.obra_id
          LEFT JOIN usuarios u ON u.id = d.solicitante_id
          LEFT JOIN usuarios a ON a.id = d.atendido_por
         WHERE d.id = :s"""), {"s": solicitacao_id}).mappings().first()
    if linha is None:
        raise ErroValidacao("Solicitação não encontrada.")
    d = dict(linha)
    regra = TIPOS.get(d["tipo"], {})
    itens = s.execute(text("""SELECT i.id, i.ordem, i.descricao, i.feito, i.feito_em,
                                     u.nome AS por
                                FROM dp_solicitacao_itens i
                                LEFT JOIN usuarios u ON u.id = i.feito_por
                               WHERE i.solicitacao_id = :s ORDER BY i.ordem"""),
                      {"s": solicitacao_id}).mappings().all()
    eventos = s.execute(text("""SELECT e.acao, e.detalhe, e.criado_em, u.nome AS por
                                  FROM dp_solicitacao_eventos e
                                  LEFT JOIN usuarios u ON u.id = e.usuario_id
                                 WHERE e.solicitacao_id = :s
                                 ORDER BY e.criado_em, e.id"""),
                        {"s": solicitacao_id}).mappings().all()
    return {
        "id": d["id"], "numero": d["numero"], "tipo": d["tipo"],
        "tipo_rotulo": regra.get("nome", d["tipo"]),
        "situacao": d["situacao"],
        "situacao_rotulo": SITUACAO_ROTULOS.get(d["situacao"], d["situacao"]),
        "colaborador_id": d["colaborador_id"],
        "pessoa": d["pessoa"] or d["nome_informado"] or "",
        "pessoa_cpf": d["pessoa_cpf"] or d["cpf_informado"] or "",
        "obra": d["obra"] or "", "obra_id": d["obra_id"],
        "solicitante": d["solicitante"] or "", "requerente": d["requerente"] or "",
        "dados": d["dados"] or {},
        "data_referencia": d["data_referencia"].isoformat() if d["data_referencia"] else None,
        "valor": float(d["valor"]) if d["valor"] is not None else None,
        "atendente": d["atendente"] or "", "resposta": d["resposta"] or "",
        "atendido_em": d["atendido_em"].isoformat() if d["atendido_em"] else None,
        "motivo_cancelamento": d["motivo_cancelamento"] or "",
        "criado_em": d["criado_em"].isoformat(),
        "aberta": d["situacao"] in ABERTAS,
        "itens": [{"id": i["id"], "descricao": i["descricao"], "feito": i["feito"],
                   "por": i["por"] or "",
                   "em": i["feito_em"].isoformat() if i["feito_em"] else None}
                  for i in itens],
        "feitos": sum(1 for i in itens if i["feito"]), "total_itens": len(itens),
        "eventos": [{"acao": e["acao"], "detalhe": e["detalhe"] or {},
                     "por": e["por"] or "", "em": e["criado_em"].isoformat()}
                    for e in eventos],
    }


def listar(s: Session, *, situacao: str = "", tipo: str = "",
           colaborador_id: Optional[int] = None, obra_ids: Optional[list[int]] = None,
           usuario_id: Optional[int] = None, limite: int = 300) -> dict[str, Any]:
    """A fila. `obra_ids` é o recorte de quem só alcança algumas obras.

    `usuario_id` é indispensável quando `obra_ids` vem preenchido: o filtro
    inclui "ou foi você quem abriu", e sem o id a consulta estoura por parâmetro
    não preenchido. (Defeito pego pelos testes antes de chegar à tela.)
    """
    onde, params = ["1 = 1"], {"limite": limite, "eu": usuario_id}
    if situacao == "ABERTAS":
        onde.append("d.situacao IN ('ABERTA', 'EM_ANALISE')")
    elif situacao:
        onde.append("d.situacao = :situacao")
        params["situacao"] = situacao
    if tipo:
        onde.append("d.tipo = :tipo")
        params["tipo"] = tipo
    if colaborador_id:
        onde.append("d.colaborador_id = :cid")
        params["cid"] = colaborador_id
    # O ESCOPO POR OBRA: quem é preso a obra vê o que é da obra dele, mais o que
    # ele mesmo abriu (pedido de pré-cadastro ainda não tem obra ligada).
    if obra_ids is not None:
        if obra_ids:
            onde.append("(d.obra_id = ANY(:obras) OR d.solicitante_id = :eu)")
            params["obras"] = obra_ids
        else:
            # Quem não alcança obra nenhuma vê só o que abriu. Sem `usuario_id`
            # não veria NADA, que é o certo: recorte sem dono não libera nada.
            onde.append("d.solicitante_id IS NOT DISTINCT FROM :eu" if usuario_id
                        else "FALSE")
    linhas = s.execute(text(f"""
        SELECT d.id, d.numero, d.tipo, d.situacao, d.colaborador_id, d.obra_id,
               d.data_referencia, d.valor, d.criado_em, d.atendido_em,
               COALESCE(c.nome, d.nome_informado) AS pessoa,
               o.codigo AS obra, u.nome AS solicitante,
               (SELECT count(*) FROM dp_solicitacao_itens i
                 WHERE i.solicitacao_id = d.id) AS total_itens,
               (SELECT count(*) FROM dp_solicitacao_itens i
                 WHERE i.solicitacao_id = d.id AND i.feito) AS feitos
          FROM dp_solicitacoes d
          LEFT JOIN colaboradores c ON c.id = d.colaborador_id
          LEFT JOIN obras o ON o.id = d.obra_id
          LEFT JOIN usuarios u ON u.id = d.solicitante_id
         WHERE {' AND '.join(onde)}
         ORDER BY (d.situacao IN ('ABERTA', 'EM_ANALISE')) DESC, d.criado_em DESC
         LIMIT :limite"""), params).mappings().all()
    fila = [{
        "id": l["id"], "numero": l["numero"], "tipo": l["tipo"],
        "tipo_rotulo": TIPOS.get(l["tipo"], {}).get("nome", l["tipo"]),
        "situacao": l["situacao"],
        "situacao_rotulo": SITUACAO_ROTULOS.get(l["situacao"], l["situacao"]),
        "pessoa": l["pessoa"] or "", "obra": l["obra"] or "",
        "solicitante": l["solicitante"] or "",
        "data_referencia": l["data_referencia"].isoformat() if l["data_referencia"] else None,
        "valor": float(l["valor"]) if l["valor"] is not None else None,
        "criado_em": l["criado_em"].isoformat(),
        "aberta": l["situacao"] in ABERTAS,
        "feitos": int(l["feitos"]), "total_itens": int(l["total_itens"]),
    } for l in linhas]
    return {
        "solicitacoes": fila,
        "indicadores": {
            "abertas": sum(1 for x in fila if x["aberta"]),
            "atendidas": sum(1 for x in fila if x["situacao"] == "ATENDIDA"),
            "recusadas": sum(1 for x in fila if x["situacao"] == "RECUSADA"),
            # O que já tem checklist começado e não terminou: é o que trava o DP.
            "em_andamento": sum(1 for x in fila
                                if x["aberta"] and 0 < x["feitos"] < x["total_itens"]),
        },
        "tipos": tipos_para_tela(),
        "situacoes": [{"valor": c, "rotulo": r} for c, r in SITUACAO_ROTULOS.items()],
    }
