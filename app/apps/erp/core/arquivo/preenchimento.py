# ============================================================================
# ERP — core/arquivo/preenchimento.py
# Arquivar o documento E preencher o cadastro, num gesto só.
#
# O PEDIDO, do dono em 10/09/2026: *"gostaria de fazer uma relação entre
# cadastro de obra e arquivo de documentos. Digamos que eu vá cadastrar uma
# nova obra, ou adicionar o arquivo da CNO, ou da ordem de serviço, ART,
# aditivos etc. — gostaria que o sistema já preenchesse os campos de cadastro
# de obra e ainda arquivasse o arquivo. Dessa forma não perco tempo."*
#
# E o princípio que ele mesmo tirou disso, que vale para o sistema inteiro:
# *"matariamos duas ações... cadastros e arquivo estarem associados quando
# fizer sentido"*. É por isso que este módulo é genérico por dentro: a obra é
# o primeiro caso, colaboradores vêm depois pelo mesmo caminho.
#
# AS TRÊS REGRAS QUE SUSTENTAM ISTO
#
#   1. CADA TIPO DE DOCUMENTO SÓ PREENCHE O QUE ELE PROVA. Uma licença
#      ambiental não define o valor do contrato, por mais que a IA leia um
#      número lá dentro. A lista por tipo abaixo é uma trava, não uma
#      sugestão: o que não está nela não é gravado nem que a IA insista.
#
#   2. O QUE JÁ ESTÁ PREENCHIDO NÃO É SOBRESCRITO SOZINHO. Campo em branco a
#      leitura preenche; campo com valor diferente vira CONFLITO — os dois
#      valores aparecem lado a lado e a pessoa escolhe. Trocar calado o que
#      alguém digitou é a maneira mais rápida de o sistema perder confiança.
#
#   3. QUEM GRAVA É A PESSOA. A leitura sugere; nada vai para o cadastro sem
#      alguém confirmar. Cadastro de obra errado contamina medição, nota
#      fiscal e retenção — sai caro e demora a aparecer.
# ============================================================================
from __future__ import annotations

import logging
import re
import unicodedata
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import Obra, ObraAditivo, Usuario

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# O que se sabe extrair, e como cada coisa se lê
#
# (campo no cadastro) → (rótulo em português, feitio, o que pedir à IA)
# ---------------------------------------------------------------------------
CAMPOS: dict[str, tuple[str, str, str]] = {
    "cno": ("Matrícula CNO/CEI", "texto",
            "matrícula CNO ou CEI da obra, como aparece no documento"),
    "cei_obra": ("CEI da obra", "texto", "número CEI, quando vier separado do CNO"),
    "art_rrt": ("ART / RRT", "texto", "número da ART ou RRT"),
    "responsavel_tecnico": ("Responsável técnico", "texto",
                            "nome do engenheiro/arquiteto responsável técnico"),
    "crea_obra": ("CREA/CAU da obra", "texto",
                  "registro CREA ou CAU do responsável técnico"),
    "contrato": ("Número do contrato", "texto",
                 "número do contrato, no formato em que o órgão escreve"),
    "valor_contrato": ("Valor do contrato", "dinheiro", "valor global do contrato"),
    "objeto": ("Objeto", "texto", "objeto do contrato, em uma linha"),
    "cliente": ("Contratante", "texto",
                "nome do órgão ou empresa CONTRATANTE (o cliente), não o da construtora"),
    "cnpj_cliente": ("CNPJ do contratante", "documento",
                     "CNPJ do contratante, só dígitos"),
    "vigencia_inicio": ("Início da vigência", "data", "data em que a vigência começa"),
    "vigencia_fim": ("Fim da vigência", "data", "data em que a vigência termina"),
    "prazo_execucao_dias": ("Prazo de execução (dias)", "inteiro",
                            "prazo de execução em DIAS (converta meses para dias se preciso)"),
    "data_base_orcamento": ("Data-base do orçamento", "data",
                            "data-base do orçamento, usada para calcular reajuste"),
    "indice_reajuste": ("Índice de reajuste", "texto",
                        "índice de reajuste previsto (INCC-DI, IPCA, IGP-M…)"),
    "retencao_contratual_pct": ("Retenção contratual (%)", "percentual",
                                "percentual de retenção contratual/garantia"),
    "caucao_pct": ("Caução (%)", "percentual", "percentual de caução"),
    "ordem_servico": ("Ordem de serviço", "texto", "número da ordem de serviço"),
    "data_ordem_servico": ("Data da ordem de serviço", "data",
                           "data de emissão da ordem de serviço"),
    "data_inicio": ("Início da obra", "data",
                    "data de início dos trabalhos determinada pela ordem de serviço"),
    "seguro_garantia": ("Apólice de seguro", "texto", "número da apólice"),
    "seguro_vigencia_fim": ("Fim da vigência do seguro", "data",
                            "até quando a apólice vale"),
    "endereco": ("Endereço da obra", "texto", "logradouro do local da obra"),
    "numero_endereco": ("Número", "texto", "número do endereço da obra"),
    "bairro": ("Bairro", "texto", "bairro da obra"),
    "cep": ("CEP", "texto", "CEP da obra, só dígitos"),
    "municipio": ("Município", "texto", "município da obra"),
    "uf": ("UF", "texto", "sigla do estado da obra"),
    "aliquota_iss": ("Alíquota de ISS (%)", "percentual",
                     "alíquota de ISS do município da obra, se o documento disser"),
}

# ---------------------------------------------------------------------------
# A TRAVA: cada tipo de documento só preenche o que ele PROVA.
#
# Uma licença ambiental não define valor de contrato, por mais que haja um
# número dentro dela. Tipo que não está aqui arquiva normalmente e não
# preenche nada — que é o certo: diário de obra e projeto não são fonte de
# cadastro.
# ---------------------------------------------------------------------------
POR_TIPO: dict[str, tuple[str, ...]] = {
    "MATRICULA-CEI-CNO": ("cno", "cei_obra", "endereco", "numero_endereco",
                          "bairro", "cep", "municipio", "uf"),
    "ART": ("art_rrt", "responsavel_tecnico", "crea_obra", "endereco",
            "numero_endereco", "bairro", "cep", "municipio", "uf", "valor_contrato"),
    "CONTRATO-OBRA": ("contrato", "valor_contrato", "objeto", "cliente",
                      "cnpj_cliente", "vigencia_inicio", "vigencia_fim",
                      "prazo_execucao_dias", "data_base_orcamento",
                      "indice_reajuste", "retencao_contratual_pct", "caucao_pct",
                      "endereco", "numero_endereco", "bairro", "cep",
                      "municipio", "uf"),
    "OS": ("ordem_servico", "data_ordem_servico", "data_inicio",
           "prazo_execucao_dias"),
    "SEGURO": ("seguro_garantia", "seguro_vigencia_fim"),
    "LICENCA": ("endereco", "numero_endereco", "bairro", "cep", "municipio", "uf"),
    # ADITIVO não preenche campo: cria um aditivo, que é registro à parte —
    # o valor vigente do contrato é o original MAIS os aditivos, e sobrescrever
    # o original apagaria a história.
    "ADITIVO": (),
}

TIPOS_QUE_VIRAM_ADITIVO = ("ADITIVO",)


# ---------------------------------------------------------------------------
# A pergunta que se acrescenta à leitura
# ---------------------------------------------------------------------------
def instrucao_de_extracao() -> str:
    """O pedaço da pergunta à IA que trata dos campos do cadastro da obra."""
    linhas = [f' "{campo}": "{pedido}"' for campo, (_, _, pedido) in CAMPOS.items()]
    return """
Além disso, quando o documento for de uma OBRA, extraia o que der para o cadastro
dela, dentro de "dados_extraidos". Campo que o documento não disser fica vazio —
NUNCA deduza de outro documento nem invente:

 "dados_extraidos": {
%s
 },
 "aditivo": {
   "numero": "número do termo aditivo",
   "tipo": "VALOR | PRAZO | VALOR_E_PRAZO | OUTRO",
   "valor": "0000.00 — acréscimo (ou supressão, com sinal negativo) de valor",
   "dias": "prazo acrescido em dias",
   "nova_vigencia_fim": "AAAA-MM-DD, quando o aditivo estender a vigência",
   "data_assinatura": "AAAA-MM-DD",
   "objeto": "o que o aditivo muda, em uma linha"
 }

Regras da extração:
- Valores com ponto decimal, sem "R$" e sem separador de milhar (1.234,56 → 1234.56).
- Percentual só o número (5%% → 5.00).
- O CONTRATANTE é o órgão/empresa que CONTRATA. A construtora (BWS e afins) é a
  CONTRATADA — nunca a ponha no campo do contratante.
- "aditivo" só quando o documento FOR um termo aditivo; nos outros, deixe vazio.
- Prazo em meses: converta para dias (1 mês = 30 dias) e diga isso em observacoes.""" % (
        ",\n".join(linhas))


# ---------------------------------------------------------------------------
# Normalizar o que veio
# ---------------------------------------------------------------------------
def _texto(v: Any) -> Optional[str]:
    t = re.sub(r"\s{2,}", " ", str(v or "")).strip()
    return t or None


def _digitos(v: Any) -> Optional[str]:
    d = re.sub(r"\D", "", str(v or ""))
    return d or None


def _data(v: Any) -> Optional[date]:
    t = str(v or "").strip()
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", t)
    if m:
        try:
            return date(*(int(x) for x in m.groups()))
        except ValueError:
            return None
    m = re.match(r"^(\d{2})/(\d{2})/(\d{2,4})$", t)
    if m:
        d, mes, a = m.groups()
        a = a if len(a) == 4 else "20" + a
        try:
            return date(int(a), int(mes), int(d))
        except ValueError:
            return None
    return None


def _numero(v: Any) -> Optional[Decimal]:
    t = str(v or "").strip().replace("R$", "").replace(" ", "")
    if not t:
        return None
    if "," in t and "." in t:
        t = t.replace(".", "").replace(",", ".")
    elif "," in t:
        t = t.replace(",", ".")
    m = re.search(r"-?\d+(?:\.\d+)?", t)
    if not m:
        return None
    try:
        return Decimal(m.group(0))
    except InvalidOperation:
        return None


def _inteiro(v: Any) -> Optional[int]:
    n = _numero(v)
    return int(n) if n is not None else None


def _converter(feitio: str, bruto: Any):
    if feitio == "data":
        return _data(bruto)
    if feitio == "dinheiro":
        n = _numero(bruto)
        return n.quantize(Decimal("0.01")) if n is not None else None
    if feitio == "percentual":
        n = _numero(bruto)
        return n.quantize(Decimal("0.01")) if n is not None else None
    if feitio == "inteiro":
        return _inteiro(bruto)
    if feitio == "documento":
        return _digitos(bruto)
    return _texto(bruto)


def _mostrar(feitio: str, valor: Any) -> str:
    if valor in (None, ""):
        return ""
    if feitio == "data":
        return valor.strftime("%d/%m/%Y") if hasattr(valor, "strftime") else str(valor)
    if feitio in ("dinheiro", "percentual"):
        inteiro, _, dec = f"{Decimal(valor):.2f}".partition(".")
        sinal, inteiro = ("-", inteiro[1:]) if inteiro.startswith("-") else ("", inteiro)
        milhar = re.sub(r"(?<=\d)(?=(\d{3})+$)", ".", inteiro)
        return f"{sinal}{milhar},{dec}"
    return str(valor)


def _mesmo(a: Any, b: Any) -> bool:
    """Dois valores são o mesmo? Comparando texto sem acento nem caixa.

    "AV. BRASIL, 100" e "Av Brasil 100" são a mesma rua — tratá-los como
    conflito faria a tela pedir decisão sobre coisa que não mudou.
    """
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, (Decimal, int, float)) or isinstance(b, (Decimal, int, float)):
        try:
            return Decimal(str(a)) == Decimal(str(b))
        except InvalidOperation:
            pass
    def _limpo(x):
        s = unicodedata.normalize("NFKD", str(x))
        s = "".join(c for c in s if not unicodedata.combining(c))
        return re.sub(r"[^a-z0-9]+", "", s.lower())
    return _limpo(a) == _limpo(b)


# ---------------------------------------------------------------------------
# Sugerir
# ---------------------------------------------------------------------------
def sugerir_para_obra(s: Session, obra_id: int, tipo_codigo: str,
                      dados: dict[str, Any]) -> dict[str, Any]:
    """O que este documento preencheria no cadastro da obra.

    Nada é gravado. Devolve, campo a campo, o que a leitura achou, o que já
    está no cadastro e se há conflito — para a tela mostrar os dois lados.
    """
    obra = s.get(Obra, obra_id)
    if obra is None:
        raise ErroValidacao("Obra não encontrada.")

    permitidos = POR_TIPO.get((tipo_codigo or "").strip().upper(), ())
    extraidos = dados.get("dados_extraidos") or {}
    campos: list[dict[str, Any]] = []

    for campo in permitidos:
        rotulo, feitio, _ = CAMPOS[campo]
        novo = _converter(feitio, extraidos.get(campo))
        if novo in (None, ""):
            continue
        atual = getattr(obra, campo, None)
        vazio = atual in (None, "")
        igual = (not vazio) and _mesmo(atual, novo)
        if igual:
            continue        # nada a decidir: já está assim
        campos.append({
            "campo": campo, "rotulo": rotulo, "feitio": feitio,
            "valor": _mostrar(feitio, novo),
            "valor_atual": _mostrar(feitio, atual),
            "conflito": not vazio,
            # Campo em branco entra marcado; conflito entra DESMARCADO — o que
            # a pessoa digitou vale mais que o que a IA leu, até ela dizer o
            # contrário.
            "marcar": vazio,
        })

    aditivo = _aditivo_sugerido(tipo_codigo, dados)
    return {
        "obra_id": obra.id, "obra": obra.codigo,
        "tipo": (tipo_codigo or "").strip().upper(),
        "campos": campos,
        "aditivo": aditivo,
        "nada_a_preencher": not campos and aditivo is None,
        "tipo_nao_preenche": (tipo_codigo or "").strip().upper() not in POR_TIPO,
    }


def nome_para_obra(s: Session, obra_id: int, sugestao: dict[str, Any]) -> str:
    """O nome do arquivo, com o dono sendo ESTA obra.

    A leitura resolve o dono pelo que lê no documento, e acerta o registro mas
    nem sempre o APELIDO: ela pode devolver o nome da obra onde o padrão usa o
    código. O resultado era uma prévia que prometia um nome e entregava outro
    — pequeno, mas é o tipo de detalhe que faz a pessoa desconfiar do resto.
    Aqui a obra é conhecida: o nome sai dela.
    """
    from app.apps.erp.core.arquivo import catalogo, nomes

    obra = s.get(Obra, obra_id)
    codigo = (sugestao.get("tipo_codigo") or "").strip().upper()
    if obra is None or not codigo:
        return sugestao.get("nome_sugerido") or ""
    try:
        tipo = catalogo.obter(s, codigo)
    except Exception:
        return sugestao.get("nome_sugerido") or ""
    return nomes.montar(
        tipo_codigo=tipo.codigo, dono=nomes.apelido_da_obra(obra),
        referencia=(sugestao.get("referencia") or ""),
        competencia=_data((sugestao.get("competencia") or "") + "-01"
                          if len(sugestao.get("competencia") or "") == 7 else ""),
        emissao=_data(sugestao.get("emissao")),
        validade=_data(sugestao.get("validade")),
        nome_original=sugestao.get("nome_original") or "",
        por_competencia=bool(tipo.por_competencia), vence=bool(tipo.vence))


def _aditivo_sugerido(tipo_codigo: str, dados: dict[str, Any]) -> Optional[dict[str, Any]]:
    if (tipo_codigo or "").strip().upper() not in TIPOS_QUE_VIRAM_ADITIVO:
        return None
    a = dados.get("aditivo") or {}
    numero = _texto(a.get("numero"))
    valor = _converter("dinheiro", a.get("valor")) or Decimal("0.00")
    dias = _inteiro(a.get("dias")) or 0
    if not numero and not valor and not dias:
        return None
    tipo = (_texto(a.get("tipo")) or "").upper()
    if tipo not in ("VALOR", "PRAZO", "VALOR_E_PRAZO", "OUTRO"):
        tipo = ("VALOR_E_PRAZO" if (valor and dias)
                else "VALOR" if valor else "PRAZO" if dias else "OUTRO")
    return {
        "numero": numero or "", "tipo": tipo,
        "valor": _mostrar("dinheiro", valor), "valor_bruto": str(valor),
        "dias": dias,
        "nova_vigencia_fim": (_data(a.get("nova_vigencia_fim")).isoformat()
                              if _data(a.get("nova_vigencia_fim")) else ""),
        "data_assinatura": (_data(a.get("data_assinatura")).isoformat()
                            if _data(a.get("data_assinatura")) else ""),
        "objeto": _texto(a.get("objeto")) or "",
    }


# ---------------------------------------------------------------------------
# Aplicar — só o que a pessoa confirmou
# ---------------------------------------------------------------------------
def aplicar_na_obra(s: Session, obra_id: int, escolhas: dict[str, Any], *,
                    tipo_codigo: str = "", documento_id: Optional[int] = None,
                    usuario: Optional[Usuario] = None) -> dict[str, Any]:
    """Grava no cadastro os campos que a pessoa marcou. Nada além deles.

    `escolhas` é {campo: valor_em_texto}. O valor vem da TELA e não da leitura
    de propósito: a pessoa pode ter corrigido antes de confirmar, e é a
    correção dela que tem de valer.
    """
    obra = s.get(Obra, obra_id)
    if obra is None:
        raise ErroValidacao("Obra não encontrada.")
    permitidos = set(POR_TIPO.get((tipo_codigo or "").strip().upper(), ()))

    mudancas: list[str] = []
    for campo, bruto in (escolhas or {}).items():
        if campo not in permitidos:
            # A trava: nem que a tela mande, um tipo grava campo que não prova.
            raise ErroValidacao(
                f"O documento do tipo {tipo_codigo} não preenche “{campo}”.")
        rotulo, feitio, _ = CAMPOS[campo]
        novo = _converter(feitio, bruto)
        if novo in (None, ""):
            continue
        antigo = getattr(obra, campo, None)
        if _mesmo(antigo, novo):
            continue
        setattr(obra, campo, novo)
        mudancas.append(f"{rotulo}: {_mostrar(feitio, antigo) or '(vazio)'} → "
                        f"{_mostrar(feitio, novo)}")

    if mudancas:
        obra.atualizado_em = datetime.now()
        s.flush()
        registrar_evento(s, "obra", obra.id, "PREENCHIDA_POR_DOCUMENTO",
                         {"tipo": tipo_codigo, "documento_id": documento_id,
                          "mudancas": mudancas},
                         usuario.id if usuario else None)
        logger.info("ERP/arquivo: obra %s preenchida por documento %s (%d campo(s))",
                    obra.codigo, tipo_codigo, len(mudancas))
    return {"mudancas": mudancas, "quantidade": len(mudancas)}


def criar_aditivo(s: Session, obra_id: int, dados: dict[str, Any], *,
                  anexo_id: Optional[int] = None,
                  usuario: Optional[Usuario] = None) -> ObraAditivo:
    """O termo aditivo vira REGISTRO, não sobrescreve o contrato.

    O valor vigente é o original mais a soma dos aditivos. Trocar o valor
    original apagaria a história — e é justamente a história que o órgão
    pergunta quando questiona a medição.
    """
    obra = s.get(Obra, obra_id)
    if obra is None:
        raise ErroValidacao("Obra não encontrada.")
    numero = _texto(dados.get("numero"))
    if not numero:
        raise ErroValidacao(
            "O aditivo precisa do número. Sem ele não dá para saber se este é "
            "o primeiro ou o terceiro — e o valor vigente sairia errado.")

    ja = [a for a in s.query(ObraAditivo).filter(
        ObraAditivo.obra_id == obra.id, ObraAditivo.numero == numero).all()]
    if ja:
        raise ErroValidacao(
            f"O aditivo {numero} já está registrado nesta obra. Se for outro, "
            f"confira o número; se for o mesmo, o arquivo foi guardado e o "
            f"cadastro não precisa mudar.")

    valor = _converter("dinheiro", dados.get("valor")) or Decimal("0.00")
    dias = _inteiro(dados.get("dias")) or 0
    tipo = (_texto(dados.get("tipo")) or "OUTRO").upper()
    a = ObraAditivo(
        obra_id=obra.id, numero=numero, tipo=tipo, valor=valor, dias=dias,
        nova_vigencia_fim=_data(dados.get("nova_vigencia_fim")),
        data_assinatura=_data(dados.get("data_assinatura")),
        objeto=_texto(dados.get("objeto")), anexo_id=anexo_id,
        criado_por=usuario.id if usuario else None)
    s.add(a)
    s.flush()

    # A vigência nova, quando o aditivo a estende, é do CONTRATO — aí sim se
    # atualiza, porque é o fim da vigência que manda nos alertas.
    if a.nova_vigencia_fim and a.nova_vigencia_fim != obra.vigencia_fim:
        obra.vigencia_fim = a.nova_vigencia_fim
        obra.atualizado_em = datetime.now()

    registrar_evento(s, "obra", obra.id, "ADITIVO_REGISTRADO",
                     {"numero": numero, "tipo": tipo, "valor": str(valor),
                      "dias": dias}, usuario.id if usuario else None)
    logger.info("ERP/arquivo: aditivo %s registrado na obra %s", numero, obra.codigo)
    return a
