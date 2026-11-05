# -*- coding: utf-8 -*-
"""
O CADASTRO COMPLETO DA PESSOA — identificação, vínculo, dependentes e história.

Pedido do dono, 04 e 05/11/2026: tirar o cadastro de colaboradores do Pipefy e
trazer para o ERP, *"porque isso está gerando muito conflito"*.

AS LISTAS DE OPÇÕES VÊM DO PIPE, de propósito. Elas já estão acertadas com o
que a folha de pagamento espera há cinco anos — inventar outras obrigaria a
converter na saída, e conversão silenciosa é onde o número erra.

TRÊS DECISÕES DELE QUE MORAM AQUI:

1. *"colaborador e prestador tudo é no mesmo local, ou seja, todo mundo é
   colaborador"* — logo `vinculo` é um CAMPO (CTPS ou PRESTADOR), não dois
   cadastros. E *"vamos manter assim o CTPS e o prestador de serviço. E o tipo
   de contrato"*: os dois ficam, porque respondem coisas diferentes — o vínculo
   diz se há carteira assinada; o contrato, qual é o regime (CLT por tempo
   determinado, PJ, autônomo RPA…).

2. *"o RP também tem que alertar, por exemplo, se a pessoa está começando a
   digitar o cadastro, esse cadastro aqui já existe"* — `conferir_cpf()`, que a
   tela chama ENQUANTO se digita. No Pipefy o aviso vinha depois de 51 campos
   preenchidos, e o card era APAGADO.

3. *"a gente tem a base dos desligados, para a gente não refazer um cadastro que
   já existe (…) essa pessoa sai, mas ele está sendo readmitido"* —
   `readmitir()`, que abre vínculo novo sem apagar o anterior.
"""
from __future__ import annotations

import logging
import re
import unicodedata
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import (
    Cargo, Colaborador, ColaboradorAlteracao, ColaboradorDependente,
    ColaboradorVinculo, Convencao, Usuario,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# As listas do pipe, em um lugar só
# ---------------------------------------------------------------------------
VINCULOS = ("CTPS", "PRESTADOR")
VINCULO_ROTULOS = {"CTPS": "Carteira assinada (CTPS)",
                   "PRESTADOR": "Prestador de serviço"}
TIPOS_CONTRATO = (
    "CLT_DETERMINADO", "CLT_INDETERMINADO", "PJ", "AUTONOMO_RPA",
    "AUTONOMO_MENSALISTA", "JOVEM_APRENDIZ", "ESTAGIO", "TEMPORARIO", "PRO_LABORE",
)
TIPO_CONTRATO_ROTULOS = {
    "CLT_DETERMINADO": "CLT (tempo determinado)",
    "CLT_INDETERMINADO": "CLT (tempo indeterminado)",
    "PJ": "PJ",
    "AUTONOMO_RPA": "Autônomo (RPA)",
    "AUTONOMO_MENSALISTA": "Autônomo mensalista",
    "JOVEM_APRENDIZ": "Jovem aprendiz",
    "ESTAGIO": "Estágio",
    "TEMPORARIO": "Temporário",
    "PRO_LABORE": "Pró-labore",
}
SEXOS = ("M", "F")
SEXO_ROTULOS = {"M": "Masculino", "F": "Feminino"}
ESTADOS_CIVIS = ("SOLTEIRO", "CASADO", "DIVORCIADO", "VIUVO", "UNIAO_ESTAVEL")
ESTADO_CIVIL_ROTULOS = {
    "SOLTEIRO": "Solteiro(a)", "CASADO": "Casado(a)", "DIVORCIADO": "Divorciado(a)",
    "VIUVO": "Viúvo(a)", "UNIAO_ESTAVEL": "União estável",
}
ESCOLARIDADES = ("NAO_ALFABETIZADO", "INFANTIL", "FUNDAMENTAL", "MEDIO",
                 "SUPERIOR", "POS", "MESTRADO", "DOUTORADO")
ESCOLARIDADE_ROTULOS = {
    "NAO_ALFABETIZADO": "Não alfabetizado", "INFANTIL": "Educação infantil",
    "FUNDAMENTAL": "Ensino fundamental", "MEDIO": "Ensino médio",
    "SUPERIOR": "Ensino superior", "POS": "Pós-graduação",
    "MESTRADO": "Mestrado", "DOUTORADO": "Doutorado",
}
ORGAOS_RG = ("SSP", "DETRAN", "MTE", "ME")
UFS = ("AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS",
       "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC",
       "SE", "SP", "TO")
CONTRATOS_EXPERIENCIA = (
    "45+45 dias", "30+60 dias", "60+30 dias", "30+30 dias", "Sem contrato de experiência",
)
# "Cartão" na categoria de transporte quer dizer que NÃO se paga em dinheiro —
# e era a origem de pagamento em duplicidade.
CATEGORIAS_ALIMENTACAO = ("SEG_QUI", "SEG_SEX", "MENSAL", "MES")
CATEGORIA_ALIMENTACAO_ROTULOS = {
    "SEG_QUI": "Segunda à quinta", "SEG_SEX": "Segunda à sexta",
    "MENSAL": "Mensal", "MES": "Mês",
}
CATEGORIAS_TRANSPORTE = ("DIARIO", "MENSAL", "SEG_E_SEX", "VALE_TRANSPORTE",
                         "DIARIO_E_VT", "VEICULO", "MOTOCICLETA", "BICICLETA", "CARTAO")
CATEGORIA_TRANSPORTE_ROTULOS = {
    "DIARIO": "Diário", "MENSAL": "Mensal", "SEG_E_SEX": "Segunda e sexta",
    "VALE_TRANSPORTE": "Vale-transporte", "DIARIO_E_VT": "Diário e vale-transporte",
    "VEICULO": "Veículo", "MOTOCICLETA": "Motocicleta", "BICICLETA": "Bicicleta",
    "CARTAO": "Cartão (não paga em dinheiro)",
}
MEIOS_TRANSPORTE = ("NENHUM", "COLETIVO", "MOTO", "BICICLETA")
MEIO_TRANSPORTE_ROTULOS = {"NENHUM": "Nenhum", "COLETIVO": "Transporte coletivo",
                           "MOTO": "Moto", "BICICLETA": "Bicicleta"}
PARENTESCOS = ("FILHO", "CONJUGE", "ENTEADO", "PAI", "MAE", "IRMAO", "AVO", "OUTRO")
PARENTESCO_ROTULOS = {
    "FILHO": "Filho(a)", "CONJUGE": "Cônjuge", "ENTEADO": "Enteado(a)",
    "PAI": "Pai", "MAE": "Mãe", "IRMAO": "Irmão/irmã", "AVO": "Avô/avó",
    "OUTRO": "Outro",
}
FORMAS_DESLIGAMENTO = ("PEDIDO_COLABORADOR", "FIM_EXPERIENCIA",
                       "AVISO_TRABALHADO", "AVISO_INDENIZADO")
FORMA_DESLIGAMENTO_ROTULOS = {
    "PEDIDO_COLABORADOR": "A pedido do colaborador",
    "FIM_EXPERIENCIA": "Encerramento de contrato de experiência",
    "AVISO_TRABALHADO": "Demissão com aviso trabalhado",
    "AVISO_INDENIZADO": "Demissão com aviso indenizado",
}
MOTIVOS_ALTERACAO = ("MERITO", "PROMOCAO", "DISSIDIO", "ENQUADRAMENTO",
                     "CORRECAO", "TRANSFERENCIA")
MOTIVO_ALTERACAO_ROTULOS = {
    "MERITO": "Mérito", "PROMOCAO": "Promoção", "DISSIDIO": "Dissídio",
    "ENQUADRAMENTO": "Enquadramento", "CORRECAO": "Correção",
    "TRANSFERENCIA": "Transferência de obra",
}


def opcoes() -> dict[str, Any]:
    """Tudo que a tela precisa para montar as caixinhas, de uma vez."""
    def par(chaves, rotulos):
        return [{"valor": c, "rotulo": rotulos.get(c, c)} for c in chaves]
    return {
        "vinculos": par(VINCULOS, VINCULO_ROTULOS),
        "tipos_contrato": par(TIPOS_CONTRATO, TIPO_CONTRATO_ROTULOS),
        "sexos": par(SEXOS, SEXO_ROTULOS),
        "estados_civis": par(ESTADOS_CIVIS, ESTADO_CIVIL_ROTULOS),
        "escolaridades": par(ESCOLARIDADES, ESCOLARIDADE_ROTULOS),
        "orgaos_rg": [{"valor": o, "rotulo": o} for o in ORGAOS_RG],
        "ufs": [{"valor": u, "rotulo": u} for u in UFS],
        "contratos_experiencia": [{"valor": c, "rotulo": c} for c in CONTRATOS_EXPERIENCIA],
        "categorias_alimentacao": par(CATEGORIAS_ALIMENTACAO, CATEGORIA_ALIMENTACAO_ROTULOS),
        "categorias_transporte": par(CATEGORIAS_TRANSPORTE, CATEGORIA_TRANSPORTE_ROTULOS),
        "meios_transporte": par(MEIOS_TRANSPORTE, MEIO_TRANSPORTE_ROTULOS),
        "parentescos": par(PARENTESCOS, PARENTESCO_ROTULOS),
        "formas_desligamento": par(FORMAS_DESLIGAMENTO, FORMA_DESLIGAMENTO_ROTULOS),
        "motivos_alteracao": par(MOTIVOS_ALTERACAO, MOTIVO_ALTERACAO_ROTULOS),
    }


# ---------------------------------------------------------------------------
# Peças
# ---------------------------------------------------------------------------
def _digitos(v: Any) -> str:
    return re.sub(r"\D", "", str(v or ""))


def _sem_acento(t: Any) -> str:
    n = unicodedata.normalize("NFKD", str(t or ""))
    return "".join(c for c in n if not unicodedata.combining(c))


def pis_valido(bruto: Any) -> bool:
    """O dígito verificador do PIS/NIS (11 dígitos, módulo 11, pesos 3→2).

    Vale a conferência porque PIS errado só aparece na hora de transmitir a
    folha, semanas depois — e aí o erro já está em todo lugar.
    """
    d = _digitos(bruto)
    if len(d) != 11 or d == d[0] * 11:
        return False
    pesos = (3, 2, 9, 8, 7, 6, 5, 4, 3, 2)
    soma = sum(int(d[i]) * pesos[i] for i in range(10))
    resto = soma % 11
    return int(d[10]) == (0 if resto < 2 else 11 - resto)


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
    texto = str(v or "").strip()
    if not texto:
        return None
    for formato in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            from datetime import datetime
            return datetime.strptime(texto, formato).date()
        except ValueError:
            continue
    raise ErroValidacao(f"Data inválida em {campo}: {v!r}. Use dia/mês/ano.")


def _booleano(v: Any) -> Optional[bool]:
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        return v
    return _sem_acento(v).strip().upper() in ("1", "SIM", "TRUE", "S", "ON")


def _da_lista(v: Any, validos: tuple, campo: str) -> Optional[str]:
    texto = str(v or "").strip().upper()
    if not texto:
        return None
    if texto not in validos:
        raise ErroValidacao(f"{campo}: valor desconhecido {v!r}.")
    return texto


# ---------------------------------------------------------------------------
# O ALERTA DE DUPLICADO — enquanto se digita
# ---------------------------------------------------------------------------
def conferir_cpf(s: Session, cpf: Any) -> dict[str, Any]:
    """Esta pessoa já existe? Responde ANTES de a pessoa preencher o resto.

    Pedido do dono: *"o RP também tem que alertar, por exemplo, se a pessoa está
    começando a digitar o cadastro, esse cadastro aqui já existe"*.

    No Pipefy isto era um cenário do Make que procurava o CPF na planilha
    DEPOIS de o card ser criado, mandava e-mail e **apagava o card** — a pessoa
    perdia 51 campos preenchidos. Aqui o aviso chega no quarto dígito.

    Três respostas possíveis, e cada uma manda uma ação diferente na tela:
      · `livre`      — segue o cadastro;
      · `ativo`      — a pessoa já trabalha aqui: abrir a ficha dela, não criar;
      · `desligado`  — é READMISSÃO: trazer o cadastro de volta, sem redigitar.
    """
    from app.apps.erp.core.cadastros.validadores import cpf_valido
    d = _digitos(cpf)
    if len(d) < 11:
        return {"situacao": "incompleto"}
    if not cpf_valido(d):
        return {"situacao": "invalido",
                "aviso": "Este CPF não passa na conferência dos dígitos — confira o número."}
    c = s.scalars(select(Colaborador).where(Colaborador.cpf == d)).first()
    if c is None:
        return {"situacao": "livre"}
    desligado = (c.situacao or "").upper() == "DESLIGADO" or (
        c.demissao is not None and c.demissao <= date.today())
    saida = {
        "situacao": "desligado" if desligado else "ativo",
        "colaborador_id": c.id, "nome": c.nome,
        "matricula": c.matricula or "", "obra_id": c.obra_id,
        "admissao": c.admissao.isoformat() if c.admissao else None,
        "demissao": c.demissao.isoformat() if c.demissao else None,
        "situacao_cadastro": c.situacao,
    }
    saida["aviso"] = (
        f"{c.nome} já tem cadastro e está DESLIGADO"
        + (f" desde {c.demissao:%d/%m/%Y}" if c.demissao else "")
        + ". É uma readmissão: traga o cadastro de volta em vez de criar outro."
    ) if desligado else (
        f"{c.nome} já tem cadastro e está ATIVO. Abra a ficha dela para corrigir "
        "o que precisa — dois cadastros com o mesmo CPF fazem a folha pagar duas vezes."
    )
    return saida


# ---------------------------------------------------------------------------
# Gravar
# ---------------------------------------------------------------------------
TEXTOS = (
    "matricula", "nome_mae", "nome_pai", "naturalidade", "rg", "pis",
    "ctps_numero", "ctps_serie", "cep", "logradouro", "numero_endereco",
    "complemento", "bairro", "municipio", "email", "cnpj_admissao",
    "razao_social_pj", "cnpj_pj", "contrato_experiencia", "observacao_auxilio",
    "requerente", "id_fortes", "convencao_categoria", "observacoes",
    "pix_chave", "pix_tipo", "banco", "agencia", "conta", "telefone",
)
DATAS = ("data_nascimento", "rg_emissao", "ctps_emissao", "data_inicio",
         "admissao", "demissao", "aviso_previo", "ultimo_dia")
DINHEIRO = ("salario", "valor_mes", "valor_diaria", "aux_alimentacao",
            "aux_transporte", "valor_gratificacao")
BOOLEANOS = ("ctps_digital", "primeiro_emprego", "mensalista", "recebe_gratificacao",
             "parcela_unica", "recebe_via_sp", "cesta_basica_extra", "vale_refeicao",
             "adicional_transferencia", "solicitar_salario_familia")
LIGACOES = ("funcao_id", "obra_id", "cargo_id", "convencao_id", "empresa_id",
            "responsavel_id")
DA_LISTA = {
    "sexo": SEXOS, "estado_civil": ESTADOS_CIVIS, "escolaridade": ESCOLARIDADES,
    "vinculo": VINCULOS, "tipo_contrato": TIPOS_CONTRATO,
    "rg_orgao": ORGAOS_RG, "rg_uf": UFS, "ctps_uf": UFS, "uf": UFS,
    "naturalidade_uf": UFS, "categoria_alimentacao": CATEGORIAS_ALIMENTACAO,
    "categoria_transporte": CATEGORIAS_TRANSPORTE, "meio_transporte": MEIOS_TRANSPORTE,
}


def salvar(s: Session, dados: dict[str, Any], usuario: Usuario) -> Colaborador:
    """Cria ou atualiza o cadastro. O CPF é a identidade e não se troca."""
    from app.apps.erp.core.cadastros.validadores import cpf_valido

    cpf = _digitos(dados.get("cpf"))
    if not cpf_valido(cpf):
        raise ErroValidacao("CPF inválido.")
    nome = " ".join(str(dados.get("nome") or "").split())
    if len(nome) < 4:
        raise ErroValidacao("Informe o nome completo.")

    c = s.scalars(select(Colaborador).where(Colaborador.cpf == cpf)).first()
    novo = c is None
    if novo:
        c = Colaborador(cpf=cpf, nome=nome)
        s.add(c)
    antes = {} if novo else {
        "cargo_id": c.cargo_id, "salario": str(c.salario or ""), "obra_id": c.obra_id}
    c.nome = nome

    if "pis" in dados and _digitos(dados["pis"]) and not pis_valido(dados["pis"]):
        raise ErroValidacao("PIS/NIS inválido — confira os 11 dígitos.")

    for campo in TEXTOS:
        if campo in dados:
            setattr(c, campo, (str(dados[campo]).strip() or None))
    for campo in DATAS:
        if campo in dados:
            setattr(c, campo, _data(dados[campo], campo))
    for campo in DINHEIRO:
        if campo in dados:
            setattr(c, campo, _dec(dados[campo], campo))
    for campo in BOOLEANOS:
        if campo in dados:
            setattr(c, campo, _booleano(dados[campo]))
    for campo, validos in DA_LISTA.items():
        if campo in dados:
            setattr(c, campo, _da_lista(dados[campo], validos, campo))
    # Escolher "—" tem de DESLIGAR o vínculo, não manter o antigo (a lição de
    # 22/09/2026, em `pessoal.salvar_colaborador`).
    for campo in LIGACOES:
        if campo in dados:
            bruto = dados[campo]
            setattr(c, campo, int(bruto) if bruto not in (None, "", 0, "0") else None)
    if "situacao" in dados:
        c.situacao = (str(dados["situacao"]).strip().upper() or "ATIVO")
    if "regime" in dados:
        c.regime = (str(dados["regime"]).strip() or "CLT")

    # A SITUAÇÃO SEGUE A DATA, e não o contrário: quem tem demissão lançada está
    # desligado, mesmo que alguém esqueça de trocar a caixinha.
    if c.demissao and (c.situacao or "").upper() == "ATIVO":
        c.situacao = "DESLIGADO"
    # O vínculo nasce CTPS quando ninguém disse — é o caso da esmagadora
    # maioria, e deixar nulo faria a folha ter de adivinhar.
    if novo and not c.vinculo:
        c.vinculo = "CTPS"

    s.flush()

    # O PRIMEIRO VÍNCULO nasce com o cadastro: sem ele, a primeira readmissão
    # não teria o que fechar, e a história começaria pela metade.
    if novo and (c.admissao or c.data_inicio):
        s.add(ColaboradorVinculo(
            colaborador_id=c.id, vinculo=c.vinculo or "CTPS",
            tipo_contrato=c.tipo_contrato, empresa_id=c.empresa_id,
            cnpj_admissao=c.cnpj_admissao, cargo_id=c.cargo_id,
            convencao_id=c.convencao_id, obra_id=c.obra_id, salario=c.salario,
            matricula=c.matricula, admissao=c.admissao or c.data_inicio,
            demissao=c.demissao, usuario_id=usuario.id if usuario else None))
        # O FLUSH AQUI NÃO É ENFEITE: a sessão do ERP é `autoflush=False`, então
        # objeto pendente não aparece em consulta. Sem isto, quem chama `salvar`
        # e em seguida `vinculo_aberto` na MESMA transação — o atendimento de um
        # pré-cadastro na fila do DP faz exatamente isso — não acharia o vínculo
        # que acabou de nascer, e abriria um segundo.
        s.flush()

    registrar_evento(s, "colaborador", c.id, "CRIADO" if novo else "ALTERADO",
                     {"nome": c.nome, "cpf": c.cpf, "antes": antes},
                     usuario.id if usuario else None)
    return c


# ---------------------------------------------------------------------------
# Dependentes
# ---------------------------------------------------------------------------
def salvar_dependente(s: Session, colaborador_id: int, dados: dict[str, Any],
                      usuario: Usuario) -> ColaboradorDependente:
    c = s.get(Colaborador, colaborador_id)
    if c is None:
        raise ErroValidacao("Colaborador não encontrado.")
    nome = " ".join(str(dados.get("nome") or "").split())
    if len(nome) < 3:
        raise ErroValidacao("Informe o nome do dependente.")
    parentesco = _da_lista(dados.get("parentesco"), PARENTESCOS, "parentesco")
    if not parentesco:
        raise ErroValidacao("Informe o parentesco.")
    cpf = _digitos(dados.get("cpf")) or None
    if cpf:
        from app.apps.erp.core.cadastros.validadores import cpf_valido
        if not cpf_valido(cpf):
            raise ErroValidacao("CPF do dependente inválido.")
    d = None
    if dados.get("id"):
        d = s.get(ColaboradorDependente, int(dados["id"]))
        if d is None or d.colaborador_id != c.id:
            raise ErroValidacao("Dependente não encontrado.")
    if d is None:
        d = ColaboradorDependente(colaborador_id=c.id, nome=nome, parentesco=parentesco)
        s.add(d)
    d.nome, d.parentesco = nome, parentesco
    d.data_nascimento = _data(dados.get("data_nascimento"), "data_nascimento")
    d.cpf = cpf
    d.salario_familia = bool(_booleano(dados.get("salario_familia")))
    d.dependente_ir = bool(_booleano(dados.get("dependente_ir")))
    d.observacoes = (str(dados.get("observacoes") or "").strip() or None)
    s.flush()
    registrar_evento(s, "colaborador", c.id, "DEPENDENTE_SALVO",
                     {"nome": d.nome, "parentesco": d.parentesco},
                     usuario.id if usuario else None)
    return d


def excluir_dependente(s: Session, colaborador_id: int, dependente_id: int,
                       usuario: Usuario) -> None:
    d = s.get(ColaboradorDependente, dependente_id)
    if d is None or d.colaborador_id != colaborador_id:
        raise ErroValidacao("Dependente não encontrado.")
    registrar_evento(s, "colaborador", colaborador_id, "DEPENDENTE_EXCLUIDO",
                     {"nome": d.nome, "parentesco": d.parentesco},
                     usuario.id if usuario else None)
    s.delete(d)


# ---------------------------------------------------------------------------
# Desligar, readmitir e trocar o vínculo
# ---------------------------------------------------------------------------
def vinculo_aberto(s: Session, colaborador_id: int) -> Optional[ColaboradorVinculo]:
    return s.scalars(select(ColaboradorVinculo).where(
        ColaboradorVinculo.colaborador_id == colaborador_id,
        ColaboradorVinculo.demissao.is_(None))).first()


def desligar(s: Session, colaborador_id: int, dados: dict[str, Any],
             usuario: Usuario) -> Colaborador:
    """Fecha o vínculo de hoje. Não apaga nada."""
    c = s.get(Colaborador, colaborador_id)
    if c is None:
        raise ErroValidacao("Colaborador não encontrado.")
    saida = _data(dados.get("data_saida") or dados.get("demissao"), "data de saída")
    if saida is None:
        raise ErroValidacao("Informe a data de saída.")
    forma = _da_lista(dados.get("forma_desligamento"), FORMAS_DESLIGAMENTO,
                      "forma de desligamento")
    c.demissao, c.situacao = saida, "DESLIGADO"
    c.aviso_previo = _data(dados.get("aviso_previo"), "aviso prévio") or c.aviso_previo
    c.ultimo_dia = _data(dados.get("ultimo_dia"), "último dia") or c.ultimo_dia
    v = vinculo_aberto(s, c.id)
    if v is not None:
        v.demissao, v.forma_desligamento = saida, forma
        v.aviso_previo, v.ultimo_dia = c.aviso_previo, c.ultimo_dia
        v.motivo = (str(dados.get("motivo") or "").strip() or None)
    s.flush()
    registrar_evento(s, "colaborador", c.id, "DESLIGADO",
                     {"data_saida": str(saida), "forma": forma,
                      "motivo": dados.get("motivo")}, usuario.id if usuario else None)
    return c


def readmitir(s: Session, colaborador_id: int, dados: dict[str, Any],
              usuario: Usuario) -> Colaborador:
    """Traz a pessoa de volta SEM apagar a passagem anterior.

    O dono: *"a gente tem a base dos desligados, para a gente não refazer um
    cadastro que já existe (…) a gente só procura a pessoa e seleciona"*.

    O que muda: o cadastro volta a ATIVO com a admissão nova, e nasce um vínculo
    NOVO apontando para o anterior. O que NÃO muda: o vínculo antigo continua
    lá, com a demissão dele — é o que a rescisão e o INSS perguntam.
    """
    c = s.get(Colaborador, colaborador_id)
    if c is None:
        raise ErroValidacao("Colaborador não encontrado.")
    admissao = _data(dados.get("admissao") or dados.get("data_inicio"), "admissão")
    if admissao is None:
        raise ErroValidacao("Informe a data de admissão da readmissão.")
    anterior = s.scalars(select(ColaboradorVinculo).where(
        ColaboradorVinculo.colaborador_id == c.id).order_by(
        ColaboradorVinculo.admissao.desc())).first()
    if anterior is not None and anterior.demissao is None:
        raise ErroValidacao(
            f"{c.nome} ainda está com vínculo aberto desde "
            f"{anterior.admissao:%d/%m/%Y}. Desligue antes de readmitir.")
    if anterior is not None and admissao <= anterior.demissao:
        raise ErroValidacao(
            f"A saída anterior foi em {anterior.demissao:%d/%m/%Y}. A readmissão "
            "precisa ser depois disso.")

    vinculo = _da_lista(dados.get("vinculo"), VINCULOS, "vínculo") or c.vinculo or "CTPS"
    novo = ColaboradorVinculo(
        colaborador_id=c.id, vinculo=vinculo,
        tipo_contrato=_da_lista(dados.get("tipo_contrato"), TIPOS_CONTRATO,
                                "tipo de contrato") or c.tipo_contrato,
        empresa_id=int(dados["empresa_id"]) if dados.get("empresa_id") else c.empresa_id,
        cnpj_admissao=(str(dados.get("cnpj_admissao") or "").strip() or c.cnpj_admissao),
        cargo_id=int(dados["cargo_id"]) if dados.get("cargo_id") else c.cargo_id,
        convencao_id=(int(dados["convencao_id"]) if dados.get("convencao_id")
                      else c.convencao_id),
        obra_id=int(dados["obra_id"]) if dados.get("obra_id") else c.obra_id,
        salario=_dec(dados.get("salario"), "salário") or c.salario,
        matricula=(str(dados.get("matricula") or "").strip() or None),
        admissao=admissao, vinculo_anterior_id=anterior.id if anterior else None,
        usuario_id=usuario.id if usuario else None)
    s.add(novo)
    # O cadastro passa a refletir o vínculo de hoje.
    c.situacao, c.demissao = "ATIVO", None
    c.aviso_previo = c.ultimo_dia = None
    c.admissao, c.data_inicio = admissao, admissao
    c.vinculo, c.tipo_contrato = novo.vinculo, novo.tipo_contrato
    c.cargo_id, c.convencao_id, c.obra_id = novo.cargo_id, novo.convencao_id, novo.obra_id
    c.empresa_id, c.cnpj_admissao = novo.empresa_id, novo.cnpj_admissao
    c.salario = novo.salario
    if novo.matricula:
        c.matricula = novo.matricula
    s.flush()
    registrar_evento(s, "colaborador", c.id, "READMITIDO",
                     {"admissao": str(admissao), "vinculo": novo.vinculo,
                      "vinculo_anterior_id": novo.vinculo_anterior_id},
                     usuario.id if usuario else None)
    return c


def trocar_vinculo(s: Session, colaborador_id: int, dados: dict[str, Any],
                   usuario: Usuario) -> Colaborador:
    """Prestador que passa a ter carteira (ou o contrário).

    O dono: *"às vezes você tem um prestador de serviço (…) inicialmente admitiu
    ele como prestador, mas quer admitir na carteira (…) e tornar ele CTPS"*.

    É DOIS vínculos, não um campo trocado: o período de prestador tem de
    continuar legível, porque o INSS e a Justiça perguntam por ele. Fecha o
    vínculo atual no dia anterior e abre o novo — sem buraco e sem sobreposição.
    """
    c = s.get(Colaborador, colaborador_id)
    if c is None:
        raise ErroValidacao("Colaborador não encontrado.")
    destino = _da_lista(dados.get("vinculo"), VINCULOS, "vínculo")
    if not destino:
        raise ErroValidacao("Informe o vínculo novo (CTPS ou prestador).")
    inicio = _data(dados.get("admissao") or dados.get("inicio"), "início do vínculo novo")
    if inicio is None:
        raise ErroValidacao("Informe a partir de quando o vínculo novo vale.")
    atual = vinculo_aberto(s, c.id)
    if atual is None:
        raise ErroValidacao(
            f"{c.nome} não tem vínculo aberto. Use a readmissão em vez da troca.")
    if atual.vinculo == destino:
        raise ErroValidacao(
            f"{c.nome} já está como {VINCULO_ROTULOS.get(destino, destino)}.")
    if inicio <= atual.admissao:
        raise ErroValidacao(
            f"O vínculo atual começou em {atual.admissao:%d/%m/%Y}. O novo precisa "
            "começar depois disso.")
    from datetime import timedelta
    atual.demissao = inicio - timedelta(days=1)
    atual.motivo = f"passou a {VINCULO_ROTULOS.get(destino, destino)}"
    novo = ColaboradorVinculo(
        colaborador_id=c.id, vinculo=destino,
        tipo_contrato=_da_lista(dados.get("tipo_contrato"), TIPOS_CONTRATO,
                                "tipo de contrato"),
        empresa_id=int(dados["empresa_id"]) if dados.get("empresa_id") else atual.empresa_id,
        cnpj_admissao=(str(dados.get("cnpj_admissao") or "").strip() or atual.cnpj_admissao),
        cargo_id=int(dados["cargo_id"]) if dados.get("cargo_id") else atual.cargo_id,
        convencao_id=(int(dados["convencao_id"]) if dados.get("convencao_id")
                      else atual.convencao_id),
        obra_id=int(dados["obra_id"]) if dados.get("obra_id") else atual.obra_id,
        salario=_dec(dados.get("salario"), "salário") or atual.salario,
        matricula=(str(dados.get("matricula") or "").strip() or atual.matricula),
        admissao=inicio, vinculo_anterior_id=atual.id,
        usuario_id=usuario.id if usuario else None)
    s.add(novo)
    c.vinculo, c.tipo_contrato = destino, novo.tipo_contrato
    c.admissao = inicio
    if novo.salario:
        c.salario = novo.salario
    s.flush()
    registrar_evento(s, "colaborador", c.id, "VINCULO_TROCADO",
                     {"de": atual.vinculo, "para": destino, "desde": str(inicio)},
                     usuario.id if usuario else None)
    return c


# ---------------------------------------------------------------------------
# Alteração de salário, cargo e obra — com motivo
# ---------------------------------------------------------------------------
def registrar_alteracao(s: Session, colaborador_id: int, dados: dict[str, Any],
                        usuario: Usuario) -> ColaboradorAlteracao:
    """Grava a mudança E o estado novo, na mesma transação.

    O dono: *"tem a questão salarial e as alterações salariais pra serem
    gravadas"*. No Pipefy o pedido de alteração de cargo existia (com cargo
    pleiteado, deferido, salário e justificativa) mas o resultado não ficava em
    lugar nenhum: o card fechava e o campo do cadastro era sobrescrito.
    """
    c = s.get(Colaborador, colaborador_id)
    if c is None:
        raise ErroValidacao("Colaborador não encontrado.")
    vigencia = _data(dados.get("vigencia"), "vigência") or date.today()
    salario_novo = _dec(dados.get("salario"), "salário")
    cargo_novo = int(dados["cargo_id"]) if dados.get("cargo_id") else None
    conv_nova = int(dados["convencao_id"]) if dados.get("convencao_id") else None
    obra_nova = int(dados["obra_id"]) if dados.get("obra_id") else None
    if not any((salario_novo, cargo_novo, conv_nova, obra_nova)):
        raise ErroValidacao("Informe o que está mudando: salário, cargo, convenção ou obra.")
    mudou_cargo = cargo_novo is not None and cargo_novo != c.cargo_id
    mudou_salario = salario_novo is not None and salario_novo != c.salario
    tipo = ("CARGO_E_SALARIO" if mudou_cargo and mudou_salario
            else "CARGO" if mudou_cargo
            else "SALARIO" if mudou_salario
            else "OBRA" if obra_nova and obra_nova != c.obra_id
            else "CONVENCAO")
    a = ColaboradorAlteracao(
        colaborador_id=c.id, vinculo_id=(vinculo_aberto(s, c.id) or ColaboradorVinculo()).id,
        tipo=tipo, vigencia=vigencia,
        cargo_de_id=c.cargo_id, cargo_para_id=cargo_novo or c.cargo_id,
        convencao_de_id=c.convencao_id, convencao_para_id=conv_nova or c.convencao_id,
        obra_de_id=c.obra_id, obra_para_id=obra_nova or c.obra_id,
        salario_de=c.salario, salario_para=salario_novo or c.salario,
        motivo=_da_lista(dados.get("motivo"), MOTIVOS_ALTERACAO, "motivo"),
        justificativa=(str(dados.get("justificativa") or "").strip() or None),
        usuario_id=usuario.id if usuario else None)
    s.add(a)
    if cargo_novo:
        c.cargo_id = cargo_novo
    if conv_nova:
        c.convencao_id = conv_nova
    if obra_nova:
        c.obra_id = obra_nova
    if salario_novo:
        c.salario = salario_novo
    v = vinculo_aberto(s, c.id)
    if v is not None:
        v.cargo_id, v.convencao_id, v.obra_id = c.cargo_id, c.convencao_id, c.obra_id
        v.salario = c.salario
    s.flush()
    registrar_evento(s, "colaborador", c.id, "ALTERACAO_REGISTRADA",
                     {"tipo": tipo, "vigencia": str(vigencia),
                      "salario_de": str(a.salario_de or ""),
                      "salario_para": str(a.salario_para or ""),
                      "motivo": a.motivo}, usuario.id if usuario else None)
    return a


# ---------------------------------------------------------------------------
# Ler a ficha
# ---------------------------------------------------------------------------
def ficha(s: Session, colaborador_id: int) -> dict[str, Any]:
    c = s.get(Colaborador, colaborador_id)
    if c is None:
        raise ErroValidacao("Colaborador não encontrado.")
    cargos = {x.id: x.nome for x in s.scalars(select(Cargo)).all()}
    convs = {x.id: x.nome for x in s.scalars(select(Convencao)).all()}
    campos = [col.name for col in Colaborador.__table__.columns]
    dados: dict[str, Any] = {}
    for campo in campos:
        v = getattr(c, campo)
        dados[campo] = (v.isoformat() if hasattr(v, "isoformat")
                        else float(v) if isinstance(v, Decimal) else v)
    dados["cargo"] = cargos.get(c.cargo_id or 0, "")
    dados["convencao"] = convs.get(c.convencao_id or 0, "")
    dados["dependentes"] = [{
        "id": d.id, "nome": d.nome, "parentesco": d.parentesco,
        "parentesco_rotulo": PARENTESCO_ROTULOS.get(d.parentesco, d.parentesco),
        "data_nascimento": d.data_nascimento.isoformat() if d.data_nascimento else None,
        "cpf": d.cpf or "", "salario_familia": d.salario_familia,
        "dependente_ir": d.dependente_ir, "observacoes": d.observacoes or "",
    } for d in s.scalars(select(ColaboradorDependente).where(
        ColaboradorDependente.colaborador_id == c.id).order_by(
        ColaboradorDependente.data_nascimento)).all()]
    dados["vinculos"] = [{
        "id": v.id, "vinculo": v.vinculo,
        "vinculo_rotulo": VINCULO_ROTULOS.get(v.vinculo, v.vinculo),
        "tipo_contrato": v.tipo_contrato or "",
        "tipo_contrato_rotulo": TIPO_CONTRATO_ROTULOS.get(v.tipo_contrato or "", ""),
        "cargo": cargos.get(v.cargo_id or 0, ""), "convencao": convs.get(v.convencao_id or 0, ""),
        "salario": float(v.salario) if v.salario else None,
        "matricula": v.matricula or "",
        "admissao": v.admissao.isoformat(),
        "demissao": v.demissao.isoformat() if v.demissao else None,
        "aberto": v.demissao is None,
        "forma_desligamento": v.forma_desligamento or "",
        "forma_rotulo": FORMA_DESLIGAMENTO_ROTULOS.get(v.forma_desligamento or "", ""),
        "motivo": v.motivo or "", "readmissao": v.vinculo_anterior_id is not None,
    } for v in s.scalars(select(ColaboradorVinculo).where(
        ColaboradorVinculo.colaborador_id == c.id).order_by(
        ColaboradorVinculo.admissao.desc())).all()]
    dados["alteracoes"] = [{
        "id": a.id, "tipo": a.tipo, "vigencia": a.vigencia.isoformat(),
        "cargo_de": cargos.get(a.cargo_de_id or 0, ""),
        "cargo_para": cargos.get(a.cargo_para_id or 0, ""),
        "salario_de": float(a.salario_de) if a.salario_de else None,
        "salario_para": float(a.salario_para) if a.salario_para else None,
        "motivo": a.motivo or "",
        "motivo_rotulo": MOTIVO_ALTERACAO_ROTULOS.get(a.motivo or "", ""),
        "justificativa": a.justificativa or "",
    } for a in s.scalars(select(ColaboradorAlteracao).where(
        ColaboradorAlteracao.colaborador_id == c.id).order_by(
        ColaboradorAlteracao.vigencia.desc())).all()]
    # O QUE FALTA PARA ADMITIR: a lista que o DP persegue, calculada aqui para a
    # tela e a Agenda dizerem a mesma coisa.
    dados["falta"] = o_que_falta(c)
    # OS PAPÉIS, separados dos campos: campo em branco é digitação que falta;
    # papel em branco é documento que ninguém pediu à pessoa. São duas conversas
    # diferentes, e misturá-las faria a ficha dizer "falta CTPS" tanto para o
    # número quanto para a imagem dela.
    dados["falta_documento"] = documentos_que_faltam(s, c.id, c.vinculo)
    return dados


CAMPOS_OBRIGATORIOS_CTPS = (
    ("data_nascimento", "data de nascimento"), ("nome_mae", "nome da mãe"),
    ("rg", "nº da identidade"), ("rg_orgao", "órgão emissor"),
    ("pis", "nº do PIS"), ("ctps_numero", "nº da CTPS"),
    ("logradouro", "endereço"), ("municipio", "município"), ("uf", "UF"),
    ("cep", "CEP"), ("telefone", "telefone"), ("cargo_id", "cargo"),
    ("obra_id", "obra"), ("admissao", "data de admissão"), ("salario", "salário"),
)


# OS SEIS PAPÉIS QUE O PIPE EXIGIA PARA ADMITIR, com o tipo do catálogo do
# Arquivo. Os quatro primeiros são de todo mundo; CTPS só de quem é CTPS.
#
# Por que isto existe: no Pipefy, o anexo era campo obrigatório do formulário —
# e os papéis iam para uma pasta do Dropbox por pessoa. No ERP eles vão para o
# Google Drive, pelo Arquivo, e a obrigatoriedade virou ESTA LISTA: o cadastro
# salva sem os papéis (senão ninguém consegue começar), mas a ficha diz, em
# letras, o que falta para a pessoa ser admitida de verdade.
DOCUMENTOS_DA_ADMISSAO = (
    ("DOC-IDENTIDADE", "documento de identificação (RG, CNH ou CPF)", False),
    ("COMPROVANTE-ENDERECO-PESSOA", "comprovante de endereço", False),
    ("FOTO-COLABORADOR", "foto do colaborador", False),
    ("ASO", "ASO (atestado de saúde ocupacional)", False),
    ("CTPS", "carteira de trabalho", True),
)


def documentos_que_faltam(s: Session, colaborador_id: int,
                          vinculo: Optional[str] = None) -> list[str]:
    """Quais dos papéis da admissão esta pessoa ainda não tem no Arquivo.

    Documento excluído some da lista de verdade (o Arquivo apaga o registro,
    não desativa), então ele volta a ser cobrado — que é o certo: se o papel foi
    excluído por estar errado, ele tem de ser pedido de novo.
    """
    from app.apps.erp.db.models.financeiro import Documento
    tem = {d.tipo_codigo for d in s.scalars(select(Documento).where(
        Documento.colaborador_id == colaborador_id)).all()}
    so_ctps = (vinculo or "CTPS").upper() != "PRESTADOR"
    return [rotulo for codigo, rotulo, exige_ctps in DOCUMENTOS_DA_ADMISSAO
            if codigo not in tem and (so_ctps or not exige_ctps)]


def o_que_falta(c: Colaborador) -> list[str]:
    """PURA. O que impede esta pessoa de ser admitida de verdade.

    Prestador não precisa de PIS nem de CTPS — e exigir isso dele foi o que, no
    pipe, levou a gente a preencher campo com "0" para o formulário deixar
    salvar.
    """
    vinculo = (c.vinculo or "CTPS").upper()
    falta = []
    for campo, rotulo in CAMPOS_OBRIGATORIOS_CTPS:
        if vinculo == "PRESTADOR" and campo in ("pis", "ctps_numero"):
            continue
        if not getattr(c, campo, None):
            falta.append(rotulo)
    return falta
