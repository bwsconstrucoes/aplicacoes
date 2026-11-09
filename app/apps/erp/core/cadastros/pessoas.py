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
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import (
    Cargo, Colaborador, ColaboradorAlteracao, ColaboradorDependente,
    ColaboradorEtapa, ColaboradorVinculo, Convencao, Usuario,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# As listas do pipe, em um lugar só
# ---------------------------------------------------------------------------
VINCULOS = ("CTPS", "PRESTADOR")
VINCULO_ROTULOS = {"CTPS": "Carteira assinada (CTPS)",
                   "PRESTADOR": "Prestador de serviço"}
# Como a PLANILHA escreve o mesmo dado (coluna D "Tipo de Cadastro" e coluna Z
# "Tipo" são a mesma coisa, confirmado por ele em 05/11/2026). A folha compara
# texto, então a palavra tem de chegar igual à que ela já lia.
VINCULO_PLANILHA = {"CTPS": "CTPS", "PRESTADOR": "Prestador de Serviço"}
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
# O QUE SAIU DE USO, e por que não sai do código (05/11/2026, dele: *"isso aqui
# a gente nem utiliza mais, cartão no transporte"*): apagar o valor deixaria sem
# explicação a ficha de quem foi cadastrado com ele, e a folha de um mês fechado
# passaria a ler vazio onde havia dado. Então ele continua existindo e some
# apenas das caixinhas de cadastro NOVO.
CATEGORIAS_TRANSPORTE_FORA_DE_USO = ("CARTAO",)
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
        "categorias_transporte": [
            {"valor": c, "rotulo": CATEGORIA_TRANSPORTE_ROTULOS.get(c, c),
             "em_uso": c not in CATEGORIAS_TRANSPORTE_FORA_DE_USO}
            for c in CATEGORIAS_TRANSPORTE],
        "meios_transporte": par(MEIOS_TRANSPORTE, MEIO_TRANSPORTE_ROTULOS),
        "parentescos": par(PARENTESCOS, PARENTESCO_ROTULOS),
        "formas_desligamento": par(FORMAS_DESLIGAMENTO, FORMA_DESLIGAMENTO_ROTULOS),
        "motivos_alteracao": par(MOTIVOS_ALTERACAO, MOTIVO_ALTERACAO_ROTULOS),
        # AS ETAPAS VÃO JUNTO (05/11/2026) porque a tela de gestão filtra e
        # conta por elas, e a frase do que fazer é o que torna a etapa útil —
        # etapa sem a frase é só um código na tela.
        "etapas": [{"valor": e, "rotulo": ETAPA_ROTULOS[e],
                    "o_que_fazer": ETAPA_O_QUE_FAZER.get(e, ""),
                    "aberta": e in ETAPAS_ABERTAS} for e in ETAPAS],
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
    # CADASTRO NOVO NASCE NA PRIMEIRA ETAPA, e tem de ser assim: é o formulário
    # inicial chegando, que ainda não foi conferido por ninguém. A IMPORTAÇÃO
    # passa `etapa="CONCLUIDO"` de propósito — aquelas 575 pessoas já trabalham
    # e já recebem, e colocá-las em conferência criaria no primeiro dia uma fila
    # de trabalho falsa com a empresa inteira dentro.
    if "etapa" in dados:
        pedida = str(dados["etapa"] or "").strip().upper()
        if pedida and pedida not in ETAPAS:
            raise ErroValidacao("Etapa desconhecida.")
        if pedida:
            c.etapa = pedida
    elif novo:
        c.etapa = "CADASTRO_INICIAL"

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
        # pedido de cadastro na fila do DP faz exatamente isso — não acharia o vínculo
        # que acabou de nascer, e abriria um segundo.
        s.flush()

    if novo:
        s.add(ColaboradorEtapa(
            colaborador_id=c.id, de=None, para=c.etapa or "CADASTRO_INICIAL",
            motivo="cadastro criado",
            usuario_id=usuario.id if usuario else None, automatico=True))
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
    # A ETAPA ANDA COM A SITUAÇÃO aqui, porque desligar encerra o trabalho sobre
    # este cadastro — e deixar a etapa para trás faria a pessoa continuar na
    # fila do DP depois de sair da empresa.
    if (c.etapa or "").upper() != "DESLIGADO":
        c.etapa = "DESLIGADO"
        s.add(ColaboradorEtapa(
            colaborador_id=c.id, de=None, para="DESLIGADO",
            motivo="desligamento lançado", automatico=True,
            usuario_id=usuario.id if usuario else None))
        s.flush()
    registrar_evento(s, "colaborador", c.id, "DESLIGADO",
                     {"data_saida": str(saida), "forma": forma,
                      "motivo": dados.get("motivo")}, usuario.id if usuario else None)
    bloquear_acesso(s, c, usuario, motivo="desligamento")
    return c


# ---------------------------------------------------------------------------
# O ACESSO AO SISTEMA DA PESSOA — migração 096
#
# Ele, em 09/11/2026: *"nosso cadastro tem o cadastro de colaboradores, tem o
# cadastro de usuários do sistema. É para haver uma correlação entre isso aí
# (…) quando uma pessoa for desativada do cadastro, for demitido, sair da
# empresa, a gente precisaria automaticamente bloquear o acesso dela."*
#
# ⚑ ESTE É O TIPO DE COISA QUE NINGUÉM LEMBRA DE FAZER À MÃO, e cujo
# esquecimento não aparece: o ex-colaborador continua entrando, vendo obra,
# vendo pagamento, e ninguém descobre porque nada quebra. Automático é a única
# forma que funciona.
#
# O QUE O BLOQUEIO FAZ: desliga o operador (`ativo = False`). **Não apaga.**
# Apagar levaria junto a autoria de tudo o que a pessoa lançou — e "quem
# aprovou este pagamento?" viraria uma pergunta sem resposta.
# ---------------------------------------------------------------------------
def bloquear_acesso(s: Session, c: Colaborador, usuario: Optional[Usuario],
                    *, motivo: str) -> bool:
    """Desliga o acesso ao ERP de quem saiu. Devolve se havia o que desligar."""
    uid = getattr(c, "usuario_id", None)
    if not uid:
        return False
    operador = s.get(Usuario, uid)
    if operador is None or not operador.ativo:
        return False
    operador.ativo = False
    s.flush()
    registrar_evento(s, "usuario", operador.id, "ACESSO_BLOQUEADO",
                     {"motivo": motivo, "colaborador_id": c.id},
                     usuario.id if usuario else None)
    registrar_evento(s, "colaborador", c.id, "ACESSO_BLOQUEADO",
                     {"motivo": motivo, "usuario_id": operador.id},
                     usuario.id if usuario else None)
    logger.info("ERP/pessoas: acesso do operador %s bloqueado por %s",
                operador.id, motivo)
    return True


def ligar_ao_operador(s: Session, colaborador_id: int, usuario_id: Optional[int],
                      usuario: Optional[Usuario]) -> Colaborador:
    """Diz que este colaborador É este operador do ERP (ou desfaz a ligação).

    ⚑ RECUSA LIGAR UM OPERADOR QUE JÁ É DE OUTRA PESSOA. Dois colaboradores
    apontando para o mesmo login fariam o desligamento de um derrubar o acesso
    do outro — e o banco tem índice único justamente por isso; aqui a recusa
    vem com recado de gente em vez de erro de banco.
    """
    c = s.get(Colaborador, colaborador_id)
    if c is None:
        raise ErroValidacao("Colaborador não encontrado.")
    if usuario_id in (None, "", 0):
        c.usuario_id = None
        s.flush()
        registrar_evento(s, "colaborador", c.id, "ACESSO_DESVINCULADO", {},
                         usuario.id if usuario else None)
        return c
    uid = int(usuario_id)
    operador = s.get(Usuario, uid)
    if operador is None:
        raise ErroValidacao("Operador não encontrado.")
    dono = s.execute(text("""
        SELECT id, nome FROM colaboradores
         WHERE usuario_id = :u AND id <> :c"""),
        {"u": uid, "c": c.id}).mappings().first()
    if dono is not None:
        raise ErroValidacao(
            f"Este acesso já é de {dono['nome']}. Um login é de uma pessoa só.")
    c.usuario_id = uid
    s.flush()
    registrar_evento(s, "colaborador", c.id, "ACESSO_VINCULADO",
                     {"usuario_id": uid, "email": operador.email},
                     usuario.id if usuario else None)
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
    # READMISSÃO VOLTA PARA A CONFERÊNCIA, não direto para concluído: o endereço
    # muda, o documento vence, a convenção da obra nova pode ser outra. Era
    # exatamente isto que o "Checklist Readmissão" do Pipefy cobrava à mão.
    de = (c.etapa or "").upper()
    c.etapa = "EM_CONFERENCIA"
    s.add(ColaboradorEtapa(
        colaborador_id=c.id, de=de or None, para="EM_CONFERENCIA",
        motivo="readmissão: conferir o cadastro antes de concluir",
        automatico=True, usuario_id=usuario.id if usuario else None))
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
    # O ACESSO AO ERP desta pessoa (migração 096). `usuario_id` já veio junto
    # com as colunas; o que a tela precisa a mais é o ENDEREÇO com que ela
    # entra e se o acesso está de pé — "tem login" e "o login funciona" são
    # perguntas diferentes, e a segunda é a que importa depois de um
    # desligamento.
    if getattr(c, "usuario_id", None):
        operador = s.get(Usuario, c.usuario_id)
        dados["usuario_email"] = operador.email if operador else ""
        dados["usuario_ativo"] = bool(operador.ativo) if operador else False
    else:
        dados["usuario_email"] = ""
        dados["usuario_ativo"] = False
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


# ---------------------------------------------------------------------------
# A LEITURA PARA A FOLHA — o ERP passa a ser a fonte
#
# Decisão do dono em 05/11/2026: *"o analisesps na parte de Folha de PGT, ele
# fará a leitura das informações e ponto a partir do ERP pra poder montar os
# dados de pgt. A ideia é essa. E em breve vamos migrar todas essas telas de
# pagamento de colaboradores para o ERP tb, pq o analisesps tb será
# desativado. Estamos num processo de migração entre omie, planilha, pipefy,
# analisesps para ERP."*
#
# Ou seja: o ERP **não escreve em planilha nenhuma**. Quem precisa do cadastro
# LÊ daqui. Esta função é esse contrato, e os nomes dos campos são de propósito
# os mesmos da tabela `analisesps.colaborador` — que é a cópia que a folha usa
# hoje, montada da aba "Dados Documentos". Assim, trocar a leitura da planilha
# por esta chamada é trocar a origem, não reescrever a folha.
#
# POR QUE FUNÇÃO E NÃO ROTA HTTP: os dois módulos rodam no MESMO processo e no
# mesmo banco. Uma rota obrigaria a inventar um segredo novo (o `CLAUDE.md`
# proíbe envvar nova para credencial) e a pagar uma volta de rede dentro da
# própria máquina. A rota existe também, mas para a TELA — conferência e
# exportação por quem opera.
#
# AS TRÊS DÚVIDAS DE 05/11/2026, RESPONDIDAS POR ELE NO MESMO DIA:
#
#   · `tipo` (coluna Z) **é o vínculo**: só tem CTPS e Prestador de Serviço, e é
#     o MESMO dado da coluna D ("Tipo de Cadastro"). Quem faz a subvariação é a
#     coluna AY, "Tipo de Contrato". Então `tipo` sai do vínculo, e não há
#     escolha a fazer. `pagamento` (diarista/mensalista) continua indo porque é
#     outra pergunta — e a folha usa as duas;
#   · `paga_por_beevale` **morreu como regra**: *"não é mais regra, visto que em
#     analisesps podemos gerar somapay ou beevale pra tudo."* Não mando;
#   · `fase` **tem significado, e é direcionamento**: ela diz o que falta fazer
#     naquele cadastro. No ERP isso virou a ETAPA do cadastro, e é ela que vai
#     no campo — não mais a situação crua.
# ---------------------------------------------------------------------------
def para_folha(s: Session, *, so_ativos: bool = True,
               obra_id: Optional[int] = None) -> list[dict[str, Any]]:
    """O cadastro de quem recebe, na forma que a folha lê. Só leitura."""
    from app.apps.erp.db.models.cadastros import Funcao, Obra

    def dinheiro(v: Any) -> Optional[float]:
        return float(v) if v is not None else None

    def dia(v: Any) -> Optional[str]:
        return v.isoformat() if v else None

    cargos = {c.id: c.nome for c in s.scalars(select(Cargo)).all()}
    funcoes = {f.id: f.nome for f in s.scalars(select(Funcao)).all()}
    convencoes = {c.id: c.nome for c in s.scalars(select(Convencao)).all()}
    obras = {o.id: (o.codigo, o.nome) for o in s.scalars(select(Obra)).all()}
    dependentes: dict[int, int] = {}
    for d in s.scalars(select(ColaboradorDependente).where(
            ColaboradorDependente.salario_familia.is_(True))).all():
        dependentes[d.colaborador_id] = dependentes.get(d.colaborador_id, 0) + 1

    saida = []
    for c in s.scalars(select(Colaborador)).all():
        ativo = (c.situacao or "ATIVO").upper() == "ATIVO" and c.demissao is None
        if so_ativos and not ativo:
            continue
        if obra_id and c.obra_id != obra_id:
            continue
        codigo_obra, nome_obra = obras.get(c.obra_id or 0, ("", ""))
        saida.append({
            "cpf": c.cpf, "nome": c.nome,
            "matricula": c.matricula or "", "celular": c.telefone or "",
            # O número do card do Pipefy, enquanto ele existir: é o que liga
            # esta pessoa ao histórico que ficou lá.
            "card_pipefy": c.ref_pipefy or "",
            "id_fortes": c.id_fortes or "",
            "cargo": cargos.get(c.cargo_id or 0) or funcoes.get(c.funcao_id or 0, ""),
            "convencao": convencoes.get(c.convencao_id or 0, ""),
            "convencao_categoria": c.convencao_categoria or "",
            "obra_codigo": codigo_obra, "obra_cadastro": codigo_obra,
            "obra_nome": nome_obra,
            # "Tipo" é o vínculo, e só tem dois valores. A subvariação é o
            # tipo de contrato, logo abaixo.
            "tipo": VINCULO_PLANILHA.get(c.vinculo or "CTPS", "CTPS"),
            "vinculo": c.vinculo or "CTPS",
            "pagamento": "MENSALISTA" if c.mensalista else "DIARISTA",
            "tipo_contrato": TIPO_CONTRATO_ROTULOS.get(c.tipo_contrato or "",
                                                       c.tipo_contrato or ""),
            "fase": (c.situacao or "ATIVO").upper(),
            "salario": dinheiro(c.salario),
            "valor_diaria": dinheiro(c.valor_diaria),
            "valor_mes": dinheiro(c.valor_mes),
            "valor_alimentacao": dinheiro(c.aux_alimentacao),
            "modo_alimentacao": CATEGORIA_ALIMENTACAO_ROTULOS.get(
                c.categoria_alimentacao or "", c.categoria_alimentacao or ""),
            "valor_transporte": dinheiro(c.aux_transporte),
            # "Cartão" quer dizer que NÃO se paga em dinheiro — a folha precisa
            # desta palavra inteira, e foi ela a origem de pagamento em dobro.
            "modo_transporte": CATEGORIA_TRANSPORTE_ROTULOS.get(
                c.categoria_transporte or "", c.categoria_transporte or ""),
            "valor_gratificacao": dinheiro(c.valor_gratificacao),
            "parcela_unica": "Sim" if c.parcela_unica else "",
            "recebe_via_sp": "Sim" if c.recebe_via_sp else "",
            "cesta_basica_extra": "Sim" if c.cesta_basica_extra else "",
            "vale_refeicao": "Sim" if c.vale_refeicao else "",
            "adicional_transferencia": "Sim" if c.adicional_transferencia else "",
            "observacao_auxilio": c.observacao_auxilio or "",
            "salario_familia": dependentes.get(c.id, 0),
            "pix_tipo": c.pix_tipo or "", "pix_chave": c.pix_chave or "",
            "pis": c.pis or "",
            "data_nascimento": dia(c.data_nascimento),
            "data_inicio": dia(c.data_inicio),
            "data_admissao": dia(c.admissao),
            "aviso_previo": dia(c.aviso_previo),
            "ultimo_dia": dia(c.ultimo_dia),
            "data_saida": dia(c.demissao),
            "ativo": ativo,
        })
    saida.sort(key=lambda x: _sem_acento(x["nome"]))
    return saida


# ===========================================================================
# AS ETAPAS DO CADASTRO — "o que falta fazer nesta pessoa"
#
# O dono, em 05/11/2026, explicando para que serviam as fases do card:
#
# > *"a parte de fase do card ela serve de direcionamento pra entender o que
# > precisa ser feito em relação àquele cadastro. (…) Nós temos o formulário
# > inicial, aí tudo vai para a fase inicial (…) ali o DP analisa as
# > informações, verifica a coerência, e termina de preencher alguns dados que
# > estão dentro dos documentos (…) Agora, isso a gente quer que seja feito via
# > IA, através da leitura dos documentos. Ou seja, a gente vai colocar o mínimo
# > possível de campos. Só os campos que não vão ter dentro de documentos — por
# > exemplo, qual vai ser o cargo, qual é a obra, qual é a empresa."*
#
# E a bifurcação, com as palavras dele:
#
# > *"dependendo se é prestador de serviço, ou se é CTPS, a gente dá um
# > determinado procedimento. Se o cara é prestador de serviço, aquele vínculo
# > de cadastro, ele se resolve apenas internamente no DP da empresa. Se for um
# > PJ (…) tem que fazer um contrato para assinar; se for um diarista, morre por
# > aí; se for um autônomo mensalista, também morre por ali (…) esses cadastros
# > finalizados, a gente já joga eles para cadastro concluído. Aí, quem é
# > carteira assinada, CTPS, a gente precisa fazer contato com a contabilidade
# > para ela fazer o registro formal no eSocial."*
#
# ETAPA ≠ SITUAÇÃO, e confundir as duas foi o defeito do Pipefy: `situacao`
# responde "esta pessoa está na empresa?" e manda na folha; `etapa` responde "o
# que falta fazer neste cadastro?" e manda na fila de trabalho do DP.
# ===========================================================================
ETAPAS = ("CADASTRO_INICIAL", "EM_CONFERENCIA", "CONTRATO_A_ASSINAR",
          "NA_CONTABILIDADE", "CONCLUIDO", "AFASTADO", "DESLIGADO", "CANCELADO")
ETAPA_ROTULOS = {
    "CADASTRO_INICIAL": "Cadastro inicial",
    "EM_CONFERENCIA": "Em conferência do DP",
    "CONTRATO_A_ASSINAR": "Contrato a assinar",
    "NA_CONTABILIDADE": "Na contabilidade",
    "CONCLUIDO": "Cadastro concluído",
    "AFASTADO": "Afastado",
    "DESLIGADO": "Desligado",
    "CANCELADO": "Cadastro cancelado",
}
# O DIRECIONAMENTO: é para isto que a etapa existe. A tela mostra esta frase,
# não o código da etapa — quem abre a fila precisa saber o que FAZER.
ETAPA_O_QUE_FAZER = {
    "CADASTRO_INICIAL": "O DP ainda não olhou. Conferir a coerência e jogar os "
                        "documentos para a leitura preencher o resto.",
    "EM_CONFERENCIA": "Conferir o que a leitura preencheu e completar o que "
                      "falta — depois disto o caminho se separa por vínculo.",
    "CONTRATO_A_ASSINAR": "Gerar o contrato do PJ, colher a assinatura e "
                          "arquivar.",
    "NA_CONTABILIDADE": "A contabilidade está fazendo o registro no eSocial. "
                        "O ERP cobra o retorno.",
    "CONCLUIDO": "Nada pendente.",
    "AFASTADO": "Acompanhar o afastamento; a pessoa continua cadastrada.",
    "DESLIGADO": "Pessoa fora da folha.",
    "CANCELADO": "Cadastro abandonado antes de virar gente na empresa.",
}
# PARA ONDE DÁ PARA IR DE CADA LUGAR. Existe para impedir o salto que o quadro
# do Pipefy permitia arrastando o card: cadastro saindo de "inicial" direto
# para "concluído" sem ninguém conferir documento nenhum.
ETAPAS_SEGUINTES = {
    "CADASTRO_INICIAL": ("EM_CONFERENCIA", "CANCELADO"),
    "EM_CONFERENCIA": ("CONTRATO_A_ASSINAR", "NA_CONTABILIDADE", "CONCLUIDO",
                       "CANCELADO"),
    "CONTRATO_A_ASSINAR": ("CONCLUIDO", "CANCELADO"),
    "NA_CONTABILIDADE": ("CONCLUIDO", "CANCELADO"),
    # De concluído dá para ir para afastamento e desligamento — e VOLTAR de
    # afastado, que é o que o pipe não deixava e obrigava a criar card novo.
    "CONCLUIDO": ("AFASTADO", "DESLIGADO", "EM_CONFERENCIA"),
    "AFASTADO": ("CONCLUIDO", "DESLIGADO"),
    # Readmissão não é mudança de etapa: é vínculo novo (ver `readmitir`), e ela
    # recoloca a pessoa em conferência.
    "DESLIGADO": ("EM_CONFERENCIA",),
    "CANCELADO": ("EM_CONFERENCIA",),
}
# As etapas em que o cadastro está PARADO esperando alguém — a fila do DP.
ETAPAS_ABERTAS = ("CADASTRO_INICIAL", "EM_CONFERENCIA", "CONTRATO_A_ASSINAR",
                  "NA_CONTABILIDADE")
# Os tipos de contrato de prestador que "morrem no DP", com as palavras dele.
CONTRATOS_QUE_EXIGEM_ASSINATURA = ("PJ",)


def caminho_depois_da_conferencia(c: Colaborador) -> tuple[str, str]:
    """Para onde este cadastro vai quando o DP termina a conferência.

    Devolve (etapa, por quê) — o "por quê" vai para a trilha e para a tela,
    porque a pessoa que opera precisa entender a bifurcação sem decorá-la.
    """
    vinculo = (c.vinculo or "CTPS").upper()
    if vinculo == "PRESTADOR":
        if (c.tipo_contrato or "") in CONTRATOS_QUE_EXIGEM_ASSINATURA:
            return ("CONTRATO_A_ASSINAR",
                    "prestador PJ: precisa de contrato assinado antes de "
                    "concluir")
        return ("CONCLUIDO",
                "prestador que não é PJ: o vínculo se resolve dentro do DP, "
                "sem contabilidade")
    return ("NA_CONTABILIDADE",
            "carteira assinada: a contabilidade faz o registro formal no "
            "eSocial")


def mover_etapa(s: Session, colaborador_id: int, para: str,
                usuario: Optional[Usuario], *, motivo: str = "",
                automatico: bool = False) -> Colaborador:
    """Move o cadastro de etapa, guardando de onde veio.

    Recusa o salto que a lista de etapas seguintes não prevê — é o que impede
    cadastro pular a conferência dos documentos.
    """
    c = s.get(Colaborador, colaborador_id)
    if c is None:
        raise ErroValidacao("Colaborador não encontrado.")
    para = (para or "").strip().upper()
    if para not in ETAPAS:
        raise ErroValidacao("Etapa desconhecida.")
    de = (c.etapa or "CONCLUIDO").upper()
    if para == de:
        return c
    if para not in ETAPAS_SEGUINTES.get(de, ()):
        raise ErroValidacao(
            f"De “{ETAPA_ROTULOS.get(de, de)}” não dá para ir direto para "
            f"“{ETAPA_ROTULOS.get(para, para)}”. "
            f"Os caminhos daqui são: "
            + ", ".join(ETAPA_ROTULOS.get(x, x)
                        for x in ETAPAS_SEGUINTES.get(de, ())) + ".")
    c.etapa = para
    s.add(ColaboradorEtapa(colaborador_id=c.id, de=de, para=para,
                           motivo=motivo or None,
                           usuario_id=usuario.id if usuario else None,
                           automatico=automatico))
    registrar_evento(s, "colaborador", c.id, "ETAPA_MUDOU",
                     {"de": de, "para": para, "motivo": motivo,
                      "automatico": automatico},
                     usuario.id if usuario else None)
    s.flush()
    return c


def concluir_conferencia(s: Session, colaborador_id: int,
                         usuario: Optional[Usuario]) -> dict[str, Any]:
    """O DP terminou de conferir: o sistema decide para onde vai, pelo vínculo.

    A decisão é AUTOMÁTICA de propósito — ela é regra, não escolha, e deixar
    alguém escolher à mão foi o que no Pipefy fez prestador cair na fila da
    contabilidade e ficar lá esperando um e-mail que nunca ia sair.

    O que falta para admitir NÃO impede a conferência: o DP termina de olhar e o
    cadastro segue, com a pendência escrita. Travar aqui faria o cadastro ficar
    parado esperando um papel que a própria contabilidade vai pedir.
    """
    c = s.get(Colaborador, colaborador_id)
    if c is None:
        raise ErroValidacao("Colaborador não encontrado.")
    if (c.etapa or "").upper() not in ("CADASTRO_INICIAL", "EM_CONFERENCIA"):
        raise ErroValidacao(
            "Este cadastro não está em conferência — está em "
            f"“{ETAPA_ROTULOS.get(c.etapa or '', c.etapa or '')}”.")
    if (c.etapa or "").upper() == "CADASTRO_INICIAL":
        mover_etapa(s, c.id, "EM_CONFERENCIA", usuario,
                    motivo="o DP começou a conferir", automatico=True)
    para, porque = caminho_depois_da_conferencia(c)
    mover_etapa(s, c.id, para, usuario, motivo=porque, automatico=True)
    return {"etapa": para, "rotulo": ETAPA_ROTULOS[para], "porque": porque,
            "falta": o_que_falta(c),
            "falta_documento": documentos_que_faltam(s, c.id, c.vinculo)}


def fila_do_cadastro(s: Session, *, etapa: str = "",
                     obra_ids: Optional[list[int]] = None) -> list[dict[str, Any]]:
    """Quem está parado, em que etapa e há quantos dias.

    É o quadro do Pipefy virado lista. O "há quantos dias" vem da trilha, e é a
    pergunta que o quadro respondia de graça só por ser um quadro.
    """
    from app.apps.erp.db.models.cadastros import Obra

    alvo = (etapa or "").strip().upper()
    obras = {o.id: o.codigo for o in s.scalars(select(Obra)).all()}
    desde: dict[int, Any] = {}
    for m in s.scalars(select(ColaboradorEtapa).order_by(
            ColaboradorEtapa.criado_em, ColaboradorEtapa.id)).all():
        desde[m.colaborador_id] = m.criado_em
    hoje = datetime.now(timezone.utc)
    saida = []
    for c in s.scalars(select(Colaborador)).all():
        etapa_dela = (c.etapa or "CONCLUIDO").upper()
        if alvo:
            if etapa_dela != alvo:
                continue
        elif etapa_dela not in ETAPAS_ABERTAS:
            continue
        if obra_ids is not None and c.obra_id not in obra_ids:
            continue
        entrou = desde.get(c.id)
        saida.append({
            "id": c.id, "nome": c.nome, "cpf": c.cpf,
            "obra": obras.get(c.obra_id or 0, ""),
            "vinculo": c.vinculo or "CTPS",
            "etapa": etapa_dela, "etapa_rotulo": ETAPA_ROTULOS.get(etapa_dela, etapa_dela),
            "o_que_fazer": ETAPA_O_QUE_FAZER.get(etapa_dela, ""),
            "desde": entrou.isoformat() if entrou else None,
            "dias_parado": (hoje - entrou).days if entrou else None,
            "falta": o_que_falta(c),
        })
    saida.sort(key=lambda x: (-(x["dias_parado"] or 0), _sem_acento(x["nome"])))
    return saida
