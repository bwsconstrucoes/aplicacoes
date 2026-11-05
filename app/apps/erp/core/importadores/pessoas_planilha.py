# -*- coding: utf-8 -*-
"""
IMPORTAR AS PESSOAS QUE ESTÃO NO PIPEFY / NA PLANILHA DO DP.

Decisão do dono em 04/11/2026: *"eu acho que a gente deve colocar só os que
estão ativos"*. São ~575 pessoas (462 com cadastro concluído + 100 em cadastros
adicionais + 12 afastados), não as 3.664 do pipe — os ~2.960 desligados **ficam
na planilha como base morta**.

O DE-PARA É POR NOME DE COLUNA, e aceita os três jeitos que esse dado aparece:

  · os rótulos do pipe do Pipefy  ("Nome Completo", "Nº Identidade", "Nº PIS");
  · os cabeçalhos da aba "Dados Documentos" (o mapa das 74 colunas que o
    cenário do Make escreve);
  · os nomes do próprio ERP ("nome", "cpf", "data_nascimento").

Assim a importação funciona tanto com o relatório exportado do Pipefy quanto
com a planilha do DP, sem ninguém ter de renomear coluna nenhuma. A comparação
ignora acento, caixa e espaço sobrando.

**O CPF É A IDENTIDADE.** Linha sem CPF, ou com CPF que não fecha os dígitos,
é recusada e aparece no relatório com o número da linha — nunca entra "mais ou
menos".

**PESSOA QUE JÁ ESTÁ NO ERP NÃO É SOBRESCRITA.** A importação lista o que
divergiu (nome, cargo, obra, salário) e deixa a decisão para quem olha. O
contrário — a planilha mandando por cima do que o DP já corrigiu na tela —
seria perder trabalho feito sem ninguém ver.

**SIMULA ANTES DE GRAVAR.** `gravar=False` devolve o relatório inteiro sem
tocar no banco: quantas entram, quantas já existem, o que vai ser criado de
cargo e de obra, e cada aviso com o número da linha. Importação de cadastro
roda uma vez e é vista por todo mundo depois.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.cadastros import cargos as svc_cargos
from app.apps.erp.core.cadastros import pessoas as svc
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.importadores.planilhas import _simples, ler_tabela
from app.apps.erp.db.models.cadastros import (
    Cargo, Colaborador, Convencao, Empresa, Obra, Usuario,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# O de-para: campo do ERP → como a coluna pode se chamar
# ---------------------------------------------------------------------------
COLUNAS: dict[str, tuple[str, ...]] = {
    # identidade
    "cpf": ("cpf", "cpf (cadastro de pessoa fisica)", "c.p.f."),
    "nome": ("nome completo", "nome", "nome do colaborador",
             "nome completo (corrigido)", "colaborador"),
    "data_nascimento": ("data de nascimento", "nascimento"),
    "sexo": ("sexo",),
    "nome_mae": ("nome da mae", "mae"),
    "nome_pai": ("nome do pai", "pai"),
    "naturalidade": ("naturalidade",),
    "naturalidade_uf": ("uf (naturalidade)", "uf naturalidade"),
    "escolaridade": ("escolaridade",),
    "estado_civil": ("estado civil",),
    # documentos
    "rg": ("no identidade", "n identidade", "identidade", "rg"),
    "rg_orgao": ("orgao emissor (documento de identificacao)", "orgao emissor"),
    "rg_emissao": ("data de emissao (documento de identificacao)",),
    "rg_uf": ("uf (documento de identificacao)",),
    "pis": ("no pis", "n pis", "pis", "pis/nis", "nit"),
    "ctps_numero": ("no ctps", "n ctps", "ctps"),
    "ctps_serie": ("serie (ctps)", "serie ctps", "serie"),
    "ctps_uf": ("uf (ctps)",),
    "ctps_emissao": ("data de emissao (ctps)",),
    "ctps_digital": ("tipo de ctps",),
    "primeiro_emprego": ("este e o primeiro emprego do colaborador?",
                         "primeiro emprego"),
    # endereço e contato
    "cep": ("cep",),
    "logradouro": ("endereco (logradouro)", "logradouro", "endereco"),
    "numero_endereco": ("numero", "numero (endereco)", "no"),
    "complemento": ("complemento",),
    "bairro": ("bairro",),
    "municipio": ("municipio", "cidade"),
    "uf": ("uf (endereco)", "uf"),
    "telefone": ("telefone", "celular"),
    "email": ("email", "e-mail"),
    # pagamento
    "pix_tipo": ("chave pix", "tipo de chave pix"),
    "pix_chave": ("valor da chave", "valor da chave pix", "chave"),
    "salario": ("salario",),
    "valor_diaria": ("valor da diaria", "diaria"),
    "valor_mes": ("valor do mes",),
    "mensalista": ("pagamento",),
    # vínculo
    "vinculo": ("tipo de documentacao para cadastro", "tipo de documentacao",
                "vinculo"),
    "tipo_contrato": ("tipo de contrato",),
    "empresa": ("empresa para registro", "empresa"),
    "cnpj_admissao": ("cnpj para admissao",),
    "razao_social_pj": ("razao social",),
    "cnpj_pj": ("cnpj",),
    "matricula": ("matricula",),
    "cargo": ("cargo", "cargo [ ]", "funcao"),
    "cbo": ("codigo cbo", "cbo"),
    "convencao": ("convencao", "convencao coletiva"),
    "contrato_experiencia": ("contrato de experiencia",),
    "data_inicio": ("data de inicio",),
    "admissao": ("data de admissao", "admissao"),
    "obra": ("codigo da obra", "obra", "obra (c. diarios)"),
    "id_fortes": ("id fortes", "codigo fortes"),
    # benefícios
    "solicitar_salario_familia": ("solicitar salario familia?", "salario familia"),
    "meio_transporte": ("meio de transporte para chegar a obra?", "meio de transporte"),
    "categoria_alimentacao": ("categoria auxilio alimentacao",),
    "aux_alimentacao": ("valor (auxilio alimentacao)", "auxilio alimentacao"),
    "categoria_transporte": ("categoria auxilio transporte",),
    "aux_transporte": ("valor (auxilio transporte)", "auxilio transporte"),
    "observacao_auxilio": ("observacao (ajuda de custo)", "observacao ajuda de custo"),
    "recebe_gratificacao": ("recebe gratificacao mensal",),
    "valor_gratificacao": ("valor da gratificacao",),
    "parcela_unica": ("recebe parcela unica",),
    "recebe_via_sp": ("recebe via sp",),
    "cesta_basica_extra": ("cesta basica extra",),
    "diversos": ("diversos",),
    # requisição
    "requerente": ("requerente",),
    "responsavel": ("responsavel pela solicitacao", "responsavel"),
    # situação / desligamento
    "fase": ("fase atual", "fase", "current phase", "situacao", "etiqueta"),
    "demissao": ("data de saida", "demissao", "data de demissao"),
    "aviso_previo": ("data do aviso previo",),
    "ultimo_dia": ("ultimo dia trabalhado",),
    # família
    "conjuge_nome": ("nome do conjuge",),
    "conjuge_nascimento": ("data de nascimento do conjuge",),
    "conjuge_cpf": ("cpf do conjuge",),
}
for _i in range(1, 6):
    COLUNAS[f"filho{_i}_nome"] = (f"nome filho {_i}",)
    COLUNAS[f"filho{_i}_nascimento"] = (f"data de nascimento filho {_i}",)

# As colunas que existem no pipe e o ERP NÃO guarda — ficam registradas aqui
# para o relatório poder dizer "esta coluna foi ignorada DE PROPÓSITO", que é
# diferente de "não reconheci esta coluna".
IGNORADAS_DE_PROPOSITO = {
    "jornada de trabalho": "a escala de trabalho é do ponto, não do cadastro",
    "objeto obra [ ]": "o objeto é da obra, e a obra já tem o campo",
    "objeto obra": "o objeto é da obra, e a obra já tem o campo",
    "salario extenso": "o ERP escreve o extenso quando precisa (num2words)",
    "alguma observacao em relacao a funcao?": "virou observação do cadastro",
    "requisicao solicitada por um terceiro?": "o requerente já diz isso",
    "quantidade de dependentes": "os dependentes vêm pelos nomes, sem teto",
    "id do card": "número do Pipefy, que morre com ele",
    "table record id": "número do Pipefy, que morre com ele",
    "table record id desligado": "número do Pipefy, que morre com ele",
    "quantidade de faltas a descontar": "falta é do ponto",
    "tipo de solicitacao": "a fila de solicitações do DP tem o pedido dela",
}

# Os rótulos do pipe traduzidos para o valor que o ERP guarda.
DE_PARA_VALOR: dict[str, dict[str, str]] = {
    "sexo": {"masculino": "M", "feminino": "F", "m": "M", "f": "F"},
    "vinculo": {"ctps": "CTPS", "prestador de servico": "PRESTADOR",
                "prestador": "PRESTADOR"},
    "estado_civil": {
        "solteiro (a)": "SOLTEIRO", "solteiro(a)": "SOLTEIRO", "solteiro": "SOLTEIRO",
        "casado (a)": "CASADO", "casado(a)": "CASADO", "casado": "CASADO",
        "divorciado (a)": "DIVORCIADO", "divorciado(a)": "DIVORCIADO",
        "divorciado": "DIVORCIADO", "viuvo (a)": "VIUVO", "viuvo(a)": "VIUVO",
        "viuvo": "VIUVO", "uniao estavel": "UNIAO_ESTAVEL",
    },
    "escolaridade": {
        "nao alfabetizado": "NAO_ALFABETIZADO", "educacao infantil": "INFANTIL",
        "ensino fundamental": "FUNDAMENTAL", "ensino medio": "MEDIO",
        "ensino superior": "SUPERIOR", "pos-graduacao": "POS",
        "pos graduacao": "POS", "mestrado": "MESTRADO", "doutorado": "DOUTORADO",
    },
    "tipo_contrato": {
        "clt (tempo determinado)": "CLT_DETERMINADO",
        "clt (tempo indeterminado)": "CLT_INDETERMINADO",
        "pj": "PJ", "autonomo (rpa)": "AUTONOMO_RPA",
        "autonomo mensalista": "AUTONOMO_MENSALISTA",
        "jovem aprendiz": "JOVEM_APRENDIZ", "estagio": "ESTAGIO",
        "temporario": "TEMPORARIO", "pro-labore": "PRO_LABORE",
        "pro labore": "PRO_LABORE",
    },
    "categoria_alimentacao": {
        "segunda a quinta": "SEG_QUI", "segunda a sexta": "SEG_SEX",
        "mensal": "MENSAL", "mes": "MES",
    },
    "categoria_transporte": {
        "diario": "DIARIO", "mensal": "MENSAL", "segunda e sexta": "SEG_E_SEX",
        "vale transporte": "VALE_TRANSPORTE", "vale-transporte": "VALE_TRANSPORTE",
        "diario e vale transporte": "DIARIO_E_VT", "veiculo": "VEICULO",
        "motocicleta": "MOTOCICLETA", "bicicleta": "BICICLETA", "cartao": "CARTAO",
    },
    "meio_transporte": {
        "nenhum": "NENHUM", "transporte coletivo": "COLETIVO", "moto": "MOTO",
        "bicicleta": "BICICLETA",
    },
    "contrato_experiencia": {
        "45 dias, prorrogavel por mais 45 dias": "45+45 dias",
        "30 dias, prorrogavel por mais 60 dias": "30+60 dias",
        "60 dias, prorrogavel por mais 30 dias": "60+30 dias",
        "30 dias, prorrogavel por mais 30 dias": "30+30 dias",
        "sem contrato de experiencia": "Sem contrato de experiência",
    },
    "pix_tipo": {"e-mail": "EMAIL", "email": "EMAIL", "telefone": "TELEFONE",
                 "cpf": "CPF", "cnpj": "CNPJ", "aleatoria": "ALEATORIA"},
}

# A fase do card que diz que a pessoa NÃO entra. O resto entra.
FASES_FORA = ("colaboradores desligados", "cadastro cancelado", "desligado",
              "cancelado", "demitido")
SIM = ("sim", "s", "1", "true", "verdadeiro", "x", "digital", "mensalista")


def _achar_colunas(linha: dict[str, str]) -> tuple[dict[str, str], list[str]]:
    """Qual coluna da planilha serve para qual campo, e o que sobrou."""
    por_simples = {_simples(k): k for k in linha}
    achadas: dict[str, str] = {}
    usadas: set[str] = set()
    for campo, nomes in COLUNAS.items():
        for nome in nomes:
            col = por_simples.get(_simples(nome))
            if col and col not in usadas:
                achadas[campo] = col
                usadas.add(col)
                break
    sobraram = [k for k in linha if k and k not in usadas]
    return achadas, sobraram


def _v(linha: dict[str, str], achadas: dict[str, str], campo: str) -> str:
    col = achadas.get(campo)
    return (linha.get(col) or "").strip() if col else ""


def _traduzir(campo: str, bruto: str) -> Optional[str]:
    """Rótulo do pipe → valor do ERP. Devolve None quando não reconhece, para
    o chamador poder avisar em vez de gravar errado."""
    if not bruto:
        return ""
    tabela = DE_PARA_VALOR.get(campo)
    if not tabela:
        return bruto
    return tabela.get(_simples(bruto))


def _cnpj(bruto: str) -> str:
    """"00.079.526/0001-09 - BWS" → "00079526000109"."""
    d = re.sub(r"\D", "", bruto or "")
    return d if len(d) == 14 else ""


def importar(s: Session, conteudo: bytes, *, gravar: bool,
             so_ativos: bool = True,
             usuario: Optional[Usuario] = None) -> dict[str, Any]:
    # DUAS TENTATIVAS, de propósito. Na primeira, o cabeçalho é a linha que
    # TRAZ a coluna do CPF — planilha de gente tem título, logotipo e filtro em
    # cima da tabela, e a aba certa não é sempre a primeira. Se nenhuma linha
    # tiver "CPF" escrito assim, cai no jeito antigo: primeira aba, primeira
    # linha. Sem a segunda tentativa, um cabeçalho escrito "C.P.F." devolveria
    # "a planilha veio vazia", que é a pior mensagem possível — não diz nada.
    linhas = ler_tabela(conteudo, colunas_esperadas=("CPF",))
    if not linhas or "cpf" not in _achar_colunas(linhas[0])[0]:
        linhas = ler_tabela(conteudo) or linhas
    if not linhas:
        raise ErroValidacao("A planilha veio vazia.")
    achadas, sobraram = _achar_colunas(linhas[0])
    if "cpf" not in achadas:
        raise ErroValidacao(
            "Não achei a coluna do CPF. Sem ela não dá para saber quem é quem — "
            "as colunas lidas foram: " + ", ".join(list(linhas[0])[:15]))

    de_proposito, nao_reconhecidas = [], []
    for col in sobraram:
        motivo = IGNORADAS_DE_PROPOSITO.get(_simples(col))
        (de_proposito if motivo else nao_reconhecidas).append(
            f"{col} — {motivo}" if motivo else col)

    rel: dict[str, Any] = {
        "gravou": gravar, "linhas": len(linhas),
        "colunas_lidas": {c: achadas[c] for c in sorted(achadas)},
        "colunas_ignoradas": de_proposito,
        "colunas_nao_reconhecidas": nao_reconhecidas,
        "criadas": 0, "ja_existiam": 0, "fora": 0, "dependentes": 0,
        "divergencias": [], "rejeitadas": [], "avisos": [],
        "cargos_novos": [], "obras_nao_achadas": [], "so_no_erp": [],
    }
    vistos: set[str] = set()
    vistos_no_erp: set[int] = set()
    cargos_novos: set[str] = set()
    obras_faltando: set[str] = set()
    # O PIS É ÚNICO NO BANCO (índice da migração 086), e a planilha do Pipefy
    # tem PIS repetido — foi digitado à mão por anos. Carregar os que já
    # existem permite RECUSAR a linha com o motivo escrito, em vez de deixar o
    # banco estourar no meio da importação.
    pis_de: dict[str, str] = {
        re.sub(r"\D", "", c.pis or ""): c.nome
        for c in s.scalars(select(Colaborador).where(Colaborador.pis.isnot(None))).all()
        if re.sub(r"\D", "", c.pis or "")}

    for n, linha in enumerate(linhas, start=2):
        try:
            # UMA LINHA RUIM NÃO DERRUBA AS OUTRAS 574: cada pessoa entra no seu
            # próprio ponto de retorno. Sem isto, um erro de banco em qualquer
            # linha aborta a transação e todas as seguintes falham em cascata,
            # por um motivo que não é o delas.
            with s.begin_nested():
                _uma(s, linha, n, achadas, rel, vistos, cargos_novos,
                     obras_faltando, pis_de, vistos_no_erp,
                     gravar=gravar, so_ativos=so_ativos, usuario=usuario)
        except ErroValidacao as e:
            rel["rejeitadas"].append({
                "linha": n, "cpf": _v(linha, achadas, "cpf"),
                "nome": _v(linha, achadas, "nome"), "motivo": str(e)})
        except Exception as e:                      # noqa: BLE001
            logger.exception("ERP: importação de pessoas, linha %d", n)
            rel["rejeitadas"].append({
                "linha": n, "cpf": _v(linha, achadas, "cpf"),
                "nome": _v(linha, achadas, "nome"),
                "motivo": f"erro inesperado: {e}"})

    # O OUTRO LADO DA CONFERÊNCIA, e ele é o que o período em paralelo exige:
    # quem está ATIVO no ERP e NÃO apareceu na planilha. Pode ser gente admitida
    # no ERP depois da exportação (certo), ou gente que saiu na planilha e
    # continua ativa aqui (errado, e a folha pagaria). Só a lista deixa ver a
    # diferença — e ela é dita, não resolvida: apagar por ausência numa planilha
    # seria desligar gente por um arquivo velho.
    rel["so_no_erp"] = [
        {"id": c.id, "cpf": c.cpf, "nome": c.nome, "matricula": c.matricula or "",
         "admissao": c.admissao.isoformat() if c.admissao else None}
        for c in s.scalars(select(Colaborador)).all()
        if c.id not in vistos_no_erp and c.demissao is None
        and (c.situacao or "ATIVO").upper() == "ATIVO"][:200]

    rel["cargos_novos"] = sorted(cargos_novos)
    rel["obras_nao_achadas"] = sorted(obras_faltando)
    rel["avisos"] = rel["avisos"][:200]
    rel["divergencias"] = rel["divergencias"][:200]
    logger.info("ERP: pessoas da planilha — %d linhas, %d criadas, %d já existiam, "
                "%d fora, %d recusadas (%s)", rel["linhas"], rel["criadas"],
                rel["ja_existiam"], rel["fora"], len(rel["rejeitadas"]),
                "gravado" if gravar else "simulação")
    return rel


# ---------------------------------------------------------------------------
# A CONFERÊNCIA LADO A LADO — é ela que sustenta o período em paralelo
#
# Decisão do dono em 05/11/2026: *"precisamos antes de desativar essa leitura
# da planilha deixar a parte cadastral de colaboradores do ERP toda funcionando
# e testada (…) ficaremos com os dados em paralelo por alguns dias."*
#
# Nesses dias, rodar a importação em modo CONFERIR (sem gravar) com a planilha
# do dia vira o relatório de diferenças: o que mudou de um lado e não do outro.
# Por isso a comparação não pode ser só de nome e salário — tem de cobrir
# exatamente o que a FOLHA lê, que é onde a diferença custa dinheiro.
# ---------------------------------------------------------------------------
def _so_digitos_dinheiro(bruto: Any) -> str:
    """"2.100,00", "2100", "R$ 2.100,00" e Decimal("2100.00") comparáveis."""
    d = re.sub(r"[^\d]", "", str(bruto or ""))
    return d.lstrip("0") or ""


def _divergencias(s: Session, ja: Colaborador, linha: dict[str, str],
                  achadas: dict[str, str], nome: str) -> list[str]:
    """O que está diferente entre a planilha e o cadastro do ERP. NÃO MUDA NADA.

    Campo vazio na planilha não é divergência — é ausência. Dizer "no ERP tem
    cargo e na planilha não" para toda coluna que o arquivo não traz faria o
    relatório ter 500 linhas e nenhuma informação.
    """
    def val(campo: str) -> str:
        return _v(linha, achadas, campo)

    div: list[str] = []

    def comparar(rotulo: str, da_planilha: str, do_erp: Any) -> None:
        if not da_planilha:
            return
        aqui = "" if do_erp in (None, "") else str(do_erp)
        if _simples(da_planilha) != _simples(aqui):
            div.append(f"{rotulo}: no ERP “{aqui or '—'}”, na planilha “{da_planilha}”")

    def comparar_dinheiro(rotulo: str, da_planilha: str, do_erp: Any) -> None:
        if not da_planilha:
            return
        if _so_digitos_dinheiro(da_planilha) != _so_digitos_dinheiro(
                f"{do_erp:.2f}" if do_erp is not None else ""):
            div.append(f"{rotulo}: no ERP {do_erp if do_erp is not None else '—'}, "
                       f"na planilha {da_planilha}")

    if nome and svc._sem_acento(nome) != svc._sem_acento(ja.nome):
        div.append(f"nome: no ERP “{ja.nome}”, na planilha “{nome}”")
    comparar("matrícula", val("matricula"), ja.matricula)
    comparar("telefone", re.sub(r"\D", "", val("telefone")),
             re.sub(r"\D", "", ja.telefone or ""))

    # O dinheiro, que é onde a diferença dói.
    comparar_dinheiro("salário", val("salario"), ja.salario)
    comparar_dinheiro("valor da diária", val("valor_diaria"), ja.valor_diaria)
    comparar_dinheiro("valor do mês", val("valor_mes"), ja.valor_mes)
    comparar_dinheiro("auxílio-alimentação", val("aux_alimentacao"), ja.aux_alimentacao)
    comparar_dinheiro("auxílio-transporte", val("aux_transporte"), ja.aux_transporte)
    comparar_dinheiro("gratificação", val("valor_gratificacao"), ja.valor_gratificacao)

    # As listas: comparadas pelo VALOR traduzido, senão "CLT (tempo
    # Indeterminado)" contra "CLT_INDETERMINADO" acusaria divergência em todo
    # mundo.
    for campo, rotulo in (("vinculo", "vínculo"), ("tipo_contrato", "tipo de contrato"),
                          ("categoria_alimentacao", "categoria da alimentação"),
                          ("categoria_transporte", "categoria do transporte")):
        bruto = val(campo)
        if bruto:
            comparar(rotulo, _traduzir(campo, bruto) or bruto, getattr(ja, campo, None))

    # Cargo e obra pelo nome, porque é assim que a planilha os escreve.
    if val("cargo"):
        cargo = s.get(Cargo, ja.cargo_id) if ja.cargo_id else None
        de_la, daqui = (svc_cargos.normalizar_nome(val("cargo")),
                        svc_cargos.normalizar_nome(cargo.nome) if cargo else "")
        if de_la != daqui:
            div.append(f"cargo: no ERP “{cargo.nome if cargo else '—'}”, "
                       f"na planilha “{val('cargo')}”")
    if val("obra"):
        obra = s.get(Obra, ja.obra_id) if ja.obra_id else None
        alvo = _simples(val("obra"))
        if not obra or alvo not in (_simples(obra.codigo), _simples(obra.nome)):
            div.append(f"obra: no ERP “{obra.codigo if obra else '—'}”, "
                       f"na planilha “{val('obra')}”")

    # As datas, comparadas como data e não como texto — "01/03/2026" e
    # "2026-03-01" são a mesma coisa.
    for campo, rotulo in (("admissao", "data de admissão"),
                          ("data_inicio", "data de início"),
                          ("demissao", "data de saída")):
        bruto = val(campo)
        if not bruto:
            continue
        try:
            da_planilha = svc._data(bruto, campo)
        except ErroValidacao:
            div.append(f"{rotulo}: a planilha trouxe “{bruto}”, que não é data")
            continue
        if da_planilha != getattr(ja, campo, None):
            aqui = getattr(ja, campo, None)
            div.append(f"{rotulo}: no ERP {aqui.strftime('%d/%m/%Y') if aqui else '—'}, "
                       f"na planilha {da_planilha.strftime('%d/%m/%Y')}")
    return div


def _uma(s: Session, linha: dict[str, str], n: int, achadas: dict[str, str],
         rel: dict[str, Any], vistos: set[str], cargos_novos: set[str],
         obras_faltando: set[str], pis_de: dict[str, str],
         vistos_no_erp: set[int], *, gravar: bool, so_ativos: bool,
         usuario: Optional[Usuario]) -> None:
    """Uma linha da planilha. Levanta ErroValidacao quando a linha não serve."""
    from app.apps.erp.core.cadastros.validadores import cpf_valido

    def val(campo: str) -> str:
        return _v(linha, achadas, campo)

    cpf = re.sub(r"\D", "", val("cpf"))
    nome = " ".join(val("nome").split())
    if not cpf and not nome:
        return                                   # linha em branco de verdade
    if len(cpf) != 11 or not cpf_valido(cpf):
        raise ErroValidacao("CPF em branco ou que não fecha os dígitos")
    if cpf in vistos:
        rel["avisos"].append(f"linha {n}: {nome or cpf} aparece de novo na planilha "
                             "— valeu a primeira linha")
        return
    vistos.add(cpf)

    # QUEM FICA FORA, pela decisão dele: só os ativos entram.
    fase = _simples(val("fase"))
    if so_ativos and (any(f in fase for f in FASES_FORA) or val("demissao")):
        rel["fora"] += 1
        return

    # Já está no ERP? Então a planilha NÃO manda por cima.
    ja = s.scalars(select(Colaborador).where(Colaborador.cpf == cpf)).first()
    if ja is not None:
        rel["ja_existiam"] += 1
        vistos_no_erp.add(ja.id)
        div = _divergencias(s, ja, linha, achadas, nome)
        if div:
            rel["divergencias"].append({"linha": n, "cpf": cpf, "nome": ja.nome,
                                        "o_que": div})
        return

    # QUEM VEM DA PLANILHA JÁ TRABALHA: nasce com o cadastro CONCLUÍDO. Se
    # entrassem em conferência, o primeiro dia do ERP teria uma fila de trabalho
    # falsa com a empresa inteira dentro — 575 pessoas para "conferir" que já
    # estão recebendo há anos.
    dados: dict[str, Any] = {"cpf": cpf, "nome": nome, "etapa": "CONCLUIDO"}

    # 1. O que vai direto, sem tradução.
    for campo in ("data_nascimento", "nome_mae", "nome_pai", "naturalidade",
                  "rg", "rg_emissao", "pis", "ctps_numero", "ctps_serie",
                  "ctps_emissao", "cep", "logradouro", "numero_endereco",
                  "complemento", "bairro", "municipio", "telefone", "email",
                  "matricula", "salario", "valor_diaria", "valor_mes",
                  "cnpj_admissao", "razao_social_pj", "cnpj_pj", "data_inicio",
                  "admissao", "aux_alimentacao", "aux_transporte",
                  "valor_gratificacao", "observacao_auxilio", "requerente",
                  "id_fortes", "pix_chave", "aviso_previo", "ultimo_dia"):
        v = val(campo)
        if v:
            dados[campo] = v
    # UF e órgão do RG são listas fechadas, mas o valor já vem na forma certa.
    for campo in ("rg_orgao", "rg_uf", "ctps_uf", "uf", "naturalidade_uf"):
        v = val(campo).strip().upper()
        if v:
            dados[campo] = v

    # 2. O que precisa de tradução — e o que não é reconhecido vira AVISO, não
    # chute: um "estado civil" errado é melhor vazio que inventado.
    for campo in ("sexo", "vinculo", "estado_civil", "escolaridade",
                  "tipo_contrato", "categoria_alimentacao", "categoria_transporte",
                  "meio_transporte", "contrato_experiencia", "pix_tipo"):
        bruto = val(campo)
        if not bruto:
            continue
        traduzido = _traduzir(campo, bruto)
        if traduzido:
            dados[campo] = traduzido
        else:
            rel["avisos"].append(
                f"linha {n}: {nome} — “{bruto}” não é um valor conhecido de "
                f"{campo.replace('_', ' ')}; ficou vazio")

    # 3. Sim/não.
    for campo in ("primeiro_emprego", "solicitar_salario_familia",
                  "recebe_gratificacao", "parcela_unica", "recebe_via_sp",
                  "cesta_basica_extra"):
        v = val(campo)
        if v:
            dados[campo] = _simples(v) in SIM
    if val("ctps_digital"):
        dados["ctps_digital"] = _simples(val("ctps_digital")) == "digital"
    if val("mensalista"):
        dados["mensalista"] = _simples(val("mensalista")) == "mensalista"
    # "Diversos" é um checklist com dois valores soltos no mesmo campo.
    diversos = _simples(val("diversos"))
    if diversos:
        dados["vale_refeicao"] = "vale refeicao" in diversos
        dados["adicional_transferencia"] = "adicional de transferencia" in diversos

    # 4. As ligações: obra, empresa, cargo e convenção.
    codigo_obra = val("obra")
    if codigo_obra:
        obra = s.scalars(select(Obra).where(Obra.codigo == codigo_obra)).first()
        if obra is None:
            # O connector do Pipefy devolve às vezes o nome, não o código.
            alvo = _simples(codigo_obra)
            obra = next((o for o in s.scalars(select(Obra)).all()
                         if _simples(o.codigo) == alvo or _simples(o.nome) == alvo), None)
        if obra is None:
            obras_faltando.add(codigo_obra)
            rel["avisos"].append(
                f"linha {n}: {nome} — obra “{codigo_obra}” não existe no ERP; "
                "a pessoa entra sem obra")
        else:
            dados["obra_id"] = obra.id

    cnpj_empresa = _cnpj(val("empresa"))
    if cnpj_empresa:
        emp = s.scalars(select(Empresa).where(Empresa.cnpj == cnpj_empresa)).first()
        if emp is not None:
            dados["empresa_id"] = emp.id
        else:
            rel["avisos"].append(
                f"linha {n}: {nome} — a empresa de registro “{val('empresa')}” não "
                "está cadastrada; ficou só o CNPJ no campo de admissão")
            dados.setdefault("cnpj_admissao", val("empresa"))

    nome_cargo = val("cargo")
    if nome_cargo:
        chave = svc_cargos.normalizar_nome(nome_cargo)
        cargo = next((c for c in s.scalars(select(Cargo)).all()
                      if svc_cargos.normalizar_nome(c.nome) == chave), None)
        if cargo is None:
            cargos_novos.add(nome_cargo.strip())
            if gravar:
                cargo = svc_cargos.achar_ou_criar_cargo(
                    s, nome_cargo, cbo=svc_cargos.normalizar_cbo(val("cbo")))
        if cargo is not None:
            dados["cargo_id"] = cargo.id

    nome_conv = val("convencao")
    if nome_conv:
        chave = svc_cargos.normalizar_nome(nome_conv)
        conv = next((c for c in s.scalars(select(Convencao)).all()
                     if svc_cargos.normalizar_nome(c.nome) == chave), None)
        if conv is None:
            rel["avisos"].append(
                f"linha {n}: {nome} — convenção “{nome_conv}” não existe; importe a "
                "planilha de cargos primeiro para a pessoa ficar ligada ao piso")
        else:
            dados["convencao_id"] = conv.id

    # O PIS é único: dois cadastros com o mesmo número fariam a folha somar a
    # vida de duas pessoas num só registro no FGTS.
    pis = re.sub(r"\D", "", str(dados.get("pis") or ""))
    if pis and pis in pis_de:
        raise ErroValidacao(
            f"o PIS {dados['pis']} já está no cadastro de {pis_de[pis]} — "
            "confira qual dos dois está errado")
    if pis:
        pis_de[pis] = nome

    if not gravar:
        rel["criadas"] += 1
        _conferir_dependentes(linha, achadas, n, rel, nome)
        return

    c = svc.salvar(s, dados, usuario)
    rel["criadas"] += 1
    rel["dependentes"] += _gravar_dependentes(s, c.id, linha, achadas, usuario)
    falta = svc.o_que_falta(c)
    if falta:
        rel["avisos"].append(f"linha {n}: {nome} entrou, mas falta " + ", ".join(falta))


# ---------------------------------------------------------------------------
# Dependentes: o pipe guardava cônjuge + cinco filhos em campos separados
# ---------------------------------------------------------------------------
def _lista_de_dependentes(linha: dict[str, str],
                          achadas: dict[str, str]) -> list[dict[str, Any]]:
    saida = []
    conjuge = _v(linha, achadas, "conjuge_nome")
    if conjuge:
        saida.append({"nome": conjuge, "parentesco": "CONJUGE",
                      "data_nascimento": _v(linha, achadas, "conjuge_nascimento"),
                      "cpf": _v(linha, achadas, "conjuge_cpf")})
    for i in range(1, 6):
        nome = _v(linha, achadas, f"filho{i}_nome")
        if nome:
            saida.append({"nome": nome, "parentesco": "FILHO",
                          "data_nascimento": _v(linha, achadas, f"filho{i}_nascimento")})
    return saida


def _conferir_dependentes(linha, achadas, n, rel, nome) -> None:
    quantos = len(_lista_de_dependentes(linha, achadas))
    if quantos:
        rel["dependentes"] += quantos


def _gravar_dependentes(s: Session, colaborador_id: int, linha: dict[str, str],
                        achadas: dict[str, str], usuario: Optional[Usuario]) -> int:
    quantos = 0
    for d in _lista_de_dependentes(linha, achadas):
        try:
            svc.salvar_dependente(s, colaborador_id, d, usuario)
            quantos += 1
        except ErroValidacao:
            # Dependente com dado pela metade não derruba o cadastro da pessoa.
            continue
    return quantos
