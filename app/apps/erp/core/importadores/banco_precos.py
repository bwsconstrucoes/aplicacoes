# ============================================================================
# ERP — core/importadores/banco_precos.py
# O HISTÓRICO DE PREÇOS ANTIGO entra no sistema.
#
# PEDIDO DO DONO, 18/09/2026:
#
#   "Como a gente já tem um histórico de banco de preços (…) vai tentar já
#   criar o nosso banco de informações de preços, que vai servir com toda
#   certeza já para o nosso sistema inteligente. Eu quero ter o histórico de
#   tudo que eu comprei (…) A grande maioria dos insumos manteve a
#   nomenclatura. Um ou outro pode ser que tenha alterado — aí você vai
#   precisar fazer essa normalização."
#
# ESTE É O ARQUIVO QUE MAIS PODE ESTRAGAR COISA NESTE SISTEMA, e vale dizer
# por quê antes do código: ele grava DEZENAS DE MILHARES de linhas que depois
# viram a frase "o menor preço deste insumo foi R$ 12,90". Se ele casar
# "Vergalhão CA50 10mm" com "Vergalhão CA60 10mm" porque os nomes são
# parecidos, ninguém vai perceber — e o comprador vai negociar contra um preço
# que nunca existiu para aquele material.
#
# Por isso a regra aqui é mais dura que no importador de fornecedores:
#
#   1. Casa por descrição EXATA (ignorando acento e caixa). Isso resolve "a
#      grande maioria", que é o que o dono descreveu.
#   2. Casa por semelhança só a partir de 0,90 — e a linha vai para o relatório
#      dizendo com quem casou, para alguém olhar.
#   3. Abaixo disso NÃO IMPORTA e NÃO CRIA INSUMO. Vai para a lista de "não
#      reconhecidos", que sai em planilha. Preço sem insumo certo é pior que
#      preço nenhum: o primeiro mente, o segundo só falta.
#
# O FORNECEDOR é mais tolerante, e a assimetria é de propósito: preço sem
# fornecedor ainda responde "quanto custou"; preço no insumo errado não
# responde nada. Casa por CNPJ, depois por razão social, depois por semelhança
# — e, quando não acha, o preço entra assim mesmo, sem fornecedor, com a linha
# contada no relatório.
#
# REIMPORTAR O MESMO ARQUIVO NÃO DUPLICA. Cada linha tem uma chave (mapa +
# insumo + fornecedor + valor + data) com índice único no banco (migração
# 077). Alguém VAI importar duas vezes — na primeira ninguém confia.
# ============================================================================
from __future__ import annotations

import hashlib
import logging
import re
import unicodedata
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from difflib import SequenceMatcher
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.cadastros.validadores import somente_digitos
from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.core.importadores.planilhas import ler_tabela
from app.apps.erp.db.models.cadastros import (
    Fornecedor, Insumo, PrecoHistorico, TipoPreco, UnidadeCompra, Usuario,
)

logger = logging.getLogger(__name__)

# As colunas que identificam a aba certa dentro da pasta. A planilha do banco
# de preços tem várias abas e a que interessa não é a primeira.
COLUNAS = ("Nº Mapa", "Insumo", "Valor", "Fornecedor")

# Semelhança mínima para casar um insumo que mudou de nome. Alto de propósito:
# ver o comentário do topo.
MINIMO_INSUMO = 0.90
MINIMO_FORNECEDOR = 0.86

# Quanto o importador grava por vez antes de mandar para o banco. A planilha
# tem dezenas de milhares de linhas e a instância divide 2 GB com os outros
# treze módulos: segurar tudo em memória até o fim é como se derruba o serviço.
LOTE = 500


def _chave(texto: Optional[str]) -> str:
    bruto = unicodedata.normalize("NFKD", (texto or "").strip().lower())
    sem_acento = "".join(c for c in bruto if not unicodedata.combining(c))
    # O espaço rígido (\xa0) vem colado do Google Sheets e faz "arame recozido"
    # não casar com "arame recozido". Já mordeu uma vez.
    return re.sub(r"\s+", " ", sem_acento.replace("\xa0", " ")).strip()


def _numeros(texto: str) -> tuple[str, ...]:
    """As partes do nome que CARREGAM NÚMERO, em ordem.

    Existe por causa de um caso que a semelhança de texto não resolve e que
    este importador foi feito para não errar: "Vergalhão CA50 5.0mm" e
    "Vergalhão CA60 5.0mm" são 95% iguais como texto e são materiais
    diferentes. O que os separa é o número — e é sempre assim em obra:
    CA50/CA60, BWG 18/BWG 14, CPII/CPIV, 6mm/8mm.

    Então a regra passa a ser: para casar por semelhança, os números têm de
    ser OS MESMOS. Semelhança alta com número diferente não é o mesmo
    material escrito de outro jeito — é outro material.
    """
    return tuple(sorted(p for p in _chave(texto).split() if any(c.isdigit() for c in p)))


def _campo(linha: dict[str, str], *nomes: str) -> str:
    for nome in nomes:
        for chave, valor in linha.items():
            if _chave(chave) == _chave(nome):
                return (valor or "").strip()
    return ""


def _decimal(texto: str) -> Optional[Decimal]:
    """Converte o valor como a planilha brasileira escreve.

    "1.234,56" e "1234.56" são o mesmo número; "10,05" e "10.05" também. O que
    distingue é a ÚLTIMA vírgula ou ponto: se vier vírgula, ela é decimal.
    """
    bruto = (texto or "").strip().replace("R$", "").replace(" ", "")
    if not bruto:
        return None
    if "," in bruto:
        bruto = bruto.replace(".", "").replace(",", ".")
    try:
        valor = Decimal(bruto)
    except (InvalidOperation, ValueError):
        return None
    return valor if valor > 0 else None


def _data(texto: str) -> Optional[date]:
    bruto = (texto or "").strip()
    if not bruto:
        return None
    for formato in ("%d/%m/%Y", "%Y-%m-%d", "%d/%m/%y", "%d-%m-%Y"):
        try:
            return datetime.strptime(bruto[:10], formato).date()
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Os dois de-para: insumo e fornecedor
# ---------------------------------------------------------------------------
class _DeParaInsumo:
    """Acha o insumo cadastrado que corresponde ao nome da planilha.

    Guarda o que já resolveu: a planilha repete o mesmo insumo centenas de
    vezes, e comparar 3.279 descrições a cada linha é o que transforma uma
    importação de dois minutos numa de duas horas.
    """

    def __init__(self, s: Session):
        self.por_chave: dict[str, int] = {}
        self.descricoes: list[tuple[str, int]] = []
        for i in s.scalars(select(Insumo)).all():
            chave = _chave(i.descricao)
            if chave and chave not in self.por_chave:
                self.por_chave[chave] = i.id
                self.descricoes.append((chave, i.id))
        self.memoria: dict[str, tuple[Optional[int], str, float]] = {}

    def achar(self, nome: str) -> tuple[Optional[int], str, float]:
        """(id do insumo, como casou, semelhança)."""
        chave = _chave(nome)
        if not chave:
            return None, "vazio", 0.0
        if chave in self.memoria:
            return self.memoria[chave]
        achado = self.por_chave.get(chave)
        if achado is not None:
            r = (achado, "exato", 1.0)
        else:
            numeros = _numeros(nome)
            melhor, escore = None, 0.0
            quase, escore_quase = None, 0.0
            for cadastrada, insumo_id in self.descricoes:
                razao = SequenceMatcher(None, chave, cadastrada).ratio()
                if razao > escore_quase:
                    quase, escore_quase = insumo_id, razao
                # A TRAVA: número diferente é material diferente, por mais
                # parecidos que os nomes sejam.
                if _numeros(cadastrada) != numeros:
                    continue
                if razao > escore:
                    melhor, escore = insumo_id, razao
            if melhor is not None and escore >= MINIMO_INSUMO:
                r = (melhor, "parecido", escore)
            else:
                # O relatório mostra a semelhança do MAIS PARECIDO, inclusive
                # do que a trava recusou: é o número que explica para o dono
                # por que aquela linha ficou de fora.
                r = (None, "nao_reconhecido", max(escore, escore_quase))
        self.memoria[chave] = r
        return r


class _DeParaFornecedor:
    def __init__(self, s: Session):
        self.por_documento: dict[str, int] = {}
        self.por_nome: dict[str, int] = {}
        self.nomes: list[tuple[str, int]] = []
        for f in s.scalars(select(Fornecedor)).all():
            doc = somente_digitos(getattr(f, "cnpj_cpf", "") or "")
            if doc:
                self.por_documento.setdefault(doc, f.id)
            for nome in (f.razao_social, f.nome_fantasia):
                chave = _chave(nome)
                if chave and chave not in self.por_nome:
                    self.por_nome[chave] = f.id
                    self.nomes.append((chave, f.id))
        self.memoria: dict[tuple[str, str], Optional[int]] = {}

    def achar(self, documento: str, nome: str) -> Optional[int]:
        doc = somente_digitos(documento or "")
        chave = (doc, _chave(nome))
        if chave in self.memoria:
            return self.memoria[chave]
        achado = self.por_documento.get(doc) if doc else None
        if achado is None and chave[1]:
            achado = self.por_nome.get(chave[1])
        if achado is None and chave[1]:
            melhor, escore = None, 0.0
            for cadastrado, fid in self.nomes:
                razao = SequenceMatcher(None, chave[1], cadastrado).ratio()
                if razao > escore:
                    melhor, escore = fid, razao
            achado = melhor if escore >= MINIMO_FORNECEDOR else None
        self.memoria[chave] = achado
        return achado


def _chave_externa(mapa: str, insumo_id: int, fornecedor: str,
                   valor: Decimal, quando: date, quantidade: str) -> str:
    """Identifica a LINHA da planilha, para reimportar não duplicar.

    Inclui a quantidade porque o mesmo mapa pede o mesmo material em duas
    quantidades diferentes (50 kg para uma obra, 60 para outra) ao mesmo preço
    — e são duas linhas de verdade, não uma repetida.
    """
    crua = f"{mapa}|{insumo_id}|{_chave(fornecedor)}|{valor}|{quando}|{quantidade}"
    return "BP:" + hashlib.sha1(crua.encode("utf-8")).hexdigest()[:24]


# ---------------------------------------------------------------------------
# A importação
# ---------------------------------------------------------------------------
def previa(s: Session, conteudo: bytes) -> dict[str, Any]:
    """O que ACONTECERIA, sem gravar nada.

    Existe porque uma carga de dezenas de milhares de preços não se desfaz com
    um botão, e porque a pergunta que o dono vai fazer antes de apertar é
    exatamente esta: "quantos insumos você não reconheceu?".
    """
    return _processar(s, conteudo, usuario=None, gravar=False)


def importar(s: Session, conteudo: bytes, usuario: Optional[Usuario]) -> dict[str, Any]:
    return _processar(s, conteudo, usuario=usuario, gravar=True)


def _processar(s: Session, conteudo: bytes, *, usuario: Optional[Usuario],
               gravar: bool) -> dict[str, Any]:
    linhas = ler_tabela(conteudo, colunas_esperadas=COLUNAS)
    # A CONFERÊNCIA DO CABEÇALHO É SEPARADA de "veio linha?" de propósito: um
    # arquivo com as colunas erradas produz linhas, e todas seriam descartadas
    # em silêncio por falta de "Insumo". O dono veria "0 preços" sem entender
    # que mandou o arquivo errado.
    faltam = [c for c in ("Insumo", "Valor")
              if not linhas or not any(_campo(l, c) for l in linhas[:50])]
    if not linhas or faltam:
        raise ErroValidacao(
            "Não achei a tabela de preços na planilha. A aba precisa ter as "
            "colunas Nº Mapa, Dt Atualiz. Mapa, Insumo, Valor e Fornecedor — "
            "mande a planilha do banco de preços inteira, em Excel, que o "
            "sistema acha a aba certa sozinho.")

    insumos = _DeParaInsumo(s)
    fornecedores = _DeParaFornecedor(s)
    unidades = {_chave(u.codigo): u.codigo
                for u in s.scalars(select(UnidadeCompra)).all()}
    ja_no_banco = set()
    if gravar:
        ja_no_banco = {r.chave_externa for r in s.scalars(select(PrecoHistorico)).all()
                       if getattr(r, "chave_externa", None)}

    relatorio = {
        "no_arquivo": len(linhas), "gravados": 0, "repetidos": 0,
        "sem_valor": 0, "sem_data": 0, "sem_fornecedor": 0,
        "casados_por_semelhanca": [], "nao_reconhecidos": {},
        "unidades_desconhecidas": set(),
        "mapas": set(), "insumos_alcancados": set(),
    }
    novos = 0

    for numero, linha in enumerate(linhas, start=2):
        nome_insumo = _campo(linha, "Insumo")
        if not nome_insumo:
            continue
        valor = _decimal(_campo(linha, "Valor", "Valor Unitário", "Preço"))
        if valor is None:
            relatorio["sem_valor"] += 1
            continue
        quando = _data(_campo(linha, "Dt Atualiz. Mapa", "Data", "Dt Mapa"))
        if quando is None:
            relatorio["sem_data"] += 1
            continue

        insumo_id, como, escore = insumos.achar(nome_insumo)
        if insumo_id is None:
            atual = relatorio["nao_reconhecidos"].setdefault(
                nome_insumo, {"insumo": nome_insumo, "linhas": 0,
                              "mais_parecido": round(escore, 2)})
            atual["linhas"] += 1
            continue
        if como == "parecido" and len(relatorio["casados_por_semelhanca"]) < 300:
            alvo = s.get(Insumo, insumo_id)
            relatorio["casados_por_semelhanca"].append({
                "planilha": nome_insumo,
                "cadastro": getattr(alvo, "descricao", ""),
                "semelhanca": round(escore, 2)})

        nome_forn = _campo(linha, "Fornecedor")
        fornecedor_id = fornecedores.achar(_campo(linha, "CNPJ", "CNPJ/CPF"),
                                           nome_forn)
        if fornecedor_id is None:
            relatorio["sem_fornecedor"] += 1

        unidade_bruta = _campo(linha, "UND", "Unidade")
        unidade = unidades.get(_chave(unidade_bruta))
        if unidade_bruta and unidade is None:
            # Unidade é chave estrangeira: gravar "KG " ou "PC" que não existe
            # derruba a linha inteira. Fica em branco e o relatório conta.
            relatorio["unidades_desconhecidas"].add(unidade_bruta)

        mapa = _campo(linha, "Nº Mapa", "N Mapa", "ID Mapa", "Mapa")
        quantidade = _campo(linha, "QTD Cotada", "Quantidade", "QTD")
        chave = _chave_externa(mapa, insumo_id, nome_forn, valor, quando, quantidade)
        relatorio["mapas"].add(mapa)
        relatorio["insumos_alcancados"].add(insumo_id)

        if chave in ja_no_banco:
            relatorio["repetidos"] += 1
            continue
        ja_no_banco.add(chave)

        if not gravar:
            relatorio["gravados"] += 1
            continue

        s.add(PrecoHistorico(
            insumo_id=insumo_id,
            especificacao=_campo(linha, "Especificação", "Especificacao") or None,
            unidade=unidade,
            preco_unitario=valor,
            quantidade=_decimal(quantidade),
            fornecedor_id=fornecedor_id,
            obra_id=None, condicao_pagamento_id=None,
            # COTADO, e não COMPRADO: a planilha é o mapa de cotação. Marcar
            # como comprado diria que a empresa aceitou pagar cada um daqueles
            # preços — inclusive os que ela recusou, que são a maioria.
            tipo=TipoPreco.COTADO,
            cotacao_id=None, data=quando,
            chave_externa=chave, origem="PLANILHA",
            comprador_nome=_campo(linha, "Responsável", "Responsavel") or None))
        relatorio["gravados"] += 1
        novos += 1
        if novos % LOTE == 0:
            s.flush()

    if gravar:
        s.flush()
        registrar_evento(s, "banco_precos", 0, "HISTORICO_IMPORTADO",
                         {"gravados": relatorio["gravados"],
                          "repetidos": relatorio["repetidos"],
                          "nao_reconhecidos": len(relatorio["nao_reconhecidos"])},
                         usuario.id if usuario else None)
        logger.info("ERP/suprimentos: banco de preços — %d preço(s) gravados, "
                    "%d repetidos, %d insumo(s) não reconhecidos",
                    relatorio["gravados"], relatorio["repetidos"],
                    len(relatorio["nao_reconhecidos"]))

    return _fechar_relatorio(relatorio)


def _fechar_relatorio(r: dict[str, Any]) -> dict[str, Any]:
    nao_reconhecidos = sorted(r["nao_reconhecidos"].values(),
                              key=lambda x: -x["linhas"])
    perdidas = sum(x["linhas"] for x in nao_reconhecidos)
    return {
        "no_arquivo": r["no_arquivo"],
        "gravados": r["gravados"],
        "repetidos": r["repetidos"],
        "sem_valor": r["sem_valor"],
        "sem_data": r["sem_data"],
        "sem_fornecedor": r["sem_fornecedor"],
        "mapas": len(r["mapas"]),
        "insumos_alcancados": len(r["insumos_alcancados"]),
        "casados_por_semelhanca": r["casados_por_semelhanca"],
        "nao_reconhecidos": nao_reconhecidos[:200],
        "quantos_nao_reconhecidos": len(nao_reconhecidos),
        "linhas_perdidas": perdidas,
        "unidades_desconhecidas": sorted(r["unidades_desconhecidas"]),
        "recado": _recado(r["gravados"], r["repetidos"], nao_reconhecidos, perdidas),
    }


def _recado(gravados: int, repetidos: int, nao_reconhecidos: list,
            perdidas: int) -> str:
    partes = [f"{gravados} preço(s)"]
    if repetidos:
        partes.append(f"{repetidos} já estavam no sistema e foram ignorados")
    if nao_reconhecidos:
        partes.append(
            f"{len(nao_reconhecidos)} nome(s) de insumo não bateram com o "
            f"cadastro, deixando {perdidas} linha(s) de fora — elas estão na "
            f"lista abaixo para você decidir")
    return ". ".join(partes) + "."
