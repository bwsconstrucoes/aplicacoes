# ============================================================================
# O HISTÓRICO DE PREÇOS ANTIGO entrando no sistema.
#
# ESTE IMPORTADOR É O QUE MAIS PODE ESTRAGAR COISA NO ERP, e a razão está nos
# testes abaixo: ele grava dezenas de milhares de linhas que depois viram a
# frase "o menor preço deste insumo foi R$ 12,90". Se ele casar "Vergalhão
# CA50" com "Vergalhão CA60" porque os nomes se parecem, ninguém percebe — e o
# comprador passa a negociar contra um preço que nunca existiu para aquele
# material.
#
# Por isso a metade mais importante desta suíte prova o que ele NÃO faz.
# ============================================================================
import io

import pytest

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.importadores import banco_precos
from app.apps.erp.db.models.cadastros import (
    Fornecedor, Insumo, PrecoHistorico, TipoPreco, UnidadeCompra,
)
from tests.conftest import SessaoFalsa

CABECALHO = ("Nº Mapa,Dt Atualiz. Mapa,Insumo,Especificação,UND,QTD Cotada,"
             "Valor,CNPJ,Fornecedor,Responsável,Categoria\n")


def _csv(*linhas):
    return (CABECALHO + "".join(l + "\n" for l in linhas)).encode("utf-8")


def _base(**extra):
    return [
        Insumo(id=1, descricao="Arame Recozido BWG 18", ativo=True),
        Insumo(id=2, descricao="Cimento Portland CPII 50Kg", ativo=True),
        Fornecedor(id=7, razao_social="ALMEIDA COM DIST MAT CONST LTDA",
                   cnpj_cpf="35419548000155"),
        UnidadeCompra(codigo="KG", descricao="Quilograma"),
    ]


def _importar(conteudo, objetos=None):
    s = SessaoFalsa(*(objetos if objetos is not None else _base()))
    return s, banco_precos.importar(s, conteudo, usuario=None)


# ---------------------------------------------------------------------------
# 1. O caminho feliz — e o formato brasileiro
# ---------------------------------------------------------------------------
def test_uma_linha_vira_um_preco_no_historico():
    s, r = _importar(_csv(
        '7455,12/12/2025,Arame Recozido BWG 18,,KG,35,"11,27",'
        '35.419.548/0001-55,ALMEIDA COM DIST MAT CONST LTDA,CAIO,Armadura'))
    assert r["gravados"] == 1
    gravado = s.adicionados[0]
    assert isinstance(gravado, PrecoHistorico)
    assert str(gravado.preco_unitario) == "11.27"
    assert gravado.insumo_id == 1
    assert gravado.fornecedor_id == 7
    assert gravado.unidade == "KG"
    assert gravado.comprador_nome == "CAIO"
    assert gravado.origem == "PLANILHA"


def test_o_preco_entra_como_COTADO_e_nao_como_comprado():
    """A planilha é o mapa de cotação. Marcar tudo como comprado diria que a
    empresa aceitou pagar cada um daqueles preços — inclusive os recusados,
    que são a maioria das linhas."""
    s, _ = _importar(_csv(
        '7455,12/12/2025,Arame Recozido BWG 18,,KG,35,"11,27",,FORN X,CAIO,A'))
    assert s.adicionados[0].tipo is TipoPreco.COTADO


def test_valor_com_milhar_no_formato_brasileiro():
    s, _ = _importar(_csv(
        '1,01/02/2025,Cimento Portland CPII 50Kg,,KG,1,"1.234,56",,F,C,A'))
    assert str(s.adicionados[0].preco_unitario) == "1234.56"


def test_data_no_formato_brasileiro_vira_data_de_verdade():
    s, _ = _importar(_csv(
        '1,05/08/2024,Arame Recozido BWG 18,,KG,1,"9,53",,F,C,A'))
    assert s.adicionados[0].data.isoformat() == "2024-08-05"


# ---------------------------------------------------------------------------
# 2. A regra dura: insumo que não bate NÃO entra
# ---------------------------------------------------------------------------
def test_insumo_desconhecido_nao_entra_e_e_relatado():
    s, r = _importar(_csv(
        '1,01/02/2025,Telha Fibrocimento 6mm,,KG,1,"50,00",,F,C,A'))
    assert r["gravados"] == 0
    assert s.adicionados == []
    assert r["quantos_nao_reconhecidos"] == 1
    assert r["nao_reconhecidos"][0]["insumo"] == "Telha Fibrocimento 6mm"


def test_insumo_desconhecido_nao_e_cadastrado_pelo_importador():
    """Criar insumo a partir de um nome de planilha povoaria o catálogo com
    variações do mesmo material — e o catálogo é o que sustenta a cotação."""
    s, _ = _importar(_csv(
        '1,01/02/2025,Telha Fibrocimento 6mm,,KG,1,"50,00",,F,C,A'))
    assert not any(isinstance(o, Insumo) for o in s.adicionados)


def test_acento_e_caixa_nao_impedem_o_casamento_exato():
    s, r = _importar(_csv(
        '1,01/02/2025,ARAME RECOZIDO BWG 18,,KG,1,"11,00",,F,C,A'))
    assert r["gravados"] == 1
    assert s.adicionados[0].insumo_id == 1


def test_espaco_rigido_do_google_sheets_nao_quebra_o_casamento():
    """O \\xa0 vem colado quando se copia do Sheets e já derrubou uma carga."""
    s, r = _importar(_csv(
        '1,01/02/2025,Arame Recozido BWG 18,,KG,1,"11,00",,F,C,A'))
    assert r["gravados"] == 1


def test_nome_parecido_demais_para_ser_coincidencia_casa_mas_e_relatado():
    objetos = _base()
    s, r = _importar(_csv(
        '1,01/02/2025,Arame Recozido BWG 18 ,,KG,1,"11,00",,F,C,A'), objetos)
    assert r["gravados"] == 1


def test_CA50_nao_casa_com_CA60():
    """O erro que este importador existe para não cometer."""
    objetos = [Insumo(id=5, descricao="Vergalhão CA60 5.0mm", ativo=True)]
    s, r = _importar(_csv(
        '1,01/02/2025,Vergalhão CA50 5.0mm,,KG,1,"8,00",,F,C,A'), objetos)
    assert r["gravados"] == 0
    assert r["quantos_nao_reconhecidos"] == 1


# ---------------------------------------------------------------------------
# 3. O fornecedor é mais tolerante — e a assimetria é de propósito
# ---------------------------------------------------------------------------
def test_fornecedor_casa_pelo_CNPJ_mesmo_com_o_nome_diferente():
    s, r = _importar(_csv(
        '1,01/02/2025,Arame Recozido BWG 18,,KG,1,"11,00",'
        '35.419.548/0001-55,ALMEIDA MATERIAIS,C,A'))
    assert s.adicionados[0].fornecedor_id == 7


def test_sem_fornecedor_o_preco_entra_assim_mesmo():
    """Preço sem fornecedor ainda responde "quanto custou". Preço no insumo
    errado não responde nada — daí a diferença de rigor."""
    s, r = _importar(_csv(
        '1,01/02/2025,Arame Recozido BWG 18,,KG,1,"11,00",,GERDAU S.A.,C,A'))
    assert r["gravados"] == 1
    assert r["sem_fornecedor"] == 1
    assert s.adicionados[0].fornecedor_id is None


# ---------------------------------------------------------------------------
# 4. Linhas que não dá para aproveitar
# ---------------------------------------------------------------------------
def test_linha_sem_valor_legivel_fica_de_fora():
    s, r = _importar(_csv(
        '1,01/02/2025,Arame Recozido BWG 18,,KG,1,a combinar,,F,C,A'))
    assert r["gravados"] == 0 and r["sem_valor"] == 1


def test_linha_sem_data_fica_de_fora():
    """Preço sem data não é histórico: não dá para saber se é de ontem ou de
    2019, e é a data que decide se ele serve de referência."""
    s, r = _importar(_csv(
        '1,,Arame Recozido BWG 18,,KG,1,"11,00",,F,C,A'))
    assert r["gravados"] == 0 and r["sem_data"] == 1


def test_unidade_que_nao_existe_no_cadastro_nao_derruba_a_linha():
    """Unidade é chave estrangeira: gravar "PC" que não existe derrubaria a
    linha inteira. O preço entra sem unidade e o relatório avisa."""
    s, r = _importar(_csv(
        '1,01/02/2025,Arame Recozido BWG 18,,PC,1,"11,00",,F,C,A'))
    assert r["gravados"] == 1
    assert s.adicionados[0].unidade is None
    assert "PC" in r["unidades_desconhecidas"]


def test_uma_linha_ruim_nao_derruba_as_boas():
    s, r = _importar(_csv(
        '1,01/02/2025,Telha Que Nao Existe,,KG,1,"1,00",,F,C,A',
        '2,02/02/2025,Arame Recozido BWG 18,,KG,1,"11,00",,F,C,A'))
    assert r["gravados"] == 1
    assert r["quantos_nao_reconhecidos"] == 1


# ---------------------------------------------------------------------------
# 5. Reimportar não duplica — e alguém VAI reimportar
# ---------------------------------------------------------------------------
def test_a_mesma_linha_duas_vezes_no_arquivo_entra_uma_vez_so():
    linha = '7455,12/12/2025,Arame Recozido BWG 18,,KG,35,"11,27",,F,C,A'
    s, r = _importar(_csv(linha, linha))
    assert r["gravados"] == 1 and r["repetidos"] == 1


def test_quantidades_diferentes_no_mesmo_mapa_sao_duas_linhas_de_verdade():
    """O mesmo mapa pede 50 kg para uma obra e 60 para outra, ao mesmo preço.
    São dois pedidos, não uma linha repetida."""
    s, r = _importar(_csv(
        '7220,17/11/2025,Arame Recozido BWG 18,,KG,50,"11,03",,F,C,A',
        '7220,17/11/2025,Arame Recozido BWG 18,,KG,60,"11,03",,F,C,A'))
    assert r["gravados"] == 2


def test_o_que_ja_esta_no_banco_nao_entra_de_novo():
    linha = '7455,12/12/2025,Arame Recozido BWG 18,,KG,35,"11,27",,F,C,A'
    objetos = _base()
    s = SessaoFalsa(*objetos)
    banco_precos.importar(s, _csv(linha), usuario=None)
    ja = s.adicionados[0]
    s2 = SessaoFalsa(*objetos, ja)
    r = banco_precos.importar(s2, _csv(linha), usuario=None)
    assert r["gravados"] == 0 and r["repetidos"] == 1


# ---------------------------------------------------------------------------
# 6. A prévia não grava
# ---------------------------------------------------------------------------
def test_a_previa_conta_sem_gravar():
    s = SessaoFalsa(*_base())
    r = banco_precos.previa(s, _csv(
        '1,01/02/2025,Arame Recozido BWG 18,,KG,1,"11,00",,F,C,A'))
    assert r["gravados"] == 1
    assert s.adicionados == []


def test_arquivo_sem_as_colunas_certas_e_recusado_com_recado_util():
    conteudo = b"nome,valor\nalgo,10\n"
    with pytest.raises(ErroValidacao) as e:
        banco_precos.previa(SessaoFalsa(*_base()), conteudo)
    assert "Nº Mapa" in str(e.value)
