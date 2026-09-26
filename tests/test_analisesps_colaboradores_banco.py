# -*- coding: utf-8 -*-
"""
O cadastro de colaboradores, com banco de verdade — 27/09/2026.

⚠️ O DUBLÊ DA SUÍTE NÃO ALCANÇA O QUE IMPORTA AQUI. A sessão dublada ignora
`WHERE`, então ela não prova nada sobre:

  - o `ON CONFLICT (cpf) DO UPDATE`, que é o que faz o botão ATUALIZAR em vez de
    duplicar. Se ele estivesse errado, apertar o botão duas vezes criaria a
    pessoa duas vezes — ou estouraria;
  - o `NUMERIC(14,2)` do dinheiro, que é o que impede o arredondamento binário
    do float. Folha errada em centavo é folha errada;
  - o filtro de quem saiu (`data_saida IS NULL`), que é `WHERE` puro. Errado
    nele, um desligado entraria na lista de pagamento;
  - a busca por nome e por CPF.
"""
from decimal import Decimal

import datetime as dt
import pathlib

import pytest

pytestmark = pytest.mark.banco


@pytest.fixture
def banco_cadastro(banco, monkeypatch):
    """Sobe o schema pelas migrações de verdade — as mesmas que o botão aplica."""
    from sqlalchemy import text

    from app.apps.analisesps import db as db_analisesps

    url = str(banco.url.render_as_string(hide_password=False))
    monkeypatch.setenv("DATABASE_URL", url)
    db_analisesps._engine = None

    pasta = pathlib.Path(db_analisesps.__file__).parent / "migracoes"
    with banco.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS analisesps CASCADE"))
        for caminho in sorted(pasta.glob("*.sql")):
            conn.execute(text(caminho.read_text(encoding="utf-8")))
        conn.commit()
    yield
    with banco.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS analisesps CASCADE"))
        conn.commit()
    db_analisesps._engine = None


def pessoa(cpf, nome, **extra):
    from app.apps.analisesps import colaboradores as col
    registro = {c: "" for c in col.CAMPOS}
    registro.update({
        "cpf": cpf, "nome": nome,
        "valor_alimentacao": None, "valor_transporte": None,
        "valor_gratificacao": None,
        "aviso_previo": None, "ultimo_dia": None, "data_saida": None,
    })
    registro.update(extra)
    return registro


def gravar(*registros):
    from app.apps.analisesps import colaboradores as col
    from app.apps.analisesps.db import conexao
    with conexao() as conn:
        return col._gravar(conn, list(registros))


def test_a_migracao_028_roda_no_postgres(banco_cadastro):
    from app.apps.analisesps import colaboradores as col
    assert col._pronto() is True
    assert col.buscar() == []


def test_grava_e_le_de_volta_com_o_link_do_card(banco_cadastro):
    from app.apps.analisesps import colaboradores as col
    gravar(pessoa("99713349334", "GERLANIO GOMES LIMA", card_pipefy="778899"))
    ficha = col.por_cpf("997.133.493-34")
    assert ficha["nome"] == "GERLANIO GOMES LIMA"
    assert ficha["link_pipefy"] == "https://app.pipefy.com/open-cards/778899"
    assert ficha["desligado"] is False


def test_apertar_o_botao_DUAS_VEZES_atualiza_e_nao_duplica(banco_cadastro):
    """É o coração do botão. `ON CONFLICT (cpf) DO UPDATE`: a tabela nunca fica
    vazia no meio do caminho, e uma segunda passada corrige em vez de somar."""
    from app.apps.analisesps import colaboradores as col
    gravar(pessoa("99713349334", "NOME ANTIGO", cargo="AJUDANTE"))
    gravar(pessoa("99713349334", "NOME NOVO", cargo="ENCARREGADO"))
    assert len(col.buscar()) == 1
    ficha = col.por_cpf("99713349334")
    assert ficha["nome"] == "NOME NOVO"
    assert ficha["cargo"] == "ENCARREGADO"


def test_o_valor_do_auxilio_volta_EXATO_do_banco(banco_cadastro):
    """NUMERIC, não float: 1198.84 em float vira 1198.8399999999999, e a folha
    fecharia com centavo de diferença sem ninguém saber de onde veio."""
    from app.apps.analisesps import colaboradores as col
    gravar(pessoa("99713349334", "ANA",
                  valor_alimentacao=Decimal("1198.84"),
                  valor_transporte=Decimal("0.01"),
                  valor_gratificacao=Decimal("12345.67")))
    ficha = col.por_cpf("99713349334")
    assert ficha["valor_alimentacao"] == Decimal("1198.84")
    assert ficha["valor_transporte"] == Decimal("0.01")
    assert ficha["valor_gratificacao"] == Decimal("12345.67")


def test_valor_NAO_PREENCHIDO_continua_None_e_nao_vira_zero(banco_cadastro):
    """"O cadastro não diz" e "o cadastro diz zero" são respostas diferentes.
    Virar zero esconderia o preenchimento faltando — e pagamento a menos."""
    from app.apps.analisesps import colaboradores as col
    gravar(pessoa("99713349334", "ANA", valor_alimentacao=None))
    assert col.por_cpf("99713349334")["valor_alimentacao"] is None


def test_quem_SAIU_fica_fora_da_lista_do_dia_a_dia(banco_cadastro):
    """`data_saida IS NULL` é WHERE puro — o dublê da suíte o ignora. Errado
    aqui, um desligado entraria na lista de pagamento."""
    from app.apps.analisesps import colaboradores as col
    gravar(pessoa("99713349334", "QUEM FICOU"),
           pessoa("03513441363", "QUEM SAIU", data_saida=dt.date(2026, 8, 31)))

    nomes = [c["nome"] for c in col.buscar()]
    assert nomes == ["QUEM FICOU"]

    # E aparece quando se pede para ver quem saiu, com a marca.
    todos = {c["nome"]: c for c in col.buscar(so_ativos=False)}
    assert set(todos) == {"QUEM FICOU", "QUEM SAIU"}
    assert todos["QUEM SAIU"]["desligado"] is True
    assert todos["QUEM FICOU"]["desligado"] is False


def test_quem_saiu_NAO_e_apagado_pela_atualizacao(banco_cadastro):
    """A data de saída é o que permite conferir uma folha antiga depois. Apagar
    a pessoa faria a folha de julho ficar sem explicação."""
    from app.apps.analisesps import colaboradores as col
    gravar(pessoa("03513441363", "QUEM SAIU", data_saida=dt.date(2026, 8, 31)))
    gravar(pessoa("99713349334", "OUTRA PESSOA"))
    assert col.por_cpf("03513441363") is not None


def test_a_busca_acha_por_pedaco_do_nome_sem_ligar_para_a_caixa(banco_cadastro):
    from app.apps.analisesps import colaboradores as col
    gravar(pessoa("99713349334", "GERLANIO GOMES LIMA"),
           pessoa("03513441363", "LUELIA MADIDA GOMES TOMAS"))
    assert len(col.buscar("gomes")) == 2
    assert [c["nome"] for c in col.buscar("GERLANIO")] == ["GERLANIO GOMES LIMA"]
    assert [c["nome"] for c in col.buscar("luelia")] == \
        ["LUELIA MADIDA GOMES TOMAS"]


def test_a_busca_acha_por_pedaco_do_CPF_com_ou_sem_pontuacao(banco_cadastro):
    from app.apps.analisesps import colaboradores as col
    gravar(pessoa("99713349334", "GERLANIO GOMES LIMA"))
    assert len(col.buscar("997.133.493-34")) == 1
    assert len(col.buscar("99713349334")) == 1
    assert len(col.buscar("133493")) == 1


def test_a_busca_sai_em_ordem_de_nome(banco_cadastro):
    """A lista é para gente ler. Ordem do banco não é ordem nenhuma."""
    from app.apps.analisesps import colaboradores as col
    gravar(pessoa("99713349334", "ZEFERINO"),
           pessoa("03513441363", "ANA"),
           pessoa("11144477735", "MARIA"))
    assert [c["nome"] for c in col.buscar()] == ["ANA", "MARIA", "ZEFERINO"]


def test_a_busca_respeita_o_teto(banco_cadastro):
    from app.apps.analisesps import colaboradores as col
    gravar(*[pessoa(f"9971334933{n % 10}{n // 10}", f"PESSOA {n:02d}")
             for n in range(12)])
    assert len(col.buscar(teto=5)) == 5


def test_varios_de_uma_vez_volta_um_dicionario_por_CPF(banco_cadastro):
    """A tela de Rateio mostra dezenas de pessoas: uma consulta por pessoa faria
    dezenas de idas ao banco a cada visita."""
    from app.apps.analisesps import colaboradores as col
    gravar(pessoa("99713349334", "GERLANIO", card_pipefy="111"),
           pessoa("03513441363", "LUELIA", card_pipefy="222"))
    fichas = col.muitos_por_cpf(["997.133.493-34", "03513441363", "00000000000"])
    assert set(fichas) == {"99713349334", "03513441363"}
    assert fichas["99713349334"]["link_pipefy"].endswith("111")


def test_varios_de_uma_vez_com_lista_vazia_nao_vai_ao_banco(banco_cadastro):
    from app.apps.analisesps import colaboradores as col
    assert col.muitos_por_cpf([]) == {}
    assert col.muitos_por_cpf(["não é cpf"]) == {}


def test_a_hora_da_atualizacao_e_a_contagem_ficam_guardadas(banco_cadastro):
    """É o que a tela mostra ao lado do botão. Sem isso o botão é caixa preta —
    e já houve um que dizia "concluída" sem ter feito nada (10/09/2026)."""
    from app.apps.analisesps import colaboradores as col
    from app.apps.analisesps.db import conexao
    from app.apps.analisesps.sincronizacao import _meta_gravar

    assert col.quando_atualizou()["quando"] == ""
    with conexao() as conn:
        _meta_gravar(conn, "cadastro_atualizado_em", "2026-09-27T14:35:00")
        _meta_gravar(conn, "cadastro_quantidade", "3480")
        _meta_gravar(conn, "cadastro_avisos", "não achei a coluna X")
    estado = col.quando_atualizou()
    assert estado["quando"].startswith("2026-09-27")
    assert estado["pessoas"] == 3480
    assert estado["avisos"] == ["não achei a coluna X"]
    assert estado["pronto"] is True


# ---------------------------------------------------------------------------
# A CARGA INTEIRA — o que o botão realmente chama
#
# A planilha é dublada, mas o CAMINHO é o de verdade: achar as colunas pelo
# nome, montar as faixas, pedir em blocos, juntar, converter e gravar no
# Postgres. É onde um erro de fiação aparece.
# ---------------------------------------------------------------------------
CABECALHO_DE_VERDADE = (
    ["CPF (Cadastro de Pessoa Física)", "Nº Registro Pipefy", "Matrícula",
     "Data de Nascimento", "Nome Completo"]                    # A..E
    + [f"coluna que não interessa {i}" for i in range(6, 23)]  # F..V
    + ["Celular", "não interessa", "não interessa", "Cargo [ ]"]  # W,X,Y,Z
    + ["Valor Auxílio Alimentação", "Modalidade Auxílio Alimentação",
       "Valor Auxílio Transporte", "Modalidade Auxílio Transporte"]  # AA..AD
    + ["Tipo", "Tipo de Contrato", "Fase Atual", "Convenção",
       "Objeto Obra [ ]", "Valor da Gratificação", "Recebe Parcela Única",
       "Paga por BeeVale", "Data do Aviso Prévio", "Último dia Trabalhado",
       "Data de Saída"]                                        # AE..AO
)


class AbaFalsa:
    """Uma aba do Sheets só com o que a carga usa. Guarda o que foi pedido."""

    def __init__(self, cabecalho, linhas):
        self.cabecalho = cabecalho
        # linhas: {numero_da_linha: {indice_da_coluna: texto}}
        self.linhas = linhas
        self.row_count = max(linhas) if linhas else 2
        self.faixas_pedidas: list = []

    def row_values(self, numero):
        assert numero == 1, "o cabeçalho está na linha 1"
        return list(self.cabecalho)

    def batch_get(self, faixas):
        """Devolve uma matriz por faixa, NA ORDEM PEDIDA — como o Sheets faz.

        E CORTA O FIM VAZIO de cada faixa, também como o Sheets faz: é isso que
        cria os tamanhos diferentes que a junção tem de repor."""
        self.faixas_pedidas.append(list(faixas))
        saida = []
        for faixa in faixas:
            inicio, fim = faixa.split(":")
            c1, l1 = _partir(inicio)
            c2, l2 = _partir(fim)
            matriz = []
            for numero in range(l1, l2 + 1):
                linha = self.linhas.get(numero, {})
                matriz.append([linha.get(c, "") for c in range(c1, c2 + 1)])
            while matriz and not any(x for x in matriz[-1]):
                matriz.pop()
            saida.append(matriz)
        return saida


def _partir(endereco):
    """"AB12" -> (índice da coluna a partir de zero, 12)."""
    letras = "".join(c for c in endereco if c.isalpha())
    numero = int("".join(c for c in endereco if c.isdigit()))
    indice = 0
    for letra in letras:
        indice = indice * 26 + (ord(letra) - 64)
    return indice - 1, numero


def _aba_com(*pessoas):
    """Monta a aba: cabeçalho na 1, os números das colunas na 2, dados da 3."""
    linhas = {2: {i: str(i + 1) for i in range(len(CABECALHO_DE_VERDADE))}}
    for n, dados in enumerate(pessoas, start=3):
        linhas[n] = dados
    return AbaFalsa(CABECALHO_DE_VERDADE, linhas)


# Os índices que interessam neste cabeçalho de teste.
I_CPF, I_CARD, I_NOME = 0, 1, 4
I_CELULAR, I_CARGO = 22, 25
I_VAL_ALI, I_MODO_ALI, I_VAL_TRA, I_MODO_TRA = 26, 27, 28, 29
I_TIPO, I_CONTRATO, I_FASE = 30, 31, 32
I_GRATIF, I_SAIDA = 35, 40


def test_a_carga_inteira_traz_o_cadastro_para_o_banco(banco_cadastro, monkeypatch):
    from app.apps.analisesps import colaboradores as col

    aba = _aba_com(
        {I_CPF: "997.133.493-34", I_NOME: "GERLANIO GOMES LIMA",
         I_CARD: "778899", I_CARGO: "ENCARREGADO", I_CELULAR: "(85) 99999-1111",
         I_VAL_ALI: "330,00", I_MODO_ALI: "Segunda à Sexta",
         I_VAL_TRA: "180,50", I_MODO_TRA: "Mensal",
         I_CONTRATO: "CLT (tempo Indeterminado)", I_FASE: "Colaboradores Ativos"},
        {I_CPF: "035.134.413-63", I_NOME: "LUELIA MADIDA GOMES TOMAS",
         I_CARD: "778900", I_GRATIF: "1.500,00", I_TIPO: "Autônomo Mensalista",
         I_SAIDA: "31/08/2026"},
    )
    monkeypatch.setattr(col, "_aba", lambda *a, **k: aba)

    resultado = col.atualizar()

    assert resultado["pessoas"] == 2
    assert resultado["ignoradas"] == 0
    assert resultado["avisos"] == [], resultado["avisos"]

    ficha = col.por_cpf("99713349334")
    assert ficha["nome"] == "GERLANIO GOMES LIMA"
    assert ficha["cargo"] == "ENCARREGADO"
    assert ficha["celular"] == "(85) 99999-1111"
    assert ficha["valor_alimentacao"] == Decimal("330.00")
    assert ficha["modo_alimentacao"] == "Segunda à Sexta"
    assert ficha["valor_transporte"] == Decimal("180.50")
    assert ficha["link_pipefy"] == "https://app.pipefy.com/open-cards/778899"
    assert ficha["desligado"] is False

    saiu = col.por_cpf("03513441363")
    assert saiu["valor_gratificacao"] == Decimal("1500.00")
    assert saiu["data_saida"] == dt.date(2026, 8, 31)
    assert saiu["desligado"] is True
    # E não aparece na lista do dia a dia.
    assert [c["nome"] for c in col.buscar()] == ["GERLANIO GOMES LIMA"]


def test_a_carga_NAO_pede_a_planilha_inteira(banco_cadastro, monkeypatch):
    """⚠️ É o ponto do desenho: pedir faixas em vez de A até a última coluna.

    Fora do custo, é o que deixa na planilha o endereço, o nome da mãe, o RG, o
    PIS e o salário — o que este módulo não busca não pode vazar por ele."""
    from app.apps.analisesps import colaboradores as col

    aba = _aba_com({I_CPF: "99713349334", I_NOME: "ANA"})
    monkeypatch.setattr(col, "_aba", lambda *a, **k: aba)
    col.atualizar()

    faixas = aba.faixas_pedidas[0]
    assert len(faixas) > 1, "pediu tudo de uma vez em vez de pedir faixas"
    # A coluna F (nome da mãe e companhia, no cabeçalho de teste) não entra em
    # nenhuma faixa pedida.
    colunas_pedidas = set()
    for faixa in faixas:
        inicio, fim = faixa.split(":")
        c1, _ = _partir(inicio)
        c2, _ = _partir(fim)
        colunas_pedidas |= set(range(c1, c2 + 1))
    assert 5 not in colunas_pedidas, "pediu uma coluna que não interessa"
    assert I_CPF in colunas_pedidas and I_NOME in colunas_pedidas


def test_a_carga_começa_na_linha_3_e_nao_cria_pessoa_de_numero(
        banco_cadastro, monkeypatch):
    """A linha 2 guarda o número de cada coluna. Lida como dado, criaria um
    colaborador chamado "5" — e ele entraria nas listas de pagamento."""
    from app.apps.analisesps import colaboradores as col

    aba = _aba_com({I_CPF: "99713349334", I_NOME: "ANA"})
    monkeypatch.setattr(col, "_aba", lambda *a, **k: aba)
    col.atualizar()

    assert [c["nome"] for c in col.buscar()] == ["ANA"]
    for faixa in aba.faixas_pedidas[0]:
        inicio, _ = faixa.split(":")
        assert _partir(inicio)[1] >= 3, f"a faixa {faixa} lê a linha do cabeçalho"


def test_linha_SEM_CPF_e_contada_e_avisada(banco_cadastro, monkeypatch):
    """Linha preenchida que não vira pessoa tem de aparecer no recado: é gente
    que o dono pensa que está no sistema e não está."""
    from app.apps.analisesps import colaboradores as col

    aba = _aba_com(
        {I_CPF: "99713349334", I_NOME: "ANA"},
        {I_CPF: "", I_NOME: "SEM CPF NENHUM"},
        {I_CPF: "123", I_NOME: "CPF PELA METADE"},
    )
    monkeypatch.setattr(col, "_aba", lambda *a, **k: aba)
    resultado = col.atualizar()

    assert resultado["pessoas"] == 1
    assert resultado["ignoradas"] == 2
    assert any("CPF válido" in a for a in resultado["avisos"])


def test_a_carga_atualiza_o_que_mudou_e_mantem_o_resto(banco_cadastro, monkeypatch):
    """Duas passadas: é o que acontece quando ele corrige o auxílio no Pipefy e
    aperta o botão."""
    from app.apps.analisesps import colaboradores as col

    monkeypatch.setattr(col, "_aba", lambda *a, **k: _aba_com(
        {I_CPF: "99713349334", I_NOME: "ANA", I_VAL_ALI: "330,00"}))
    col.atualizar()
    assert col.por_cpf("99713349334")["valor_alimentacao"] == Decimal("330.00")

    # Ele corrige no card; a automação leva para a planilha; ele aperta o botão.
    monkeypatch.setattr(col, "_aba", lambda *a, **k: _aba_com(
        {I_CPF: "99713349334", I_NOME: "ANA", I_VAL_ALI: "412,50"}))
    col.atualizar()
    assert col.por_cpf("99713349334")["valor_alimentacao"] == Decimal("412.50")
    assert len(col.buscar()) == 1


def test_coluna_de_auxilio_com_OUTRO_NOME_vira_aviso_na_carga(
        banco_cadastro, monkeypatch):
    """⚠️ O nome das colunas de auxílio é o único que não está confirmado. Se a
    planilha usar outro, o valor fica em branco — e a carga tem de DIZER, porque
    campo em branco vira pagamento a menos."""
    from app.apps.analisesps import colaboradores as col

    cabecalho = list(CABECALHO_DE_VERDADE)
    cabecalho[I_VAL_ALI] = "VL AUX ALIM"      # um nome que ninguém previu
    aba = AbaFalsa(cabecalho, {2: {}, 3: {I_CPF: "99713349334", I_NOME: "ANA",
                                          I_VAL_ALI: "330,00"}})
    monkeypatch.setattr(col, "_aba", lambda *a, **k: aba)

    resultado = col.atualizar()
    assert resultado["pessoas"] == 1
    assert any("Valor Auxílio Alimenta" in a for a in resultado["avisos"])
    # E o valor NÃO foi gravado errado: ficou em branco, com o aviso.
    assert col.por_cpf("99713349334")["valor_alimentacao"] is None


def test_aba_SEM_a_coluna_do_CPF_recusa_com_o_cabecalho_de_verdade(
        banco_cadastro, monkeypatch):
    """Sem CPF não há cadastro. E o recado diz o cabeçalho que a planilha TEM —
    dizer só "não achei" deixaria o dono sem saber o que consertar."""
    from app.apps.analisesps import colaboradores as col

    aba = AbaFalsa(["Nome Completo", "Outra Coisa"], {2: {}})
    monkeypatch.setattr(col, "_aba", lambda *a, **k: aba)

    with pytest.raises(col.ErroDoCadastro) as erro:
        col.atualizar()
    assert "CPF" in str(erro.value)
    assert "Outra Coisa" in str(erro.value), "o recado tem de dizer o cabeçalho"


def test_a_carga_guarda_a_hora_e_a_contagem_para_a_tela(banco_cadastro, monkeypatch):
    from app.apps.analisesps import colaboradores as col

    monkeypatch.setattr(col, "_aba", lambda *a, **k: _aba_com(
        {I_CPF: "99713349334", I_NOME: "ANA"}))
    col.atualizar()

    estado = col.quando_atualizou()
    assert estado["pessoas"] == 1
    assert estado["quando"], "a hora tem de ficar guardada"
    assert estado["pronto"] is True


def test_planilha_de_muitas_linhas_e_lida_em_MAIS_DE_UM_bloco(
        banco_cadastro, monkeypatch):
    """A leitura em blocos é o que mantém o pico de memória em poucos MB numa
    instância de 2 GB dividida com 17 módulos."""
    from app.apps.analisesps import colaboradores as col

    monkeypatch.setattr(col, "LINHAS_POR_BLOCO", 3)
    aba = _aba_com(*[{I_CPF: f"9971334933{n}", I_NOME: f"PESSOA {n}"}
                     for n in range(8)])
    monkeypatch.setattr(col, "_aba", lambda *a, **k: aba)

    resultado = col.atualizar()
    assert resultado["pessoas"] == 8
    assert len(aba.faixas_pedidas) >= 3, "leu tudo de uma vez"
