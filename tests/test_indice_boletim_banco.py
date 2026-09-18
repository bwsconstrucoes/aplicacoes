"""COLAR O BOLETIM INTEIRO — a tabela como ela chega ao dono.

18/09/2026, ele mandou o formato em que os índices chegam:

    Mês/Ano        Índice     Variação No mês  Variação No ano  Variação 12 meses
    julho/2025     1210,471   0,91             4,39             7,41
    agosto/2025    1216,706   0,52             4,93             7,22

Até então, alimentar a tabela com esse papel eram DOIS trabalhos separados:
digitar a variação mês a mês, e depois informar o número de UM mês noutro campo
para a régua bater. Ele já tem a tabela inteira copiada da fonte — colar faz os
dois de uma vez.

O que estes testes seguram:

1. **O formato dele é entendido como está** — com cabeçalho, com mês por
   extenso, com vírgula decimal e separado por TAB. Se ele tiver de editar o
   texto antes de colar, não resolveu nada.
2. **A variação entra e o número-índice vira a régua** — e o mês mais recente
   da colagem é o que ancora, porque é o que ele acabou de conferir no papel.
3. **Boletim que não bate consigo mesmo vira AVISO**, não silêncio: se
   número ÷ número anterior discorda da variação declarada, ou a colagem
   misturou INCC-DI com INCC-M, ou trocou de coluna — e nos dois casos o erro
   entra com cara de certo e contamina todo reajuste dali para a frente.
4. **A conferência não grava nada.**

COM BANCO DE VERDADE porque grava, recalcula a série inteira com ORDER BY e
mexe na tabela da âncora.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.indices import bcb
from app.apps.erp.db.models.cadastros import IndiceAncora, IndiceEconomico

pytestmark = pytest.mark.banco

# O texto que o dono colou, tal e qual — TAB entre as colunas, cabeçalho junto.
BOLETIM_DO_DONO = (
    "Mês/Ano\tÍndice\tVariação No mês\tVariação No ano\tVariação 12 meses\n"
    "julho/2025\t1210,471\t0,91\t4,39\t7,41\n"
    "agosto/2025\t1216,706\t0,52\t4,93\t7,22\n"
    "setembro/2025\t1218,747\t0,17\t5,11\t6,78\n"
    "outubro/2025\t1222,356\t0,30\t5,42\t6,37\n"
    "novembro/2025\t1225,633\t0,27\t5,70\t6,23\n")


def _linha(s, mes, codigo="INCC-DI"):
    return s.get(IndiceEconomico, (codigo, mes))


# ---------------------------------------------------------------------------
# Ler o formato dele
# ---------------------------------------------------------------------------
def test_o_formato_do_dono_e_entendido_como_esta():
    lido = bcb.ler_boletim(BOLETIM_DO_DONO)

    assert [l["competencia"] for l in lido["linhas"]] == [
        date(2025, 7, 1), date(2025, 8, 1), date(2025, 9, 1),
        date(2025, 10, 1), date(2025, 11, 1)]
    assert lido["linhas"][0]["numero_indice"] == Decimal("1210.471")
    assert lido["linhas"][0]["variacao_pct"] == Decimal("0.91")
    assert len(lido["ignoradas"]) == 1, "só o cabeçalho fica de fora"


@pytest.mark.parametrize("escrito, esperado", [
    ("julho/2025", date(2025, 7, 1)),
    ("07/2025", date(2025, 7, 1)),
    ("jul/25", date(2025, 7, 1)),
    ("2025-07", date(2025, 7, 1)),
    ("Julho de 2025", date(2025, 7, 1)),
    ("março/2026", date(2026, 3, 1)),
])
def test_o_mes_e_aceito_de_varios_jeitos(escrito, esperado):
    """Quem copia de fontes diferentes traz o mês escrito de jeitos diferentes."""
    assert bcb._competencia_do_boletim(escrito) == esperado


@pytest.mark.parametrize("escrito", ["Mês/Ano", "Índice", "", "total geral"])
def test_o_que_nao_e_mes_nao_vira_mes(escrito):
    assert bcb._competencia_do_boletim(escrito) is None


def test_numero_com_ponto_de_milhar_e_virgula_decimal():
    assert bcb._numero_br("1.210,471") == Decimal("1210.471")
    assert bcb._numero_br("0,91") == Decimal("0.91")
    assert bcb._numero_br("—") is None


# ---------------------------------------------------------------------------
# O que a colagem faz no banco
# ---------------------------------------------------------------------------
def test_a_variacao_de_cada_mes_entra_na_tabela(sessao_real):
    s = sessao_real
    bcb.importar_boletim(s, texto=BOLETIM_DO_DONO)

    assert _linha(s, date(2025, 7, 1)).variacao_pct == Decimal("0.910000")
    assert _linha(s, date(2025, 11, 1)).variacao_pct == Decimal("0.270000")
    assert _linha(s, date(2025, 8, 1)).fonte == "BOLETIM"


def test_o_mes_mais_recente_da_colagem_vira_a_regua(sessao_real):
    """É o mês que ele acabou de conferir no papel — é dele que a régua sai."""
    s = sessao_real
    bcb.importar_boletim(s, texto=BOLETIM_DO_DONO)

    ancora = s.get(IndiceAncora, "INCC-DI")
    assert ancora.competencia == date(2025, 11, 1)
    assert ancora.numero_indice == Decimal("1225.633000")
    assert _linha(s, date(2025, 11, 1)).numero_indice == Decimal("1225.633000")


def test_os_meses_anteriores_ficam_a_centesimos_do_boletim(sessao_real):
    """Ancorado em novembro, julho volta para PERTO do número do papel.

    Não é igual, e o motivo é do boletim, não do sistema: a variação é
    publicada com DUAS casas, e voltar quatro meses dividindo por números
    arredondados acumula centésimos. Sobre 1.210,471 a diferença fica na
    terceira casa — 0,013%.

    A escolha por trás disto: a régua é alinhada pelo mês MAIS RECENTE, que é o
    que ele acabou de conferir e o que aparece nos reajustes de agora. O preço
    é a diferença de centésimos nos meses antigos, e ela é dita na tela.
    """
    s = sessao_real
    bcb.importar_boletim(s, texto=BOLETIM_DO_DONO)

    julho = _linha(s, date(2025, 7, 1)).numero_indice
    assert abs(julho - Decimal("1210.471")) < Decimal("0.30")
    assert abs(julho - Decimal("1210.471")) / Decimal("1210.471") < Decimal("0.0005")


def test_o_boletim_do_dono_nao_gera_aviso_de_diferenca(sessao_real):
    """Cinco meses seguidos ficam dentro da folga — nada a avisar.

    Avisar sobre 0,013% seria assustar à toa, e aviso que aparece sempre deixa
    de ser lido justamente quando importa.
    """
    r = bcb.importar_boletim(sessao_real, texto=BOLETIM_DO_DONO)
    assert not any("centésimos do papel" in a for a in r["avisos"])


def test_diferenca_grande_contra_o_papel_e_dita_na_propria_tela(sessao_real):
    """Se o dono for comparar com o papel, o sistema tem de ter avisado antes.

    Descobrir sozinho que o número da tela não bate com o boletim é como esta
    história começou, em 14/09/2026. Aqui os dois meses colados estão longe um
    do outro, então a régua não tem como fechar nos dois — e isso precisa estar
    escrito, não descoberto.
    """
    s = sessao_real
    texto = ("janeiro/2025\t1000,000\t0,50\n"
             "dezembro/2025\t1100,000\t0,40\n")

    r = bcb.importar_boletim(s, texto=texto)

    assert any("centésimos do papel" in a for a in r["avisos"])
    assert any("01/2025" in a for a in r["avisos"])


def test_rodar_de_novo_com_o_mesmo_texto_nao_muda_nada(sessao_real):
    s = sessao_real
    bcb.importar_boletim(s, texto=BOLETIM_DO_DONO)
    antes = {i.competencia: i.numero_indice for i in
             s.query(IndiceEconomico).filter_by(codigo="INCC-DI").all()}

    r = bcb.importar_boletim(s, texto=BOLETIM_DO_DONO)

    depois = {i.competencia: i.numero_indice for i in
              s.query(IndiceEconomico).filter_by(codigo="INCC-DI").all()}
    assert depois == antes
    assert r["gravados"] == 0
    assert all(m["situacao"] == "igual ao que já havia" for m in r["meses"])


def test_o_mes_que_muda_de_valor_e_dito_com_o_valor_antigo(sessao_real):
    """Sobrescrever calado é como um número errado sobrevive a três conferências."""
    s = sessao_real
    s.add(IndiceEconomico(codigo="INCC-DI", competencia=date(2025, 8, 1),
                          variacao_pct=Decimal("9.99"), fonte="MANUAL"))
    s.flush()

    r = bcb.importar_boletim(s, texto=BOLETIM_DO_DONO)

    agosto = [m for m in r["meses"] if m["competencia"] == "08/2025"][0]
    assert agosto["situacao"] == "mudou"
    assert agosto["antes"] == "9,99"
    assert agosto["variacao_pct"] == "0,52"


# ---------------------------------------------------------------------------
# As travas
# ---------------------------------------------------------------------------
def test_boletim_que_nao_bate_consigo_mesmo_vira_aviso(sessao_real):
    """Índice e variação discordando = série trocada ou coluna trocada.

    Aconteceu no primeiro texto que o dono mandou: dezembro dizia 0,27%, mas de
    1.225,633 para 1.228,161 a variação é 0,21%.
    """
    s = sessao_real
    texto = ("novembro/2025\t1225,633\t0,27\n"
             "dezembro/2025\t1228,161\t0,27\n")

    r = bcb.importar_boletim(s, texto=texto)

    incoerencia = [a for a in r["avisos"] if "misturou" in a]
    assert len(incoerencia) == 1
    assert "12/2025" in incoerencia[0]
    assert "0,21%" in incoerencia[0]


def test_boletim_coerente_nao_acusa_incoerencia(sessao_real):
    """Nenhum aviso de série trocada — o de arredondamento é outro assunto."""
    r = bcb.importar_boletim(sessao_real, texto=BOLETIM_DO_DONO)
    assert not any("misturou" in a for a in r["avisos"])


def test_variacao_absurda_e_recusada_antes_de_gravar(sessao_real):
    """80% num mês é dígito trocado, e um dígito trocado contamina a série toda."""
    s = sessao_real
    with pytest.raises(ErroValidacao, match="não existe"):
        bcb.importar_boletim(s, texto="julho/2025\t1210,471\t80,00\n")


def test_texto_sem_nenhuma_linha_reconhecida_e_recusado(sessao_real):
    with pytest.raises(ErroValidacao, match="primeira coluna"):
        bcb.importar_boletim(sessao_real, texto="qualquer coisa\noutra coisa\n")


def test_mes_que_ainda_nao_terminou_fica_de_fora(sessao_real):
    s = sessao_real
    futuro = date(date.today().year + 1, 6, 1)
    texto = f"junho/{futuro.year}\t1300,000\t0,40\n"

    r = bcb.importar_boletim(s, texto=texto)

    assert _linha(s, futuro) is None
    assert any("ainda não terminou" in a for a in r["avisos"])


def test_a_conferencia_nao_grava_nada(sessao_real):
    s = sessao_real

    r = bcb.importar_boletim(s, texto=BOLETIM_DO_DONO, simular=True)

    assert r["simulacao"] is True
    assert len(r["meses"]) == 5
    assert _linha(s, date(2025, 7, 1)) is None, "conferência que grava não é conferência"
    assert s.get(IndiceAncora, "INCC-DI") is None


def test_dá_para_colar_sem_deixar_a_regua_mudar(sessao_real):
    """Quem só quer as variações não é obrigado a mexer na régua."""
    s = sessao_real

    bcb.importar_boletim(s, texto=BOLETIM_DO_DONO, ancorar=False)

    assert _linha(s, date(2025, 7, 1)).variacao_pct == Decimal("0.910000")
    assert s.get(IndiceAncora, "INCC-DI") is None
