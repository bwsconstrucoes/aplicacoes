# ============================================================================
# O QUE PRECISA SER COBRADO — e, principalmente, o que NÃO precisa.
#
# O dono foi específico sobre o risco, em 18/09/2026:
#
#   "De repente o fornecedor não respondeu pelo e-mail, respondeu pelo
#   WhatsApp. Aí o comprador ainda não alimentou o sistema, e na verdade o
#   fornecedor já respondeu."
#
# Ou seja: o erro caro aqui não é deixar de cobrar alguém — é COBRAR QUEM JÁ
# RESPONDEU. O primeiro custa um dia; o segundo custa o fornecedor. Metade
# destes testes existe para provar que a lista sabe ficar calada.
# ============================================================================
from datetime import date, datetime, timedelta, timezone

import pytest

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.suprimentos import cobranca
from app.apps.erp.db.models.cadastros import (
    Cotacao, CotacaoFornecedor, CotacaoPreco, EnvioEmail, Fornecedor,
    StatusCotacao,
)
from tests.conftest import SessaoFalsa, novo_usuario
from app.apps.erp.db.models.cadastros import PerfilUsuario

HOJE = date(2026, 9, 18)


def _envio(coluna_id, dias_atras, situacao="ENVIADO"):
    return EnvioEmail(
        id=100 + coluna_id, entidade_tipo="cotacao", entidade_id=1,
        destinatario_tipo="cotacao_fornecedor", destinatario_id=coluna_id,
        para=["v@f.com"], copia=[], assunto="Cotação", corpo="…",
        anexos=[], situacao=situacao,
        criado_em=datetime(HOJE.year, HOJE.month, HOJE.day, 9, 0,
                           tzinfo=timezone.utc) - timedelta(days=dias_atras))


def _cenario(*, dias=6, com_preco=False, respondido=False, sem_interesse=False,
             situacao_envio="ENVIADO", cobrado_ha=None,
             status=StatusCotacao.ABERTA):
    cot = Cotacao(id=1, numero="COT-0001", titulo="Cimento — Fortaleza",
                  status=status, criado_por=1,
                  criado_em=datetime(2026, 9, 1, tzinfo=timezone.utc))
    forn = Fornecedor(id=7, razao_social="ALMEIDA COM DIST MAT CONST LTDA",
                      nome_fantasia="Almeida")
    coluna = CotacaoFornecedor(id=3, cotacao_id=1, fornecedor_id=7, frete=0,
                               desconto=0, acrescimo_percentual=0, ordem=0,
                               contatos_ids=[], cobrancas=0,
                               sem_interesse=sem_interesse)
    if respondido:
        coluna.respondido_em = datetime(2026, 9, 15, tzinfo=timezone.utc)
        coluna.respondido_canal = "WHATSAPP"
    if cobrado_ha is not None:
        coluna.cobrado_em = datetime(
            HOJE.year, HOJE.month, HOJE.day, tzinfo=timezone.utc
        ) - timedelta(days=cobrado_ha)
    objetos = [cot, forn, coluna, _envio(3, dias, situacao_envio)]
    if com_preco:
        objetos.append(CotacaoPreco(id=9, cotacao_fornecedor_id=3,
                                    cotacao_item_id=1, preco_unitario=10))
    return SessaoFalsa(*objetos), coluna


# ---------------------------------------------------------------------------
# 1. Quando o fornecedor ENTRA na lista
# ---------------------------------------------------------------------------
def test_sem_resposta_e_fora_da_carencia_entra_na_lista():
    s, _ = _cenario(dias=6)
    r = cobranca.sugerir(s, hoje=HOJE)
    assert r["a_cobrar"] == 1
    assert r["blocos"][0]["fornecedores"][0]["dias"] == 6


def test_a_linha_diz_o_que_o_sistema_sabe_e_nao_mais_que_isso():
    """"Disparada há 6 dias, nenhum preço lançado" é verdade. "Não respondeu"
    seria chute — e é o chute que faria o comprador cobrar quem respondeu."""
    s, _ = _cenario(dias=6)
    porque = cobranca.sugerir(s, hoje=HOJE)["blocos"][0]["fornecedores"][0]["porque"]
    assert "nenhum preço lançado" in porque
    assert "não respondeu" not in porque.lower()


# ---------------------------------------------------------------------------
# 2. Quando ele NÃO entra — a metade que importa
# ---------------------------------------------------------------------------
def test_dentro_da_carencia_nao_entra():
    """Cobrar no dia seguinte ao envio ensina o vendedor a ignorar a cobrança."""
    s, _ = _cenario(dias=1)
    r = cobranca.sugerir(s, hoje=HOJE)
    assert r["a_cobrar"] == 0 and r["esperando"] == 1


def test_quem_teve_preco_lancado_nao_e_cobrado():
    s, _ = _cenario(dias=20, com_preco=True)
    r = cobranca.sugerir(s, hoje=HOJE)
    assert r["a_cobrar"] == 0 and r["ja_responderam"] == 1


def test_quem_respondeu_por_whatsapp_nao_e_cobrado():
    """A nuance que o dono levantou: respondeu por fora, preço ainda não
    digitado, e o sistema tem de ficar calado."""
    s, _ = _cenario(dias=20, respondido=True)
    assert cobranca.sugerir(s, hoje=HOJE)["a_cobrar"] == 0


def test_quem_avisou_que_nao_vai_cotar_nao_e_cobrado():
    s, _ = _cenario(dias=20, sem_interesse=True)
    assert cobranca.sugerir(s, hoje=HOJE)["a_cobrar"] == 0


def test_cobrado_ontem_espera_o_dia_seguinte():
    s, _ = _cenario(dias=20, cobrado_ha=0)
    r = cobranca.sugerir(s, hoje=HOJE)
    assert r["a_cobrar"] == 0 and r["esperando"] == 1


def test_envio_que_falhou_nao_gera_cobranca():
    """Silêncio depois de um e-mail que nunca saiu é problema nosso. Cobrar o
    fornecedor por isso queima a confiança do comprador na tela inteira."""
    s, _ = _cenario(dias=20, situacao_envio="FALHOU")
    r = cobranca.sugerir(s, hoje=HOJE)
    assert r["a_cobrar"] == 0 and r["nao_disparadas"] == 1


def test_cotacao_fechada_sai_da_cobranca():
    s, _ = _cenario(dias=20, status=StatusCotacao.FECHADA)
    assert cobranca.sugerir(s, hoje=HOJE)["a_cobrar"] == 0


# ---------------------------------------------------------------------------
# 3. Urgência — e por que ela não é enfeite
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("dias,esperado", [
    (2, "NORMAL"), (3, "MEDIA"), (5, "ALTA"), (12, "ATRASADO")])
def test_a_urgencia_cresce_com_o_silencio(dias, esperado):
    s, _ = _cenario(dias=dias)
    assert cobranca.sugerir(s, hoje=HOJE)["blocos"][0]["urgencia"] == esperado


# ---------------------------------------------------------------------------
# 4. As duas saídas
# ---------------------------------------------------------------------------
def test_marcar_resposta_exige_dizer_por_onde_chegou():
    """Sem o canal não dá para medir tempo de resposta depois — e é essa
    medida que um dia torna o sistema inteligente."""
    s, coluna = _cenario()
    u = novo_usuario(1, PerfilUsuario.ADMIN)
    s.objetos.append(u)
    with pytest.raises(ErroValidacao):
        cobranca.marcar_resposta(s, 3, canal="", usuario=u)


def test_marcar_resposta_carimba_o_canal_e_a_hora():
    s, coluna = _cenario()
    u = novo_usuario(1, PerfilUsuario.ADMIN)
    s.objetos.append(u)
    cobranca.marcar_resposta(s, 3, canal="whatsapp", quem="Seu Zé", usuario=u)
    assert coluna.respondido_canal == "WHATSAPP"
    assert coluna.respondido_em is not None
    assert coluna.respondido_por == "Seu Zé"


def test_sem_interesse_e_diferente_de_nao_responder():
    """Contar as duas juntas diria, daqui a um ano, que o fornecedor atencioso
    é relapso."""
    s, coluna = _cenario()
    u = novo_usuario(1, PerfilUsuario.ADMIN)
    s.objetos.append(u)
    cobranca.marcar_sem_interesse(s, 3, motivo="sem estoque", usuario=u)
    assert coluna.sem_interesse is True
    assert coluna.motivo_sem_interesse == "sem estoque"
    assert coluna.respondido_canal is None       # não respondeu proposta


def test_cobrar_sem_marcar_ninguem_e_recusado():
    s, _ = _cenario()
    u = novo_usuario(1, PerfilUsuario.ADMIN)
    with pytest.raises(ErroValidacao):
        cobranca.cobrar(s, [], u)
