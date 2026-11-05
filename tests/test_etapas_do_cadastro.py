# -*- coding: utf-8 -*-
"""AS ETAPAS DO CADASTRO (migração 089).

O dono, em 05/11/2026, explicando para que serviam as fases do card do Pipefy:

> *"a parte de fase do card ela serve de direcionamento pra entender o que
> precisa ser feito em relação àquele cadastro (…) dependendo se é prestador de
> serviço, ou se é CTPS, a gente dá um determinado procedimento. Se o cara é
> prestador de serviço, aquele vínculo de cadastro, ele se resolve apenas
> internamente no DP (…) se for um PJ, tem que fazer um contrato para assinar;
> se for um diarista, morre por aí (…) quem é carteira assinada, CTPS, a gente
> precisa fazer contato com a contabilidade para ela fazer o registro formal."*

O que estes testes defendem:
  1. a BIFURCAÇÃO pelo vínculo é automática — era aqui que o Pipefy errava, com
     alguém arrastando o card para a fila errada;
  2. **não dá para pular a conferência** dos documentos;
  3. etapa ≠ situação: desligar move a etapa, mas quem manda na folha é a
     situação.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core.cadastros import pessoas as svc

banco_ = pytest.mark.banco

CPF_A = "52998224725"
CPF_B = "11144477735"


# ---------------------------------------------------------------------------
# SEM BANCO: a bifurcação é função pura do vínculo e do tipo de contrato
# ---------------------------------------------------------------------------
class PessoaFalsa:
    def __init__(self, vinculo, tipo_contrato=None):
        self.vinculo, self.tipo_contrato = vinculo, tipo_contrato


@pytest.mark.parametrize("vinculo, contrato, esperado", [
    ("CTPS", "CLT_INDETERMINADO", "NA_CONTABILIDADE"),
    ("CTPS", None, "NA_CONTABILIDADE"),
    ("PRESTADOR", "PJ", "CONTRATO_A_ASSINAR"),
    ("PRESTADOR", "AUTONOMO_RPA", "CONCLUIDO"),
    ("PRESTADOR", "AUTONOMO_MENSALISTA", "CONCLUIDO"),
    ("PRESTADOR", None, "CONCLUIDO"),
])
def test_o_caminho_depois_da_conferencia_sai_do_vinculo(vinculo, contrato, esperado):
    etapa, porque = svc.caminho_depois_da_conferencia(
        PessoaFalsa(vinculo, contrato))
    assert etapa == esperado
    assert len(porque) > 10, "a bifurcação tem de explicar por que foi para lá"


def test_prestador_que_nao_e_pj_morre_no_dp():
    """As palavras dele: *"se for um diarista, morre por aí; se for um autônomo
    mensalista, também morre por ali."*"""
    etapa, porque = svc.caminho_depois_da_conferencia(
        PessoaFalsa("PRESTADOR", "AUTONOMO_RPA"))
    assert etapa == "CONCLUIDO"
    assert "dentro do DP" in porque


def test_toda_etapa_tem_rotulo_e_diz_o_que_fazer():
    """A etapa existe para DIRECIONAR. Etapa sem a frase do que fazer é só um
    código na tela, e aí ela não serve para nada."""
    for e in svc.ETAPAS:
        assert e in svc.ETAPA_ROTULOS, f"etapa {e} sem rótulo em português"
        assert len(svc.ETAPA_O_QUE_FAZER.get(e, "")) > 10, \
            f"etapa {e} não diz o que fazer"


def test_de_toda_etapa_da_para_sair():
    """Etapa sem saída é cadastro preso para sempre."""
    for e in svc.ETAPAS:
        assert svc.ETAPAS_SEGUINTES.get(e), f"etapa {e} não tem para onde ir"


def test_nao_da_para_ir_do_inicial_direto_para_concluido():
    """É o salto que o quadro do Pipefy permitia arrastando o card: cadastro
    saindo de "inicial" para "concluído" sem ninguém conferir documento."""
    assert "CONCLUIDO" not in svc.ETAPAS_SEGUINTES["CADASTRO_INICIAL"]


# ---------------------------------------------------------------------------
# COM BANCO
# ---------------------------------------------------------------------------
@pytest.fixture
def dono(sessao_real):
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario
    u = Usuario(nome="DP das etapas", email="dp@etapas.teste", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"),
                perfil=PerfilUsuario.DEPARTAMENTO_PESSOAL, telefone="5585900000002")
    sessao_real.add(u)
    sessao_real.flush()
    return u


@banco_
def test_cadastro_novo_nasce_na_primeira_etapa(sessao_real, dono):
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Chegou Agora Aqui"}, dono)
    assert c.etapa == "CADASTRO_INICIAL"


@banco_
def test_quem_vem_da_importacao_nasce_concluido(sessao_real, dono):
    """Aquelas 575 pessoas já trabalham e já recebem. Em conferência, elas
    criariam no primeiro dia uma fila de trabalho falsa com a empresa inteira
    dentro."""
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Ja Trabalha Aqui",
                                 "etapa": "CONCLUIDO"}, dono)
    assert c.etapa == "CONCLUIDO"


@banco_
def test_a_trilha_da_etapa_comeca_junto_com_o_cadastro(sessao_real, dono):
    """Sem a trilha, "este cadastro está há quanto tempo parado?" não tem
    resposta — e era essa a pergunta que o quadro respondia de graça."""
    from app.apps.erp.db.models.cadastros import ColaboradorEtapa
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Chegou Agora Aqui"}, dono)
    trilha = sessao_real.query(ColaboradorEtapa).filter_by(colaborador_id=c.id).all()
    assert [t.para for t in trilha] == ["CADASTRO_INICIAL"]
    assert trilha[0].automatico is True


@banco_
def test_a_conferencia_manda_ctps_para_a_contabilidade(sessao_real, dono):
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Carteira Assinada Silva",
                                 "vinculo": "CTPS"}, dono)
    r = svc.concluir_conferencia(sessao_real, c.id, dono)
    assert r["etapa"] == "NA_CONTABILIDADE"
    assert "eSocial" in r["porque"]
    assert c.etapa == "NA_CONTABILIDADE"


@banco_
def test_a_conferencia_conclui_o_prestador_que_nao_e_pj(sessao_real, dono):
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Diarista Da Obra",
                                 "vinculo": "PRESTADOR",
                                 "tipo_contrato": "AUTONOMO_RPA"}, dono)
    r = svc.concluir_conferencia(sessao_real, c.id, dono)
    assert r["etapa"] == "CONCLUIDO"


@banco_
def test_o_prestador_pj_para_no_contrato_a_assinar(sessao_real, dono):
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Prestador Pj Ltda",
                                 "vinculo": "PRESTADOR", "tipo_contrato": "PJ"}, dono)
    r = svc.concluir_conferencia(sessao_real, c.id, dono)
    assert r["etapa"] == "CONTRATO_A_ASSINAR"


@banco_
def test_a_conferencia_nao_trava_por_falta_de_dado(sessao_real, dono):
    """O DP termina de olhar e o cadastro segue, com a pendência escrita.
    Travar aqui faria o cadastro ficar parado esperando um papel que a própria
    contabilidade vai pedir."""
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Falta Tudo Nele",
                                 "vinculo": "CTPS"}, dono)
    r = svc.concluir_conferencia(sessao_real, c.id, dono)
    assert r["etapa"] == "NA_CONTABILIDADE"
    assert r["falta"], "a pendência tem de vir escrita, não impedir"


@banco_
def test_conferir_duas_vezes_e_recusado_com_a_etapa_de_agora(sessao_real, dono):
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Ja Foi Conferido",
                                 "vinculo": "CTPS"}, dono)
    svc.concluir_conferencia(sessao_real, c.id, dono)
    from app.apps.erp.core.comum.auditoria import ErroValidacao
    with pytest.raises(ErroValidacao, match="Na contabilidade"):
        svc.concluir_conferencia(sessao_real, c.id, dono)


@banco_
def test_o_salto_proibido_e_recusado_dizendo_os_caminhos(sessao_real, dono):
    from app.apps.erp.core.comum.auditoria import ErroValidacao
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Quer Pular Etapa"}, dono)
    with pytest.raises(ErroValidacao) as e:
        svc.mover_etapa(sessao_real, c.id, "CONCLUIDO", dono)
    assert "não dá para ir direto" in str(e.value)
    assert "Em conferência do DP" in str(e.value), \
        "a recusa tem de DIZER por onde dá para ir"


@banco_
def test_desligar_move_a_etapa_e_a_pessoa_sai_da_fila(sessao_real, dono):
    """Etapa ≠ situação, mas desligar encerra o trabalho sobre o cadastro —
    deixar a etapa para trás faria a pessoa continuar na fila do DP depois de
    sair da empresa."""
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Vai Sair Da Empresa",
                                 "etapa": "CONCLUIDO", "admissao": "2026-01-05"}, dono)
    svc.desligar(sessao_real, c.id, {"data_saida": "2026-10-31",
                                     "forma_desligamento": "AVISO_TRABALHADO"}, dono)
    assert c.etapa == "DESLIGADO" and c.situacao == "DESLIGADO"
    fila = svc.fila_do_cadastro(sessao_real)
    assert c.id not in [p["id"] for p in fila]


@banco_
def test_readmissao_volta_para_a_conferencia(sessao_real, dono):
    """O endereço muda, o documento vence, a convenção da obra nova pode ser
    outra. Era isto que o "Checklist Readmissão" do Pipefy cobrava à mão."""
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Vai Voltar Depois",
                                 "etapa": "CONCLUIDO", "admissao": "2025-01-05"}, dono)
    svc.desligar(sessao_real, c.id, {"data_saida": "2025-12-31",
                                     "forma_desligamento": "PEDIDO_COLABORADOR"}, dono)
    svc.readmitir(sessao_real, c.id, {"admissao": "2026-06-01"}, dono)
    assert c.etapa == "EM_CONFERENCIA"


@banco_
def test_a_fila_mostra_quem_esta_parado_e_ha_quantos_dias(sessao_real, dono):
    svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Esta Parado Aqui"}, dono)
    svc.salvar(sessao_real, {"cpf": CPF_B, "nome": "Esta Pronto Ja",
                             "etapa": "CONCLUIDO"}, dono)
    fila = svc.fila_do_cadastro(sessao_real)
    nomes = [p["nome"] for p in fila]
    assert "Esta Parado Aqui" in nomes
    assert "Esta Pronto Ja" not in nomes, "concluído não é fila de trabalho"
    p = next(x for x in fila if x["nome"] == "Esta Parado Aqui")
    assert p["dias_parado"] is not None
    assert len(p["o_que_fazer"]) > 10
