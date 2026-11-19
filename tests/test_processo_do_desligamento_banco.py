"""O PROCESSO DO DESLIGAMENTO — com banco de verdade (19/11/2026).

Pedido dele: *"É feita a solicitação, o DP analisa, o DP encaminha o pedido
para contabilidade (…) A contabilidade retorna a documentação rescisória (…)
colher assinaturas e devolver para o DP (…) o lançamento financeiro da
rescisão, como também (…) a multa rescisória. Vai ser feito esses dois
lançamentos pagos, e só então a solicitação se encerra."*

E sobre quem continua: *"Quando eu coloco sim (…) O ponto dele continua
ativo, ele continua ativo. Mas o DP precisa validar aquela informação (…) o
contrato de trabalho da pessoa precisa mudar (…) A data de saída dele vai ser
apagada (…) As outras informações têm que estar registradas, mas no
histórico."*

POR QUE COM BANCO: os passos são LIDOS de outros registros (o pedido à
contabilidade, o lançamento pago) por `WHERE`, e a trava de "uma SP para um
passo só" é consulta. O dublê ignoraria os dois.
"""
from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest
from sqlalchemy import text

from app.apps.erp.core import contabilidade as svc_ct
from app.apps.erp.core import desligamento_processo as proc
from app.apps.erp.core import pessoal_solicitacoes as svc
from app.apps.erp.core.cadastros import pessoas as svc_pessoas
from app.apps.erp.core.comum.auditoria import ErroValidacao

pytestmark = pytest.mark.banco

HOJE = dt.date.today()
CPF_A = "52998224725"
CPF_B = "11144477735"


@pytest.fixture
def mundo(sessao_real):
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import (
        Categoria, Fornecedor, Obra, PerfilUsuario as P, RegimeTributario,
        TipoPessoa, Usuario,
    )

    def gente(apelido, perfil):
        u = Usuario(nome=apelido, email=f"{apelido}@desl.teste", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-123"), perfil=perfil,
                    telefone="5585900000000")
        sessao_real.add(u)
        return u

    dp = gente("dp-desl", P.DEPARTAMENTO_PESSOAL)
    obra_user = gente("obra-desl", P.SUPERVISOR_OBRA)
    o = Obra(codigo="DESL-01", nome="Obra do desligamento", status="ATIVA")
    forn = Fornecedor(tipo_pessoa=TipoPessoa.PJ, cnpj_cpf="11444777000161",
                      razao_social="QUEM RECEBE LTDA",
                      regime_tributario=RegimeTributario.NAO_INFORMADO)
    cat = Categoria(codigo="9.9.99", descricao="Rescisões",
                    grupo_codigo="9", grupo_nome="Pessoal", natureza="RESULTADO")
    sessao_real.add_all([o, forn, cat])
    sessao_real.flush()
    carteira = svc_pessoas.salvar(sessao_real, {
        "cpf": CPF_A, "nome": "Carteira Da Silva",
        "admissao": (HOJE - dt.timedelta(days=400)).isoformat(),
        "obra_id": o.id, "salario": "1800", "vinculo": "CTPS",
        "data_nascimento": "1990-01-01", "pis": "12016254396"}, dp)
    prestador = svc_pessoas.salvar(sessao_real, {
        "cpf": CPF_B, "nome": "Prestador De Souza",
        "admissao": (HOJE - dt.timedelta(days=300)).isoformat(),
        "obra_id": o.id, "vinculo": "PRESTADOR"}, dp)
    sessao_real.flush()
    return {"s": sessao_real, "dp": dp, "obra_user": obra_user, "obra": o,
            "forn": forn, "cat": cat, "carteira": carteira,
            "prestador": prestador}


def _desligamento(mundo, *, forma="Demissão com Aviso Trabalhado",
                  continua="Não, está saindo da empresa",
                  saida=None, ultimo=None):
    s = mundo["s"]
    p = svc.abrir(s, {
        "tipo": "DESLIGAMENTO", "colaborador_id": mundo["carteira"].id,
        "forma_desligamento": forma,
        "aviso_previo": (HOJE - dt.timedelta(days=40)).isoformat(),
        "continua_prestador": continua}, mundo["obra_user"])
    dados = {"data_saida": (saida or HOJE + dt.timedelta(days=5)).isoformat()}
    if ultimo:
        dados["ultimo_dia"] = ultimo.isoformat()
    r = svc.atender(s, p["id"], {"dados": dados}, mundo["dp"])
    s.flush()
    return r


def _titulo(mundo, sp, status="APROVADO"):
    from app.apps.erp.db.models.financeiro import (
        FormaPagamento, StatusTitulo, Titulo, TipoTitulo,
    )
    t = Titulo(numero_sp=sp, tipo=TipoTitulo.T1_MATERIAL_NFE,
               fornecedor_id=mundo["forn"].id, descricao=f"lançamento {sp}",
               valor_bruto=Decimal("1000"), valor_liquido=Decimal("1000"),
               competencia=dt.date(2026, 9, 1), categoria_id=mundo["cat"].id,
               forma_pagamento=FormaPagamento.PIX,
               status=StatusTitulo[status], solicitante_id=mundo["dp"].id,
               colaborador_id=mundo["carteira"].id)
    mundo["s"].add(t)
    mundo["s"].flush()
    return t


def _pagar(mundo, t):
    mundo["s"].execute(text("UPDATE titulos SET status = 'PAGO' WHERE id = :i"),
                       {"i": t.id})
    mundo["s"].flush()


def _ler(mundo, sid):
    return svc.ler(mundo["s"], sid)


def _estado(sol, codigo):
    return next(p for p in sol["processo"]["passos"]
                if p["codigo"] == codigo)["estado"]


# ---------------------------------------------------------------------------
# 1. Atender abre o processo — não fecha o pedido
# ---------------------------------------------------------------------------
def test_atender_o_desligamento_de_carteira_abre_o_processo(mundo):
    r = _desligamento(mundo)
    assert r["situacao"] == "EM_ANDAMENTO"
    assert r["situacao_rotulo"] == "Em andamento"
    assert r["aberta"] and not r["a_atender"]
    assert _estado(r, "REGISTRADO") == "FEITO"
    assert _estado(r, "CONTABILIDADE") == "PENDENTE"
    assert not r["processo"]["pode_encerrar"]
    # quem não continua não tem o passo da continuidade
    assert not any(p["codigo"] == "CONTINUIDADE" for p in r["processo"]["passos"])


def test_o_processo_nao_se_encerra_pela_metade(mundo):
    r = _desligamento(mundo)
    with pytest.raises(ErroValidacao) as e:
        proc.encerrar(mundo["s"], r, mundo["dp"])
    assert "Falta" in str(e.value)
    assert "contabilidade" in str(e.value).lower()


def test_em_andamento_nao_se_cancela_nem_se_atende_de_novo(mundo):
    r = _desligamento(mundo)
    with pytest.raises(ErroValidacao) as e:
        svc.cancelar(mundo["s"], r["id"], {"motivo": "mudou de ideia"},
                     mundo["obra_user"])
    assert "cancelamento de desligamento" in str(e.value)
    with pytest.raises(ErroValidacao):
        svc.atender(mundo["s"], r["id"], {}, mundo["dp"])


def test_desligamento_de_prestador_continua_fechando_no_atendimento(mundo):
    s = mundo["s"]
    p = svc.abrir(s, {"tipo": "DESLIGAMENTO",
                      "colaborador_id": mundo["prestador"].id,
                      "data_saida": (HOJE + dt.timedelta(days=3)).isoformat()},
                  mundo["obra_user"])
    r = svc.atender(s, p["id"], {}, mundo["dp"])
    assert r["situacao"] == "ATENDIDA"
    assert r["processo"] is None


# ---------------------------------------------------------------------------
# 2. A cadeia inteira, do pedido à multa paga
# ---------------------------------------------------------------------------
def test_a_cadeia_inteira_ate_encerrar(mundo):
    s = mundo["s"]
    r = _desligamento(mundo, ultimo=HOJE + dt.timedelta(days=5))
    sid = r["id"]

    # a contabilidade: rascunho ainda não conta; enviado conta
    ped = svc_ct.abrir(s, {"template": "DESLIGAMENTO",
                           "colaborador_id": mundo["carteira"].id,
                           "solicitacao_id": sid,
                           "dados": {"forma_desligamento": "AVISO_TRABALHADO"}},
                       mundo["dp"])
    assert _estado(_ler(mundo, sid), "CONTABILIDADE") == "PENDENTE"
    s.execute(text("UPDATE dp_pedidos SET situacao = 'ENVIADO', "
                   " prazo = :p WHERE id = :i"),
              {"p": HOJE + dt.timedelta(days=3), "i": ped["id"]})
    sol = _ler(mundo, sid)
    assert _estado(sol, "CONTABILIDADE") == "FEITO"
    assert _estado(sol, "DOCUMENTACAO") == "AGUARDANDO"

    # a documentação volta
    s.execute(text("UPDATE dp_pedidos SET situacao = 'RESPONDIDO' WHERE id = :i"),
              {"i": ped["id"]})
    assert _estado(_ler(mundo, sid), "DOCUMENTACAO") == "FEITO"

    # o que acontece fora do sistema: o DP marca
    proc.marcar(s, _ler(mundo, sid), {"passo": "ENCAMINHADA", "acao": "feito"},
                mundo["dp"])
    proc.marcar(s, _ler(mundo, sid), {"passo": "ASSINADA", "acao": "feito"},
                mundo["dp"])

    # os dois lançamentos: lançado ainda não é pago
    trct = _titulo(mundo, "SP-DESL-1")
    multa = _titulo(mundo, "SP-DESL-2")
    proc.marcar(s, _ler(mundo, sid), {"passo": "RESCISAO_PAGA",
                                      "acao": "lancamento",
                                      "numero_sp": "sp-desl-1"}, mundo["dp"])
    proc.marcar(s, _ler(mundo, sid), {"passo": "MULTA_PAGA",
                                      "acao": "lancamento",
                                      "numero_sp": "SP-DESL-2"}, mundo["dp"])
    sol = _ler(mundo, sid)
    assert _estado(sol, "RESCISAO_PAGA") == "AGUARDANDO"
    assert not sol["processo"]["pode_encerrar"]

    _pagar(mundo, trct)
    sol = _ler(mundo, sid)
    assert _estado(sol, "RESCISAO_PAGA") == "FEITO"
    assert not sol["processo"]["pode_encerrar"], "falta a multa"
    _pagar(mundo, multa)
    sol = _ler(mundo, sid)
    assert sol["processo"]["pode_encerrar"]

    proc.encerrar(s, sol, mundo["dp"])
    fim = _ler(mundo, sid)
    assert fim["situacao"] == "ATENDIDA"
    assert any(e["acao"] == "PROCESSO_ENCERRADO" for e in fim["eventos"])


def test_a_mesma_sp_nao_paga_dois_passos(mundo):
    s = mundo["s"]
    r = _desligamento(mundo)
    _titulo(mundo, "SP-UNICA")
    proc.marcar(s, r, {"passo": "RESCISAO_PAGA", "acao": "lancamento",
                       "numero_sp": "SP-UNICA"}, mundo["dp"])
    with pytest.raises(ErroValidacao) as e:
        proc.marcar(s, _ler(mundo, r["id"]),
                    {"passo": "MULTA_PAGA", "acao": "lancamento",
                     "numero_sp": "SP-UNICA"}, mundo["dp"])
    assert "dois lançamentos" in str(e.value)


def test_sp_cancelada_nao_serve_e_sp_que_nao_existe_e_recusada(mundo):
    s = mundo["s"]
    r = _desligamento(mundo)
    _titulo(mundo, "SP-MORTA", status="CANCELADO")
    for numero in ("SP-MORTA", "SP-QUE-NAO-EXISTE"):
        with pytest.raises(ErroValidacao):
            proc.marcar(s, r, {"passo": "RESCISAO_PAGA", "acao": "lancamento",
                               "numero_sp": numero}, mundo["dp"])


def test_passo_que_o_sistema_le_so_se_marca_a_mao_com_motivo(mundo):
    s = mundo["s"]
    r = _desligamento(mundo)
    with pytest.raises(ErroValidacao) as e:
        proc.marcar(s, r, {"passo": "CONTABILIDADE", "acao": "feito"}, mundo["dp"])
    assert "por fora do sistema" in str(e.value)
    proc.marcar(s, r, {"passo": "CONTABILIDADE", "acao": "feito",
                       "observacao": "mandei pelo e-mail da contabilidade em 10/11"},
                mundo["dp"])
    assert _estado(_ler(mundo, r["id"]), "CONTABILIDADE") == "FEITO"


def test_dispensar_passo_exige_motivo(mundo):
    s = mundo["s"]
    r = _desligamento(mundo)
    with pytest.raises(ErroValidacao):
        proc.marcar(s, r, {"passo": "MULTA_PAGA", "acao": "nao_se_aplica"},
                    mundo["dp"])
    proc.marcar(s, r, {"passo": "MULTA_PAGA", "acao": "nao_se_aplica",
                       "observacao": "acordo homologado sem multa"}, mundo["dp"])
    assert _estado(_ler(mundo, r["id"]), "MULTA_PAGA") == "NAO_SE_APLICA"


def test_pedido_de_demissao_nao_tem_multa(mundo):
    r = _desligamento(mundo, forma="À Pedido do Colaborador")
    assert _estado(r, "MULTA_PAGA") == "NAO_SE_APLICA"


# ---------------------------------------------------------------------------
# 3. Continua como prestador
# ---------------------------------------------------------------------------
def test_quem_continua_segue_ativo_e_sem_data_de_saida(mundo):
    ultimo = HOJE - dt.timedelta(days=2)
    r = _desligamento(mundo, continua="Sim, continua como prestador de serviço",
                      saida=ultimo, ultimo=ultimo)
    c = mundo["s"].get(svc_pessoas.Colaborador, mundo["carteira"].id)
    assert c.situacao == "ATIVO" and c.demissao is None
    assert c.ultimo_dia == ultimo
    assert _estado(r, "CONTINUIDADE") == "PENDENTE"
    # a virada do dia não o pega: não há data de saída
    from app.apps.erp.core.cadastros import desligamentos_do_dia
    desligamentos_do_dia.aplicar(mundo["s"], hoje=HOJE)
    assert mundo["s"].get(svc_pessoas.Colaborador, c.id).situacao == "ATIVO"
    # e aparece na fila do que está em andamento, pedindo a confirmação
    fila = proc.em_andamento(mundo["s"])
    meu = next(x for x in fila if x["id"] == r["id"])
    assert meu["continuidade_a_confirmar"]


def test_confirmar_que_continua_antes_do_ultimo_dia_e_recusado(mundo):
    ultimo = HOJE + dt.timedelta(days=5)
    r = _desligamento(mundo, continua="Sim, continua como prestador de serviço",
                      saida=ultimo, ultimo=ultimo)
    assert _estado(r, "CONTINUIDADE") == "AGUARDANDO"
    with pytest.raises(ErroValidacao) as e:
        proc.confirmar_continuidade(mundo["s"], r, {
            "continua": "1", "tipo_contrato": "AUTONOMO_RPA"}, mundo["dp"])
    assert "ainda não chegou" in str(e.value)


def test_confirmar_que_continua_vira_prestador_e_guarda_a_carteira_no_historico(mundo):
    s = mundo["s"]
    ultimo = HOJE - dt.timedelta(days=1)
    r = _desligamento(mundo, continua="Sim, continua como prestador de serviço",
                      saida=ultimo, ultimo=ultimo)
    proc.confirmar_continuidade(s, r, {
        "continua": "1", "tipo_contrato": "AUTONOMO_RPA",
        "valor_diaria": "120"}, mundo["dp"])
    s.flush()
    c = s.get(svc_pessoas.Colaborador, mundo["carteira"].id)
    assert c.vinculo == "PRESTADOR" and c.tipo_contrato == "AUTONOMO_RPA"
    assert c.situacao == "ATIVO"
    assert c.demissao is None and c.ultimo_dia is None and c.aviso_previo is None
    assert c.salario is None and c.valor_diaria == Decimal("120")
    assert c.admissao == HOJE   # o dia seguinte ao último dia

    vinculos = s.execute(text("""
        SELECT vinculo, admissao, demissao, salario, vinculo_anterior_id, id
          FROM colaborador_vinculos WHERE colaborador_id = :c
         ORDER BY admissao"""), {"c": c.id}).mappings().all()
    assert [v["vinculo"] for v in vinculos] == ["CTPS", "PRESTADOR"]
    assert vinculos[0]["demissao"] == ultimo
    assert vinculos[0]["salario"] == Decimal("1800"), "o salário fica no histórico"
    assert vinculos[1]["vinculo_anterior_id"] == vinculos[0]["id"]

    # a trilha campo a campo diz o que mudou
    mudancas = svc_pessoas.alteracoes_do_cadastro(s, c.id)
    assert any(m["campo"] == "vinculo" and m["para"] == "PRESTADOR"
               for m in mudancas)

    sol = _ler(mundo, r["id"])
    assert _estado(sol, "CONTINUIDADE") == "FEITO"
    with pytest.raises(ErroValidacao):
        proc.confirmar_continuidade(s, sol, {"continua": "0"}, mundo["dp"])


def test_mudou_de_ideia_nao_continua_e_desliga_normal(mundo):
    """*"de repente no meio do caminho decidiu-se que ele não continua mais"*"""
    s = mundo["s"]
    saida = HOJE + dt.timedelta(days=10)
    r = _desligamento(mundo, continua="Sim, continua como prestador de serviço",
                      saida=saida)
    # o "não" vale a qualquer momento, inclusive antes da saída
    proc.confirmar_continuidade(s, r, {"continua": "0",
                                       "observacao": "a obra desistiu"},
                                mundo["dp"])
    s.flush()
    c = s.get(svc_pessoas.Colaborador, mundo["carteira"].id)
    assert c.demissao == saida and c.vinculo == "CTPS"
    assert c.situacao == "ATIVO", "a saída é futura: segue até o dia"
    sol = _ler(mundo, r["id"])
    assert sol["dados"]["continua_prestador"] == "0"
    assert _estado(sol, "CONTINUIDADE") == "FEITO", "a decisão fica à vista"


def test_concluir_a_rescisao_nao_desliga_quem_continua(mundo):
    """A contabilidade devolver a rescisão movia o cadastro para "Desligado".
    Para quem continua como prestador, isso o tiraria do ponto."""
    s = mundo["s"]
    ultimo = HOJE - dt.timedelta(days=1)
    r = _desligamento(mundo, continua="Sim, continua como prestador de serviço",
                      saida=ultimo, ultimo=ultimo)
    ped = svc_ct.abrir(s, {"template": "DESLIGAMENTO",
                           "colaborador_id": mundo["carteira"].id,
                           "solicitacao_id": r["id"],
                           "dados": {"forma_desligamento": "AVISO_TRABALHADO",
                                     "data_saida": ultimo.isoformat(),
                                     "continua_prestador": "1"}},
                       mundo["dp"])
    assert "Data de saída: " + ultimo.strftime("%d/%m/%Y") in ped["corpo"]
    s.execute(text("UPDATE dp_pedidos SET situacao = 'RESPONDIDO' WHERE id = :i"),
              {"i": ped["id"]})
    svc_ct.concluir(s, ped["id"], mundo["dp"])
    c = s.get(svc_pessoas.Colaborador, mundo["carteira"].id)
    assert (c.etapa or "") != "DESLIGADO" and c.situacao == "ATIVO"


def test_o_vigia_avisa_da_continuidade_nao_confirmada(mundo):
    """Enquanto o DP não confirma, a pessoa segue de carteira no cadastro — e
    a folha e a contabilidade enxergam um CTPS que já saiu."""
    from app.apps.erp.core import agente_dp
    ultimo = HOJE - dt.timedelta(days=3)
    _desligamento(mundo, continua="Sim, continua como prestador de serviço",
                  saida=ultimo, ultimo=ultimo)
    achados = agente_dp.vigia(mundo["s"])
    meus = [a for a in achados if a["tipo"] == "continuidade_a_confirmar"
            and a["colaborador_id"] == mundo["carteira"].id]
    assert meus, "a continuidade não confirmada tem de aparecer no vigia"


def test_a_rescisao_pedida_DEPOIS_de_virar_prestador_usa_a_carteira(mundo):
    """⚑ Achado dirigindo a tela (19/11/2026): confirmada a continuidade, o
    cadastro diz "prestador, sem último dia, admitido hoje" — e o pedido de
    rescisão, feito depois (o caso comum), era recusado por "falta o último
    dia". A rescisão é da CARTEIRA que fechou, e ela está no histórico."""
    s = mundo["s"]
    ultimo = HOJE - dt.timedelta(days=1)
    admissao_ctps = mundo["carteira"].admissao
    r = _desligamento(mundo, continua="Sim, continua como prestador de serviço",
                      saida=ultimo, ultimo=ultimo)
    proc.confirmar_continuidade(s, r, {"continua": "1",
                                       "tipo_contrato": "AUTONOMO_RPA"},
                                mundo["dp"])
    s.flush()
    ped = svc_ct.abrir(s, {"template": "DESLIGAMENTO",
                           "colaborador_id": mundo["carteira"].id,
                           "solicitacao_id": r["id"],
                           "dados": {"forma_desligamento": "AVISO_TRABALHADO"}},
                       mundo["dp"])
    corpo = ped["corpo"]
    assert f"Admissão: {admissao_ctps:%d/%m/%Y}" in corpo
    assert f"Último dia trabalhado: {ultimo:%d/%m/%Y}" in corpo
    assert f"Data de saída: {ultimo:%d/%m/%Y}" in corpo
    assert "Salário: R$ 1.800,00" in corpo
    assert "Continua trabalhando como prestador de serviço: sim" in corpo
