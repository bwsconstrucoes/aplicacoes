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
                  saida=None, ultimo=None, assinar_aviso=True):
    s = mundo["s"]
    p = svc.abrir(s, {
        "tipo": "DESLIGAMENTO", "colaborador_id": mundo["carteira"].id,
        "forma_desligamento": forma,
        "aviso_previo": (HOJE - dt.timedelta(days=40)).isoformat(),
        # 24/11/2026: no pedido de demissão a obra diz se a pessoa trabalha o aviso
        **({"cumpre_aviso": "Sim — vai trabalhar os 30 dias do aviso"}
           if "Pedido" in forma else {}),
        "continua_prestador": continua}, mundo["obra_user"])
    dados = {"data_saida": (saida or HOJE + dt.timedelta(days=5)).isoformat()}
    if ultimo:
        dados["ultimo_dia"] = ultimo.isoformat()
    r = svc.atender(s, p["id"], {"dados": dados}, mundo["dp"])
    s.flush()
    if assinar_aviso:
        _assina_aviso(mundo, r["id"])
    return r


def _assina_aviso(mundo, sid):
    """O que a obra faz no celular (`termos.assinar`), sem o PIN do ponto: o
    aviso que o atendimento gerou passa a ASSINADO na tela."""
    n = mundo["s"].execute(text("""
        UPDATE dp_termos SET situacao = 'ASSINADO', forma = 'NA_TELA',
               assinado_em = now()
         WHERE solicitacao_id = :s AND situacao = 'ABERTO'"""), {"s": sid}).rowcount
    mundo["s"].flush()
    return n


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


def _documento(mundo, tipo, texto_, tamanho=50000):
    import uuid
    s = mundo["s"]
    anexo = s.execute(text("""
        INSERT INTO anexos (entidade_tipo, entidade_id, nome_arquivo,
                            hash_sha256, tamanho_bytes, mime_type, conteudo)
        VALUES ('colaborador', 1, 'doc.pdf', :h, :t, 'application/pdf',
                '\\x00'::bytea) RETURNING id"""),
        {"h": uuid.uuid4().hex, "t": tamanho}).scalar_one()
    return s.execute(text("""
        INSERT INTO documentos (tipo_codigo, anexo_id, nome_padronizado,
                                colaborador_id, origem, texto)
        VALUES (:t, :a, :n, :c, 'TESTE', :x) RETURNING id"""),
        {"t": tipo, "a": anexo, "n": f"{tipo} {uuid.uuid4().hex[:6]}",
         "c": mundo["carteira"].id, "x": texto_}).scalar_one()


def _devolve(mundo, devolucao_id, texto_, tamanho=60000):
    """O que `portal.devolver` faz, sem o Drive: arquiva e marca DEVOLVIDA."""
    doc = _documento(mundo, "RESCISAO-ASSINADA", texto_, tamanho)
    mundo["s"].execute(text("""
        UPDATE dp_devolucoes SET situacao = 'DEVOLVIDA', documento_id = :d,
               devolvido_em = now() WHERE id = :i"""),
        {"d": doc, "i": devolucao_id})
    mundo["s"].flush()


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
                      "saida_prestador": "Fim do serviço ou do contrato",
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

    # a assinatura vai e volta pelo sistema: a pendência para a obra…
    doc_ct = _documento(mundo, "RESCISAO", "termo de rescisao CPF 52998224725")
    s.execute(text("UPDATE dp_pedidos SET documento_id = :d WHERE id = :i"),
              {"d": doc_ct, "i": ped["id"]})
    dv = proc.mandar_para_assinar(s, _ler(mundo, sid), {}, mundo["dp"])
    sol = _ler(mundo, sid)
    assert _estado(sol, "ENCAMINHADA") == "FEITO"
    assert _estado(sol, "ASSINADA") == "AGUARDANDO"
    # …a obra devolve pelo portal, o agente confere, e o DP valida
    _devolve(mundo, dv["id"], "rescisao assinada 529.982.247-25")
    sol = _ler(mundo, sid)
    passo = next(p for p in sol["processo"]["passos"] if p["codigo"] == "ASSINADA")
    assert passo["estado"] == "PENDENTE" and passo["a_validar"]
    assert any(a["ok"] and "CPF" in a["texto"] for a in passo["analise"])
    assert any("assinatura" in a["texto"] for a in passo["analise"]), \
        "o agente tem de dizer que NÃO enxerga assinatura"
    proc.validar_assinada(s, sol, {"acao": "validar"}, mundo["dp"])
    assert _estado(_ler(mundo, sid), "ASSINADA") == "FEITO"

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


def test_assinada_recusada_volta_para_a_obra(mundo):
    s = mundo["s"]
    r = _desligamento(mundo)
    dv = proc.mandar_para_assinar(s, r, {"sem_arquivo": "1"}, mundo["dp"])
    # sem texto legível: o agente diz que não deu para ler
    _devolve(mundo, dv["id"], "", tamanho=900)
    passo = next(p for p in _ler(mundo, r["id"])["processo"]["passos"]
                 if p["codigo"] == "ASSINADA")
    assert any(a["ok"] is False and "pequeno" in a["texto"] for a in passo["analise"])
    assert any("escaneado" in a["texto"] for a in passo["analise"])
    with pytest.raises(ErroValidacao):
        proc.validar_assinada(s, _ler(mundo, r["id"]), {"acao": "recusar"},
                              mundo["dp"])
    proc.validar_assinada(s, _ler(mundo, r["id"]),
                          {"acao": "recusar", "motivo": "faltou a assinatura"},
                          mundo["dp"])
    assert _estado(_ler(mundo, r["id"]), "ASSINADA") == "AGUARDANDO"


def test_mandar_para_assinar_sem_o_arquivo_da_contabilidade_pede_confirmacao(mundo):
    r = _desligamento(mundo)
    with pytest.raises(ErroValidacao) as e:
        proc.mandar_para_assinar(mundo["s"], r, {}, mundo["dp"])
    assert "ainda não voltou" in str(e.value)


# ---------------------------------------------------------------------------
# O AVISO PRÉVIO ASSINADO ANTES DA CONTABILIDADE (20/11/2026, noite)
# ---------------------------------------------------------------------------
def test_o_aviso_nasce_no_atendimento_e_trava_a_rescisao_ate_voltar(mundo):
    """Ele: *"o DP analisou, concordou, aí aquele documento retorna para quem
    pediu para ele coletar a assinatura do colaborador (…) O DP verifica e
    manda para frente, para a contabilidade. Já com o aviso assinado."*"""
    from app.apps.erp.core import termos
    s = mundo["s"]
    r = _desligamento(mundo, assinar_aviso=False, ultimo=HOJE + dt.timedelta(days=5))
    sid = r["id"]
    t_ = termos.do_pedido(s, sid)
    assert t_ is not None and t_["situacao"] == "ABERTO"
    assert t_["tipo"] == "AVISO-PREVIO" and "TRABALHADO" in t_["texto"]
    assert _estado(_ler(mundo, sid), "AVISO") == "AGUARDANDO"
    # a obra vê o documento para levar à pessoa
    assert any(x["id"] == t_["id"] for x in termos.pendentes(s, None))
    with pytest.raises(ErroValidacao, match="aviso prévio ainda não voltou"):
        svc_ct.abrir(s, {"template": "DESLIGAMENTO",
                         "colaborador_id": mundo["carteira"].id,
                         "solicitacao_id": sid, "dados": {}}, mundo["dp"])
    _assina_aviso(mundo, sid)
    assert _estado(_ler(mundo, sid), "AVISO") == "FEITO"
    ped = svc_ct.abrir(s, {"template": "DESLIGAMENTO",
                           "colaborador_id": mundo["carteira"].id,
                           "solicitacao_id": sid, "dados": {}}, mundo["dp"])
    assert "Aviso prévio assinado" in ped["corpo"]


def test_assinar_na_tela_pede_o_pin_da_pessoa_e_gera_o_pdf(mundo, monkeypatch):
    from app.apps.erp.core import termos
    from app.apps.erp.core.arquivo import service as svc_arq
    s = mundo["s"]
    sid = _desligamento(mundo, assinar_aviso=False)["id"]
    t_ = termos.do_pedido(s, sid)
    pins = []
    monkeypatch.setattr(termos, "_conferir_pin", lambda cpf, pin: pins.append((cpf, pin)))
    guardado = {}

    class Doc:
        id = None

    def arquivar(s_, conteudo, nome, **kw):
        guardado["pdf"] = conteudo
        guardado["tipo"] = kw["tipo_codigo"]
        d = Doc()
        d.id = _documento(mundo, kw["tipo_codigo"], "aviso")
        return d
    monkeypatch.setattr(svc_arq, "arquivar", arquivar)
    import base64
    import io
    from PIL import Image
    buf = io.BytesIO()
    import os
    Image.frombytes("RGB", (120, 60), os.urandom(120 * 60 * 3)).save(buf, format="PNG")
    png = "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode()
    with pytest.raises(ErroValidacao):
        termos.assinar(s, t_["id"], None, pin="123456", assinatura="lixo",
                       conduzido_por={"colaborador_id": None, "nome": "Adm"})
    r = termos.assinar(s, t_["id"], None, pin="123456", assinatura=png,
                       conduzido_por={"colaborador_id": None, "nome": "Adm Obra"})
    assert r["assinado"] and pins and pins[0][1] == "123456"
    assert guardado["pdf"].startswith(b"%PDF") and guardado["tipo"] == "AVISO-PREVIO"
    assert termos.concluido(termos.ler(s, t_["id"]))
    # fora do alcance da obra: não encontrado
    t2 = termos.abrir(s, colaborador_id=mundo["carteira"].id, tipo="AVISO-PREVIO",
                      titulo="x", texto_="y", solicitacao_id=sid, usuario=mundo["dp"])
    from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado
    with pytest.raises(ErroNaoEncontrado):
        termos.para_assinar(s, t2["id"], [-1])


def test_recusa_de_assinatura_precisa_de_duas_testemunhas_e_do_dp(mundo, monkeypatch):
    from app.apps.erp.core import termos
    from app.apps.erp.core.arquivo import service as svc_arq
    s = mundo["s"]
    sid = _desligamento(mundo, assinar_aviso=False)["id"]
    t_ = termos.do_pedido(s, sid)

    class Doc:
        id = None

    def arquivar(s_, conteudo, nome, **kw):
        d = Doc()
        d.id = _documento(mundo, kw["tipo_codigo"], "recusa")
        return d
    monkeypatch.setattr(svc_arq, "arquivar", arquivar)
    from tests.test_ponto_assinatura_folha_banco import _png   # type: ignore
    with pytest.raises(ErroValidacao, match="duas testemunhas"):
        termos.registrar_recusa(s, t_["id"], None, motivo="não quis assinar",
                                testemunhas=[{"nome": "Fulano de Tal"}],
                                conduzido_por={"nome": "Adm"})
    # 25/11/2026: o nome sozinho não basta — CPF válido e a assinatura de cada uma
    so_nomes = [{"nome": "Fulano de Tal"}, {"nome": "Beltrano Silva"}]
    with pytest.raises(ErroValidacao, match="CPF"):
        termos.registrar_recusa(s, t_["id"], None, motivo="não quis assinar",
                                testemunhas=so_nomes, conduzido_por={"nome": "Adm"})
    sem_assinar = [{"nome": "Fulano de Tal", "cpf": "71460238001"},
                   {"nome": "Beltrano Silva", "cpf": "28625587887"}]
    with pytest.raises(ErroValidacao, match="assinatura"):
        termos.registrar_recusa(s, t_["id"], None, motivo="não quis assinar",
                                testemunhas=sem_assinar, conduzido_por={"nome": "Adm"})
    mesma = [{"nome": "Fulano de Tal", "cpf": "71460238001", "assinatura": _png()},
             {"nome": "Beltrano Silva", "cpf": "714.602.380-01", "assinatura": _png()}]
    with pytest.raises(ErroValidacao, match="pessoas diferentes"):
        termos.registrar_recusa(s, t_["id"], None, motivo="não quis assinar",
                                testemunhas=mesma, conduzido_por={"nome": "Adm"})
    termos.registrar_recusa(s, t_["id"], None, motivo="não quis assinar",
                            testemunhas=[{**x, "assinatura": _png()} for x in sem_assinar],
                            conduzido_por={"nome": "Adm"})
    gravadas = s.execute(text("SELECT testemunhas FROM dp_termos WHERE id = :i"),
                         {"i": t_["id"]}).scalar()
    assert [x["cpf"] for x in gravadas] == ["71460238001", "28625587887"]
    assert all("png" not in x and "assinatura" not in x for x in gravadas), \
        "a assinatura vai para o PDF, não para o banco"
    assert _estado(_ler(mundo, sid), "AVISO") == "PENDENTE", "o DP ainda não validou"
    termos.validar(s, t_["id"], mundo["dp"], aceitar=True)
    assert _estado(_ler(mundo, sid), "AVISO") == "FEITO"


def test_fim_de_experiencia_nao_tem_aviso(mundo):
    sid = _desligamento(mundo, forma="Encerramento de Contrato de Experiência",
                        assinar_aviso=False)["id"]
    assert _estado(_ler(mundo, sid), "AVISO") == "NAO_SE_APLICA"



def test_o_aso_demissional_vai_junto_e_nao_trava(mundo):
    """Ele (21/11/2026): *"O demissional já pode ficar como cobrança do
    retorno junto com a documentação assinada rescisória, mas por enquanto não
    é obrigatório."*"""
    s = mundo["s"]
    sid = _desligamento(mundo, ultimo=HOJE + dt.timedelta(days=5))["id"]
    proc.mandar_para_assinar(s, _ler(mundo, sid), {"sem_arquivo": "1"}, mundo["dp"])
    sol = _ler(mundo, sid)
    aso = next(p for p in sol["processo"]["passos"] if p["codigo"] == "ASO_DEMISSIONAL")
    assert aso["estado"] == "AGUARDANDO" and aso["opcional"]
    assert sol["processo"]["proximo"] != aso["rotulo"], "opcional não vira o 'falta'"
    n = s.execute(text("SELECT count(*) FROM dp_devolucoes WHERE colaborador_id = :c "
                       "AND documento_esperado = 'ASO'"),
                  {"c": mundo["carteira"].id}).scalar()
    assert n == 1


# ---------------------------------------------------------------------------
# O plano B: o papel assinado chegou ao escritório (23/11/2026)
# ---------------------------------------------------------------------------
def _arquivo_falso(mundo, monkeypatch):
    from app.apps.erp.core.arquivo import service as svc_arq

    class Doc:
        id = None

    def arquivar(s_, conteudo, nome, **kw):
        d = Doc()
        d.id = _documento(mundo, kw["tipo_codigo"], "papel")
        return d
    monkeypatch.setattr(svc_arq, "arquivar", arquivar)


def test_aviso_assinado_no_papel_o_dp_anexa_e_o_processo_segue(mundo, monkeypatch):
    """Ele: *"e se (…) a pessoa assinou num papel e encaminhou para cá para a
    gente anexar? (…) a gente tem que ter uma alternativa, plano B."*"""
    from app.apps.erp.core import termos
    from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado
    _arquivo_falso(mundo, monkeypatch)
    s = mundo["s"]
    sid = _desligamento(mundo, assinar_aviso=False,
                        ultimo=HOJE + dt.timedelta(days=5))["id"]
    t_ = termos.do_pedido(s, sid)
    passo = next(p for p in _ler(mundo, sid)["processo"]["passos"] if p["codigo"] == "AVISO")
    assert passo["termo"]["situacao"] == "ABERTO"      # a tela mostra o botão
    with pytest.raises(ErroValidacao):
        svc.papel_no_escritorio(s, sid, "termo_papel", t_["id"], b"", "x.pdf", mundo["dp"])
    with pytest.raises(ErroNaoEncontrado):
        svc.papel_no_escritorio(s, sid + 999, "termo_papel", t_["id"], b"%PDF",
                                "x.pdf", mundo["dp"])
    svc.papel_no_escritorio(s, sid, "termo_papel", t_["id"], b"%PDF", "x.pdf", mundo["dp"])
    t2 = termos.ler(s, t_["id"])
    assert t2["forma"] == "PAPEL" and termos.concluido(t2), "o DP anexou: já vale"
    assert _estado(_ler(mundo, sid), "AVISO") == "FEITO"
    # e a rescisão à contabilidade pode ser montada, como na tela
    svc_ct.abrir(s, {"template": "DESLIGAMENTO", "colaborador_id": mundo["carteira"].id,
                     "solicitacao_id": sid, "dados": {}}, mundo["dp"])


def test_documentacao_rescisoria_chegou_no_escritorio(mundo, monkeypatch):
    _arquivo_falso(mundo, monkeypatch)
    s = mundo["s"]
    r = _desligamento(mundo)
    proc.mandar_para_assinar(s, r, {"sem_arquivo": "1"}, mundo["dp"])
    passo = next(p for p in _ler(mundo, r["id"])["processo"]["passos"]
                 if p["codigo"] == "ASSINADA")
    assert passo["estado"] == "AGUARDANDO" and passo["devolucao_aberta_id"]
    svc.papel_no_escritorio(s, r["id"], "devolucao_papel", passo["devolucao_aberta_id"],
                            b"%PDF", "rescisao.pdf", mundo["dp"])
    passo = next(p for p in _ler(mundo, r["id"])["processo"]["passos"]
                 if p["codigo"] == "ASSINADA")
    assert passo.get("a_validar"), "chegou: vai para a validação do DP, como pelo portal"
    proc.validar_assinada(s, _ler(mundo, r["id"]), {"acao": "validar"}, mundo["dp"])
    assert _estado(_ler(mundo, r["id"]), "ASSINADA") == "FEITO"


def test_rescisao_de_quem_veio_sem_historico_de_vinculo_usa_a_forma_do_pedido(mundo):
    """23/11/2026, ele ao montar o pedido: *"falta no cadastro de (…): forma do
    desligamento."* A pessoa importada sem histórico de vínculo não tinha onde
    a forma ficar gravada — mas a solicitação a trazia."""
    s = mundo["s"]
    s.execute(text("DELETE FROM colaborador_vinculos WHERE colaborador_id = :c"),
              {"c": mundo["carteira"].id})
    s.flush()
    r = _desligamento(mundo, ultimo=HOJE + dt.timedelta(days=5))
    with pytest.raises(ErroValidacao, match="forma do desligamento"):
        svc_ct.abrir(s, {"template": "DESLIGAMENTO", "colaborador_id": mundo["carteira"].id,
                         "solicitacao_id": r["id"], "dados": {}}, mundo["dp"])
    ped = svc_ct.abrir(s, {"template": "DESLIGAMENTO",
                           "colaborador_id": mundo["carteira"].id,
                           "solicitacao_id": r["id"],
                           "dados": {"forma_desligamento": r["dados"]["forma_desligamento"]}},
                       mundo["dp"])
    assert ped["numero"]


# ---------------------------------------------------------------------------
# O pedido de demissão: sai de imediato ou trabalha o aviso; a carta da obra
# (24/11/2026)
# ---------------------------------------------------------------------------
def _pedido_de_demissao(mundo, cumpre=None):
    dados = {"tipo": "DESLIGAMENTO", "colaborador_id": mundo["carteira"].id,
             "forma_desligamento": "À Pedido do Colaborador",
             "aviso_previo": HOJE.isoformat(),
             "continua_prestador": "Não, está saindo da empresa"}
    if cumpre is not None:
        dados["cumpre_aviso"] = cumpre
    return svc.abrir(mundo["s"], dados, mundo["obra_user"])


def test_pedido_de_demissao_pergunta_se_sai_de_imediato(mundo):
    """Ele: *"o mais comum é que o colaborador peça para sair de imediato (…)
    vamos ajustar para (…) trabalhar de uma forma ou de outra."*"""
    from app.apps.erp.core import termos
    with pytest.raises(ErroValidacao, match="trabalhar o aviso"):
        _pedido_de_demissao(mundo)
    p = _pedido_de_demissao(mundo, "Não — sai de imediato (pede dispensa do aviso)")
    assert p["dados"]["cumpre_aviso"] == "0"
    svc.atender(mundo["s"], p["id"], {"dados": {"data_saida": HOJE.isoformat()}},
                mundo["dp"])
    mundo["s"].flush()
    t_ = termos.do_pedido(mundo["s"], p["id"])
    assert t_["tipo"] == "PEDIDO-DEMISSAO" and "dispensa" in t_["texto"]
    assert t_["situacao"] == "ABERTO", "sem carta, a pessoa assina pelo portal"


def test_a_carta_anexada_pela_obra_vale_como_pedido_assinado(mundo):
    """Ele: *"pode ser que a pessoa na obra já tenha feito a cartinha e a gente
    vai anexar ao pedido."*"""
    from app.apps.erp.core import termos
    s = mundo["s"]
    p = _pedido_de_demissao(mundo, "Sim — vai trabalhar os 30 dias do aviso")
    doc = _documento(mundo, "PEDIDO-DEMISSAO", "carta escrita à mão")
    s.execute(text("UPDATE documentos SET referencia = :r WHERE id = :i"),
              {"r": f"{p['numero']} — enviado pela obra", "i": doc})
    s.flush()
    svc.atender(s, p["id"], {"dados": {"data_saida": (HOJE + dt.timedelta(days=30)).isoformat()}},
                mundo["dp"])
    s.flush()
    t_ = termos.ler(s, termos.do_pedido(s, p["id"])["id"])
    assert t_["forma"] == "PAPEL" and termos.concluido(t_), "a carta já vale"
    assert _estado(_ler(mundo, p["id"]), "AVISO") == "FEITO"


def test_pedido_de_demissao_nao_anda_sem_a_carta(mundo):
    """Ele (25/11/2026): *"pedido de demissão tem que ter carta de pedido
    sempre, seja pelo painel, seja anexado."* Sem carta anexada, o passo fica
    esperando a assinatura pelo portal — e o DP não consegue dá-lo por feito
    nem dispensá-lo à mão."""
    s = mundo["s"]
    p = _pedido_de_demissao(mundo, "Não — sai de imediato (pede dispensa do aviso)")
    svc.atender(s, p["id"], {"dados": {"data_saida": HOJE.isoformat()}}, mundo["dp"])
    s.flush()
    sol = _ler(mundo, p["id"])
    aviso = next(x for x in sol["processo"]["passos"] if x["codigo"] == "AVISO")
    assert aviso["sem_marca_manual"] is True
    for acao in ("feito", "nao_se_aplica"):
        with pytest.raises(ErroValidacao, match="sempre tem a carta"):
            proc.marcar(s, sol, {"passo": "AVISO", "acao": acao,
                                 "observacao": "assinou no papel por fora"},
                        mundo["dp"])
    assert "carta de pedido de demissão" in proc.aviso_pendente(s, p["id"])


def test_no_pedido_de_demissao_a_recusa_de_assinar_nao_vale(mundo, app_real):
    """25/11/2026: o pedido de demissão é a vontade da pessoa — recusar não
    pode virar o documento assinado (a recusa validada contava como
    concluído). No aviso prévio a recusa continua valendo."""
    from tests.conftest import como                      # type: ignore
    from app.apps.erp.core import portal as svc_portal
    from app.apps.erp.core import termos
    assert svc_portal.recusa_nao_vale("PEDIDO-DEMISSAO")
    assert svc_portal.recusa_nao_vale("AUTORIZACAO-DESCONTO")
    assert not svc_portal.recusa_nao_vale("AVISO-PREVIO")
    assert not svc_portal.recusa_nao_vale("AVISO-FERIAS")
    s = mundo["s"]
    p = _pedido_de_demissao(mundo, "Não — sai de imediato (pede dispensa do aviso)")
    svc.atender(s, p["id"], {"dados": {"data_saida": HOJE.isoformat()}}, mundo["dp"])
    s.commit()
    t_ = termos.do_pedido(s, p["id"])
    cli = como(app_real, mundo["dp"].id)
    g = cli.get(f"/erp/portal/api/termos/{t_['id']}").get_json()
    assert g["ok"] and "vontade da própria pessoa" in g["termo"]["sem_recusa"]
    r = cli.post(f"/erp/portal/api/termos/{t_['id']}", json={
        "acao": "recusa", "motivo": "não quis assinar",
        "testemunhas": [{"nome": "Fulano de Tal"}, {"nome": "Beltrano Silva"}]})
    assert r.status_code == 400 and "vontade" in r.get_json()["erro"]
    with pytest.raises(ErroValidacao, match="vontade"):       # a porta do ponto também
        termos.registrar_recusa(s, t_["id"], None, motivo="não quis assinar",
                                testemunhas=[], conduzido_por={"nome": "Adm"})
    assert termos.do_pedido(s, p["id"])["situacao"] == "ABERTO"


def test_o_pdf_da_recusa_traz_cpf_e_assinatura_das_testemunhas():
    """25/11/2026: *"faz as testemunhas assinarem na tela com CPF."*"""
    import base64
    from datetime import datetime, timezone
    import fitz
    from app.apps.erp.core import termos
    from tests.test_ponto_assinatura_folha_banco import _png   # type: ignore
    png = base64.b64decode(_png().split(",", 1)[1])
    pdf = termos.documento_pdf(
        {"numero": "TRM-1", "titulo": "Aviso prévio", "texto": "texto do aviso",
         "pessoa": "Fulano", "cpf": "52998224725", "tipo": "AVISO-PREVIO",
         "lacre": "abc123"},
        assinatura=None, quando=datetime.now(timezone.utc), forma="RECUSA",
        conduzido_por="Adm", motivo="não quis assinar",
        testemunhas=[{"nome": "Ana Souza", "cpf": "71460238001", "png": png},
                     {"nome": "Rui Lima", "cpf": "28625587887", "png": png}])
    doc = fitz.open(stream=pdf, filetype="pdf")
    texto = "".join(p.get_text() for p in doc)
    assert "714.602.380-01" in texto and "286.255.878-87" in texto
    assert sum(len(p.get_image_info()) for p in doc) >= 2, "as duas assinaturas"
