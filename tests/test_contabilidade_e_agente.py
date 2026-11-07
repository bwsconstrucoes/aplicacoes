# -*- coding: utf-8 -*-
"""OS PEDIDOS À CONTABILIDADE E O AGENTE DE DP (migração 090).

Metodologia inteira em `app/apps/erp/AGENTE_DP.md`. O que estes testes
defendem, por ordem de dano:

  1. **o agente nasce DESLIGADO** — pedido dele com todas as letras;
  2. **pedido não sai pela metade**: falta dado no cadastro, não envia;
  3. **retorno que não bate NÃO conclui** — carimbar como certo um documento
     errado é o problema que ninguém pega hoje;
  4. **a cobrança respeita o teto e o fim de semana**;
  5. **tudo o que o agente faz fica marcado como dele.**
"""
from __future__ import annotations

import pytest

from datetime import date, timedelta

from app.apps.erp.core import agente_dp as ag
from app.apps.erp.core import contabilidade as ct
from app.apps.erp.core.comum.auditoria import ErroValidacao

banco_ = pytest.mark.banco

CPF_A = "52998224725"
CPF_B = "11144477735"
PIS_A = "123.45678.90-0"


# ===========================================================================
# SEM BANCO
# ===========================================================================
def test_todo_template_diz_o_que_espera_de_volta_e_em_quantos_dias():
    """Sem as duas coisas o pedido não tem como ser cobrado nem conferido:
    "esperar" exige saber o quê e até quando."""
    for chave, t in ct.TEMPLATES.items():
        assert t["espera"], f"{chave} não diz que documento espera"
        assert t["prazo"] >= 1, f"{chave} sem prazo"
        assert t["cobrancas"] >= 1, f"{chave} sem teto de cobrança"
        assert t["conferir"], f"{chave} não diz o que conferir no retorno"
        assert len(t["explicacao"]) > 10, f"{chave} sem explicação em português"


def test_a_documentacao_rescisoria_nao_e_template_separado():
    """Dele: *"tem solicitação de documentação rescisória, mas isso aqui fica
    dentro do processo de demissão."* Ela é o RETORNO do desligamento."""
    assert "DOCUMENTACAO_RESCISORIA" not in ct.TEMPLATES
    assert ct.TEMPLATES["DESLIGAMENTO"]["espera"] == "RESCISAO"


def test_o_prazo_e_em_dias_uteis():
    """Contabilidade não responde no sábado, e prazo que vence no fim de semana
    gera cobrança na segunda por um atraso que não existiu."""
    from datetime import date
    # Sexta 06/11/2026 + 2 úteis = terça 10/11
    assert ct.dias_uteis_depois(date(2026, 11, 6), 2) == date(2026, 11, 10)
    # Quinta + 1 útil = sexta
    assert ct.dias_uteis_depois(date(2026, 11, 5), 1) == date(2026, 11, 6)


def test_toda_rotina_do_agente_diz_o_que_nao_analisa():
    """O limite escrito é o que impede a próxima pessoa ampliar o agente para
    dentro de decisão que é de gente."""
    for chave, r in ag.ROTINAS.items():
        assert len(r["analisa"]) > 20, f"{chave} não diz o que analisa"
        assert len(r["nao_analisa"]) > 20, f"{chave} não diz o que NÃO analisa"
        assert "usa_ia" in r


def test_seis_das_oito_rotinas_nao_usam_ia():
    """É o desenho: o agente começa útil e burro. Se a parte de IA nunca ficar
    boa, o pedido esquecido — o custo que mais dói — já está resolvido por
    conta de calendário.

    A LEITURA DA CAIXA entrou em 05/11/2026 e **não usa IA**: casar resposta com
    pedido é cabeçalho de e-mail e expressão regular, e baixar anexo é
    biblioteca padrão. Quem precisa de IA é só entender o CONTEÚDO do
    documento (a rotina de conferência) e ler documento de identidade."""
    sem_ia = [k for k, v in ag.ROTINAS.items() if not v["usa_ia"]]
    assert set(sem_ia) == {"triagem", "despacho", "cobranca", "caixa",
                           "vigia", "relatorio"}
    assert {k for k, v in ag.ROTINAS.items() if v["usa_ia"]} == {
        "leitura", "conferencia"}


# ===========================================================================
# COM BANCO
# ===========================================================================
@pytest.fixture
def dono(sessao_real):
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario
    u = Usuario(nome="DP da contabilidade", email="dp@ct.teste", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"),
                perfil=PerfilUsuario.DEPARTAMENTO_PESSOAL, telefone="5585900000003")
    sessao_real.add(u)
    sessao_real.flush()
    return u


@pytest.fixture
def pessoa(sessao_real, dono):
    """Alguém com o cadastro COMPLETO o bastante para a admissão poder ser
    pedida — é o que o template exige."""
    from app.apps.erp.core.cadastros import cargos as svc_cargos
    from app.apps.erp.core.cadastros import pessoas as svc
    from app.apps.erp.db.models.cadastros import Empresa, Obra
    obra = Obra(codigo="CT-01", nome="Obra da contabilidade", status="ATIVA")
    emp = Empresa(razao_social="BWS CONSTRUCOES LTDA", cnpj="00079526000109",
                  nome_fantasia="BWS")
    sessao_real.add_all([obra, emp])
    cargo = svc_cargos.achar_ou_criar_cargo(sessao_real, "PEDREIRO", cbo="7152-10")
    sessao_real.flush()
    return svc.salvar(sessao_real, {
        "cpf": CPF_A, "nome": "Pessoa Para Admitir", "data_nascimento": "1990-03-15",
        "pis": PIS_A, "cargo_id": cargo.id, "obra_id": obra.id,
        "empresa_id": emp.id, "salario": "2.100,00",
        # ADMISSÃO RELATIVA A HOJE, NUNCA DATA FIXA.
        #
        # Isto quebrou de verdade: a data era "2026-11-01" e o teste da ordem
        # dos eventos passava todo dia até 06/11. No dia 07 a admissão
        # completou 6 dias, o aviso "a admissão foi há N dias e o registro só
        # está sendo pedido agora" passou a disparar, e um evento AVISO entrou
        # no meio da trilha. Nada no código tinha mudado — só o calendário.
        #
        # Data fixa em teste é bomba-relógio: ela não falha quando o código
        # erra, falha quando o tempo passa, e o estrago aparece justamente no
        # dia de publicar.
        "admissao": date.today().isoformat(),
        "vinculo": "CTPS", "tipo_contrato": "CLT_INDETERMINADO",
        "etapa": "CONCLUIDO"}, dono)


@banco_
def test_o_agente_nasce_desligado(sessao_real, banco):
    """Pedido dele com todas as letras: *"a gente não vai começar com ele
    ativo."* A migração semeia as sete rotinas DESLIGADAS."""
    for r in ag.configuracao(sessao_real):
        assert r["modo"] == "DESLIGADA", f"a rotina {r['chave']} nasceu ligada"


@banco_
def test_rodar_com_tudo_desligado_nao_faz_nada_e_diz_isso(sessao_real, dono):
    r = ag.rodar(sessao_real, dono)
    assert r["rotinas_ligadas"] == []
    assert "não fez" in r["aviso"]


@banco_
def test_ligar_uma_rotina_guarda_o_modo_e_os_parametros(sessao_real, dono):
    ag.definir(sessao_real, "cobranca", modo_novo="SUGERE",
               params={"nao_cobrar_fim_de_semana": False}, usuario=dono)
    assert ag.modo(sessao_real, "cobranca") == "SUGERE"
    assert ag.parametros(sessao_real, "cobranca")["nao_cobrar_fim_de_semana"] is False
    # Os parâmetros que ninguém mexeu continuam valendo pelo padrão.
    assert "dias_parado" in ag.parametros(sessao_real, "vigia")


@banco_
def test_modo_invalido_e_recusado(sessao_real, dono):
    with pytest.raises(ErroValidacao, match="DESLIGADA"):
        ag.definir(sessao_real, "cobranca", modo_novo="MAIS_OU_MENOS", usuario=dono)


@banco_
def test_pedido_nao_sai_pela_metade(sessao_real, dono):
    """E-mail incompleto custa dois dias de ida e volta com a contabilidade. A
    recusa DIZ o que falta, com o nome do campo em português."""
    from app.apps.erp.core.cadastros import pessoas as svc
    c = svc.salvar(sessao_real, {"cpf": CPF_B, "nome": "Cadastro Pela Metade"}, dono)
    with pytest.raises(ErroValidacao) as e:
        ct.abrir(sessao_real, {"template": "ASSINATURA_CTPS",
                               "colaborador_id": c.id}, dono)
    assert "nº do PIS" in str(e.value) and "cargo" in str(e.value)


@banco_
def test_o_pedido_nasce_em_rascunho_com_o_texto_montado(sessao_real, dono, pessoa):
    """Separar abrir de enviar é o que permite o DP ver o texto e corrigir
    ANTES de a contabilidade ler."""
    r = ct.abrir(sessao_real, {"template": "ASSINATURA_CTPS",
                               "colaborador_id": pessoa.id}, dono)
    assert r["numero"].startswith("CT-")
    p = ct.ler(sessao_real, r["id"])
    assert p["situacao"] == "RASCUNHO"
    assert p["documento_esperado"] == "FICHA-REGISTRO"
    assert "Pessoa Para Admitir" in p["corpo"]
    assert "PEDREIRO" in p["corpo"]
    assert "2.100,00" in p["corpo"]
    assert p["prazo"] is None, "o prazo só começa a contar quando o e-mail sai"


@banco_
def test_o_que_falta_para_pedir_e_consultavel_antes_de_tentar(sessao_real, dono,
                                                              pessoa):
    """A tela precisa avisar ANTES de alguém apertar enviar."""
    from app.apps.erp.db.models.cadastros import Colaborador
    assert ct.o_que_falta_para_pedir(sessao_real, "ASSINATURA_CTPS", pessoa) == []
    outro = sessao_real.get(Colaborador, pessoa.id)
    outro.pis = None
    sessao_real.flush()
    assert "nº do PIS" in ct.o_que_falta_para_pedir(
        sessao_real, "ASSINATURA_CTPS", outro)


@banco_
def test_o_retorno_que_bate_deixa_concluir(sessao_real, dono, pessoa):
    r = ct.abrir(sessao_real, {"template": "ASSINATURA_CTPS",
                               "colaborador_id": pessoa.id}, dono)
    sessao_real.execute(
        __import__("sqlalchemy").text(
            "UPDATE dp_pedidos SET situacao='ENVIADO' WHERE id=:i"),
        {"i": r["id"]})
    saida = ct.receber(sessao_real, r["id"], dono, lido={
        "nome": "Pessoa Para Admitir", "cpf": CPF_A, "cargo": "PEDREIRO",
        "salario": "2100,00",
        "admissao": date.today().strftime("%d/%m/%Y")})
    assert saida["situacao"] == "RESPONDIDO" and saida["divergencias"] == []
    fim = ct.concluir(sessao_real, r["id"], dono)
    assert fim["concluido"] is True


@banco_
def test_o_retorno_que_nao_bate_abre_divergencia_e_nao_conclui(sessao_real, dono,
                                                               pessoa):
    """É o problema que ninguém pega hoje: a ficha de registro vem com salário
    diferente do combinado, e isso só aparece meses depois."""
    r = ct.abrir(sessao_real, {"template": "ASSINATURA_CTPS",
                               "colaborador_id": pessoa.id}, dono)
    saida = ct.receber(sessao_real, r["id"], dono, lido={
        "nome": "Pessoa Para Admitir", "cpf": CPF_A, "cargo": "SERVENTE",
        "salario": "1800,00",
        "admissao": date.today().strftime("%d/%m/%Y")})
    assert saida["situacao"] == "DIVERGENTE"
    campos = {d["campo"] for d in saida["divergencias"]}
    assert campos == {"cargo", "salario"}
    with pytest.raises(ErroValidacao, match="divergência"):
        ct.concluir(sessao_real, r["id"], dono)


@banco_
def test_a_divergencia_aceita_exige_motivo_escrito(sessao_real, dono, pessoa):
    r = ct.abrir(sessao_real, {"template": "ASSINATURA_CTPS",
                               "colaborador_id": pessoa.id}, dono)
    ct.receber(sessao_real, r["id"], dono, lido={"salario": "1800,00"})
    with pytest.raises(ErroValidacao, match="por que"):
        ct.aceitar_divergencia(sessao_real, r["id"], dono, motivo="ok")
    ct.aceitar_divergencia(sessao_real, r["id"], dono,
                           motivo="o salário do contrato é o da ficha; o cadastro estava errado")
    assert ct.ler(sessao_real, r["id"])["situacao"] == "RESPONDIDO"


@banco_
def test_campo_que_o_documento_nao_trouxe_nao_e_divergencia(sessao_real, dono,
                                                            pessoa):
    """Ausência não é diferença — acusar ausência faria toda conferência
    falhar."""
    r = ct.abrir(sessao_real, {"template": "ASSINATURA_CTPS",
                               "colaborador_id": pessoa.id}, dono)
    saida = ct.receber(sessao_real, r["id"], dono, lido={"cpf": CPF_A})
    assert saida["situacao"] == "RESPONDIDO"


@banco_
def test_retorno_sem_nada_lido_fica_sem_conferencia_e_a_tela_sabe(sessao_real,
                                                                  dono, pessoa):
    """Nunca fingir que conferiu."""
    r = ct.abrir(sessao_real, {"template": "ASSINATURA_CTPS",
                               "colaborador_id": pessoa.id}, dono)
    saida = ct.receber(sessao_real, r["id"], dono, lido={})
    assert saida["conferido"] is False


@banco_
def test_concluir_o_desligamento_move_a_etapa(sessao_real, dono, pessoa):
    """O template manda, e é o retorno da contabilidade que fecha o ciclo."""
    from app.apps.erp.core.cadastros import pessoas as svc
    svc.desligar(sessao_real, pessoa.id, {"data_saida": "2026-11-30",
                                          "ultimo_dia": "2026-11-30",
                                          "forma_desligamento": "AVISO_TRABALHADO"}, dono)
    r = ct.abrir(sessao_real, {"template": "DESLIGAMENTO",
                               "colaborador_id": pessoa.id,
                               "dados": {"forma_desligamento": "AVISO_TRABALHADO"}},
                 dono)
    ct.receber(sessao_real, r["id"], dono, lido={"cpf": CPF_A})
    fim = ct.concluir(sessao_real, r["id"], dono)
    assert pessoa.etapa == "DESLIGADO"
    assert fim["concluido"] is True


@banco_
def test_a_trilha_do_pedido_fica_em_ordem_estavel(sessao_real, dono, pessoa):
    """Em Postgres `now()` é o início da transação: sem o `id` no desempate, o
    histórico mostra "concluído" antes de "enviado"."""
    r = ct.abrir(sessao_real, {"template": "ASSINATURA_CTPS",
                               "colaborador_id": pessoa.id}, dono)
    ct.receber(sessao_real, r["id"], dono, lido={"cpf": CPF_A})
    ct.concluir(sessao_real, r["id"], dono)
    tipos = [e["tipo"] for e in ct.ler(sessao_real, r["id"])["eventos"]]
    assert tipos == ["ABERTO", "RESPONDIDO", "CONCLUIDO"]


@banco_
def test_cancelar_exige_motivo(sessao_real, dono, pessoa):
    r = ct.abrir(sessao_real, {"template": "ASSINATURA_CTPS",
                               "colaborador_id": pessoa.id}, dono)
    with pytest.raises(ErroValidacao, match="motivo"):
        ct.cancelar(sessao_real, r["id"], dono, motivo="")
    ct.cancelar(sessao_real, r["id"], dono, motivo="a pessoa desistiu de entrar")
    assert ct.ler(sessao_real, r["id"])["situacao"] == "CANCELADO"


# ---------------------------------------------------------------------------
# A COBRANÇA
# ---------------------------------------------------------------------------
@banco_
def test_a_cobranca_desligada_nao_faz_nada(sessao_real, dono):
    r = ag.rodar_cobranca(sessao_real, dono)
    assert r == {"modo": "DESLIGADA", "olhou": 0, "cobrou": 0, "sugeriu": 0}


@banco_
def test_a_cobranca_nao_roda_no_fim_de_semana(sessao_real, dono):
    from datetime import date
    ag.definir(sessao_real, "cobranca", modo_novo="SUGERE", usuario=dono)
    r = ag.rodar_cobranca(sessao_real, dono, hoje=date(2026, 11, 7))   # sábado
    assert r.get("pulou") == "fim de semana"


@banco_
def test_no_modo_sugere_a_cobranca_escreve_o_que_faria(sessao_real, dono, pessoa):
    """O modo do meio: dá para ligar sem medo e ver se ele acerta."""
    from datetime import date

    from sqlalchemy import text
    r = ct.abrir(sessao_real, {"template": "ASSINATURA_CTPS",
                               "colaborador_id": pessoa.id}, dono)
    sessao_real.execute(text("""UPDATE dp_pedidos
                                   SET situacao='ENVIADO', prazo=:p
                                 WHERE id=:i"""),
                        {"p": date(2026, 1, 10), "i": r["id"]})
    ag.definir(sessao_real, "cobranca", modo_novo="SUGERE", usuario=dono)
    saida = ag.rodar_cobranca(sessao_real, dono, hoje=date(2026, 11, 5))
    assert saida["sugeriu"] == 1
    g = ag.sugestoes(sessao_real, rotina="cobranca")
    assert g and g[0]["acao"] == "cobrar_pedido"
    assert "venceu" in g[0]["explicacao"]


@banco_
def test_a_mesma_sugestao_nao_se_repete(sessao_real, dono, pessoa):
    """Rotina que roda todo dia geraria a mesma linha todo dia, e a tela viraria
    um monte de ruído igual."""
    primeira = ag.sugerir(sessao_real, rotina="vigia", alvo_tipo="colaborador",
                          alvo_id=pessoa.id, acao="falta_dado",
                          explicacao="falta o PIS")
    segunda = ag.sugerir(sessao_real, rotina="vigia", alvo_tipo="colaborador",
                         alvo_id=pessoa.id, acao="falta_dado",
                         explicacao="falta o PIS")
    assert primeira is not None and segunda is None


@banco_
def test_sugestao_recusada_guarda_o_motivo(sessao_real, dono, pessoa):
    gid = ag.sugerir(sessao_real, rotina="vigia", alvo_tipo="colaborador",
                     alvo_id=pessoa.id, acao="ativo_sem_obra",
                     explicacao="ativo e sem obra")
    ag.recusar_sugestao(sessao_real, gid, dono, motivo="é do escritório")
    assert ag.sugestoes(sessao_real, situacao="ABERTA") == []
    recusadas = ag.sugestoes(sessao_real, situacao="RECUSADA")
    assert recusadas[0]["motivo"] == "é do escritório"


@banco_
def test_sugestao_de_acao_desconhecida_nao_executa_nada(sessao_real, dono, pessoa):
    """A lista de ações aplicáveis é FECHADA: aprovar não pode virar "executa
    qualquer coisa que estava escrita no banco"."""
    gid = ag.sugerir(sessao_real, rotina="vigia", alvo_tipo="colaborador",
                     alvo_id=pessoa.id, acao="ativo_sem_obra",
                     explicacao="ativo e sem obra")
    with pytest.raises(ErroValidacao, match="só um aviso"):
        ag.aplicar_sugestao(sessao_real, gid, dono)


# ---------------------------------------------------------------------------
# A TRIAGEM
# ---------------------------------------------------------------------------
@banco_
def test_a_triagem_aponta_desligamento_de_quem_ja_saiu(sessao_real, dono, pessoa):
    """O DP ia descobrir isso só ao abrir o pedido — e aí volta para a obra por
    WhatsApp, e o pedido anda um dia por pergunta."""
    from app.apps.erp.core import pessoal_solicitacoes as sp
    from app.apps.erp.core.cadastros import pessoas as svc
    svc.desligar(sessao_real, pessoa.id, {"data_saida": "2026-11-30",
                                          "forma_desligamento": "AVISO_TRABALHADO"},
                 dono)
    p = sp.abrir(sessao_real, {"tipo": "DESLIGAMENTO", "colaborador_id": pessoa.id,
                               "data_saida": "2026-12-15"}, dono)
    r = ag.triagem(sessao_real, p["id"])
    assert any("já está desligado" in x for x in r["incoerencias"])


@banco_
def test_a_triagem_aponta_salario_abaixo_do_piso(sessao_real, dono, pessoa):
    """Ninguém sabe de cabeça o piso de 62 cargos em 13 convenções, e pagar
    abaixo do piso é passivo."""
    from datetime import date

    from app.apps.erp.core import pessoal_solicitacoes as sp
    from app.apps.erp.core.cadastros import cargos as svc_cargos
    conv = svc_cargos.achar_ou_criar_convencao(sessao_real, "CE-Fortaleza")
    pessoa.convencao_id = conv.id
    sessao_real.flush()
    cc = svc_cargos.cruzamento(sessao_real, pessoa.cargo_id, conv.id, criar=True)
    svc_cargos.definir_piso(sessao_real, cc.id, salario="2.000,00",
                            vigencia=date(2026, 1, 1), usuario=dono)
    p = sp.abrir(sessao_real, {"tipo": "ALTERACAO_CARGO",
                               "colaborador_id": pessoa.id,
                               "cargo_id": pessoa.cargo_id,
                               "salario": "1.500,00"}, dono)
    r = ag.triagem(sessao_real, p["id"])
    assert any("ABAIXO do piso" in x for x in r["incoerencias"])


@banco_
def test_a_triagem_nao_julga_merito(sessao_real, dono, pessoa):
    """Se vale a pena contratar, se o salário é justo — decisão de gente."""
    from app.apps.erp.core import pessoal_solicitacoes as sp
    p = sp.abrir(sessao_real, {"tipo": "ALTERACAO_CARGO",
                               "colaborador_id": pessoa.id,
                               "cargo_id": pessoa.cargo_id,
                               "salario": "90.000,00"}, dono)
    r = ag.triagem(sessao_real, p["id"])
    assert r["incoerencias"] == [], "salário alto não é incoerência, é decisão"


# ---------------------------------------------------------------------------
# O VIGIA
# ---------------------------------------------------------------------------
@banco_
def test_o_vigia_acha_admissao_de_ctps_sem_ficha_de_registro(sessao_real, dono,
                                                             pessoa):
    """O cadastro está concluído aqui dentro e a pessoa não existe no eSocial.
    É o pior dos silêncios."""
    achados = ag.vigia(sessao_real)
    tipos = {(a["tipo"], a["colaborador_id"]) for a in achados}
    assert ("admissao_sem_registro", pessoa.id) in tipos


@banco_
def test_o_vigia_para_de_apontar_quando_o_pedido_existe(sessao_real, dono, pessoa):
    """Apontar o que já tem pedido aberto seria cobrar duas vezes a mesma
    coisa, por dois caminhos diferentes."""
    ct.abrir(sessao_real, {"template": "ASSINATURA_CTPS",
                           "colaborador_id": pessoa.id}, dono)
    achados = ag.vigia(sessao_real)
    assert ("admissao_sem_registro", pessoa.id) not in {
        (a["tipo"], a["colaborador_id"]) for a in achados}


@banco_
def test_o_vigia_acha_ativo_sem_obra(sessao_real, dono):
    from app.apps.erp.core.cadastros import pessoas as svc
    c = svc.salvar(sessao_real, {"cpf": CPF_B, "nome": "Ativo Sem Obra"}, dono)
    achados = ag.vigia(sessao_real)
    assert ("ativo_sem_obra", c.id) in {(a["tipo"], a["colaborador_id"])
                                        for a in achados}


@banco_
def test_o_relatorio_diz_o_que_fazer_e_nao_so_numeros(sessao_real, dono, pessoa):
    r = ag.relatorio(sessao_real)
    assert r["linhas"], "relatório sem nenhuma linha não serve"
    assert all(isinstance(x, str) and len(x) > 10 for x in r["linhas"])


@banco_
def test_o_aviso_nao_trava_o_pedido_de_ferias(sessao_real, dono, pessoa):
    """Falta trava; aviso só pede um olhar. Férias antes de doze meses existe
    (coletivas, proporcional), e travar impediria o caso legítimo."""
    r = ct.abrir(sessao_real, {"template": "FERIAS",
                               "colaborador_id": pessoa.id,
                               "dados": {"inicio": "2026-12-01", "dias": "30"}},
                 dono)
    assert r["numero"].startswith("CT-")
    assert any("período aquisitivo" in a for a in r["avisos"])
    eventos = [e["tipo"] for e in ct.ler(sessao_real, r["id"])["eventos"]]
    assert "AVISO" in eventos, "o aviso fica na trilha, não só na tela"


# ---------------------------------------------------------------------------
# O E-MAIL DA CONTABILIDADE, CADASTRADO PELA TELA
#
# Pedido do dono em 06/11/2026: *"o e-mail da contabilidade eu acho que tem
# que ser colocado no aplicativo (…) deixa o campo, cria um local para eu
# poder adicionar essa informação."* Está certo: endereço que muda é dado de
# tela, não de variável de ambiente — trocá-lo não pode exigir publicação.
# ---------------------------------------------------------------------------
@pytest.mark.banco
def test_o_email_da_contabilidade_e_gravado_e_lido(sessao_real, dono, banco):
    from app.apps.erp.core import contabilidade as svc

    assert svc.endereco_padrao(sessao_real) == []
    r = svc.definir_enderecos(
        sessao_real, "contabilidade@escritorio.com.br", dono)
    assert r == ["contabilidade@escritorio.com.br"]
    assert svc.endereco_padrao(sessao_real) == ["contabilidade@escritorio.com.br"]


@pytest.mark.banco
def test_aceita_mais_de_um_endereco_porque_ferias_acontecem(sessao_real, dono,
                                                            banco):
    """Mandar só para uma pessoa faz o pedido parar nas férias dela."""
    from app.apps.erp.core import contabilidade as svc

    r = svc.definir_enderecos(
        sessao_real, "um@escritorio.com.br; dois@escritorio.com.br", dono)
    assert r == ["um@escritorio.com.br", "dois@escritorio.com.br"]


@pytest.mark.banco
def test_endereco_torto_e_recusado_antes_de_gravar(sessao_real, dono, banco):
    """Endereço errado só apareceria semanas depois, num pedido que a
    contabilidade nunca recebeu e que o ERP ficou cobrando de ninguém."""
    from app.apps.erp.core import contabilidade as svc
    from app.apps.erp.core.comum.auditoria import ErroValidacao

    svc.definir_enderecos(sessao_real, "bom@escritorio.com.br", dono)
    for torto in ("contabilidade@escritorio", "fulano@gmail,com",
                  "sem arroba", "tem espaco@escritorio.com.br"):
        with pytest.raises(ErroValidacao):
            svc.definir_enderecos(sessao_real, torto, dono)
    # e o que estava gravado antes continua lá: a recusa não apaga nada
    assert svc.endereco_padrao(sessao_real) == ["bom@escritorio.com.br"]


@pytest.mark.banco
def test_o_mesmo_endereco_duas_vezes_entra_uma_vez_so(sessao_real, dono, banco):
    from app.apps.erp.core import contabilidade as svc

    r = svc.definir_enderecos(
        sessao_real, "a@x.com.br, A@X.com.br, b@x.com.br", dono)
    assert r == ["a@x.com.br", "b@x.com.br"]


@pytest.mark.banco
def test_trocar_o_endereco_fica_registrado(sessao_real, dono, banco):
    """Trocar isto desvia TODO pedido novo — desvio silencioso é descoberto
    tarde demais."""
    from sqlalchemy import text as _t
    from app.apps.erp.core import contabilidade as svc

    svc.definir_enderecos(sessao_real, "antes@x.com.br", dono)
    svc.definir_enderecos(sessao_real, "depois@x.com.br", dono)
    n = sessao_real.execute(_t(
        "SELECT count(*) FROM eventos"
        " WHERE entidade_tipo = 'parametro'"
        "   AND detalhe->>'chave' = :c"), {"c": svc.CHAVE_EMAILS}).scalar()
    assert n >= 2


@pytest.mark.banco
def test_sem_endereco_o_pedido_nao_sai_e_a_mensagem_diz_onde_preencher(
        sessao_real, dono, pessoa, banco):
    """A recusa tem de dizer ONDE resolver, não só que falta."""
    from app.apps.erp.core import contabilidade as svc
    from app.apps.erp.core.comum.auditoria import ErroValidacao

    r = svc.abrir(sessao_real, {"colaborador_id": pessoa.id,
                                "template": "ASSINATURA_CTPS"}, dono)
    with pytest.raises(ErroValidacao) as e:
        svc.enviar(sessao_real, r["id"], dono)
    assert "Contabilidade" in str(e.value)


# ---------------------------------------------------------------------------
# O AVISO QUE NINGUÉM TESTAVA — e foi ele que quebrou a publicação
#
# `avisos_do_pedido` compara a admissão com HOJE. Nenhum teste cobria isso, e
# por isso ninguém sabia que ele podia entrar na trilha de eventos de um
# pedido. Em 07/11/2026 a admissão fixa do fixture completou 6 dias, o aviso
# disparou, e o teste da ordem dos eventos quebrou na hora de publicar — sem
# nada no código ter mudado.
#
# Agora as datas vão EXPLÍCITAS, calculadas a partir de hoje. O teste passa a
# pinar o comportamento em vez de depender do calendário.
# ---------------------------------------------------------------------------
class _Pessoa:
    """O mínimo que `avisos_do_pedido` lê. Sem banco: a função só olha
    `nome` e `admissao` no caminho da CTPS."""

    def __init__(self, admissao):
        self.id = 1
        self.nome = "Fulano de Tal"
        self.admissao = admissao


@pytest.mark.parametrize("dias, avisa", [
    (0, False),    # admitido hoje
    (5, False),    # no limite, ainda não avisa
    (6, True),     # passou — é aqui que o comportamento vira
    (40, True),
])
def test_o_aviso_de_registro_atrasado_vira_no_sexto_dia(dias, avisa):
    c = _Pessoa(date.today() - timedelta(days=dias))
    avisos = ct.avisos_do_pedido(None, "ASSINATURA_CTPS", c)
    assert bool(avisos) is avisa
    if avisa:
        assert "eSocial" in avisos[0]


def test_sem_admissao_nao_ha_aviso_de_atraso():
    """Cadastro sem data de admissão não pode virar aviso de atraso."""
    assert ct.avisos_do_pedido(None, "ASSINATURA_CTPS", _Pessoa(None)) == []


def test_o_aviso_de_atraso_e_so_da_assinatura_de_ctps():
    c = _Pessoa(date.today() - timedelta(days=90))
    for outro in ("DEPENDENTE", "CONJUGE", "ALTERACAO_CARGO"):
        assert ct.avisos_do_pedido(None, outro, c) == []
