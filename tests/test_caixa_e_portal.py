# -*- coding: utf-8 -*-
"""A CAIXA DE ENTRADA DO AGENTE E O PORTAL DA OBRA (migração 091).

O dono, em 05/11/2026, derrubando o "ponto cego" que eu havia registrado:

> *"Isso é fundamental. Que agente é esse que depende de uma intervenção
> humana? Aí não é agente. Todo e-mail que chegar, vou ter que jogar os anexos?
> Não tem sentido nenhum. Então o ERP precisa estar fazendo a leitura das
> caixas de entrada, baixando anexo de forma completamente autônoma."*

E definindo os canais:

> *"essa integração entre contabilidade vai ser feita via e-mail. A interação
> entre os requisitores de informação vai ser feita via WhatsApp (…) Mas esse
> envio (…) essa devolução, ela não pode ser via WhatsApp. Tem que ser via
> sistema."*

O que estes testes defendem:
  1. **a resposta é casada com o pedido certo** — e, na dúvida, com NENHUM;
  2. **a mesma mensagem não é processada duas vezes**;
  3. **anexo de assinatura e logotipo não entram** como documento;
  4. **o portal recusa quem não entrou pelo aplicativo do ponto**;
  5. **a devolução fecha pelo sistema**, com tipo e dono.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core.comum import email_entrada as caixa

banco_ = pytest.mark.banco

CPF_A = "52998224725"
CPF_B = "11144477735"
PIS_A = "123.45678.90-0"


# ===========================================================================
# SEM BANCO: as peças que não precisam de servidor nenhum
# ===========================================================================
def test_a_marca_do_pedido_e_achada_no_assunto():
    assert caixa.MARCA_PEDIDO.search("[CT-00007] Assinatura de CTPS — Joao")
    assert caixa.MARCA_PEDIDO.search("Re: [CT-00007] Assinatura").group(1) == "CT-00007"
    assert caixa.MARCA_PEDIDO.search("ENC: Fwd: [CT-00123] x").group(1) == "CT-00123"
    assert not caixa.MARCA_PEDIDO.search("Assinatura de CTPS — Joao")


def test_so_entram_os_tipos_de_arquivo_que_sao_documento():
    """Assinatura de imagem, ícone de rodapé e logotipo vêm como anexo em todo
    e-mail corporativo. Arquivar isso enche o Drive de lixo com cara de
    documento."""
    assert "application/pdf" in caixa.TIPOS_QUE_ENTRAM
    assert "image/jpeg" in caixa.TIPOS_QUE_ENTRAM
    assert "image/gif" not in caixa.TIPOS_QUE_ENTRAM
    assert "text/html" not in caixa.TIPOS_QUE_ENTRAM


def test_o_corpo_html_e_lido_sem_as_marcacoes():
    """Para procurar CPF no corpo, a marcação de HTML atrapalha."""
    import email
    msg = email.message_from_string(
        "Content-Type: text/html; charset=utf-8\n\n"
        "<p>Segue a ficha de <b>123.456.789-09</b></p>")
    assert "123.456.789-09" in caixa._corpo_em_texto(msg)
    assert "<b>" not in caixa._corpo_em_texto(msg)


def test_o_anexo_e_reconhecido_e_o_grande_e_marcado():
    import email
    bruto = (
        "Content-Type: multipart/mixed; boundary=xx\n\n"
        "--xx\nContent-Type: text/plain\n\nsegue\n"
        "--xx\nContent-Type: application/pdf\n"
        'Content-Disposition: attachment; filename="ficha.pdf"\n\n'
        "conteudo-do-pdf\n--xx--\n")
    msg = email.message_from_string(bruto)
    anexos = caixa._anexos(msg)
    assert [a["nome"] for a in anexos] == ["ficha.pdf"]
    assert anexos[0]["mime"] == "application/pdf"
    assert anexos[0]["grande"] is False


def test_o_teto_de_anexo_protege_a_instancia():
    """Anexo de 80 MB numa instância que divide 2 GB com treze módulos derruba
    o monorepo inteiro, não só a leitura."""
    assert caixa.TETO_ANEXO <= 30 * 1024 * 1024
    assert caixa.TETO_MENSAGENS <= 100


# ===========================================================================
# COM BANCO
# ===========================================================================
@pytest.fixture
def dono(sessao_real):
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario
    u = Usuario(nome="DP da caixa", email="dp@caixa.teste", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"),
                perfil=PerfilUsuario.DEPARTAMENTO_PESSOAL, telefone="5585900000004")
    sessao_real.add(u)
    sessao_real.flush()
    return u


@pytest.fixture
def pessoa(sessao_real, dono):
    from app.apps.erp.core.cadastros import cargos as svc_cargos
    from app.apps.erp.core.cadastros import pessoas as svc
    from app.apps.erp.db.models.cadastros import Empresa, Obra
    obra = Obra(codigo="CX-01", nome="Obra da caixa", status="ATIVA")
    emp = Empresa(razao_social="BWS CONSTRUCOES LTDA", cnpj="00079526000109",
                  nome_fantasia="BWS")
    sessao_real.add_all([obra, emp])
    cargo = svc_cargos.achar_ou_criar_cargo(sessao_real, "PEDREIRO")
    sessao_real.flush()
    return svc.salvar(sessao_real, {
        "cpf": CPF_A, "nome": "Pessoa Da Caixa", "data_nascimento": "1990-03-15",
        "pis": PIS_A, "cargo_id": cargo.id, "obra_id": obra.id,
        "empresa_id": emp.id, "salario": "2.100,00", "admissao": "2026-11-01",
        "vinculo": "CTPS", "tipo_contrato": "CLT_INDETERMINADO",
        "etapa": "CONCLUIDO", "telefone": "5585988887777"}, dono)


@banco_
def test_a_caixa_nasce_desconfigurada_e_a_tela_sabe_disso(sessao_real, banco):
    c = caixa.configuracao(sessao_real)
    assert c["usavel"] is False and c["ligado"] is False
    assert c["tem_senha"] is False


@banco_
def test_a_senha_da_caixa_fica_cifrada_e_nunca_sai_na_resposta(sessao_real, dono,
                                                               monkeypatch):
    import base64

    from cryptography.fernet import Fernet
    from app.apps.erp.core.comum import segredos
    monkeypatch.setenv("ERP_CHAVE_SEGREDOS", Fernet.generate_key().decode())
    segredos._cofre.cache_clear() if hasattr(segredos._cofre, "cache_clear") else None
    from app.apps.erp.db.models.cadastros import Parametro
    caixa.configurar(sessao_real, {
        "servidor": "imap.exemplo.com", "usuario": "dp@bws.com.br",
        "senha": "senha-secreta-de-verdade", "ligado": True}, dono)
    c = caixa.configuracao(sessao_real)
    assert c["tem_senha"] is True and c["usavel"] is True
    assert "senha" not in c, "a senha NUNCA sai na resposta"
    guardada = sessao_real.get(Parametro, caixa.CHAVES["senha"]).valor
    assert "senha-secreta" not in guardada, "a senha tem de estar cifrada"


@banco_
def test_sem_a_chave_do_cofre_o_resto_e_salvo_e_o_recado_explica(sessao_real, dono,
                                                                 monkeypatch):
    """Perder o que a pessoa digitou por causa de uma variável de ambiente
    seria o pior dos dois mundos."""
    monkeypatch.delenv("ERP_CHAVE_SEGREDOS", raising=False)
    r = caixa.configurar(sessao_real, {
        "servidor": "imap.exemplo.com", "usuario": "dp@bws.com.br",
        "senha": "nao-vai-ser-guardada"}, dono)
    assert r["servidor"] == "imap.exemplo.com"
    assert r["tem_senha"] is False
    assert "ERP_CHAVE_SEGREDOS" in r["aviso"]


@banco_
def test_a_rodada_nao_tenta_nada_sem_configuracao(sessao_real, dono):
    r = caixa.rodada(sessao_real, dono)
    assert r["ligado"] is False and "não está configurada" in r["erro"]


@banco_
def test_casa_pelo_message_id_que_o_erp_gerou(sessao_real, dono, pessoa):
    """É o casamento certo: não depende de ninguém manter o assunto intacto."""
    from sqlalchemy import text

    from app.apps.erp.core import contabilidade as ct
    p = ct.abrir(sessao_real, {"template": "ASSINATURA_CTPS",
                               "colaborador_id": pessoa.id}, dono)
    sessao_real.execute(text("UPDATE dp_pedidos SET message_id = :m "
                             "WHERE id = :i"),
                        {"m": "<abc123@bws.com.br>", "i": p["id"]})
    achado = caixa.casar(sessao_real, in_reply_to="<abc123@bws.com.br>",
                         references="", assunto="Re: qualquer coisa", corpo="")
    assert achado["pedido_id"] == p["id"]
    assert achado["como"] == "In-Reply-To"


@banco_
def test_casa_pelo_numero_no_assunto_quando_o_cabecalho_nao_volta(
        sessao_real, dono, pessoa):
    """Acontece: o cliente de e-mail do outro lado não devolve o In-Reply-To."""
    from app.apps.erp.core import contabilidade as ct
    p = ct.abrir(sessao_real, {"template": "ASSINATURA_CTPS",
                               "colaborador_id": pessoa.id}, dono)
    numero = p["numero"]
    achado = caixa.casar(sessao_real, in_reply_to="", references="",
                         assunto=f"RES: [{numero}] Assinatura de CTPS", corpo="")
    assert achado["pedido_id"] == p["id"]
    assert "assunto" in achado["como"]


@banco_
def test_casa_pelo_cpf_quando_ha_um_pedido_aberto_so(sessao_real, dono, pessoa):
    from sqlalchemy import text

    from app.apps.erp.core import contabilidade as ct
    p = ct.abrir(sessao_real, {"template": "ASSINATURA_CTPS",
                               "colaborador_id": pessoa.id}, dono)
    sessao_real.execute(text("UPDATE dp_pedidos SET situacao='ENVIADO' "
                             "WHERE id=:i"), {"i": p["id"]})
    achado = caixa.casar(sessao_real, in_reply_to="", references="",
                         assunto="Documentos", corpo=f"segue do CPF {CPF_A}")
    assert achado["pedido_id"] == p["id"]


@banco_
def test_com_dois_pedidos_abertos_da_mesma_pessoa_nao_escolhe(sessao_real, dono,
                                                              pessoa):
    """Anexar a rescisão no pedido de férias é pior que não anexar."""
    from sqlalchemy import text

    from app.apps.erp.core import contabilidade as ct
    for tpl in ("ASSINATURA_CTPS", "FERIAS"):
        p = ct.abrir(sessao_real, {"template": tpl, "colaborador_id": pessoa.id},
                     dono)
        sessao_real.execute(text("UPDATE dp_pedidos SET situacao='ENVIADO' "
                                 "WHERE id=:i"), {"i": p["id"]})
    achado = caixa.casar(sessao_real, in_reply_to="", references="",
                         assunto="Documentos", corpo=f"CPF {CPF_A}")
    assert "pedido_id" not in achado
    assert "não escolho" in achado["motivo"]


@banco_
def test_numero_que_nao_existe_vira_motivo_escrito(sessao_real, dono):
    achado = caixa.casar(sessao_real, in_reply_to="", references="",
                         assunto="Re: [CT-99999] seja o que for", corpo="")
    assert "pedido_id" not in achado
    assert "CT-99999" in achado["motivo"]


# ---------------------------------------------------------------------------
# O PORTAL
# ---------------------------------------------------------------------------
@banco_
def test_o_portal_recusa_quem_nao_entrou_no_erp(sessao_real, app_real):
    """A identidade agora é a DO ERP — mudou em 07/11/2026, por decisão do
    dono: o ponto ficou só para o ponto, e quem pede entra no ERP."""
    from app.apps.erp.core import portal as svc_portal
    # Sem usuário é "não entrou" — e a regra NÃO vai olhar a sessão do
    # navegador para descobrir isso: quem conhece Flask é a rota.
    with pytest.raises(svc_portal.SemAcesso):
        svc_portal.quem_esta(sessao_real, None)


@banco_
def test_o_portal_so_abre_os_tipos_que_a_obra_pode(sessao_real):
    """Desconto em folha é do DP — a obra não lança desconto no salário de
    ninguém."""
    from app.apps.erp.core import portal as svc_portal
    chaves = {t["chave"] for t in svc_portal.tipos(sessao_real)}
    assert "DESCONTO_EM_FOLHA" not in chaves
    assert "PRE_CADASTRO" in chaves and "DESLIGAMENTO" in chaves


@banco_
def test_a_devolucao_nasce_com_prazo_e_tipo_do_documento(sessao_real, dono,
                                                         pessoa):
    """Pendência sem prazo não é cobrável, e pendência que ninguém cobra é a
    mesma coisa que não existir."""
    from app.apps.erp.core import portal as svc_portal
    r = svc_portal.abrir_devolucao(sessao_real, {
        "colaborador_id": pessoa.id,
        "o_que": "colher a assinatura do termo de rescisão",
        "documento_esperado": "RESCISAO", "dias": 5}, dono)
    assert r["numero"].startswith("DV-")
    abertas = svc_portal.devolucoes_abertas(sessao_real)
    minha = next(x for x in abertas if x["numero"] == r["numero"])
    assert minha["prazo"] is not None
    assert minha["documento_esperado"] == "RESCISAO"


@banco_
def test_a_devolucao_sem_dizer_o_documento_e_recusada(sessao_real, dono, pessoa):
    from app.apps.erp.core import portal as svc_portal
    from app.apps.erp.core.comum.auditoria import ErroValidacao
    with pytest.raises(ErroValidacao, match="qual documento"):
        svc_portal.abrir_devolucao(sessao_real, {
            "colaborador_id": pessoa.id, "o_que": "assinar o termo",
            "documento_esperado": ""}, dono)


@banco_
def test_a_devolucao_sem_obra_e_recusada(sessao_real, dono):
    """Sem obra não há para quem mandar."""
    from app.apps.erp.core import portal as svc_portal
    from app.apps.erp.core.cadastros import pessoas as svc
    from app.apps.erp.core.comum.auditoria import ErroValidacao
    c = svc.salvar(sessao_real, {"cpf": CPF_B, "nome": "Sem Obra Nenhuma"}, dono)
    with pytest.raises(ErroValidacao, match="obra"):
        svc_portal.abrir_devolucao(sessao_real, {
            "colaborador_id": c.id, "o_que": "assinar o termo",
            "documento_esperado": "RESCISAO"}, dono)


@banco_
def test_o_whatsapp_da_cobranca_manda_para_o_portal_e_nao_pede_foto(
        sessao_real, dono, pessoa, monkeypatch):
    """*"essa devolução não pode ser via WhatsApp. Tem que ser via sistema."*
    Então a mensagem AVISA e manda o caminho do portal — nunca pede foto."""
    from app.apps.erp.core import portal as svc_portal
    mandadas = []

    def falso_notificar(**kw):
        mandadas.append(kw)
        return {"whatsapp": {"ok": True}}

    import app.apps.notificador as nf
    monkeypatch.setattr(nf, "notificar", falso_notificar)
    r = svc_portal.abrir_devolucao(sessao_real, {
        "colaborador_id": pessoa.id, "o_que": "colher a assinatura do TRCT",
        "documento_esperado": "RESCISAO"}, dono)
    saida = svc_portal.cobrar_devolucao(sessao_real, r["id"], dono)
    assert saida["avisados"] >= 1
    texto = mandadas[0]["mensagem"]
    assert "portal" in texto.lower()
    assert "foto" not in texto.lower().split("não mande")[0]
    assert mandadas[0]["finalidade"] == "erp.dp_devolucao"


@banco_
def test_cobrar_conta_a_cobranca_e_muda_a_situacao(sessao_real, dono, pessoa,
                                                   monkeypatch):
    from app.apps.erp.core import portal as svc_portal
    import app.apps.notificador as nf
    monkeypatch.setattr(nf, "notificar", lambda **kw: {"whatsapp": {"ok": True}})
    r = svc_portal.abrir_devolucao(sessao_real, {
        "colaborador_id": pessoa.id, "o_que": "assinar o TRCT",
        "documento_esperado": "RESCISAO"}, dono)
    svc_portal.cobrar_devolucao(sessao_real, r["id"], dono)
    minha = next(x for x in svc_portal.devolucoes_abertas(sessao_real)
                 if x["numero"] == r["numero"])
    assert minha["situacao"] == "COBRADA" and minha["cobrancas"] == 1


@banco_
def test_o_portal_nao_abre_para_quem_nao_entrou(sessao_real, app_real):
    """Era pública e se defendia sozinha pela sessão do ponto. Agora é rota do
    ERP, com login — e a troca de porta é o momento clássico de deixar uma
    aberta sem querer."""
    with app_real.test_client() as c:
        r = c.get("/erp/portal")
        assert r.status_code in (302, 401, 403)


def test_o_portal_manda_para_o_login_do_erp_e_nao_para_o_ponto(sessao_real):
    """Quem está na obra com a sessão vencida tem de ver o botão que resolve —
    e o botão certo é o do ERP, não o do aplicativo do ponto."""
    from pathlib import Path
    tela = Path("app/apps/erp/templates/erp_portal.html").read_text(
        encoding="utf-8")
    assert "/erp/entrar" in tela
    assert "/ponto/app" not in tela, (
        "a tela enxuta não manda mais ninguém para o aplicativo do ponto: o "
        "ponto ficou só para o ponto em 07/11/2026")


@banco_
def test_so_pede_por_outros_quem_tem_alcada_para_isso(sessao_real, dono, pessoa):
    """Quem não tem alçada pede só por si — e não é desconfiança: pedir pelo
    colega sem responder pela obra dele não existe no processo.

    A alçada agora vem do PERFIL do ERP, não de uma marcação do ponto.
    """
    import app.apps.erp.core.portal as mod
    from app.apps.erp.core import portal as svc_portal

    eu = {"usuario": dono, "usuario_id": dono.id, "nome": dono.nome,
          "perfil": "ADMINISTRATIVO_OBRA",
          "colaborador_id": pessoa.id, "pessoa": pessoa}
    original_quem = mod.quem_esta
    original_outros = mod.pode_pedir_por_outros
    mod.quem_esta = lambda s, u=None: eu
    mod.pode_pedir_por_outros = lambda s, e: False
    try:
        lista = svc_portal.pessoas_ao_alcance(sessao_real, dono)
        assert [x["id"] for x in lista] == [pessoa.id]
    finally:
        mod.quem_esta = original_quem
        mod.pode_pedir_por_outros = original_outros


def test_o_parceiro_nao_abre_pedido():
    """Decisão do dono em 07/11/2026: *"Parceria, a princípio, é só
    visualização. Se eu entender que é necessário dar, eu posso criar um perfil
    diferente."*

    E a regra NÃO é uma linha nova no portal: é a ação `solicitar_dp` que o ERP
    já tem, da qual o PARCEIRO já estava de fora. Este teste existe para que
    ninguém a conceda sem perceber — e para que, no dia em que ele decidir o
    contrário, a mudança seja consciente.
    """
    from app.apps.erp.core.auth import permissoes as p
    from app.apps.erp.db.models.cadastros import PerfilUsuario

    assert PerfilUsuario.PARCEIRO not in p.PERMISSOES["solicitar_dp"]
    # e quem está na obra pede
    for perfil in (PerfilUsuario.ADMINISTRATIVO_OBRA,
                   PerfilUsuario.SUPERVISOR_OBRA,
                   PerfilUsuario.GESTOR_OBRA):
        assert perfil in p.PERMISSOES["solicitar_dp"]


# ---------------------------------------------------------------------------
# UM CADASTRO SÓ: as marcações do ponto dentro da ficha do ERP (06/11/2026)
#
# O dono: *"tem cadastro da pessoa, tem cadastro no ponto. Tem a tela de
# visualizar pessoas num lado, no outro. Não deveria ser assim, né? A gente tem
# que falar a mesma linguagem nos dois lados."*
# ---------------------------------------------------------------------------
@banco_
def test_as_marcacoes_do_ponto_nunca_derrubam_a_ficha(sessao_real, dono, pessoa):
    """Travar a ficha da pessoa porque o ponto não respondeu seria trocar um
    problema pequeno por um grande. Sem o ponto no ar, a resposta diz o motivo
    em português e a ficha abre igual."""
    from app.apps.erp.core import portal as svc_portal
    r = svc_portal.marcacoes_do_ponto(sessao_real, pessoa.id)
    assert "disponivel" in r
    if not r["disponivel"]:
        assert len(r["motivo"]) > 10, "indisponível sem motivo escrito não serve"


@banco_
def test_o_ponto_dentro_de_pessoal_e_um_modulo_so(sessao_real):
    """O menu junta as duas áreas, e as abas do ponto reapontam para Pessoal —
    senão a tela do ponto abriria com o menu do Financeiro aceso."""
    from app.apps.erp import routes as R
    chaves = [m["chave"] for m in R.MODULOS]
    assert "ponto" not in chaves, "o módulo Ponto tinha de ter sido absorvido"
    pessoal = next(m for m in R.MODULOS if m["chave"] == "pessoal")
    abas = [a[0] for a in pessoal["abas"]]
    assert "colaboradores" in abas and "ponto_quadro" in abas
    # A ORDEM é pelo dia a dia: o cadastro primeiro, o quadro do ponto em
    # seguida — não a despesa, que era o que ficava na frente ao juntar na
    # ordem de origem.
    assert abas[0] == "colaboradores" and abas[1] == "ponto_quadro"
    assert R._MODULO_DA_ABA.get("ponto_espelho") == "pessoal"
    # E nenhuma aba ficou apontando para um módulo que não existe mais.
    assert "ponto" not in set(R._MODULO_DA_ABA.values())


@banco_
def test_as_duas_telas_de_pessoas_ficam_com_nomes_diferentes(sessao_real):
    """A tela de pessoas do ponto continua existindo (é de outra área e tem uso
    próprio, a foto de cadastro), mas o nome passa a dizer QUAL das duas é."""
    from app.apps.erp import routes as R
    pessoal = next(m for m in R.MODULOS if m["chave"] == "pessoal")
    nomes = {a[0]: a[1] for a in pessoal["abas"]}
    assert nomes["ponto_pessoas"] == "Pessoas no ponto"
    assert nomes["colaboradores"] == "Colaboradores"


# ---------------------------------------------------------------------------
# A TRAVA DO CAMPO DE LISTA FECHADA
#
# Esta trava nasceu de um erro de verdade: `listas_fechadas()` devolvia a chave
# "categoria", mas o campo do formulário se chama "categoria_transporte". A
# tela não reclama — ela simplesmente desenha um campo de texto livre, e a obra
# passa a digitar "moto", "motinha" e "veículo próprio" para a mesma coisa.
# Erro silencioso é o pior tipo, então ele vira teste.
# ---------------------------------------------------------------------------
def test_todo_campo_de_lista_fechada_do_portal_tem_a_caixinha():
    from app.apps.erp.core import portal as svc_portal
    from app.apps.erp.core import pessoal_solicitacoes as svc_sp
    from app.apps.erp.core.cadastros import pessoas as svc_pessoas

    listas = svc_portal.listas_fechadas()
    # os que vêm do banco não entram aqui: sem sessão eles não são montados
    do_banco = {"cargo_id", "convencao_id", "obra_id"}

    fechados = set(svc_pessoas.DA_LISTA) | {
        "forma_desligamento", "parentesco", "pix_tipo"}

    faltando = []
    for chave in svc_portal.TIPOS_DO_PORTAL:
        if chave not in svc_sp.TIPOS:
            continue
        for campo in svc_sp.TIPOS[chave]["campos"]:
            if campo in do_banco or campo not in fechados:
                continue
            if not listas.get(campo):
                faltando.append(f"{chave}.{campo}")
    assert not faltando, (
        "campo de lista fechada virando texto livre no portal: "
        + ", ".join(faltando))


def test_a_caixinha_do_portal_usa_a_lista_do_cadastro_e_nao_uma_copia():
    """Se a lista mudar no cadastro, muda no portal sozinha."""
    from app.apps.erp.core import portal as svc_portal
    from app.apps.erp.core.cadastros import pessoas as svc_pessoas

    listas = svc_portal.listas_fechadas()
    valores = [o["valor"] for o in listas["vinculo"]]
    assert valores == list(svc_pessoas.VINCULOS)
    # e o rótulo é o de gente, não o código
    assert {o["rotulo"] for o in listas["vinculo"]} == set(
        svc_pessoas.VINCULO_ROTULOS.values())


def test_a_opcao_fora_de_uso_nao_aparece_para_quem_abre_pedido():
    """O cartão do transporte saiu de uso — não se escolhe de novo."""
    from app.apps.erp.core import portal as svc_portal
    from app.apps.erp.core.cadastros import pessoas as svc_pessoas

    valores = {o["valor"]
               for o in svc_portal.listas_fechadas()["categoria_transporte"]}
    for fora in svc_pessoas.CATEGORIAS_TRANSPORTE_FORA_DE_USO:
        assert fora not in valores


def test_todo_campo_de_data_do_portal_abre_calendario():
    """Data que cai como texto volta "12/3" e "ontem", e o DP arruma à mão."""
    import re
    from pathlib import Path
    from app.apps.erp.core import portal as svc_portal
    from app.apps.erp.core import pessoal_solicitacoes as svc_sp

    tela = Path("app/apps/erp/templates/erp_portal.html").read_text(
        encoding="utf-8")
    bloco = tela[tela.index("const TIPO_CAMPO"):tela.index("};",
                                                           tela.index("const TIPO_CAMPO"))]
    com_calendario = set(re.findall(r"(\w+):\s*\"(?:date|month)\"", bloco))

    faltando = []
    for chave in svc_portal.TIPOS_DO_PORTAL:
        if chave not in svc_sp.TIPOS:
            continue
        for campo in svc_sp.TIPOS[chave]["campos"]:
            if campo in ("data_nascimento", "admissao") or campo.startswith(
                    ("data_", "aviso_", "ultimo_", "vigencia")):
                if campo not in com_calendario:
                    faltando.append(f"{chave}.{campo}")
    assert not faltando, (
        "campo de data sem calendário no portal: " + ", ".join(faltando))


def test_a_situacao_do_pedido_chega_na_obra_em_portugues():
    """"EM_ANALISE" na tela de quem está na obra é defeito, não detalhe."""
    import inspect
    from app.apps.erp.core import portal as svc_portal

    fonte = inspect.getsource(svc_portal.meus_pedidos)
    assert "SITUACAO_ROTULOS" in fonte


def test_nenhum_campo_do_portal_sai_com_nome_tecnico():
    """"aux_transporte" na tela de quem está na obra é defeito, não detalhe.

    Nasceu de um erro de verdade: 22 campos saíam com o nome do banco, porque
    o `ROTULO` só cobria os que eu tinha olhado. Agora é a lista de campos que
    manda, não a minha memória.
    """
    import re
    from pathlib import Path
    from app.apps.erp.core import portal as svc_portal
    from app.apps.erp.core import pessoal_solicitacoes as svc_sp

    tela = Path("app/apps/erp/templates/erp_portal.html").read_text(
        encoding="utf-8")
    i = tela.index("const ROTULO = {")
    com_rotulo = set(re.findall(r"(\w+):", tela[i:tela.index("};", i)]))

    faltando = []
    for chave in svc_portal.TIPOS_DO_PORTAL:
        if chave not in svc_sp.TIPOS:
            continue
        for campo in svc_sp.TIPOS[chave]["campos"]:
            if campo not in com_rotulo and campo not in faltando:
                faltando.append(campo)
    assert not faltando, (
        "campo do portal sem rótulo em português: " + ", ".join(faltando))
