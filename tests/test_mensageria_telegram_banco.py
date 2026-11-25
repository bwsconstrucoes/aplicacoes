# -*- coding: utf-8 -*-
"""O cadastro do Telegram e o convite com banco de verdade (migração 103):
quem pode, a busca do chat, a troca de conta, a trava de repetição do convite,
o link pessoal, a cópia da planilha e o painel da tela."""
from __future__ import annotations

import contextlib
import datetime as dt

import pytest
from sqlalchemy import text

from app.apps.mensageria import core, db as mdb, telegram_cadastro as tc

pytestmark = pytest.mark.banco


@pytest.fixture
def conn(banco, monkeypatch):
    with banco.connect() as c:
        trans = c.begin()
        for t in ("convites", "telegram_cadastros", "telegram_tentativas", "envios", "parametros"):
            c.execute(text(f"DELETE FROM mensageria.{t}"))

        @contextlib.contextmanager
        def _mesma():
            yield c
        monkeypatch.setattr(mdb, "conexao", _mesma)
        monkeypatch.setattr(mdb, "disponivel", lambda: True)
        yield c
        trans.rollback()


def _colab(conn, nome, cpf, telefone, situacao="ATIVO"):
    return conn.execute(text("""INSERT INTO colaboradores (nome, cpf, telefone, situacao)
                                VALUES (:n, :c, :t, :s) RETURNING id"""),
                        {"n": nome, "c": cpf, "t": telefone, "s": situacao}).scalar()


@pytest.fixture
def gente(conn):
    return {
        "ana": _colab(conn, "Ana Lima", "11111111111", "(85) 99999-0001"),
        "bia": _colab(conn, "Bia Rocha", "22222222222", "85 9999-0002"),
        "cid": _colab(conn, "Cid Desligado", "33333333333", "(85) 99999-0003", "DESLIGADO"),
        "dan": _colab(conn, "Dan Sem Telefone", "44444444444", ""),
    }


def _falhas(conn, n, telefone="85999990001", cpf=""):
    """Mensagens do Telegram que não chegaram (como o registro anota). Com a hora
    do relógio: dentro da transação do teste o now() do banco não anda, e em
    produção cada envio é uma transação."""
    for _ in range(n):
        conn.execute(text("""INSERT INTO mensageria.envios (tipo, canal, destinatario, cpf, texto, status, criado_em)
                             VALUES ('erp.titulo_pago', 'telegram', :d, :c, 'pago', 'SEM_DESTINO',
                                     clock_timestamp() + interval '1 second')"""), {"d": telefone, "c": cpf})


def _ana(conn):
    return tc.achar_pessoa(conn, cpf="11111111111")


def test_a_migracao_103_ligou_o_convite_e_criou_as_tabelas(conn):
    tabelas = {r[0] for r in conn.execute(text(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'mensageria'"))}
    assert {"telegram_cadastros", "convites"} <= tabelas


def test_so_colaborador_ativo_com_telefone_pode(conn, gente):
    nomes = [p["nome"] for p in tc.pessoas_elegiveis(conn)]
    assert "Ana Lima" in nomes and "Bia Rocha" in nomes
    assert "Cid Desligado" not in nomes and "Dan Sem Telefone" not in nomes
    assert tc.achar_pessoa(conn, telefone="5585999990001")["nome"] == "Ana Lima"
    assert tc.achar_pessoa(conn, telefone="85999990002")["nome"] == "Bia Rocha"   # com o nono dígito
    assert tc.achar_pessoa(conn, cpf="33333333333") is None


def test_o_chat_e_achado_pelo_cpf_pelo_telefone_e_pela_pessoa(conn, gente):
    tc.gravar_cadastro(conn, chat_id=900, telefone="+55 85 99999-0001", origem="TELEFONE")
    cad = tc.cadastro_por_chat(conn, 900)
    assert cad["cpf"] == "11111111111" and cad["colaborador_id"] == gente["ana"]
    assert tc.chat_id_de(conn, cpf="11111111111") == "900"
    assert tc.chat_id_de(conn, telefone="8599990001") == "900"
    # O RH trocou o telefone da Ana: o chat é achado pela pessoa (telefone → CPF)
    conn.execute(text("UPDATE colaboradores SET telefone = '85988887777' WHERE id = :i"), {"i": gente["ana"]})
    assert tc.chat_id_de(conn, telefone="5585988887777") == "900"
    assert tc.chat_id_de(conn, telefone="5585911112222") is None


def test_trocar_de_conta_do_telegram_desliga_a_antiga(conn, gente):
    tc.gravar_cadastro(conn, chat_id=1, cpf="11111111111", telefone="85999990001")
    tc.gravar_cadastro(conn, chat_id=2, cpf="11111111111", telefone="85999990001")
    assert tc.cadastro_por_chat(conn, 1) is None and tc.chat_id_de(conn, cpf="11111111111") == "2"


def test_o_convite_respeita_o_intervalo_e_quem_ja_tem(conn, gente):
    saiu = []
    ok = lambda tel, texto: (saiu.append((tel, texto)), {"whatsapp": {"ok": True}})[1]  # noqa: E731
    r = tc.convidar(_ana(conn), motivo="AUTOMATICO", tipo_que_falhou="erp.titulo_pago", enviar=ok)
    assert r["status"] == "ENVIADO" and saiu[0][0] == "5585999990001"
    assert "https://t.me/" in saiu[0][1] and "?start=c_" in saiu[0][1] and "Ana" in saiu[0][1]
    linha = conn.execute(text("SELECT status, motivo, tipo_que_falhou FROM mensageria.convites")).one()
    assert tuple(linha) == ("ENVIADO", "AUTOMATICO", "erp.titulo_pago")
    # Outro automático no mesmo dia, e um manual na mesma hora: não saem
    assert tc.convidar(_ana(conn), motivo="AUTOMATICO", enviar=ok)["status"] == "RECUSADO"
    assert "já recebeu convite" in tc.convidar(_ana(conn), motivo="MANUAL", enviar=ok)["detalhe"]
    # Oito dias depois, sem novas mensagens perdidas: ainda não (a trava é dupla)
    depois = core._agora() + dt.timedelta(days=8)
    r = tc.convidar(_ana(conn), motivo="AUTOMATICO", enviar=ok, agora=depois)
    assert r["status"] == "RECUSADO" and "mais 5 mensagens" in r["detalhe"]
    _falhas(conn, 4)
    assert "mais 1 mensagem que não chegue" in tc.convidar(_ana(conn), motivo="AUTOMATICO", enviar=ok,
                                                           agora=depois)["detalhe"]
    _falhas(conn, 1)
    assert tc.convidar(_ana(conn), motivo="AUTOMATICO", enviar=ok, agora=depois)["status"] == "ENVIADO"
    # Quem já tem Telegram não é convidado
    tc.gravar_cadastro(conn, chat_id=55, cpf="22222222222")
    bia = tc.achar_pessoa(conn, cpf="22222222222")
    assert tc.convidar(bia, motivo="MANUAL", enviar=ok)["detalhe"] == "já tem Telegram cadastrado"
    assert len(saiu) == 2


def test_convite_que_nao_saiu_nao_conta_para_o_intervalo(conn, gente):
    falha = lambda tel, texto: {"mensageria": {"ok": None, "detalhe": "WhatsApp desligado"}}  # noqa: E731
    assert tc.convidar(_ana(conn), motivo="AUTOMATICO", enviar=falha)["status"] == "DESLIGADO"
    ok = lambda tel, texto: {"whatsapp": {"ok": True}}  # noqa: E731
    assert tc.convidar(_ana(conn), motivo="AUTOMATICO", enviar=ok)["status"] == "ENVIADO"


def test_o_automatico_decide_e_manda_em_segundo_plano(conn, gente, monkeypatch):
    feitos = []
    monkeypatch.setattr(tc, "_em_segundo_plano", lambda f: feitos.append(f))
    r = tc.convidar_se_preciso(telefone="85999990001", tipo_que_falhou="erp.titulo_pago")
    assert r["status"] == "AGENDADO" and len(feitos) == 1
    assert tc.convidar_se_preciso(telefone="5585911112222")["detalhe"].startswith("não é colaborador")
    core.gravar_parametro(conn, tc.CONVITE_AUTOMATICO, "0")
    assert "desligado" in tc.convidar_se_preciso(telefone="85999990001")["detalhe"]


def test_o_link_pessoal_reconhece_a_pessoa_e_se_gasta(conn, gente):
    tc.convidar(_ana(conn), motivo="MANUAL", enviar=lambda t, x: {"whatsapp": {"ok": True}})
    codigo = conn.execute(text("SELECT codigo FROM mensageria.convites")).scalar()
    assert tc.abrir_link(conn, "nao-existe", 1) is None
    convite = tc.abrir_link(conn, codigo, 321)
    assert convite["nome"] == "Ana Lima"
    assert tc.convite_aberto_do_chat(conn, 321)["cpf"] == "11111111111"
    tc.marcar_usado(conn, 321, "11111111111")
    assert tc.abrir_link(conn, codigo, 321) is None
    assert tc.convite_aberto_do_chat(conn, 321) is None


PLANILHA = [["Data", "CPF", "Telefone", "ID Telegram", "Nome", "Origem", "Obs"],
            ["01/10/2026", "111.111.111-11", "5585999990001", "700", "ANA LIMA", "TELEFONE", ""],
            ["01/10/2026", "22222222222", "5585900001111", "702", "BIA", "CPF", ""],      # telefone ≠ do ERP
            ["01/10/2026", "33333333333", "5585999990003", "703", "CID", "TELEFONE", ""],  # desligado
            ["01/10/2026", "", "5585977776666", "701", "Fulano de fora", "CPF", ""],
            ["01/10/2026", "", "", "", "linha vazia", "", ""]]


def test_migrar_a_planilha_cruza_com_o_erp_e_nao_duplica(conn, gente):
    assert tc.migrado(conn) is False
    r = tc.migrar_planilha(conn, PLANILHA, por="Marcelo")
    assert r["contagem"] == {"ok": 1, "divergente": 1, "fora": 2, "ja_estava": 0, "sem_chat": 1}
    assert [x["nome"] for x in r["divergente"]] == ["Bia Rocha"]
    assert r["divergente"][0]["telefone_erp"] == "558599990002"
    assert {x["nome"] for x in r["fora"]} == {"CID", "Fulano de fora"}
    assert tc.migrado(conn) is True
    assert tc.cadastro_por_chat(conn, 700)["colaborador_id"] == gente["ana"]
    assert tc.cadastro_por_chat(conn, 702)["cpf"] == "22222222222"          # divergente entra, ativo
    assert tc.cadastro_por_chat(conn, 703) is None and tc.cadastro_por_chat(conn, 701) is None
    assert tc.relatorio_migracao(conn)["contagem"]["ok"] == 1
    r2 = tc.migrar_planilha(conn, PLANILHA)
    assert (r2["novos"], r2["atualizados"], r2["contagem"]["ja_estava"]) == (0, 0, 2)


def test_o_painel_mostra_quem_nao_recebe_e_o_que_conferir(conn, gente):
    tc.gravar_cadastro(conn, chat_id=701, telefone="5585977776666", nome="Fulano de fora")
    tc.gravar_cadastro(conn, chat_id=800, cpf="22222222222")
    for _ in range(3):
        core.registrar(conn, tipo="erp.titulo_pago", canal="telegram", destinatario="5585999990001",
                       cpf="11111111111", texto="pago", nome_arquivo="", status="SEM_DESTINO", detalhe="")
    d = tc.painel(conn)
    por = {p["nome"]: p for p in d["pessoas"]}
    assert por["Ana Lima"]["telegram"] is False and por["Ana Lima"]["falhas_30d"] == 3
    assert por["Bia Rocha"]["telegram"] is True
    assert d["totais"]["com_falha"] == 1 and d["totais"]["a_conferir"] == 1
    assert [p["nome"] for p in tc.painel(conn, filtro="falha")["pessoas"]] == ["Ana Lima"]
    assert [p["nome"] for p in tc.painel(conn, busca="9999-0002")["pessoas"]] == ["Bia Rocha"]
    assert d["a_conferir"][0]["nome"] == "Fulano de fora"


def test_o_automatico_para_no_maximo_por_pessoa_e_a_tela_ainda_pode(conn, gente):
    """Pedido do dono: não convidar a cada mensagem perdida, para não ser banido de novo."""
    ok = lambda tel, texto: {"whatsapp": {"ok": True}}  # noqa: E731
    core.gravar_parametro(conn, tc.CONVITE_FALHAS_ENTRE, "2")
    core.gravar_parametro(conn, tc.CONVITE_MAXIMO, "2")
    agora = core._agora()
    _falhas(conn, 1, telefone="5585999990001")
    assert tc.convidar(_ana(conn), motivo="AUTOMATICO", enviar=ok, agora=agora)["status"] == "ENVIADO"
    # o registro guarda o telefone com 55 ou sem; as duas formas contam
    _falhas(conn, 1, telefone="5585999990001")
    _falhas(conn, 1, cpf="11111111111", telefone="")
    agora += dt.timedelta(days=8)
    conn.execute(text("UPDATE mensageria.envios SET criado_em = now() + interval '1 day'"))
    assert tc.convidar(_ana(conn), motivo="AUTOMATICO", enviar=ok, agora=agora)["status"] == "ENVIADO"
    _falhas(conn, 5)
    conn.execute(text("UPDATE mensageria.envios SET criado_em = now() + interval '9 days'"))
    r = tc.convidar(_ana(conn), motivo="AUTOMATICO", enviar=ok, agora=agora + dt.timedelta(days=8))
    assert r["status"] == "RECUSADO" and "só pela tela" in r["detalhe"]
    r = tc.convidar(_ana(conn), motivo="MANUAL", enviar=ok, agora=agora + dt.timedelta(days=8))
    assert r["status"] == "ENVIADO"



def test_a_fila_do_dp_e_quem_nao_tem_telefone(conn, gente):
    tc.registrar_tentativa(conn, chat_id=9, telefone="+55 85 91111-2222", nome_telegram="Zé",
                           nome_erp="Ana Lima", motivo="TELEFONE_NAO_BATE")
    fila = tc.tentativas_pendentes(conn)
    assert len(fila) == 1 and fila[0]["telefone"] == "5585911112222" and fila[0]["nome_erp"] == "Ana Lima"
    tc.gravar_cadastro(conn, chat_id=9, cpf="11111111111")            # o DP acertou e ela se cadastrou
    assert tc.tentativas_pendentes(conn) == []
    assert [c["nome"] for c in tc.sem_telefone(conn)] == ["Dan Sem Telefone"]
    d = tc.painel(conn)
    assert d["totais"]["sem_telefone"] == 1


def test_o_contracheque_do_robo_le_o_colaborador_do_erp(conn, gente):
    ana = tc.colaborador_por_cpf(conn, "111.111.111-11")
    assert ana["nome"] == "Ana Lima" and not ana["desligado"] and ana["status"] == ""
    cid = tc.colaborador_por_cpf(conn, "33333333333")
    assert cid["desligado"] and cid["status"] == "Colaboradores Desligados"
    assert tc.colaborador_por_cpf(conn, "99999999999") is None


def test_a_ficha_mostra_o_telegram_e_as_mensagens_da_pessoa(conn, gente):
    tc.gravar_cadastro(conn, chat_id=4321, cpf="11111111111", telefone="5585900000000")   # outro número
    core.registrar(conn, tipo="erp.titulo_pago", canal="telegram", destinatario="4321", cpf="",
                   texto="Seu título foi pago", nome_arquivo="", status="ENVIADO", detalhe="")
    core.registrar(conn, tipo="ponto.qr", canal="whatsapp", destinatario="5585999990001", cpf="",
                   texto="QR", nome_arquivo="", status="ENVIADO", detalhe="")
    d = tc.resumo_da_pessoa(conn, gente["ana"])
    assert d["telegram"]["chat_id"] == "4321" and d["telegram"]["telefone_diverge"] is True
    assert {m["tipo"] for m in d["mensagens"]} == {"erp.titulo_pago", "ponto.qr"}
    assert d["pode_cadastrar"] is True
    assert tc.resumo_da_pessoa(conn, gente["dan"])["pode_cadastrar"] is False
    with pytest.raises(ValueError):
        tc.resumo_da_pessoa(conn, 10 ** 9)


def test_o_painel_de_mensagens_conta_por_dia_hora_tipo_e_pessoa(conn, gente):
    from app.apps.mensageria import painel
    tc.gravar_cadastro(conn, chat_id=4321, cpf="11111111111")
    linhas = [("erp.titulo_pago", "telegram", "4321", "", "ENVIADO"),
              ("erp.titulo_pago", "telegram", "5585999990002", "22222222222", "SEM_DESTINO"),
              ("ponto.qr", "whatsapp", "5585999990001", "", "ENVIADO"),
              ("ponto.qr", "whatsapp", "85999990001", "", "ENVIADO"),
              ("ponto.qr", "whatsapp", "5585999990001", "", "LIMITE"),
              ("externo.telegram_enviar", "telegram", "5585977776666", "", "SEM_DESTINO")]
    for tipo, canal, dest, cpf, status in linhas:
        core.registrar(conn, tipo=tipo, canal=canal, destinatario=dest, cpf=cpf, texto="x", nome_arquivo="",
                       status=status, detalhe="")
    d = painel.dados(conn, {"dias": "7"})
    k = d["kpis"]
    assert (k["total"], k["entregues"], k["nao_chegaram"], k["barradas"]) == (6, 3, 2, 1)
    assert (k["telegram"], k["whatsapp"], k["taxa_entrega"]) == (1, 2, 50)
    hoje = d["por_dia"][-1]
    assert (hoje["telegram"], hoje["whatsapp"], hoje["nao_chegaram"], hoje["barradas"]) == (1, 2, 2, 1)
    assert len(d["por_dia"]) == 7 and len(d["por_hora"]) == 24
    assert sum(h["telegram"] + h["whatsapp"] for h in d["por_hora"]) == 3
    por_pessoa = {g["nome"]: g for g in d["por_pessoa"]}
    # O chat, o telefone com 55 e o telefone sem 55 são a mesma Ana
    assert por_pessoa["Ana Lima"]["total"] == 4 and por_pessoa["Ana Lima"]["whatsapp"] == 2
    assert por_pessoa["Bia Rocha"]["nao_chegaram"] == 1
    assert any(n.startswith("Fora do cadastro") for n in por_pessoa)
    assert d["por_tipo"][0]["tipo"] == "ponto.qr" and d["por_tipo"][0]["total"] == 3
    # Filtros
    assert painel.dados(conn, {"dias": "7", "canal": "whatsapp"})["kpis"]["total"] == 3
    assert painel.dados(conn, {"dias": "7", "situacao": "nao_chegou"})["kpis"]["total"] == 2
    assert painel.dados(conn, {"dias": "7", "modulo": "ponto"})["kpis"]["total"] == 3
    assert painel.dados(conn, {"dias": "7", "pessoa": "ana"})["kpis"]["total"] == 4
    assert painel.dados(conn, {"dias": "7", "pessoa": "9999-0002"})["kpis"]["total"] == 1
    assert painel.dados(conn, {"dias": "7", "tipo": "erp.titulo_pago"})["kpis"]["total"] == 2
    # Colaborador sem telefone (não pode se cadastrar) ainda é reconhecido pelo nome
    core.registrar(conn, tipo="ponto.qr", canal="whatsapp", destinatario="", cpf="44444444444", texto="x",
                   nome_arquivo="", status="BARRADO", detalhe="sem celular")
    assert "Dan Sem Telefone" in {g["nome"] for g in painel.dados(conn, {"dias": "7"})["por_pessoa"]}


def test_o_chatbot_do_whatsapp_le_o_erp_depois_da_migracao(conn, gente, monkeypatch):
    from app.apps.chatbot import sheets_cache
    monkeypatch.setattr(sheets_cache, "_carregar_planilha", lambda: [{"cpf": "x", "nome": "da planilha", "tel": "", "status": ""}])
    sheets_cache.invalidar_cache()
    assert sheets_cache.get_colaboradores()[0]["nome"] == "da planilha"      # antes de migrar
    core.gravar_parametro(conn, tc.MIGRADO_EM, "2026-11-14")
    sheets_cache.invalidar_cache()
    ana = sheets_cache.buscar_por_cpf("11111111111")
    assert ana["nome"] == "Ana Lima" and ana["tel"] == "5585999990001"
    assert sheets_cache.esta_desligado(sheets_cache.buscar_por_cpf("33333333333"))
    sheets_cache.invalidar_cache()


# ===========================================================================
# A GESTÃO DOS CADASTROS (17/11/2026)
#
# Ele, olhando a tela: *"poucos filtros, pouca informação, pouca correlação com
# o cadastro de colaboradores do ERP (…) eu preciso ter alguma gestão e
# compreender pessoas que estão cadastradas, não estão cadastradas, estão com
# problema."* E, sobre uma pessoa específica: *"está dizendo que não está
# recebendo mensagens. Aí eu não sei o que é que está acontecendo."*
#
# O que os casos abaixo prendem é justamente o que faltava: o ID do Telegram na
# resposta, o veredito por pessoa, quem não tem telefone APARECENDO na lista, e
# a obra/cargo vindos do cadastro do ERP.
# ===========================================================================
def test_o_id_do_telegram_vai_para_a_tela(conn, gente):
    """Pedido dele, com a razão: *"não aparece o ID do Telegram (…) até para a
    gente comparar, de repente se der algum problema."* O dado sempre esteve na
    tabela; só não subia."""
    tc.gravar_cadastro(conn, chat_id=12345678, cpf="22222222222")

    d = tc.painel(conn)

    bia = next(p for p in d["pessoas"] if p["nome"] == "Bia Rocha")
    assert bia["chat_id"] == "12345678"
    # … e quem não tem Telegram não ganha id inventado
    ana = next(p for p in d["pessoas"] if p["nome"] == "Ana Lima")
    assert ana["chat_id"] == ""


def test_o_id_do_telegram_tambem_e_buscavel(conn, gente):
    """Se ele está na tela para comparar, tem de dar para procurar por ele."""
    tc.gravar_cadastro(conn, chat_id=987654321, cpf="22222222222")

    achados = [p["nome"] for p in tc.painel(conn, busca="987654321")["pessoas"]]

    assert achados == ["Bia Rocha"]


def test_cada_pessoa_ganha_um_veredito_em_portugues(conn, gente):
    tc.gravar_cadastro(conn, chat_id=700, cpf="22222222222")

    por = {p["nome"]: p for p in tc.painel(conn)["pessoas"]}

    assert por["Bia Rocha"]["diagnostico"] == "RECEBE"
    assert por["Ana Lima"]["diagnostico"] == "SEM_TELEGRAM"
    assert por["Dan Sem Telefone"]["diagnostico"] == "SEM_TELEFONE"
    # o veredito vem com rótulo e com O QUE FAZER — é o que vira gestão
    assert por["Dan Sem Telefone"]["diagnostico_rotulo"].startswith("Não recebe")
    assert "DP" in por["Dan Sem Telefone"]["diagnostico_explicacao"]
    # o desligado da empresa não entra na lista de gestão
    assert "Cid Desligado" not in por


def test_quem_nao_tem_telefone_APARECE_na_lista(conn, gente):
    """A tela antiga escondia essa gente numa tabela no pé da página. É
    justamente quem nunca vai receber nada até o DP acertar o cadastro."""
    nomes = [p["nome"] for p in tc.painel(conn)["pessoas"]]

    assert "Dan Sem Telefone" in nomes
    # … e ela NÃO entra em quem pode ser convidado, porque o convite vai por
    # WhatsApp: as duas perguntas são diferentes e continuam separadas.
    assert "Dan Sem Telefone" not in [p["nome"] for p in tc.pessoas_elegiveis(conn)]


def test_telefone_incompleto_e_um_veredito_proprio(conn):
    """Telefone sem DDD não serve para convite, e dizer "sem Telegram" para
    essa pessoa mandaria o DP convidar quem não tem como receber."""
    _colab(conn, "Eva Telefone Curto", "55555555555", "99999")

    por = {p["nome"]: p for p in tc.painel(conn)["pessoas"]}

    assert por["Eva Telefone Curto"]["diagnostico"] == "TELEFONE_CURTO"


def test_telegram_com_outro_numero_RECEBE_mas_fica_marcado(conn, gente):
    """A mensagem chega (o sistema acha pelo CPF), então dizer "não recebe"
    seria mentira — mas o DP precisa acertar o telefone."""
    tc.gravar_cadastro(conn, chat_id=701, cpf="22222222222", telefone="5585911112222")

    por = {p["nome"]: p for p in tc.painel(conn)["pessoas"]}

    assert por["Bia Rocha"]["diagnostico"] == "RECEBE_OUTRO_NUMERO"
    assert por["Bia Rocha"]["telegram"] is True
    assert por["Bia Rocha"]["telefone_diverge"] is True


def test_a_obra_e_o_cargo_vem_do_cadastro_do_erp(conn):
    """A correlação que ele pediu: *"pouca correlação com o cadastro de
    colaboradores do ERP. Tem que estar mais integrado isso aqui."*"""
    obra = conn.execute(text("INSERT INTO obras (codigo, nome, status) "
                             "VALUES ('MSG-1', 'Creche do Bairro', 'ATIVA') RETURNING id")).scalar()
    cargo = conn.execute(text("INSERT INTO cargos (nome) VALUES ('Servente') "
                              "RETURNING id")).scalar()
    conn.execute(text("INSERT INTO colaboradores (nome, cpf, telefone, situacao, obra_id, cargo_id) "
                      "VALUES ('Fab Obra', '66666666666', '85999998888', 'ATIVO', :o, :c)"),
                 {"o": obra, "c": cargo})

    p = next(x for x in tc.painel(conn)["pessoas"] if x["nome"] == "Fab Obra")

    assert "Creche do Bairro" in p["obra"]
    assert p["cargo"] == "Servente"
    assert p["colaborador_id"], "a tela precisa do id para linkar a ficha no Pessoal"


def test_os_filtros_novos_recortam_a_lista(conn, gente):
    tc.gravar_cadastro(conn, chat_id=702, cpf="22222222222")

    semtel = [p["nome"] for p in tc.painel(conn, filtro="semtelefone")["pessoas"]]
    problema = [p["nome"] for p in tc.painel(conn, filtro="problema")["pessoas"]]
    recebe = [p["nome"] for p in tc.painel(conn, filtro="com")["pessoas"]]

    assert semtel == ["Dan Sem Telefone"]
    assert sorted(problema) == ["Ana Lima", "Dan Sem Telefone"]
    assert recebe == ["Bia Rocha"]


def test_os_numeros_do_cabecalho_batem_com_os_filtros(conn, gente):
    """Número que não bate com a lista que ele abre é pior que número nenhum."""
    tc.gravar_cadastro(conn, chat_id=703, cpf="22222222222")

    t = tc.painel(conn)["totais"]

    assert t["com_telegram"] == len(tc.painel(conn, filtro="com")["pessoas"])
    assert t["sem_telegram"] == len(tc.painel(conn, filtro="sem")["pessoas"])
    assert t["sem_telefone"] == len(tc.painel(conn, filtro="semtelefone")["pessoas"])
    assert t["telefone_diverge"] == len(tc.painel(conn, filtro="diverge")["pessoas"])


def test_o_painel_explica_todo_veredito_que_ele_usa(conn, gente):
    """A tela desenha a explicação a partir do que o servidor manda. Veredito
    sem verbete sairia como caixa vazia."""
    d = tc.painel(conn)

    usados = {p["diagnostico"] for p in d["pessoas"]}
    assert usados <= set(d["diagnosticos"])
    for v in d["diagnosticos"].values():
        assert v["rotulo"] and v["explicacao"] and v["cor"]


# ===========================================================================
# O TESTE ACEITA TELEFONE **OU** ID DO TELEGRAM (17/11/2026)
#
# Ele: *"quando eu boto o Telegram, o canal, fica até incompatível a digitação
# do ID. (…) Eu gostaria de poder botar também o ID. Tipo assim, você botar o
# ID ou o telefone, porque fica mais uma opção, mais direta de teste."*
#
# ⚑ O QUE ESTES CASOS GUARDAM é a parte que erraria em silêncio: um ID do
# Telegram tem 9 ou 10 dígitos e um telefone com DDD tem 10 ou 11 — eles se
# SOBREPÕEM. Contar dígitos mandaria mensagem para o lugar errado sem avisar.
# A regra é PROCURAR o valor entre os IDs cadastrados.
# ===========================================================================
def _destino(canal, valor):
    from app.apps.mensageria import gestao
    return gestao._destino_do_teste(canal, valor)


def test_id_cadastrado_e_reconhecido_como_id(conn, gente):
    tc.gravar_cadastro(conn, chat_id=1234567890, cpf="22222222222", nome="Bia Rocha")

    tipo, valor, como = _destino("telegram", "1234567890")

    assert tipo == "chat" and valor == "1234567890"
    assert "ID 1234567890" in como and "Bia Rocha" in como, (
        "a resposta tem de dizer de quem é o ID — é assim que ele vê se o "
        "sistema leu o que ele quis dizer")


def test_telefone_continua_sendo_telefone(conn, gente):
    tipo, valor, como = _destino("telegram", "(85) 99999-0001")

    assert tipo == "telefone" and valor == "5585999990001"
    assert "telefone" in como


def test_o_55_e_posto_quando_falta(conn):
    assert _destino("whatsapp", "85999990001")[1] == "5585999990001"
    assert _destino("whatsapp", "5585999990001")[1] == "5585999990001"


def test_numero_com_cara_de_telefone_que_E_um_id_cadastrado_vale_como_id(conn, gente):
    """O caso que faz a regra existir: 11 dígitos serve de telefone E pode ser
    um ID. Quem está cadastrado ganha."""
    tc.gravar_cadastro(conn, chat_id=85999990002, cpf="22222222222")

    tipo, valor, _ = _destino("telegram", "85999990002")

    assert tipo == "chat" and valor == "85999990002"


def test_id_de_grupo_e_negativo_e_nao_ha_ambiguidade(conn):
    """Grupo do Telegram tem ID negativo, e telefone negativo não existe."""
    tipo, valor, _ = _destino("telegram", "-1001234567890")

    assert tipo == "chat" and valor == "-1001234567890"


def test_no_whatsapp_o_id_do_telegram_nao_vale(conn, gente):
    """Aceitar um ID de Telegram no WhatsApp mandaria mensagem para um número
    inventado."""
    tc.gravar_cadastro(conn, chat_id=85999990002, cpf="22222222222")

    tipo, valor, _ = _destino("whatsapp", "85999990002")

    assert tipo == "telefone" and valor == "5585999990002"


def test_vazio_e_curto_sao_recusados_com_recado_que_ensina(conn):
    import pytest as _pytest
    with _pytest.raises(ValueError) as e1:
        _destino("telegram", "")
    with _pytest.raises(ValueError) as e2:
        _destino("telegram", "9999")
    with _pytest.raises(ValueError) as e3:
        _destino("whatsapp", "9999")

    assert "ID do Telegram" in str(e1.value)
    assert "Cadastro do Telegram" in str(e2.value), "o recado diz ONDE achar o ID"
    assert "ID" not in str(e3.value), "no WhatsApp não se oferece o que não serve"


def test_id_nao_cadastrado_com_cara_de_telefone_vira_telefone(conn):
    """Sem cadastro para confirmar, o palpite é telefone — e a resposta diz
    isso, para ele ver que não foi pelo caminho que esperava."""
    tipo, _, como = _destino("telegram", "1234567890")

    assert tipo == "telefone" and "telefone" in como


# ===========================================================================
# NÃO MANDAR MENSAGEM PARA QUEM FOI DESLIGADO (17/11/2026)
#
# Decisão dele, dita com todas as letras: *"essa correlação entre o ERP e as
# pessoas é fundamental, tendo em vista a questão do bloqueio de envio de
# mensagens para as pessoas desligadas. Por isso que tem que estar tudo
# correlacionado."*
#
# ⚠️ O QUE ESTES CASOS PROTEGEM é o lado perigoso da trava: barrar quem NÃO
# devia. Um aviso que não sai e ninguém nota é pior que um aviso a mais.
# ===========================================================================
def test_quem_foi_desligado_e_barrado(conn, gente):
    quem = tc.desligado_da_empresa(conn, telefone="85999990003")

    assert quem and quem["nome"] == "Cid Desligado"
    assert "desligada da empresa" in tc.motivo_do_bloqueio(conn, telefone="85999990003")


def test_pelo_cpf_tambem(conn, gente):
    assert tc.desligado_da_empresa(conn, cpf="333.333.333-33")


def test_quem_esta_ativo_NAO_e_barrado(conn, gente):
    assert tc.desligado_da_empresa(conn, telefone="85999990001") is None
    assert tc.motivo_do_bloqueio(conn, telefone="85999990001") is None


def test_numero_que_nao_e_de_ninguem_NAO_e_barrado(conn, gente):
    """Fornecedor, contador, o celular de quem não é colaborador: barrar o
    desconhecido fecharia metade dos avisos do sistema."""
    assert tc.motivo_do_bloqueio(conn, telefone="85911110000") is None


def test_recontratado_com_cadastro_antigo_NAO_e_barrado(conn, gente):
    """O caso real que a ordem das perguntas resolve: a pessoa tem uma ficha
    DESLIGADA (o vínculo antigo) e outra ATIVA. Procurar "existe desligado com
    este telefone?" primeiro a barraria."""
    # O CPF é único na tabela, então o vínculo antigo fica sem CPF (é como o
    # cadastro repetido aparece de verdade: mesmo telefone, ficha incompleta).
    _colab(conn, "Ana Lima (vínculo antigo)", "", "(85) 99999-0001", "DESLIGADO")

    assert tc.motivo_do_bloqueio(conn, cpf="11111111111") is None
    assert tc.motivo_do_bloqueio(conn, telefone="85999990001") is None


def test_operador_do_erp_que_nao_e_colaborador_NAO_e_barrado(conn):
    """O parceiro da obra e o contador operam o sistema e não são colaboradores
    da BWS."""
    from app.apps.erp.core.auth.service import gerar_hash
    conn.execute(text("DELETE FROM usuarios WHERE email = 'parceiro.msg@teste.com'"))
    conn.execute(text("INSERT INTO usuarios (nome, email, senha_hash, perfil, ativo, telefone) "
                      "VALUES ('Parceiro da obra', 'parceiro.msg@teste.com', :h, 'PARCEIRO', "
                      "        TRUE, '85988887777')"), {"h": gerar_hash("x" * 12)})
    _colab(conn, "Homônimo Desligado", "", "85988887777", "DESLIGADO")

    assert tc.motivo_do_bloqueio(conn, telefone="85988887777") is None


def test_a_trava_pode_ser_desligada_pela_tela(conn, gente):
    """Ela nasce ligada — ele pediu o bloqueio. A chave existe para o caso de
    ele precisar avisar alguém que já saiu, sem esperar código novo."""
    assert core.bloqueia_desligados(conn) is True
    assert tc.motivo_do_bloqueio(conn, telefone="85999990003")

    core.gravar_parametro(conn, core.BLOQUEAR_DESLIGADOS, "0", "teste")

    assert core.bloqueia_desligados(conn) is False
    assert tc.motivo_do_bloqueio(conn, telefone="85999990003") is None


def test_o_notificador_barra_e_registra_o_motivo(conn, gente, monkeypatch):
    """A trava inteira, pelo caminho de verdade: o aviso NÃO sai, e fica no
    registro dizendo quem e por quê — nada acontece em silêncio."""
    from app.apps import notificador
    mandou = []
    monkeypatch.setattr(notificador, "_tg_notificar",
                        lambda **k: mandou.append(k) or {"ok": True})
    monkeypatch.setattr(notificador, "_wa_enviar_texto",
                        lambda *a, **k: mandou.append(a) or {"ok": True})

    r = notificador.notificar(telefone="85999990003", mensagem="oi",
                              finalidade="erp.titulo_pago")

    assert mandou == [], "a mensagem NÃO pode sair"
    assert "Cid Desligado" in r["mensageria"]["detalhe"]
    linha = conn.execute(text("SELECT status, detalhe FROM mensageria.envios "
                              " ORDER BY id DESC LIMIT 1")).mappings().first()
    assert linha["status"] == "BARRADO"
    assert "desligada da empresa" in linha["detalhe"]


def test_o_notificador_deixa_passar_quem_esta_ativo(conn, gente, monkeypatch):
    from app.apps import notificador
    mandou = []
    monkeypatch.setattr(notificador, "_tg_notificar",
                        lambda **k: mandou.append(k) or {"ok": True, "chat_id": "1"})

    notificador.notificar(telefone="85999990001", mensagem="oi",
                          finalidade="erp.titulo_pago")

    assert len(mandou) == 1


# ---------------------------------------------------------------------------
# O veredito na FICHA da pessoa — "por que esta não recebe?"
# ---------------------------------------------------------------------------
def test_a_ficha_conclui_em_vez_de_mostrar_so_os_pedacos(conn, gente):
    tc.gravar_cadastro(conn, chat_id=4242, cpf="22222222222")

    d = tc.resumo_da_pessoa(conn, gente["bia"])

    assert d["diagnostico"] == "RECEBE"
    assert d["telegram"]["chat_id"] == "4242"
    assert d["diagnostico_rotulo"] and d["diagnostico_explicacao"]


def test_a_ficha_de_quem_saiu_diz_que_o_robo_continua_respondendo(conn, gente):
    """O cuidado que faz o bloqueio não virar um problema novo: quem saiu ainda
    consegue pedir o último contracheque."""
    d = tc.resumo_da_pessoa(conn, gente["cid"])

    assert d["diagnostico"] == "DESLIGADO_DA_EMPRESA"
    assert "contracheque" in d["diagnostico_explicacao"]


def test_a_ficha_e_a_tela_de_gestao_nunca_discordam(conn, gente):
    """As duas respondem a mesma pergunta; se divergirem, uma está mentindo."""
    tc.gravar_cadastro(conn, chat_id=4343, cpf="22222222222")
    da_lista = {p["colaborador_id"]: p["diagnostico"] for p in tc.painel(conn)["pessoas"]}

    for cid, esperado in da_lista.items():
        if cid:
            assert tc.resumo_da_pessoa(conn, cid)["diagnostico"] == esperado


# ---------------------------------------------------------------------------
# 23/11/2026 — o caso do Manoel: recebia, e depois da migração parou, porque o
# telefone dele no cadastro estava SEM DDD. A migração o deixou "fora".
# ---------------------------------------------------------------------------
def _manoel(conn, telefone="98877-6655"):
    return _colab(conn, "Manoel Sem DDD", "66666666666", telefone)


def test_migracao_traz_pelo_cpf_quem_tem_telefone_sem_ddd(conn, gente):
    m = _manoel(conn)
    planilha = [PLANILHA[0], ["01/10/2026", "666.666.666-66", "5585988776655", "800", "MANOEL", "TELEFONE", ""]]

    r = tc.migrar_planilha(conn, planilha, por="Marcelo")

    assert r["contagem"]["fora"] == 0
    assert [x["nome"] for x in r["divergente"]] == ["Manoel Sem DDD"]   # entra, e o DP completa o DDD
    cad = tc.cadastro_por_chat(conn, 800)
    assert cad["colaborador_id"] == m and cad["ativo"] and "sem DDD" in cad["observacao"]


def test_migracao_traz_pelo_final_do_numero_quando_a_planilha_nao_tem_cpf(conn, gente):
    m = _manoel(conn, "988776655")
    planilha = [PLANILHA[0], ["01/10/2026", "", "5585988776655", "801", "MANOEL", "TELEFONE", ""]]

    tc.migrar_planilha(conn, planilha)

    assert tc.cadastro_por_chat(conn, 801)["colaborador_id"] == m


def test_final_igual_em_duas_pessoas_nao_liga_ninguem(conn, gente):
    _manoel(conn, "988776655")
    _colab(conn, "Outro Sem DDD", "77777777777", "98877-6655")

    assert tc.achar_pessoa(conn, telefone="5585988776655") is None


def test_o_robo_acha_pelo_numero_confirmado_quem_esta_sem_ddd(conn, gente):
    """É o caminho do botão 📱: o número que o Telegram confirmou termina com
    o do cadastro. Com e sem o nono dígito."""
    _manoel(conn)
    assert tc.achar_pessoa(conn, telefone="+55 85 98877-6655")["nome"] == "Manoel Sem DDD"
    assert tc.achar_pessoa(conn, telefone="85 8877-6655")["nome"] == "Manoel Sem DDD"
    assert tc.telefone_bate("5585988776655", tc.achar_pessoa(conn, telefone="5585988776655"))
    assert tc.achar_pessoa(conn, telefone="5585911112222") is None


def test_envio_pelo_telefone_sem_ddd_do_cadastro_acha_o_telegram(conn, gente):
    _manoel(conn)
    tc.gravar_cadastro(conn, chat_id=802, cpf="66666666666", telefone="5585988776655")
    assert tc.chat_id_de(conn, telefone="98877-6655") == "802"


def test_quem_esta_sem_ddd_e_ativo_nao_e_barrado_como_desligado(conn, gente):
    _manoel(conn)
    assert tc.desligado_da_empresa(conn, telefone="5585988776655") is None


def test_migrar_de_novo_nao_religa_quem_foi_desligado_na_tela(conn, gente):
    tc.migrar_planilha(conn, PLANILHA)
    cad = tc.cadastro_por_chat(conn, 700)
    tc.desvincular(conn, cad["id"], "Marcelo")

    tc.migrar_planilha(conn, PLANILHA)

    assert tc.cadastro_por_chat(conn, 700) is None


def test_migrar_de_novo_nao_derruba_a_conta_nova_de_quem_trocou_de_telegram(conn, gente):
    tc.migrar_planilha(conn, PLANILHA)
    tc.gravar_cadastro(conn, chat_id=7000, cpf="11111111111", telefone="5585999990001")   # trocou de conta
    assert tc.cadastro_por_chat(conn, 700) is None

    tc.migrar_planilha(conn, PLANILHA)

    assert tc.cadastro_por_chat(conn, 7000)["ativo"]
    assert tc.cadastro_por_chat(conn, 700) is None


# ---------------------------------------------------------------------------
# 23/11/2026 — o ID do Telegram na ficha, editável
# ---------------------------------------------------------------------------
@pytest.fixture
def telegram_falso(monkeypatch):
    contas = {"555001": {"ok": True, "nome": "Ana L.", "usuario": "analima", "tipo": "private"}}

    def _falso(chat_id):
        return contas.get(chat_id) or {"ok": False, "erro": "o Telegram não conhece esta conversa"}
    monkeypatch.setattr(tc, "_consultar_telegram", _falso)
    return contas


def test_trocar_o_id_confere_primeiro_e_so_grava_com_confirmacao(conn, gente, telegram_falso):
    previa = tc.trocar_telegram_da_pessoa(conn, gente["ana"], "555001", por="Marcelo")
    assert previa["nome_telegram"] == "Ana L." and previa["gravado"] is False
    assert tc.cadastro_por_chat(conn, 555001) is None

    r = tc.trocar_telegram_da_pessoa(conn, gente["ana"], "555001", por="Marcelo", confirmar=True)

    cad = tc.cadastro_por_chat(conn, 555001)
    assert r["gravado"] and cad["colaborador_id"] == gente["ana"] and cad["origem"] == "FICHA"
    assert cad["telefone"] == ""            # o número é confirmado no próximo /start (regra rígida)
    assert "Marcelo" in cad["observacao"]


def test_trocar_o_id_desliga_o_antigo(conn, gente, telegram_falso):
    tc.gravar_cadastro(conn, chat_id=4321, cpf="11111111111", telefone="5585999990001")

    r = tc.trocar_telegram_da_pessoa(conn, gente["ana"], "555001", por="Marcelo", confirmar=True)

    assert r["anterior"] == "4321"
    assert tc.cadastro_por_chat(conn, 4321) is None
    assert tc.resumo_da_pessoa(conn, gente["ana"])["telegram"]["chat_id"] == "555001"


def test_id_que_o_telegram_nao_conhece_e_recusado(conn, gente, telegram_falso):
    with pytest.raises(ValueError, match="não conhece"):
        tc.trocar_telegram_da_pessoa(conn, gente["ana"], "999999", confirmar=True)
    assert tc.cadastro_por_chat(conn, 999999) is None


def test_id_de_outra_pessoa_nao_e_tomado(conn, gente, telegram_falso):
    tc.gravar_cadastro(conn, chat_id=555001, cpf="22222222222", telefone="5585999990002")
    with pytest.raises(ValueError, match="Bia Rocha"):
        tc.trocar_telegram_da_pessoa(conn, gente["ana"], "555001", confirmar=True)
    assert tc.cadastro_por_chat(conn, 555001)["cpf"] == "22222222222"


def test_id_vazio_desliga_e_desligado_da_empresa_nao_ganha_id(conn, gente, telegram_falso):
    tc.gravar_cadastro(conn, chat_id=4321, cpf="11111111111", telefone="5585999990001")
    tc.trocar_telegram_da_pessoa(conn, gente["ana"], "", por="Marcelo", confirmar=True)
    assert tc.resumo_da_pessoa(conn, gente["ana"])["telegram"] is None
    with pytest.raises(ValueError, match="desligada"):
        tc.trocar_telegram_da_pessoa(conn, gente["cid"], "555001")
    with pytest.raises(LookupError):
        tc.trocar_telegram_da_pessoa(conn, 10 ** 9, "555001")


# ---------------------------------------------------------------------------
# 23/11/2026 — a aba Enviadas diz PARA QUEM foi, e o nome abre a ficha
# ---------------------------------------------------------------------------
def test_enviadas_diz_o_nome_de_quem_recebeu(conn, gente):
    from app.apps.mensageria import painel
    tc.gravar_cadastro(conn, chat_id=4321, cpf="22222222222", telefone="5585999990002")
    core.registrar(conn, tipo="erp.titulo_pago", canal="telegram", destinatario="4321", cpf="",
                   texto="pago", nome_arquivo="", status="ENVIADO", detalhe="")             # pelo ID
    core.registrar(conn, tipo="ponto.qr", canal="whatsapp", destinatario="5585999990001", cpf="",
                   texto="QR", nome_arquivo="", status="ENVIADO", detalhe="")              # pelo telefone
    core.registrar(conn, tipo="ponto.qr", canal="whatsapp", destinatario="5511900000000", cpf="",
                   texto="QR", nome_arquivo="", status="ENVIADO", detalhe="")              # de ninguém

    por = {l["destinatario"]: l for l in painel.envios_com_pessoa(conn)}

    assert (por["4321"]["para_nome"], por["4321"]["para_colaborador_id"]) == ("Bia Rocha", gente["bia"])
    assert (por["5585999990001"]["para_nome"], por["5585999990001"]["para_colaborador_id"]) == ("Ana Lima", gente["ana"])
    assert (por["5511900000000"]["para_nome"], por["5511900000000"]["para_colaborador_id"]) == ("", None)


def test_enviadas_busca_pelo_nome(conn, gente):
    from app.apps.mensageria import painel
    tc.gravar_cadastro(conn, chat_id=4321, cpf="22222222222", telefone="5585999990002")
    core.registrar(conn, tipo="erp.titulo_pago", canal="telegram", destinatario="4321", cpf="",
                   texto="pago", nome_arquivo="", status="ENVIADO", detalhe="")
    core.registrar(conn, tipo="ponto.qr", canal="whatsapp", destinatario="5585999990001", cpf="",
                   texto="QR", nome_arquivo="", status="ENVIADO", detalhe="")

    assert [l["para_nome"] for l in painel.envios_com_pessoa(conn, busca="bia")] == ["Bia Rocha"]
    assert [l["para_nome"] for l in painel.envios_com_pessoa(conn, busca="QR")] == ["Ana Lima"]   # texto ainda vale


def test_aviso_do_make_com_55_repetido_acha_o_telegram(conn, gente):
    tc.gravar_cadastro(conn, chat_id=8972386892, cpf="11111111111", telefone="5585999990001")
    assert tc.chat_id_de(conn, telefone="55555585999990001") == "8972386892"


def test_enviadas_da_nome_a_quem_esta_com_telefone_sem_ddd(conn, gente):
    from app.apps.mensageria import painel
    m = _colab(conn, "Fulano Sem DDD", "88888888888", "99102-1992")
    core.registrar(conn, tipo="sem_tipo", canal="telegram", destinatario="5582991021992", cpf="",
                   texto="SP validada", nome_arquivo="", status="SEM_DESTINO", detalhe="")
    l = painel.envios_com_pessoa(conn)[0]
    assert (l["para_nome"], l["para_colaborador_id"]) == ("Fulano Sem DDD", m)
