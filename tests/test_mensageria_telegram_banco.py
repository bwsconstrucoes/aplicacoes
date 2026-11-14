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
    assert r["contagem"] == {"ok": 1, "divergente": 1, "fora": 2, "sem_chat": 1}
    assert [x["nome"] for x in r["divergente"]] == ["Bia Rocha"]
    assert r["divergente"][0]["telefone_erp"] == "558599990002"
    assert {x["nome"] for x in r["fora"]} == {"CID", "Fulano de fora"}
    assert tc.migrado(conn) is True
    assert tc.cadastro_por_chat(conn, 700)["colaborador_id"] == gente["ana"]
    assert tc.cadastro_por_chat(conn, 702)["cpf"] == "22222222222"          # divergente entra, ativo
    assert tc.cadastro_por_chat(conn, 703) is None and tc.cadastro_por_chat(conn, 701) is None
    assert tc.relatorio_migracao(conn)["contagem"]["ok"] == 1
    r2 = tc.migrar_planilha(conn, PLANILHA)
    assert (r2["novos"], r2["atualizados"]) == (0, 2)


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
