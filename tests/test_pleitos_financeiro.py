"""Os pleitos do financeiro: tipos, subtipos, campos e condições (27/11/2026).

O pedido do dono: *"Não tem sentido por exemplo exibir campo de boleto quando o
tipo de pagamento for pix. Portanto revise toda essa lógica pra funcionar
corretamente."* — e a mesma lógica vale para o ERP, nos lançamentos que existem
nos dois lugares.

O catálogo é UMA fonte (`core/titulos/pleitos.py`): o portal desenha por ele, a
tela Lançar do ERP lê dele e o servidor confere por ele. Estes testes cobram a
regra, a coerência interna do catálogo e o mapa que o dono revisa.
"""
from __future__ import annotations

import pathlib

import pytest

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.titulos import pleitos
from app.apps.erp.db.models.cadastros import PerfilUsuario, TipoTitulo, Usuario


def _pagamento(**extra):
    base = {
        "subtipo": "NOTA_MATERIAL", "fornecedor_id": 1, "forma_pagamento": "PIX",
        "fornecedor_conta_id": 9, "documento_numero": "123",
        "data_emissao_doc": "2026-11-20", "descricao": "Cimento para a laje",
        "valor_bruto": "100.00", "obra_id": 3, "categoria_id": 4,
        "parcelas": [{"vencimento": "2026-12-10", "valor": "100.00",
                      "linha_digitavel": ""}],
    }
    base.update(extra)
    return base


# ---------------------------------------------------------------- a forma manda
def test_pix_descarta_a_linha_digitavel_e_boleto_descarta_a_conta():
    """O exemplo dele: campo de boleto num Pix não tem sentido — e também não
    pode ir para dentro do título sem ninguém conferir."""
    pix = pleitos.limpar_pela_forma({"forma_pagamento": "PIX", "fornecedor_conta_id": 9,
                                     "parcelas": [{"linha_digitavel": "123"}]})
    assert pix["parcelas"][0]["linha_digitavel"] == ""
    assert pix["fornecedor_conta_id"] == 9
    boleto = pleitos.limpar_pela_forma({"forma_pagamento": "BOLETO",
                                        "fornecedor_conta_id": 9,
                                        "parcelas": [{"linha_digitavel": "123"}]})
    assert boleto["fornecedor_conta_id"] is None
    assert boleto["parcelas"][0]["linha_digitavel"] == "123"
    debito = pleitos.limpar_pela_forma({"forma_pagamento": "DEBITO_AUTOMATICO",
                                        "fornecedor_conta_id": 9,
                                        "parcelas": [{"linha_digitavel": "1"}]})
    assert debito["fornecedor_conta_id"] is None
    assert debito["parcelas"][0]["linha_digitavel"] == ""


def test_boleto_exige_a_linha_digitavel_e_a_guia_nao():
    with pytest.raises(ErroValidacao, match="linha digitável da parcela 1"):
        pleitos.conferir_pagamento(_pagamento(forma_pagamento="BOLETO"), porta="portal")
    guia = _pagamento(subtipo="GUIA_TRIBUTO", forma_pagamento="GUIA",
                      mes_referencia="2026-10")
    assert pleitos.conferir_pagamento(guia, porta="portal")["tipo"] == "T8_TRIBUTO_GUIA"


def test_pix_e_ted_exigem_a_conta_do_credor():
    with pytest.raises(ErroValidacao, match="Conta do credor"):
        pleitos.conferir_pagamento(_pagamento(fornecedor_conta_id=None), porta="portal")


def test_forma_que_nao_serve_ao_subtipo_e_recusada_com_as_que_servem():
    """Pessoa física não emite boleto; guia não se paga por Pix."""
    with pytest.raises(ErroValidacao, match="Formas que servem: Pix, Transferência"):
        pleitos.conferir_pagamento(_pagamento(subtipo="PESSOA_FISICA",
                                              forma_pagamento="BOLETO"), porta="portal")
    with pytest.raises(ErroValidacao, match="não se paga por Pix"):
        pleitos.conferir_pagamento(_pagamento(subtipo="GUIA_TRIBUTO",
                                              mes_referencia="2026-10"), porta="portal")


# ------------------------------------------------------------ o subtipo manda
def test_nota_exige_o_numero_e_a_data_de_emissao():
    with pytest.raises(ErroValidacao, match="Nº da nota fiscal") as e:
        pleitos.conferir_pagamento(_pagamento(documento_numero="",
                                              data_emissao_doc=None), porta="portal")
    assert "Data de emissão" in str(e.value)


def test_conta_de_consumo_pede_o_mes_e_descarta_a_data_de_emissao():
    d = pleitos.conferir_pagamento(_pagamento(
        subtipo="CONCESSIONARIA", forma_pagamento="DEBITO_AUTOMATICO",
        mes_referencia="2026-10", documento_numero=""), porta="portal")
    assert "data_emissao_doc" not in d, "a conta de consumo é do mês, não do dia"
    assert d["competencia"] == "2026-10"
    with pytest.raises(ErroValidacao, match="Mês de referência"):
        pleitos.conferir_pagamento(_pagamento(
            subtipo="CONCESSIONARIA", forma_pagamento="DEBITO_AUTOMATICO"), porta="portal")


def test_despesa_sem_nota_exige_justificativa_de_verdade():
    sem = _pagamento(subtipo="SEM_NOTA", justificativa_excecao="")
    with pytest.raises(ErroValidacao, match="Por que não há nota"):
        pleitos.conferir_pagamento(sem, porta="portal")
    curta = _pagamento(subtipo="SEM_NOTA", justificativa_excecao="não tem")
    with pytest.raises(ErroValidacao, match="mínimo 15"):
        pleitos.conferir_pagamento(curta, porta="portal")


def test_campo_de_outro_subtipo_e_descartado():
    """A justificativa de exceção esquecida numa nota de material marcaria o
    título com um motivo que ninguém escreveu para ele."""
    d = pleitos.conferir_pagamento(_pagamento(justificativa_excecao="qualquer coisa longa"),
                                   porta="portal")
    assert "justificativa_excecao" not in d
    assert "mes_referencia" not in d


def test_chave_de_acesso_errada_e_recusada():
    with pytest.raises(ErroValidacao, match="44 dígitos"):
        pleitos.conferir_pagamento(_pagamento(chave_acesso="1234"), porta="portal")


def test_subtipo_do_escritorio_nao_entra_pelo_portal():
    folha = _pagamento(subtipo="FOLHA_ENCARGOS", mes_referencia="2026-10")
    with pytest.raises(ErroValidacao, match="Escolha o que está sendo pago"):
        pleitos.conferir_pagamento(folha, porta="portal")
    assert pleitos.conferir_pagamento(folha, porta="erp")["tipo"] == "T7_FOLHA_ENCARGOS"


# --------------------------------------------------------------- documentos
def test_nota_com_boleto_exige_os_dois_documentos():
    with pytest.raises(ErroValidacao, match="Falta anexar: Boleto"):
        pleitos.conferir_pagamento(
            _pagamento(forma_pagamento="BOLETO", fornecedor_conta_id=None,
                       parcelas=[{"vencimento": "2026-12-10", "valor": "100",
                                  "linha_digitavel": "1" * 47}]),
            porta="portal", documentos=["NOTA"])
    # em Pix o boleto não existe — só a nota é cobrada
    pleitos.conferir_pagamento(_pagamento(), porta="portal", documentos=["NOTA"])
    with pytest.raises(ErroValidacao, match="Falta anexar: Nota fiscal"):
        pleitos.conferir_pagamento(_pagamento(), porta="portal", documentos=[])


def test_quem_nao_declara_documentos_nao_e_cobrado_deles():
    """Integração que chama a rota por fora não sabe dos anexos: a regra dos
    campos vale, a dos documentos não tem como valer."""
    pleitos.conferir_pagamento(_pagamento(), porta="erp", documentos=None)


def test_recibo_ou_comprovante_servem_para_a_despesa_sem_nota():
    d = _pagamento(subtipo="SEM_NOTA", justificativa_excecao="feira livre, não emite nota")
    pleitos.conferir_pagamento(d, porta="portal", documentos=["COMPROVANTE"])
    with pytest.raises(ErroValidacao, match="Recibo ou comprovante"):
        pleitos.conferir_pagamento(d, porta="portal", documentos=["OUTRO"])


# ------------------------------------------------------------ fundo fixo
def test_adiantamento_exige_o_periodo_e_reembolso_nao():
    item = {"data_despesa": "2026-11-10", "descricao": "almoço equipe",
            "valor": "50", "obra_id": 1}
    with pytest.raises(ErroValidacao, match="Período"):
        pleitos.conferir_fundo_fixo({"fundo_fixo_tipo": "ADIANTAMENTO", "itens": [item]})
    pleitos.conferir_fundo_fixo({"fundo_fixo_tipo": "REEMBOLSO", "itens": [item]})


def test_despesa_do_fundo_fixo_sem_obra_ou_sem_data_e_recusada():
    with pytest.raises(ErroValidacao, match="obra da despesa 1"):
        pleitos.conferir_fundo_fixo({"fundo_fixo_tipo": "REEMBOLSO", "itens": [
            {"data_despesa": "2026-11-10", "descricao": "almoço", "valor": "50"}]})
    with pytest.raises(ErroValidacao, match="data da despesa da despesa 1"):
        pleitos.conferir_fundo_fixo({"fundo_fixo_tipo": "REEMBOLSO", "itens": [
            {"descricao": "almoço", "valor": "50", "obra_id": 1}]})


def test_fatura_de_cartao_nao_passa_pela_regra_do_fundo_fixo():
    assert pleitos.conferir_fundo_fixo({"modalidade": "CARTAO"})["modalidade"] == "CARTAO"


# ------------------------------------------------------------ empreita
def _contrato(**extra):
    base = {"modo": "MEDICAO", "obra_id": 1, "fornecedor_id": 2,
            "objeto": "alvenaria de vedação", "data_inicio": "2026-11-01",
            "exige_foto": True,
            "itens": [{"descricao": "alvenaria", "unidade": "m²",
                       "quantidade": "100", "preco_unitario": "30"}]}
    base.update(extra)
    return base


def test_contrato_por_medicao_exige_a_planilha_com_unidade():
    with pytest.raises(ErroValidacao, match="Planilha de serviços"):
        pleitos.conferir_contrato(_contrato(itens=[]))
    with pytest.raises(ErroValidacao, match="unidade"):
        pleitos.conferir_contrato(_contrato(itens=[{"descricao": "x", "quantidade": "1",
                                                    "preco_unitario": "2"}]))


def test_contrato_por_medicao_descarta_o_valor_total_digitado():
    """Um valor total esquecido passaria por cima da soma da planilha."""
    d = pleitos.conferir_contrato(_contrato(valor_total="999999"))
    assert "valor_total" not in d


def test_contrato_em_parcelas_exige_valor_e_parcelas_e_descarta_a_planilha():
    with pytest.raises(ErroValidacao, match="Valor total do contrato"):
        pleitos.conferir_contrato(_contrato(modo="PARCELAS"))
    d = pleitos.conferir_contrato(_contrato(modo="PARCELAS", valor_total="5000",
                                            parcelas_previstas="5"))
    assert d["itens"] == []


def test_medicao_exige_o_periodo():
    """É o período que pega medir duas vezes o mesmo serviço (crítica M3)."""
    with pytest.raises(ErroValidacao, match="Período medido"):
        pleitos.conferir_medicao({"valor_medido": "100"}, jeito="SEM_PLANILHA",
                                 contrato_id=1)


def test_medicao_pela_planilha_descarta_valor_e_exige_quantidade():
    ok = pleitos.conferir_medicao({"periodo_inicio": "2026-11-01",
                                   "periodo_fim": "2026-11-15", "valor_medido": "999",
                                   "itens": [{"contrato_item_id": 1, "quantidade": "10"}]},
                                  jeito="PLANILHA", contrato_id=1)
    assert "valor_medido" not in ok
    with pytest.raises(ErroValidacao, match="Quanto foi executado"):
        pleitos.conferir_medicao({"periodo_inicio": "2026-11-01",
                                  "periodo_fim": "2026-11-15",
                                  "itens": [{"contrato_item_id": 1, "quantidade": "0"}]},
                                 jeito="PLANILHA", contrato_id=1)


# --------------------------------------------------- o catálogo é coerente
def test_todo_subtipo_aponta_um_tipo_que_existe_e_formas_que_existem():
    for chave, st in pleitos.SUBTIPOS_PAGAMENTO.items():
        TipoTitulo(st["tipo"])
        assert st["formas"], chave
        assert set(st["formas"]) <= set(pleitos.FORMAS), chave
        for d in st["documentos"]:
            cats = d["categoria"] if isinstance(d["categoria"], tuple) else (d["categoria"],)
            assert set(cats) <= set(pleitos.DOCUMENTOS_ROTULOS), chave


def test_toda_etiqueta_de_documento_e_aceita_pelo_arquivo():
    from app.apps.erp.core.documentos.armazenamento import CATEGORIAS
    assert set(pleitos.DOCUMENTOS_ROTULOS) <= set(CATEGORIAS)


def test_condicao_so_fala_de_campo_que_existe():
    """Condição escrita sobre um campo que não existe nunca dispara — e o campo
    some ou fica opcional sem ninguém perceber."""
    grupos = [pleitos.campos_do_pagamento(k) for k in pleitos.SUBTIPOS_PAGAMENTO] + [
        list(pleitos.CAMPOS_FUNDO_FIXO), list(pleitos.CAMPOS_CONTRATO)]
    for campos in grupos:
        nomes = {c["nome"] for c in campos}
        for c in campos:
            for cond in (c.get("visivel_se"), c.get("obrigatorio_se")):
                for campo in (cond or {}):
                    assert campo in nomes, (c["nome"], campo)


def test_a_sugestao_pela_leitura_so_aponta_o_que_existe():
    from app.apps.erp.core.documentos.leitor import TIPOS
    assert set(pleitos.SUBTIPO_PELO_DOCUMENTO) <= set(TIPOS)
    assert set(pleitos.SUBTIPO_PELO_DOCUMENTO.values()) <= set(pleitos.SUBTIPOS_PAGAMENTO)
    assert set(pleitos.CATEGORIA_PELO_DOCUMENTO) <= set(TIPOS)


def test_o_mapa_do_repositorio_e_o_que_o_codigo_gera():
    """O dono revisa o mapa. Mapa velho é revisar uma regra que não existe."""
    arquivo = (pathlib.Path(__file__).resolve().parents[1]
               / "app" / "apps" / "erp" / "PLEITOS_FINANCEIRO.md")
    assert arquivo.read_text(encoding="utf-8").strip() == pleitos.mapa_em_markdown().strip(), (
        "rode `python -m app.apps.erp.core.titulos.pleitos` para gerar o mapa de novo")


# --------------------------------------------------- portal: só o administrador
def test_a_area_financeiro_do_portal_abre_so_para_o_administrador():
    from app.apps.erp.core import portal as svc_portal
    from app.apps.erp.core.titulos import portal_financeiro as pf
    assert pf.EM_TESTE_SO_ADMIN, "abrir para a obra é decisão dele — este teste muda junto"
    admin = Usuario(nome="Adm", email="adm@t.com", perfil=PerfilUsuario.ADMIN, ativo=True)
    obra = Usuario(nome="Obra", email="obra@t.com",
                   perfil=PerfilUsuario.ADMINISTRATIVO_OBRA, ativo=True)
    obra.permissoes_extras = {}
    admin.permissoes_extras = {}

    def financeiro(u):
        return next(a for a in svc_portal.areas(None, {"usuario": u})
                    if a["chave"] == "financeiro")

    assert financeiro(admin)["pronta"] is True
    fechada = financeiro(obra)
    assert fechada["pronta"] is False, "a obra continua vendo a área fechada"
    assert fechada["enquanto_isso"], "e a frase do que fazer enquanto isso"


def test_rotas_do_financeiro_do_portal_moram_no_portal():
    """Quem é da obra só abre telas em /erp/portal… — tela fora disso leva de
    volta ao portal (24/11/2026)."""
    from app.main import app
    rotas = [r.rule for r in app.url_map.iter_rules()
             if r.endpoint.startswith("erp.") and "financeiro" in r.endpoint
             and "portal" in r.endpoint]
    assert rotas and all(r.startswith("/erp/portal") for r in rotas), rotas
