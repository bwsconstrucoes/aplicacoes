"""Anexar um documento à pergunta — e o aviso que não pode sumir da tela.

ESTA É A ÚNICA PARTE DA TELA DE PERGUNTAR EM QUE A RESPOSTA VEM DA IA.

Todo o resto é número calculado pelo sistema, por código escrito e testado.
Aqui não: a IA lê o arquivo e diz o que viu. E ela lê errado às vezes — troca
um dígito do valor, confunde a data de emissão com a de vencimento.

Por que vale mesmo assim: **o documento está na mão de quem perguntou.** Dá
para conferir olhando o papel. É diferente de um total somado sobre dez mil
lançamentos, que ninguém tem como recalcular — e é por isso que aquele é
código e este pode ser IA.

A condição para isso continuar aceitável é a tela DIZER de onde veio a
resposta. Se o aviso amarelo sumir, a leitura da IA passa a se parecer com os
números calculados, e a distinção inteira se perde. É isso que este arquivo
guarda.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.apps.erp import routes

TELA = Path("app/apps/erp/templates/erp_perguntar.html").read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# O aviso que separa "eu calculei" de "eu li"
# ---------------------------------------------------------------------------
def test_a_tela_avisa_que_a_leitura_veio_da_ia():
    assert "lido do arquivo pela IA, não calculado pelo sistema" in TELA


def test_a_tela_manda_conferir_no_documento():
    assert "Confira no próprio documento" in TELA


def test_a_tela_deixa_claro_que_nada_foi_gravado():
    """Ler não é arquivar. Quem anexa para perguntar não espera que o arquivo
    entre no acervo da empresa — e quem quer arquivar precisa saber que ainda
    não arquivou."""
    assert "Nada foi gravado" in TELA
    assert "/erp/arquivo" in TELA


def test_o_aviso_e_do_tipo_que_chama_atencao():
    """Escondido num rodapé cinza, o aviso não cumpre função nenhuma."""
    cartao = TELA.split('id="cartao-documento"')[1][:900]
    assert 'class="aviso alerta"' in cartao


# ---------------------------------------------------------------------------
# O que a leitura devolve para a tela
# ---------------------------------------------------------------------------
def test_campo_vazio_nao_aparece_como_zero_nem_como_traco():
    """Preencher buraco com valor padrão é o jeito mais fácil de o sistema
    afirmar o que ele não sabe. Campo que a IA não leu simplesmente não vem."""
    resumo = routes._resumo_do_documento(
        {"tipo_documento": "NFSE", "emitente_nome": "", "valor_total": None,
         "numero_documento": "  ", "descricao": "Serviço de alvenaria"},
        "nota.pdf")
    rotulos = {l["rotulo"] for l in resumo["linhas"]}
    assert rotulos == {"Tipo", "Descrição"}


def test_a_string_None_tambem_e_buraco():
    """A IA às vezes devolve o texto "None". Passar isso adiante mostraria
    "None" na tela como se fosse conteúdo."""
    resumo = routes._resumo_do_documento({"emitente_nome": "None"}, "x.pdf")
    assert resumo["linhas"] == []


def test_o_que_a_ia_nao_conseguiu_ler_e_devolvido(monkeypatch):
    """A IA declara a dúvida dela em "observacoes". Engolir isso deixaria a
    tabela com cara de completa."""
    resumo = routes._resumo_do_documento(
        {"tipo_documento": "RECIBO",
         "observacoes": "o valor está borrado na foto"}, "foto.jpg")
    assert "borrado" in resumo["observacoes"]


def test_a_tela_mostra_a_duvida_da_ia():
    assert "O que ficou em" in TELA and "dúvida" in TELA


# ---------------------------------------------------------------------------
# Nada sai no formato do banco
#
# A primeira versão desta tela mostrou, no navegador: "NFSE", "2026-09-02",
# "12480.00" e "11222333000144". Está tudo certo — e tudo ilegível. É o mesmo
# defeito que a tabela de respostas teve no mesmo dia, e a mesma correção:
# quem formata é o servidor, num lugar só.
# ---------------------------------------------------------------------------
def _valores(lido):
    r = routes._resumo_do_documento(lido, "x.pdf")
    return {l["rotulo"]: l["valor"] for l in r["linhas"]}


def test_o_tipo_do_documento_sai_por_extenso():
    assert _valores({"tipo_documento": "NFSE"})["Tipo"] == \
        "Nota fiscal de serviço (NFS-e)"
    assert _valores({"tipo_documento": "FATURA_CONCESSIONARIA"})["Tipo"] == \
        "Fatura de concessionária"


def test_tipo_que_a_ia_inventou_nao_derruba_nada():
    """A IA às vezes devolve um tipo fora da lista. Mostrar o que ela disse é
    melhor que apagar — mas não pode quebrar a tela."""
    assert _valores({"tipo_documento": "COISA_ESTRANHA"})["Tipo"] == "COISA_ESTRANHA"


def test_a_data_sai_em_portugues():
    assert _valores({"data_emissao": "2026-09-02"})["Emissão"] == "02/09/2026"


def test_a_competencia_sai_como_mes_e_ano():
    assert _valores({"competencia": "2026-09"})["Competência"] == "09/2026"


def test_data_que_nao_e_data_volta_como_veio():
    """Inventar uma data a partir de texto estranho é pior que mostrar o
    texto — o número inventado parece certo."""
    assert _valores({"data_emissao": "ilegível"})["Emissão"] == "ilegível"


def test_o_valor_sai_com_real_e_virgula():
    assert _valores({"valor_total": "12480.00"})["Valor total"] == "R$ 12.480,00"


def test_o_cnpj_sai_pontuado():
    assert _valores({"emitente_documento": "11222333000144"})[
        "CNPJ/CPF de quem emitiu"] == "11.222.333/0001-44"


def test_o_cpf_tambem():
    assert _valores({"emitente_documento": "12345678901"})[
        "CNPJ/CPF de quem emitiu"] == "123.456.789-01"


def test_documento_com_tamanho_estranho_volta_como_veio():
    assert _valores({"emitente_documento": "123"})[
        "CNPJ/CPF de quem emitiu"] == "123"


def test_nenhum_valor_sai_com_ponto_decimal_americano():
    """Varredura: qualquer campo de dinheiro tem de passar pelo formatador."""
    valores = _valores({"valor_total": "1234.50", "valor_liquido": "999.99"})
    for rotulo, valor in valores.items():
        assert not valor.endswith(".50"), f"{rotulo} saiu em formato americano"
        assert not valor.endswith(".99"), f"{rotulo} saiu em formato americano"


def test_a_formatacao_reusa_o_modulo_de_sempre():
    """Uma segunda função de formatar dinheiro divergiria da primeira no dia
    em que alguém corrigisse só uma."""
    import inspect
    fonte = inspect.getsource(routes._resumo_do_documento)
    assert "core.comum.formato import" in fonte


def test_os_rotulos_sao_em_portugues_de_gente():
    resumo = routes._resumo_do_documento(
        {"emitente_nome": "Ferragens LTDA", "emitente_documento": "123",
         "valor_total": "100.00", "obra_mencionada": "CREPETRIUNFO"}, "x.pdf")
    rotulos = [l["rotulo"] for l in resumo["linhas"]]
    assert rotulos == ["Quem emitiu", "CNPJ/CPF de quem emitiu",
                       "Valor total", "Obra citada"]
    for r in rotulos:
        assert "_" not in r, f"{r} é nome de campo do banco, não de tela"


# ---------------------------------------------------------------------------
# Reuso: é o MESMO leitor do Arquivo
# ---------------------------------------------------------------------------
def test_a_leitura_reusa_o_leitor_do_arquivo():
    """Um segundo leitor viraria uma segunda opinião sobre o mesmo papel —
    e os dois divergiriam no dia em que alguém melhorasse só um."""
    import inspect
    fonte = inspect.getsource(routes.api_pergunta_com_documento)
    assert "core.documentos.leitor import" in fonte
    assert "ler_documento" in fonte


def test_a_leitura_declara_a_operacao_para_o_painel_de_consumo():
    """Sem declarar, o gasto entra como 'leitura_documento' e some no meio do
    Arquivo — e aí não dá para saber quanto o assistente custa."""
    import inspect
    fonte = inspect.getsource(routes.api_pergunta_com_documento)
    assert 'operacao="pergunta_com_documento"' in fonte


@pytest.mark.banco
def test_sem_arquivo_a_resposta_e_clara_e_nao_um_erro_feio(sessao_real, app_real):
    """Anexar nada não pode virar tela branca com erro 500."""
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import PerfilUsuario as P, Usuario
    from conftest import como

    u = Usuario(nome="Marcelo", email="chefe@bws.test",
                senha_hash=gerar_hash("senha-de-teste"), perfil=P.ADMIN)
    sessao_real.add(u)
    sessao_real.flush()

    for rota, campo in (("/erp/api/perguntar/documento", "documento"),
                        ("/erp/api/perguntar/ouvir", "audio")):
        r = como(app_real, u.id).post(rota, data={})
        assert r.status_code == 400, rota
        corpo = r.get_json()
        assert corpo["ok"] is False
        assert campo in corpo["erro"] or "chegou" in corpo["erro"]


@pytest.mark.banco
def test_quem_teve_o_acesso_ao_erp_tirado_nao_anexa_nem_fala(sessao_real, app_real):
    """As duas rotas GASTAM IA — cada anexo e cada áudio viram dólar na conta.

    Hoje todo cargo entra no ERP, então o caso que existe de verdade é o da
    pessoa que teve a entrada tirada no cadastro dela (a exceção por pessoa da
    migração 032). É esse desligamento que este teste percorre: se ele não
    valer aqui, qualquer um com uma sessão paga transcrição à vontade.
    """
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import (
        PerfilUsuario as P, Usuario, UsuarioPermissao,
    )
    from conftest import como

    desligado = Usuario(nome="Entrada suspensa", email="fora@bws.test",
                        senha_hash=gerar_hash("senha-de-teste"), perfil=P.LANCADOR)
    sessao_real.add(desligado)
    sessao_real.flush()
    sessao_real.add(UsuarioPermissao(usuario_id=desligado.id, acao="ver_erp",
                                     concedida=False))
    sessao_real.flush()

    for rota in ("/erp/api/perguntar/documento", "/erp/api/perguntar/ouvir"):
        r = como(app_real, desligado.id).post(rota, data={})
        assert r.status_code == 403, f"{rota} deixou entrar quem não vê o ERP"


def test_frase_nao_sai_em_fonte_de_maquina_de_escrever():
    """Fonte de largura fixa serve para alinhar dígito, não para ler frase.

    Na primeira versão TODO valor saía assim, e o nome do fornecedor e a
    descrição do serviço ficavam com cara de código de sistema.
    """
    r = routes._resumo_do_documento(
        {"emitente_nome": "FERRAGENS LTDA", "descricao": "Alvenaria",
         "valor_total": "10.00", "emitente_documento": "11222333000144",
         "data_emissao": "2026-09-02"}, "x.pdf")
    por_rotulo = {l["rotulo"]: l["dados"] for l in r["linhas"]}
    assert por_rotulo["Quem emitiu"] is False
    assert por_rotulo["Descrição"] is False
    assert por_rotulo["Valor total"] is True
    assert por_rotulo["CNPJ/CPF de quem emitiu"] is True
    assert por_rotulo["Emissão"] is True


def test_a_tela_obedece_a_marca_em_vez_de_decidir_sozinha():
    assert 'l.dados ? "dados" : ""' in TELA
