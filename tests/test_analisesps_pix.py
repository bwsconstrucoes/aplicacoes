# -*- coding: utf-8 -*-
"""
O QR PIX — 20/09/2026.

Nasceu de um defeito que passou meses sem ser visto porque o QR *parecia*
certo: a imagem aparecia, o aplicativo do banco lia, e só ali recusava. O
dono mandou o payload gerado para uma chave de e-mail:

    00020126590014br.gov.bcb.pix0137Chave Pix: carlosgaldino884@gmail.com...

O campo 01 (a chave) levava o rótulo "Chave Pix: " junto. Não era feio — era
um código de pagamento inútil.

Os testes daqui guardam DUAS coisas diferentes:

  1. que o rótulo sai, em todas as formas que a planilha usa para escrevê-lo;
  2. que quem monta o QR passa pela mesma limpeza — o defeito não era a
     limpeza, que existia e funcionava, era o caminho do QR não usá-la.
"""

import re

import pytest

from app.apps.analisesps import pagamentos, pix_brcode


def campo_da_chave(payload: str) -> str:
    """Devolve o conteúdo do campo 01 de dentro do bloco 26 (a chave Pix)."""
    m = re.search(r"26(\d{2})", payload)
    assert m, "payload sem o bloco 26 (conta do recebedor)"
    conta = payload[m.end():m.end() + int(m.group(1))]
    # dentro da conta: 00 = "br.gov.bcb.pix", 01 = a chave
    m2 = re.search(r"01(\d{2})", conta)
    assert m2, "bloco 26 sem o campo 01 (a chave)"
    return conta[m2.end():m2.end() + int(m2.group(1))]


# ---------------------------------------------------------------------------
# O RÓTULO
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("escrito", [
    "Chave Pix: carlosgaldino884@gmail.com",
    "chave pix: carlosgaldino884@gmail.com",
    "CHAVE PIX carlosgaldino884@gmail.com",
    "Chave Pix - carlosgaldino884@gmail.com",
    "Chave Pix – carlosgaldino884@gmail.com",
    "Chave Pix:carlosgaldino884@gmail.com",
    "Chave: carlosgaldino884@gmail.com",
    "PIX: carlosgaldino884@gmail.com",
    "  carlosgaldino884@gmail.com  ",
    "carlosgaldino884@gmail.com (Nubank)",
])
def test_o_rotulo_sai_de_todas_as_formas_que_a_planilha_escreve(escrito):
    """Cada linha destas já apareceu, ou apareceria: quem digita a coluna de
    informação de pagamento escreve para um humano ler, não para o banco."""
    assert pix_brcode.normalizar_chave(escrito) == "carlosgaldino884@gmail.com"
    assert pagamentos.extrair_chave(escrito) == "carlosgaldino884@gmail.com"


def test_a_chave_que_comeca_com_as_letras_de_pix_nao_e_mutilada():
    """"pixelado@..." começa com "pix" — e é uma chave inteira. Só o rótulo
    com separador ("Pix:") é rótulo."""
    assert pix_brcode.normalizar_chave("pixelado@x.com") == "pixelado@x.com"


def test_so_o_rotulo_nao_e_chave_nenhuma():
    """Célula com o rótulo e nada depois é cadastro faltando, não chave."""
    assert pagamentos.extrair_chave("Chave Pix:") is None
    assert pagamentos.classificar("Pix", "Chave Pix:")["falta_chave"] is True


# ---------------------------------------------------------------------------
# O PAYLOAD GERADO
# ---------------------------------------------------------------------------
def test_o_payload_leva_so_a_chave_e_fecha_o_crc():
    """O teste que teria pegado o defeito: olhar DENTRO do payload, não só a
    função de limpeza."""
    png, carga = pagamentos.gerar_pix(
        "Chave Pix: carlosgaldino884@gmail.com", 724.24,
        "CARLOS GALDINO DOS SANTOS")
    assert campo_da_chave(carga) == "carlosgaldino884@gmail.com"
    assert "Chave" not in carga and "chave" not in carga
    assert pix_brcode.validar_payload(carga)["crc_ok"], "CRC não fecha"
    assert png[:4] == b"\x89PNG"


def test_o_payload_de_telefone_e_de_cpf_continua_como_era():
    """A limpeza do rótulo não podia mexer no desempate entre CPF e celular,
    que custou tempo para acertar."""
    _, cel = pagamentos.gerar_pix("Chave Pix: (81) 98391-5233", 10, "F")
    assert campo_da_chave(cel) == "+5581983915233"
    _, cpf = pagamentos.gerar_pix("068.663.434-96", 10, "F")
    assert campo_da_chave(cpf) == "06866343496"


def test_sem_chave_o_qr_nao_e_gerado():
    """Um QR com o campo da chave vazio abre, é lido e só é recusado na hora
    de pagar. Recusar aqui deixa escrever o motivo na tela."""
    with pytest.raises(ValueError) as erro:
        pagamentos.gerar_pix("Chave Pix:", 10, "F")
    assert "chave" in str(erro.value).lower()


def test_o_copia_e_cola_com_email_dentro_nao_e_despedacado():
    """Um payload pronto tem espaços legítimos (nome e cidade do recebedor) e
    pode ter "@" na chave. A limpeza do rótulo não pode encostar nele."""
    pronto = pix_brcode.montar_payload("carlos@x.com", "10.00",
                                       nome="CARLOS GALDINO DOS SANTOS")
    _, carga = pagamentos.gerar_pix(pronto, 10, "F", copia_cola=True)
    assert carga == pronto
    assert pix_brcode.validar_payload(carga)["crc_ok"]
    assert pagamentos.eh_copia_cola(pronto) is True


# ---------------------------------------------------------------------------
# O CAMINHO DA TELA
# ---------------------------------------------------------------------------
def test_a_tela_nao_monta_o_qr_com_o_texto_cru_da_planilha():
    """O defeito NÃO estava na limpeza: estava em o QR não passar por ela.

    Este teste vigia o caminho, não o resultado, porque é o caminho que se
    perde: basta alguém voltar a ler `info_pgt` direto e entregar ao gerador.
    """
    from pathlib import Path
    fonte = (Path(__file__).resolve().parents[1] / "app" / "apps"
             / "analisesps" / "web.py").read_text(encoding="utf-8")
    trecho = fonte[fonte.index("def _codigo_de_pagamento"):]
    trecho = trecho[:trecho.index("@bp.route")]
    assert "pagamentos.classificar(" in trecho, \
        "o QR voltou a ser montado sem passar pela classificação"
    assert 'registro.get("info_pgt") or ""' not in trecho, \
        "o texto cru da planilha voltou a ir direto para o gerador"
