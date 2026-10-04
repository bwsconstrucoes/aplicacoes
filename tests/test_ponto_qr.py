# -*- coding: utf-8 -*-
"""Ponto — QR Code pessoal, bilhete do tablet, ritmo da fila de WhatsApp,
sinais da foto e de fraude. As regras puras, sem banco."""
from __future__ import annotations

import datetime as dt
import io
import random

import pytest

from app.apps.ponto import horario
from app.apps.ponto.core import alertas, envios, fotos, marcacoes, mosaico, qr
from app.apps.ponto.erros import ErroDeValidacao


def _local(ano, mes, dia, hh, mm=0):
    return dt.datetime(ano, mes, dia, hh, mm, tzinfo=horario.FUSO)


class TestQrDoApp:
    def test_vale_na_janela_e_morre_depois(self):
        agora = 1_790_000_000.0
        conteudo = qr.conteudo_do_app(42, agora=agora)
        assert conteudo.startswith("BWSP2.42.")
        lido = qr.ler(None, conteudo, agora=dt.datetime.fromtimestamp(agora + 5, dt.timezone.utc))
        assert lido.problema is None and lido.colaborador_id == 42 and lido.identificacao == "QR_APP"
        velho = qr.ler(None, conteudo, agora=dt.datetime.fromtimestamp(agora + 120, dt.timezone.utc))
        assert velho.antigo and "trocou" in velho.problema

    def test_assinatura_adulterada_nao_identifica(self):
        conteudo = qr.conteudo_do_app(42)
        outra_pessoa = conteudo.replace("BWSP2.42.", "BWSP2.43.")
        assert qr.ler(None, outra_pessoa).problema == "QR Code não reconhecido"
        assert qr.ler(None, "https://exemplo.com").problema == "QR Code não reconhecido"
        assert qr.ler(None, "BWSP2.lixo").problema == "QR Code não reconhecido"

    def test_imagem_sai_em_png_e_jpeg(self):
        assert qr.imagem("BWSP1.abc")[:8] == b"\x89PNG\r\n\x1a\n"
        assert qr.imagem("BWSP1.abc", formato="JPEG", legenda="JOÃO")[:2] == b"\xff\xd8"

    def test_token_do_whatsapp_e_imprevisivel(self):
        a, b = qr.novo_token(), qr.novo_token()
        assert a != b and a.startswith("BWSP1.") and len(a) >= 30
        assert qr.hash_token(a) != a and len(qr.hash_token(a)) == 64


class TestBilhete:
    def test_vale_dois_minutos_e_so_no_aparelho(self):
        agora = 1_790_000_000.0
        b = qr.emitir_bilhete(7, 3, "QR_WHATSAPP", agora=agora)
        assert qr.conferir_bilhete(b, 3, agora=agora + 60) == (7, "QR_WHATSAPP")
        with pytest.raises(ErroDeValidacao, match="outro aparelho"):
            qr.conferir_bilhete(b, 4, agora=agora + 60)
        with pytest.raises(ErroDeValidacao, match="venceu"):
            qr.conferir_bilhete(b, 3, agora=agora + 121)

    def test_adulterado_nao_passa(self):
        b = qr.emitir_bilhete(7, 3, "CPF")
        trocado = "8" + b[1:]
        with pytest.raises(ErroDeValidacao):
            qr.conferir_bilhete(trocado, 3)
        with pytest.raises(ErroDeValidacao):
            qr.conferir_bilhete("lixo", 3)


class TestRitmoDaFila:
    def test_automatico_so_de_segunda_a_sabado_no_horario_comercial(self):
        domingo = _local(2026, 10, 4, 10)
        segunda_8h = _local(2026, 10, 5, 8)
        segunda_18h = _local(2026, 10, 5, 18)
        assert not envios.dentro_da_janela("QR", "ROTACAO", domingo)
        assert envios.dentro_da_janela("QR", "ROTACAO", segunda_8h)
        assert not envios.dentro_da_janela("QR", "ROTACAO", segunda_18h)
        assert not envios.dentro_da_janela("AVISO_FOTO", "", _local(2026, 10, 5, 7, 0))

    def test_pedido_da_pessoa_sai_qualquer_dia_ate_as_22h(self):
        assert envios.dentro_da_janela("QR", "PEDIDO", _local(2026, 10, 4, 21, 30))   # domingo
        assert not envios.dentro_da_janela("QR", "PEDIDO", _local(2026, 10, 4, 22, 30))
        assert not envios.dentro_da_janela("MOSAICO", "", _local(2026, 10, 5, 5, 30))

    def test_horario_sorteado_cai_na_janela_e_pula_o_domingo(self):
        rng = random.Random(1)
        horas = [envios.sortear_horario(rng, dt.date(2026, 10, 4)) for _ in range(200)]   # um domingo
        assert all(h.date() == dt.date(2026, 10, 5) for h in horas)
        assert all(dt.time(7, 30) <= h.timetz().replace(tzinfo=None) < dt.time(17, 30) for h in horas)
        assert len({(h.hour, h.minute) for h in horas}) > 50      # espalhado, não em lote

    def test_troca_do_qr_entre_7_e_14_dias_e_varia_por_pessoa(self):
        rng = random.Random(2)
        base = _local(2026, 10, 5, 9)
        dias = [(envios.sortear_proxima_troca(rng, base).date() - base.date()).days for _ in range(300)]
        assert min(dias) >= 7 and max(dias) <= 15          # 15 = sábado sorteado empurra o domingo
        assert len(set(dias)) >= 6

    def test_espaco_entre_mensagens(self):
        rng = random.Random(3)
        valores = [envios.espaco_entre_envios(rng) for _ in range(100)]
        assert min(valores) >= 30 and max(valores) <= 90

    def test_telefone(self):
        assert envios.telefone_valido("(85) 99999-1111") == "5585999991111"
        assert envios.telefone_valido("5585999991111") == "5585999991111"
        assert envios.telefone_valido("123") is None and envios.telefone_valido(None) is None

    def test_ligar_e_desligar(self):
        assert envios.validar_ligado(True) == "1" and envios.validar_ligado(False) == ""
        with pytest.raises(ErroDeValidacao):
            envios.validar_ligado("talvez")


class TestSinaisDaFoto:
    @staticmethod
    def _imagem(cor=(200, 150, 100), tamanho=(120, 160), listras=False):
        from PIL import Image, ImageDraw
        img = Image.new("RGB", tamanho, cor)
        if listras:
            d = ImageDraw.Draw(img)
            for x in range(0, tamanho[0], 12):
                d.rectangle([x, 0, x + 5, tamanho[1]], fill=(20, 20, 20))
        return img

    def test_preta_e_lisa(self):
        lum, con, _ = fotos.sinais(self._imagem((0, 0, 0)))
        assert lum == 0 and con == 0
        assert mosaico.sinais_da_foto(lum, con) == ["escura", "sem rosto visível"]
        lum, con, _ = fotos.sinais(self._imagem(listras=True))
        assert mosaico.sinais_da_foto(lum, con) == []

    def test_a_mesma_foto_tem_a_mesma_impressao(self):
        a = fotos.sinais(self._imagem(listras=True))[2]
        b = fotos.sinais(self._imagem(listras=True))[2]
        c = fotos.sinais(self._imagem(cor=(30, 90, 200)).rotate(90, expand=True))[2]
        assert fotos.distancia_dhash(a, b) == 0
        assert fotos.distancia_dhash(a, c) > 3
        assert fotos.distancia_dhash(None, a) is None

    def test_preparar_guarda_os_sinais(self):
        import base64
        s = io.BytesIO()
        self._imagem(listras=True).save(s, format="JPEG")
        f = fotos.preparar(base64.b64encode(s.getvalue()).decode())
        assert f.luminancia is not None and len(f.dhash) == 64


class TestFraude:
    @staticmethod
    def _b(pessoa, segundos, mid=None):
        return {"id": mid or pessoa * 100 + segundos, "colaborador_id": pessoa,
                "timestamp_servidor": dt.datetime(2026, 10, 5, 10, 0, tzinfo=dt.timezone.utc)
                + dt.timedelta(seconds=segundos)}

    def test_fila_rapida_de_cinco_pessoas(self):
        batidas = [self._b(p, i * 5) for i, p in enumerate([1, 2, 3, 4, 5])]
        assert len(alertas.sequencias_rapidas(batidas)) == 1
        assert alertas.sequencias_rapidas(batidas[:4]) == []

    def test_fila_com_intervalo_normal_nao_acusa(self):
        batidas = [self._b(p, i * 25) for i, p in enumerate([1, 2, 3, 4, 5, 6])]
        assert alertas.sequencias_rapidas(batidas) == []

    def test_mesma_pessoa_quebra_a_fila(self):
        batidas = [self._b(p, i * 4) for i, p in enumerate([1, 2, 2, 3, 4, 5])]
        assert alertas.sequencias_rapidas(batidas) == []

    def test_foto_repetida(self):
        h = "f" * 64
        quase = "e" + "f" * 63         # 1 bit de diferença
        longe = "0" * 64
        fotos_ = [
            {"marcacao_id": 1, "colaborador_id": 1, "sha256": "a", "dhash": h, "nova": False},
            {"marcacao_id": 2, "colaborador_id": 1, "sha256": "b", "dhash": quase, "nova": True},
            {"marcacao_id": 3, "colaborador_id": 1, "sha256": "c", "dhash": longe, "nova": True},
            {"marcacao_id": 4, "colaborador_id": 1, "sha256": "c", "dhash": None, "nova": False},
        ]
        pares = {(a["marcacao_id"], b["marcacao_id"]) for a, b in alertas.fotos_repetidas(fotos_)}
        assert pares == {(1, 2), (3, 4)}

    def test_foto_antiga_com_antiga_nao_e_julgada_de_novo(self):
        fotos_ = [{"marcacao_id": 1, "colaborador_id": 1, "sha256": "a", "dhash": None, "nova": False},
                  {"marcacao_id": 2, "colaborador_id": 1, "sha256": "a", "dhash": None, "nova": False}]
        assert alertas.fotos_repetidas(fotos_) == []

    def test_todo_codigo_novo_tem_gravidade_e_rotulo(self):
        for codigo in ("SEM_FOTO", "FOTO_ESCURA", "FOTO_REPETIDA", "SEQUENCIA_RAPIDA",
                       "QR_ANTIGO_USADO", "TENTATIVAS_DE_CPF", "MOSAICO_PENDENTE"):
            assert codigo in alertas.GRAVIDADE and codigo in alertas.ROTULO


class TestSemFotoNoTablet:
    def test_vai_para_analise(self):
        status, motivos = marcacoes.decidir(origem="PWA", dentro_da_cerca=True, motivo_cerca=None,
                                            diferenca_relogio_s=0, situacao_pessoa="ATIVO",
                                            obra_na_lista_da_pessoa=True, sem_foto_no_tablet=True)
        assert status == "EM_ANALISE" and motivos == [marcacoes.MOTIVO_SEM_FOTO_NO_TABLET]
        status, _ = marcacoes.decidir(origem="PWA", dentro_da_cerca=True, motivo_cerca=None,
                                      diferenca_relogio_s=0, situacao_pessoa="ATIVO",
                                      obra_na_lista_da_pessoa=True)
        assert status == "VALIDA"
