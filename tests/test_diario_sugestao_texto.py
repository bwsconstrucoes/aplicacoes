# -*- coding: utf-8 -*-
"""
A SUGESTÃO DE TEXTO DO DIÁRIO POR IA (10/11/2026).

Ele: *"eu gostaria que a IA fizesse a leitura do relato, tanto dos serviços
como não da parte da fiscalização, que a gente não pode alterar (…) poder
sugerir um melhor texto (…) correção ortográfica, um melhor português, numa
linguagem inclusive um pouco mais de obra, mais técnica (…) E quando ela
sugerisse, eu pudesse editar a sugestão e gravar."*

⚑ O TESTE MAIS IMPORTANTE DESTE ARQUIVO é o da fiscalização. Tudo o mais aqui
é conforto; aquele é o que impede a IA de reescrever a única parte do diário
que a obra não controla — e que é justamente a que dá valor jurídico ao
documento.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core import diario_texto as svc
from app.apps.erp.core.comum.auditoria import ErroValidacao


class _Resposta:
    """O mínimo que a função lê da resposta da OpenAI."""
    class _Escolha:
        def __init__(self, texto):
            self.message = type("M", (), {"content": texto})()

    def __init__(self, texto):
        self.choices = [self._Escolha(texto)]
        self.usage = type("U", (), {"prompt_tokens": 100,
                                    "completion_tokens": 50})()


def _dublê(monkeypatch, devolve, guarda=None):
    """Põe um dublê no lugar da OpenAI e, se pedirem, guarda o que foi pedido."""
    class _Cliente:
        class chat:
            class completions:
                @staticmethod
                def create(**kw):
                    if guarda is not None:
                        guarda.update(kw)
                    return _Resposta(devolve)
    monkeypatch.setattr(svc, "_cliente", lambda: _Cliente())
    monkeypatch.setattr(svc, "_consumo", lambda *a, **k: None)


# ---------------------------------------------------------------------------
# A TRAVA
# ---------------------------------------------------------------------------
def test_a_observacao_da_fiscalizacao_NUNCA_passa_pela_ia():
    """ESTE É O TESTE QUE IMPORTA.

    O que dá valor jurídico ao diário é a parte que a obra NÃO escreve. Uma IA
    que "melhora" o texto da fiscalização destrói exatamente isso — e a recusa
    é por LISTA FECHADA, não por um `if` que alguém apaga sem perceber.
    """
    assert "obs_fiscalizacao" not in svc.CAMPOS
    assert "obs_fiscalizacao_por" not in svc.CAMPOS
    with pytest.raises(ErroValidacao) as e:
        svc.sugerir("obs_fiscalizacao", "A fiscalização esteve na obra e "
                                        "aprovou o serviço executado hoje.")
    assert "fiscalização" in str(e.value)


def test_campo_inventado_tambem_e_recusado():
    """A lista é fechada nos dois sentidos: nada entra sem estar escrito."""
    with pytest.raises(ErroValidacao):
        svc.sugerir("assinado_nome", "Fulano de Tal da Silva Sauro Neto")


def test_os_campos_que_aceitam_sugestao_sao_os_que_a_obra_escreve():
    assert set(svc.CAMPOS) == {"relato", "ocorrencias", "historico",
                               "equipamentos"}


# ---------------------------------------------------------------------------
# O CAMINHO NORMAL
# ---------------------------------------------------------------------------
def test_texto_curto_nao_gasta_chamada_de_ia():
    """Cobrar IA para arrumar "FOLGA" é desperdício, e não há o que melhorar."""
    with pytest.raises(ErroValidacao) as e:
        svc.sugerir("relato", "FOLGA")
    assert str(svc.MINIMO) in str(e.value)


def test_texto_gigante_e_recusado_antes_de_viajar():
    with pytest.raises(ErroValidacao):
        svc.sugerir("relato", "x" * (svc.TETO + 1))


def test_a_sugestao_volta_com_o_original_do_lado(monkeypatch):
    """⚑ SUGESTÃO SEM O ORIGINAL AO LADO É TROCA ÀS CEGAS. A tela mostra os
    dois, e quem decide é a pessoa."""
    _dublê(monkeypatch, "1. Assentamento de piso intertravado no "
                        "estacionamento frontal da escola.")
    r = svc.sugerir("relato", "1- piso intetravado estacionamento frent escola")
    assert r["original"] == "1- piso intetravado estacionamento frent escola"
    assert "intertravado" in r["sugestao"]
    assert r["mudou"] is True
    assert r["campo"] == "relato"


def test_quando_nao_ha_o_que_mudar_a_resposta_diz_isso(monkeypatch):
    """A tela não abre o diálogo à toa."""
    texto = "Concretagem da laje do pavimento tipo 3."
    _dublê(monkeypatch, texto)
    assert svc.sugerir("relato", texto)["mudou"] is False


def test_a_instrucao_proibe_inventar_servico(monkeypatch):
    """⚑ UM DIÁRIO COM UM SERVIÇO A MAIS É PROVA FALSA. A proibição tem de
    estar no pedido que viaja, não só no comentário do código."""
    pedido = {}
    _dublê(monkeypatch, "Concretagem da laje.", guarda=pedido)
    svc.sugerir("relato", "concretage da laje do predio")
    mandado = pedido["messages"][0]["content"]
    assert "NÃO acrescente" in mandado
    assert "NÃO remova" in mandado
    assert "diário de obra" in mandado.lower()
    # O texto original vai junto, inteiro.
    assert "concretage da laje do predio" in mandado


def test_a_temperatura_e_baixa(monkeypatch):
    """Revisão de documento não é lugar para criatividade."""
    pedido = {}
    _dublê(monkeypatch, "Concretagem da laje.", guarda=pedido)
    svc.sugerir("relato", "concretage da laje do predio")
    assert pedido["temperature"] <= 0.3


def test_crase_e_aspas_em_volta_sao_tiradas(monkeypatch):
    """O modelo às vezes devolve ```texto``` ou "texto" mesmo mandado não."""
    _dublê(monkeypatch, '```\n"Concretagem da laje do bloco A."\n```')
    r = svc.sugerir("relato", "concretage da laje do bloco a")
    assert r["sugestao"] == "Concretagem da laje do bloco A."


def test_sugestao_vazia_nao_apaga_o_texto_da_pessoa(monkeypatch):
    """O pior resultado possível seria trocar o relato por nada."""
    _dublê(monkeypatch, "   ")
    with pytest.raises(ErroValidacao):
        svc.sugerir("relato", "concretage da laje do predio")


def test_falha_da_ia_avisa_que_o_texto_nao_se_perdeu(monkeypatch):
    """Quem acabou de escrever cinco linhas precisa ouvir isso."""
    def _explode():
        raise RuntimeError("timeout")
    monkeypatch.setattr(svc, "_cliente", _explode)
    monkeypatch.setattr(svc, "_consumo", lambda *a, **k: None)
    with pytest.raises(ErroValidacao) as e:
        svc.sugerir("relato", "concretage da laje do predio inteiro")
    assert "não se perdeu" in str(e.value)


def test_sem_chave_configurada_o_recado_e_de_gente(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with pytest.raises(ErroValidacao) as e:
        svc.sugerir("relato", "concretage da laje do predio inteiro")
    assert "à mão" in str(e.value)


def test_cada_campo_pede_um_formato_diferente(monkeypatch):
    """Relato vira lista numerada; ocorrência é texto corrido. Mandar os dois
    com a mesma instrução daria um diário com ocorrência em tópicos."""
    pedidos = {}
    _dublê(monkeypatch, "qualquer coisa", guarda=pedidos)
    svc.sugerir("relato", "fiz a laje e o piso tambem hoje cedo")
    de_relato = pedidos["messages"][0]["content"]
    svc.sugerir("ocorrencias", "choveu muito e parou o servico de tarde")
    de_ocorrencia = pedidos["messages"][0]["content"]
    assert "itens numerados" in de_relato
    assert "itens numerados" not in de_ocorrencia
