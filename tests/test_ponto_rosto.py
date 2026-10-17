# -*- coding: utf-8 -*-
"""Ponto — a conferência do rosto pela AWS (pedido do dono, 06/10/2026). As
regras de exclusão e a leitura da resposta da AWS, sem banco e sem AWS."""
from __future__ import annotations

import datetime as dt

from app.apps.ponto.core import rosto


def _m(dia_semana: int, hh: int, mm: int = 0) -> dt.datetime:
    base = dt.datetime(2026, 10, 5, hh, mm)            # 05/10/2026 é uma segunda
    return base + dt.timedelta(days=dia_semana)


def test_padrao_confere_tudo():
    assert rosto.entra_na_regra(dict(rosto.PADRAO), obra_id=1, momento_local=_m(0, 7), primeira_do_dia=False)


def test_exclusoes_por_obra_dia_horario_e_so_a_primeira():
    cfg = {**rosto.PADRAO, "obras_excluidas": [9], "dias_excluidos": [6],
           "horarios_excluidos": [{"de": "11:00", "ate": "13:30"}, {"de": "22:00", "ate": "05:00"}]}
    assert not rosto.entra_na_regra(cfg, obra_id=9, momento_local=_m(0, 7), primeira_do_dia=True)
    assert not rosto.entra_na_regra(cfg, obra_id=1, momento_local=_m(6, 7), primeira_do_dia=True)   # domingo
    assert not rosto.entra_na_regra(cfg, obra_id=1, momento_local=_m(0, 12), primeira_do_dia=True)
    assert rosto.entra_na_regra(cfg, obra_id=1, momento_local=_m(0, 13, 30), primeira_do_dia=True)
    assert not rosto.entra_na_regra(cfg, obra_id=1, momento_local=_m(0, 23), primeira_do_dia=True)  # vira a noite
    assert not rosto.entra_na_regra(cfg, obra_id=1, momento_local=_m(1, 4), primeira_do_dia=True)
    assert rosto.entra_na_regra({**cfg, "so_primeira_do_dia": True}, obra_id=1, momento_local=_m(0, 7),
                                primeira_do_dia=True)
    assert not rosto.entra_na_regra({**cfg, "so_primeira_do_dia": True}, obra_id=1, momento_local=_m(0, 17),
                                    primeira_do_dia=False)


class _AwsFalsa:
    def __init__(self, resposta=None, erro=None):
        self.resposta, self.erro = resposta, erro

    def compare_faces(self, **_):
        if self.erro:
            raise self.erro
        return self.resposta


class InvalidParameterException(Exception):
    pass


def test_le_a_resposta_da_aws():
    assert rosto.comparar(_AwsFalsa({"FaceMatches": [{"Similarity": 98.4}]}), b"a", b"b", 90) == \
        ("MESMA_PESSOA", 98.4, "")
    r = rosto.comparar(_AwsFalsa({"FaceMatches": [{"Similarity": 61.0}]}), b"a", b"b", 90)
    assert r[0] == "OUTRA_PESSOA" and "61 %" in r[2]
    assert rosto.comparar(_AwsFalsa({"FaceMatches": [], "UnmatchedFaces": [{}]}), b"a", b"b", 90)[0] == "OUTRA_PESSOA"
    assert rosto.comparar(_AwsFalsa({"FaceMatches": [], "UnmatchedFaces": []}), b"a", b"b", 90)[0] == "SEM_ROSTO"
    assert rosto.comparar(_AwsFalsa(erro=InvalidParameterException("no face")), b"a", b"b", 90)[0] == "SEM_ROSTO"


def test_erro_da_aws_vira_frase_com_o_que_fazer():
    """15/10/2026: a tela mostrava "is not authorized to per." — cortada no meio."""
    from app.apps.ponto.core import rosto

    class AccessDeniedException(Exception):
        pass

    negado = AccessDeniedException("An error occurred (AccessDeniedException) when calling the CompareFaces "
                                   "operation: User: arn:aws:iam::1:user/x is not authorized to perform: "
                                   "rekognition:CompareFaces")
    assert "usuário x" in rosto.explicar_erro_aws(negado)
    base = ("An error occurred (AccessDeniedException) when calling the CompareFaces operation: User: "
            "arn:aws:iam::1:user/bws-ponto-rosto is not authorized to perform: rekognition:CompareFaces ")
    for fim, esperado in (("with an explicit deny in a service control policy", "NEGAR"),
                          ("because no service control policy allows the rekognition:CompareFaces action",
                           "FullAWSAccess"),
                          ("with an explicit deny in a permissions boundary", "LIMITE DE PERMISSÕES"),
                          ("with an explicit deny in an identity-based policy", "NEGAR"),
                          ("because no identity-based policy allows the rekognition:CompareFaces action",
                           "nenhuma política DESTE usuário")):
        frase = rosto.explicar_erro_aws(AccessDeniedException(base + fim))
        assert esperado in frase and "bws-ponto-rosto" in frase, frase
    chave = Exception("An error occurred (UnrecognizedClientException) when calling the CompareFaces operation")
    assert "AWS_ACCESS_KEY_ID" in rosto.explicar_erro_aws(chave)
    assert rosto.explicar_erro_aws(Exception("outra coisa")).startswith("a AWS respondeu com erro: outra coisa")


def test_o_teste_da_aws_acha_a_regiao_liberada(monkeypatch):
    """17/10/2026: "a AWS recusou (…) uma regra da organização (SCP) bloqueia". O
    teste tenta região por região: SCP de região → aponta a que funciona; SCP do
    serviço → diz que é na conta que administra a organização."""
    import boto3
    from app.apps.ponto.core import rosto

    class Negado(Exception):
        pass

    class Cliente:
        def __init__(self, servico, region_name=None, config=None):
            self.regiao = region_name

        def get_caller_identity(self):
            return {"Account": "123456789012", "Arn": "arn:aws:iam::123456789012:user/bws-ponto-rosto"}

        def compare_faces(self, **_):
            if self.regiao in LIBERADAS:
                raise type("InvalidParameterException", (Exception,), {})("There are no faces in the image")
            raise Negado("AccessDeniedException: User: arn:aws:iam::1:user/bws-ponto-rosto is not authorized to "
                         "perform: rekognition:CompareFaces with an explicit deny in a service control policy")

    monkeypatch.setattr(boto3, "client", lambda servico, **kw: Cliente(servico, **kw))
    monkeypatch.setattr(rosto, "ler", lambda conn: {**rosto.PADRAO, "regiao": "us-east-1"})
    monkeypatch.setattr(rosto, "credenciais_ok", lambda: True)
    LIBERADAS = {"eu-west-1"}
    r = rosto.testar_aws(None)
    assert r["conta"]["numero"] == "123456789012" and r["conta"]["usuario"] == "bws-ponto-rosto"
    assert r["regioes"]["us-east-1"]["situacao"] == "SCP" and r["regioes"]["eu-west-1"]["situacao"] == "LIBERADA"
    assert r["conclusao"]["codigo"] == "TROCAR_REGIAO" and r["conclusao"]["regiao"] == "eu-west-1"
    # o serviço inteiro bloqueado: é na conta que administra a organização
    assert rosto.concluir({k: {"situacao": "SCP"} for k in rosto.REGIOES_DO_TESTE}, "us-east-1")["codigo"] == "SCP"
    assert rosto.concluir({"us-east-1": {"situacao": "LIBERADA"}}, "us-east-1")["codigo"] == "OK"
    assert rosto.classificar_erro(None) == "LIBERADA"
    assert rosto.classificar_erro(Exception("AccessDeniedException: no identity-based policy allows")) == "IAM"
