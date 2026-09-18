"""O OFÍCIO, o DEFERIMENTO e a DEMORA POR ÓRGÃO (Acompanhamento, pedaço 3).

Pedido do dono, junto com o módulo: *"gerar ofícios, dar entrada, lançar alguma
coisa que a gente protocolou"*. É o item mais barato e o que mais economiza
tempo, porque o texto é quase sempre o mesmo — o que muda são dados que o ERP
já tem.

O que estes testes seguram, e por quê:

1. **O número não repete.** Sequência por empresa e por ano, com restrição
   única no banco. Dois "OF 012/2026" é coisa que só aparece quando o órgão
   reclama.
2. **O rascunho NÃO numera.** Numerar o que a pessoa vai descartar deixaria
   buraco na sequência, e buraco em sequência de ofício é pergunta do órgão.
3. **O corpo fica como foi enviado.** Regerar do modelo meses depois daria
   outro texto, e o papel do órgão e o sistema divergiriam em silêncio.
4. **O deferimento PROPÕE, não aplica.** Mexer sozinho no prazo de um contrato
   é mexer em dinheiro, e o erro só apareceria numa medição recusada.
5. **A média de demora não mente:** só processo encerrado com protocolo entra,
   e a contagem de quantos formaram a média viaja junto.

COM BANCO DE VERDADE porque numera com restrição única, arquiva documento,
registra aditivo e mexe na obra.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.apps.erp.core.acompanhamento import oficios
from app.apps.erp.core.acompanhamento import processos as svc
from app.apps.erp.core.arquivo import catalogo
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.db.models.cadastros import (
    Empresa, Obra, PerfilUsuario as P, Processo, ProcessoOficio, Usuario,
)

from conftest import como

pytestmark = pytest.mark.banco

HOJE = date(2026, 9, 18)


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    catalogo.aplicar(s)          # o tipo OFICIO precisa existir para arquivar
    empresa = Empresa(razao_social="BWS CONSTRUÇÕES LTDA", cnpj="11444777000161",
                      municipio="Eusébio", uf="CE", logradouro="Rua das Obras",
                      numero="100", bairro="Centro")
    s.add(empresa); s.flush()
    obra = Obra(codigo="OFIC-A", nome="Creche do Eusébio", empresa_id=empresa.id,
                contrato="045/2025", objeto="Construção de creche tipo 1",
                municipio="Eusébio", uf="CE",
                vigencia_fim=date(2026, 12, 31))
    chefe = Usuario(nome="Chefe", email="chefe.of@teste.local", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.ADMIN)
    s.add_all([obra, chefe]); s.flush()

    processo = svc.criar(s, {"assunto": "Aditivo de prazo nº 2",
                             "tipo": "ADITIVO_PRAZO", "obra_id": obra.id,
                             "orgao": "Prefeitura de Eusébio"}, chefe)
    s.flush()
    return {"s": s, "empresa": empresa, "obra": obra, "chefe": chefe,
            "processo": processo}


# ---------------------------------------------------------------------------
# O rascunho
# ---------------------------------------------------------------------------
def test_o_rascunho_traz_o_que_o_erp_ja_sabe(cenario):
    r = oficios.montar(cenario["s"], cenario["processo"].id, hoje=HOJE)

    assert "Contrato nº 045/2025" in r["corpo"]
    assert "Creche do Eusébio" in r["corpo"]
    assert "ADITIVO DE PRAZO" in r["corpo"]
    assert r["destinatario"] == "Prefeitura de Eusébio"
    assert r["empresa"].startswith("BWS CONSTRUÇÕES LTDA")
    assert r["local_e_data"] == "Eusébio, 18 de setembro de 2026"


def test_o_que_o_erp_nao_sabe_fica_em_branco_e_nao_inventado(cenario):
    """Espaço em branco é pedido de atenção; valor inventado passa batido."""
    r = oficios.montar(cenario["s"], cenario["processo"].id, hoje=HOJE)
    assert "____________" in r["corpo"]


def test_o_rascunho_nao_numera_nada(cenario):
    """Numerar o que vai ser descartado deixa buraco na sequência."""
    s = cenario["s"]

    r = oficios.montar(s, cenario["processo"].id, hoje=HOJE)

    assert r["numero_previsto"] == "OF 001/2026"
    assert s.query(ProcessoOficio).count() == 0


def test_tipo_sem_modelo_proprio_usa_o_texto_padrao(cenario):
    s, chefe = cenario["s"], cenario["chefe"]
    outro = svc.criar(s, {"assunto": "Coisa solta", "tipo": "OUTRO",
                          "obra_id": cenario["obra"].id}, chefe)
    s.flush()

    r = oficios.montar(s, outro.id, hoje=HOJE)

    assert "Vimos, por meio deste, solicitar" in r["corpo"]


# ---------------------------------------------------------------------------
# Gerar
# ---------------------------------------------------------------------------
def test_gerar_numera_arquiva_e_lanca_andamento(cenario):
    s, chefe = cenario["s"], cenario["chefe"]
    pid = cenario["processo"].id
    rascunho = oficios.montar(s, pid, hoje=HOJE)

    o = oficios.gerar(s, pid, rascunho, chefe, hoje=HOJE)
    s.flush()

    assert o.numero == "OF 001/2026"
    assert o.documento_id is not None, "o PDF tem de ir para o Arquivo"
    detalhe = svc.detalhe(s, pid)
    assert any("OF 001/2026" in a["texto"] for a in detalhe["andamentos"]), (
        "ofício que sai e não aparece no histórico faz alguém mandar o segundo")


def test_a_sequencia_continua_de_onde_parou(cenario):
    s, chefe = cenario["s"], cenario["chefe"]
    pid = cenario["processo"].id
    base = oficios.montar(s, pid, hoje=HOJE)

    a = oficios.gerar(s, pid, base, chefe, hoje=HOJE); s.flush()
    b = oficios.gerar(s, pid, base, chefe, hoje=HOJE); s.flush()

    assert (a.numero, b.numero) == ("OF 001/2026", "OF 002/2026")


def test_a_sequencia_e_por_ano(cenario):
    s, chefe = cenario["s"], cenario["chefe"]
    pid = cenario["processo"].id
    base = oficios.montar(s, pid, hoje=HOJE)

    oficios.gerar(s, pid, base, chefe, hoje=date(2026, 12, 20)); s.flush()
    virada = oficios.gerar(s, pid, base, chefe, hoje=date(2027, 1, 5)); s.flush()

    assert virada.numero == "OF 001/2027"


def test_a_sequencia_e_por_empresa(cenario):
    """Duas empresas da casa têm numeração independente, como no papel."""
    s, chefe = cenario["s"], cenario["chefe"]
    outra = Empresa(razao_social="BWS PARTICIPAÇÕES LTDA", cnpj="34028316000103")
    s.add(outra); s.flush()
    obra2 = Obra(codigo="OFIC-B", nome="Posto", empresa_id=outra.id)
    s.add(obra2); s.flush()
    p2 = svc.criar(s, {"assunto": "Licença", "tipo": "LICENCA",
                       "obra_id": obra2.id}, chefe)
    s.flush()

    oficios.gerar(s, cenario["processo"].id,
                  oficios.montar(s, cenario["processo"].id, hoje=HOJE),
                  chefe, hoje=HOJE)
    s.flush()
    da_outra = oficios.gerar(s, p2.id, oficios.montar(s, p2.id, hoje=HOJE),
                             chefe, hoje=HOJE)
    s.flush()

    assert da_outra.numero == "OF 001/2026"


def test_o_corpo_fica_guardado_como_foi_enviado(cenario):
    """Regerar do modelo meses depois daria outro texto — e o papel do órgão e
    o sistema divergiriam sem ninguém perceber."""
    s, chefe = cenario["s"], cenario["chefe"]
    pid = cenario["processo"].id
    r = oficios.montar(s, pid, hoje=HOJE)
    r["corpo"] = "Texto inteiramente reescrito pela pessoa que enviou."

    o = oficios.gerar(s, pid, r, chefe, hoje=HOJE); s.flush()

    assert o.corpo == "Texto inteiramente reescrito pela pessoa que enviou."


def test_oficio_sem_texto_e_recusado(cenario):
    s, chefe = cenario["s"], cenario["chefe"]
    with pytest.raises(ErroValidacao, match="sem texto"):
        oficios.gerar(s, cenario["processo"].id, {"corpo": "   "}, chefe)


def test_o_pdf_sai_com_o_numero_e_o_destinatario(cenario):
    s, chefe = cenario["s"], cenario["chefe"]
    pid = cenario["processo"].id
    o = oficios.gerar(s, pid, oficios.montar(s, pid, hoje=HOJE), chefe, hoje=HOJE)
    s.flush()

    from app.apps.erp.db.models.financeiro import Anexo, Documento
    d = s.get(Documento, o.documento_id)
    anexo = s.get(Anexo, d.anexo_id)
    assert anexo.conteudo[:4] == b"%PDF"
    assert len(anexo.conteudo) > 800


def test_pela_rota_o_oficio_de_processo_de_fora_responde_404(cenario, app_real):
    s, chefe = cenario["s"], cenario["chefe"]
    outra_obra = Obra(codigo="OFIC-Z", nome="Obra de fora")
    preso = Usuario(nome="Ruan", email="ruan.of@teste.local", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-123"),
                    perfil=P.ADMINISTRATIVO_OBRA)
    s.add_all([outra_obra, preso]); s.flush()
    de_fora = svc.criar(s, {"assunto": "x", "tipo": "OUTRO",
                            "obra_id": outra_obra.id}, chefe)
    s.flush()

    r = como(app_real, preso.id).get(f"/erp/api/acompanhamento/{de_fora.id}/oficio")

    assert r.status_code == 404


# ---------------------------------------------------------------------------
# O deferimento que fecha o ciclo
# ---------------------------------------------------------------------------
def test_a_proposta_diz_o_que_muda_e_nao_muda_nada(cenario):
    s = cenario["s"]
    antes = cenario["obra"].vigencia_fim

    pr = svc.proposta_de_deferimento(s, cenario["processo"].id)

    assert pr["propoe"] is True
    assert pr["o_que_muda"] == "o fim da vigência da obra"
    assert pr["vigencia_atual"] == "2026-12-31"
    assert pr["numero_sugerido"] == "", "número inventado vira divergência"
    assert cenario["obra"].vigencia_fim == antes


def test_processo_que_nao_e_aditivo_nao_propoe_nada(cenario):
    s, chefe = cenario["s"], cenario["chefe"]
    licenca = svc.criar(s, {"assunto": "Licença", "tipo": "LICENCA",
                            "obra_id": cenario["obra"].id}, chefe)
    s.flush()

    assert svc.proposta_de_deferimento(s, licenca.id) == {"propoe": False}


def test_aplicar_o_deferimento_estende_a_vigencia_da_obra(cenario):
    """É isto que faz o alerta 'Vigência vencida' sumir sozinho do painel."""
    s, chefe = cenario["s"], cenario["chefe"]

    r = svc.aplicar_deferimento(s, cenario["processo"].id, {
        "numero": "2º TA", "dias": 90,
        "nova_vigencia_fim": "2027-03-31",
        "data_assinatura": "2026-09-18"}, chefe)
    s.flush()

    assert r["numero"] == "2º TA"
    assert cenario["obra"].vigencia_fim == date(2027, 3, 31)
    assert cenario["processo"].situacao == "DEFERIDO"


def test_o_deferimento_deixa_o_andamento_contando_a_historia(cenario):
    s, chefe = cenario["s"], cenario["chefe"]

    svc.aplicar_deferimento(s, cenario["processo"].id, {
        "numero": "2º TA", "nova_vigencia_fim": "2027-03-31"}, chefe)
    s.flush()

    textos = [a["texto"] for a in svc.detalhe(s, cenario["processo"].id)["andamentos"]]
    assert any("2º TA" in t and "31/03/2027" in t for t in textos)


def test_aditivo_sem_numero_e_recusado(cenario):
    """O número é o do termo assinado — inventar vira divergência com o órgão."""
    s, chefe = cenario["s"], cenario["chefe"]
    with pytest.raises(ErroValidacao, match="número"):
        svc.aplicar_deferimento(s, cenario["processo"].id,
                                {"nova_vigencia_fim": "2027-03-31"}, chefe)


def test_nao_da_para_deferir_o_que_nao_e_aditivo(cenario):
    s, chefe = cenario["s"], cenario["chefe"]
    licenca = svc.criar(s, {"assunto": "Licença", "tipo": "LICENCA",
                            "obra_id": cenario["obra"].id}, chefe)
    s.flush()

    with pytest.raises(ErroValidacao, match="não vira aditivo"):
        svc.aplicar_deferimento(s, licenca.id, {"numero": "1"}, chefe)


# ---------------------------------------------------------------------------
# Quanto cada órgão demora
# ---------------------------------------------------------------------------
def _encerrado(s, chefe, *, orgao, tipo, protocolo_em, encerrado_em, obra_id):
    p = svc.criar(s, {"assunto": f"{tipo} em {orgao}", "tipo": tipo,
                      "obra_id": obra_id, "orgao": orgao}, chefe)
    p.protocolado_em = protocolo_em
    p.situacao = "DEFERIDO"
    p.encerrado_em = datetime.combine(encerrado_em, datetime.min.time(),
                                      tzinfo=timezone.utc)
    s.flush()
    return p


def test_a_media_sai_por_orgao_e_por_tipo(cenario):
    s, chefe, obra = cenario["s"], cenario["chefe"], cenario["obra"]
    _encerrado(s, chefe, orgao="Prefeitura X", tipo="ADITIVO_PRAZO",
               protocolo_em=date(2026, 1, 10), encerrado_em=date(2026, 2, 9),
               obra_id=obra.id)
    _encerrado(s, chefe, orgao="Prefeitura X", tipo="ADITIVO_PRAZO",
               protocolo_em=date(2026, 3, 1), encerrado_em=date(2026, 3, 21),
               obra_id=obra.id)

    r = svc.demora_por_orgao(s, obras_permitidas=None)

    linha = [l for l in r["linhas"]
             if l["orgao"] == "Prefeitura X" and l["tipo"] == "ADITIVO_PRAZO"][0]
    assert linha["concluidos"] == 2
    assert linha["media_dias"] == 25          # (30 + 20) / 2
    assert (linha["menor"], linha["maior"]) == (20, 30)
    assert "2 processos" in linha["confianca"]


def test_um_caso_so_e_dito_que_nao_e_media(cenario):
    """Ler 'média 4' de um caso como se fosse regra é o erro que isto evita."""
    s, chefe, obra = cenario["s"], cenario["chefe"], cenario["obra"]
    _encerrado(s, chefe, orgao="Prefeitura Y", tipo="LICENCA",
               protocolo_em=date(2026, 1, 1), encerrado_em=date(2026, 1, 5),
               obra_id=obra.id)

    linha = [l for l in svc.demora_por_orgao(s, obras_permitidas=None)["linhas"]
             if l["orgao"] == "Prefeitura Y"][0]
    assert linha["media_dias"] == 4
    assert "um caso só" in linha["confianca"]


def test_o_que_ainda_esta_aberto_fica_fora_da_media(cenario):
    """O aberto não demorou — está demorando. Misturar puxaria a média para
    baixo justamente por causa dos que travaram."""
    s = cenario["s"]

    r = svc.demora_por_orgao(s, obras_permitidas=None)

    linha = [l for l in r["linhas"] if l["orgao"] == "Prefeitura de Eusébio"][0]
    assert linha["concluidos"] == 0
    assert linha["abertos"] == 1
    assert linha["media_dias"] is None
    assert "sem processo concluído" in linha["confianca"]


def test_encerrado_sem_data_de_protocolo_nao_entra_na_conta(cenario):
    """Sem protocolo não há de onde contar, e chutar a data daria número com
    cara de certo."""
    s, chefe, obra = cenario["s"], cenario["chefe"], cenario["obra"]
    p = svc.criar(s, {"assunto": "Sem protocolo", "tipo": "CERTIDAO",
                      "obra_id": obra.id, "orgao": "Prefeitura Z"}, chefe)
    p.situacao = "DEFERIDO"
    p.encerrado_em = datetime.now(timezone.utc)
    s.flush()

    linha = [l for l in svc.demora_por_orgao(s, obras_permitidas=None)["linhas"]
             if l["orgao"] == "Prefeitura Z"][0]
    assert linha["concluidos"] == 0


def test_a_demora_respeita_o_escopo_por_obra(cenario):
    s = cenario["s"]
    assert svc.demora_por_orgao(s, obras_permitidas=[])["linhas"] == []


def test_falha_ao_arquivar_nao_some_do_historico(cenario, monkeypatch):
    """O ofício vale mesmo sem arquivamento — mas a falha não pode ser calada.

    Ofício que existe e não está no Arquivo é o que ninguém acha no dia em que
    o órgão pergunta.
    """
    from app.apps.erp.core.arquivo import service as arquivo

    s, chefe = cenario["s"], cenario["chefe"]
    pid = cenario["processo"].id

    def _recusa(*a, **k):
        raise ErroValidacao("Tipo de documento desconhecido: OFICIO.")
    monkeypatch.setattr(arquivo, "arquivar", _recusa)

    o = oficios.gerar(s, pid, oficios.montar(s, pid, hoje=HOJE), chefe, hoje=HOJE)
    s.flush()

    assert o.numero == "OF 001/2026", "o número não se perde por causa do arquivo"
    assert o.documento_id is None
    textos = [a["texto"] for a in svc.detalhe(s, pid)["andamentos"]]
    assert any("não foi arquivado" in t for t in textos)
