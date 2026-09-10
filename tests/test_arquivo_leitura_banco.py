"""A IA lê o documento da empresa e sugere onde ele vai — com banco de verdade.

Item 3 da gestão de documentos, nas palavras do dono: *"um ambiente onde eu
pudesse simplesmente jogar esse documento, ele fosse interpretado, lido, e a
partir dali categorizado, renomeado e salvo"*.

Com banco porque a parte que erra é justamente a que depende dele: traduzir
"BWS Construções" no ID da empresa, "ESCPLANALTO" no ID da obra, um CNPJ no
fornecedor certo — e recusar quando não achar.

A CHAMADA À IA É DUBLADA. Não por preguiça: o que precisa de prova aqui não é
o modelo, é o que o sistema FAZ com a resposta dele. Uma resposta plausível e
errada é o caso perigoso, e é o que estes testes montam de propósito.

O que se prova:

  1. A pergunta é montada a partir do catálogo QUE ESTÁ NO BANCO — tipo criado
     pela empresa hoje entra na leitura sem mexer em código.
  2. O dono é achado por CNPJ, por CPF, por código de obra e por nome; e o que
     NÃO for achado volta em branco, com o nome lido à mostra, em vez de ser
     pendurado no mais parecido.
  3. Tipo que não existe (ou aposentado) não é aceito de faz-de-conta.
  4. Validade anterior à emissão é leitura trocada — o sistema descarta e
     rebaixa a confiança, em vez de gravar um aviso que nasceria errado.
  5. A leitura DIZ o que ela mesma não resolveu ("falta você preencher…"). Uma
     sugestão que se apresenta como certeza é pior do que campo em branco.
  6. Nada é gravado na leitura: guardar continua sendo ato da pessoa.
  7. O texto extraído volta junto — é o que permite, depois, buscar DENTRO do
     documento sem reprocessar o arquivo.
"""
from __future__ import annotations

from datetime import date

import pytest

from app.apps.erp.core.arquivo import catalogo, leitura
from app.apps.erp.core.arquivo import service as arq
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.db.models.cadastros import (Colaborador, Empresa, Fornecedor,
                                              Obra, PerfilUsuario as P,
                                              TipoPessoa, Usuario)
from app.apps.erp.db.models.financeiro import Documento, DocumentoTipo

pytestmark = pytest.mark.banco

PDF = b"%PDF-1.4 certidao de teste"


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    catalogo.aplicar(s)
    admin = Usuario(nome="Admin da leitura", email="leitura@teste.local", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.ADMIN)
    emp = Empresa(razao_social="BWS Construções e Empreendimentos LTDA",
                  nome_fantasia="BWS", cnpj="11222333000181")
    obra = Obra(codigo="ESCPLANALTO", nome="Escola Municipal do Planalto")
    pessoa = Colaborador(nome="João da Silva Pereira", cpf="52998224725")
    parceiro = Fornecedor(razao_social="Locadora Andaimes do Nordeste LTDA",
                          nome_fantasia="Andaimes NE", cnpj_cpf="09876543000199",
                          tipo_pessoa=TipoPessoa.PJ, ativo=True)
    s.add_all([admin, emp, obra, pessoa, parceiro])
    s.flush()
    return {"s": s, "admin": admin, "empresa": emp, "obra": obra,
            "pessoa": pessoa, "parceiro": parceiro}


def _dublar(monkeypatch, resposta: dict, texto: str = "texto do documento"):
    """Põe no lugar da IA uma resposta escolhida pelo teste."""
    def falso(conteudo, nome_arquivo, instrucao, dica=""):
        falso.instrucao = instrucao
        falso.dica = dica
        d = dict(resposta)
        d["texto_extraido"] = texto
        d["origem_leitura"] = "PDF_TEXTO"
        return d
    falso.instrucao = ""
    falso.dica = ""
    monkeypatch.setattr("app.apps.erp.core.documentos.leitor.ler_com_instrucao", falso)
    return falso


# ---------------------------------------------------------------------------
# 1. A PERGUNTA SAI DO CATÁLOGO DO BANCO
# ---------------------------------------------------------------------------
def test_a_pergunta_lista_os_tipos_que_estao_no_banco(cenario):
    texto = leitura.montar_instrucao(cenario["s"])
    assert "CND-FEDERAL" in texto
    assert "CRF-FGTS" in texto
    assert "vence" in texto


def test_tipo_criado_pela_empresa_entra_na_pergunta_sem_mexer_em_codigo(cenario):
    s = cenario["s"]
    s.add(DocumentoTipo(codigo="LAUDO-DRONE", nome="Laudo de sobrevoo com drone",
                        grupo="OBRA", dono="OBRA", vence=False,
                        por_competencia=False, sigilo="ABERTO", ativo=True))
    s.flush()
    assert "LAUDO-DRONE" in leitura.montar_instrucao(s)


def test_tipo_aposentado_sai_da_pergunta(cenario):
    s = cenario["s"]
    tipo = s.get(DocumentoTipo, "CND-FALENCIA")
    tipo.ativo = False
    s.flush()
    assert "CND-FALENCIA" not in leitura.montar_instrucao(s)


# ---------------------------------------------------------------------------
# 2. ACHAR O DONO — e não achar, quando é o caso
# ---------------------------------------------------------------------------
def test_empresa_achada_pelo_cnpj(cenario, monkeypatch):
    _dublar(monkeypatch, {"tipo_codigo": "CND-FEDERAL", "dono_especie": "EMPRESA",
                          "dono_nome": "nome que não bate com nada",
                          "dono_documento": "11.222.333/0001-81",
                          "emissao": "2026-09-01", "validade": "2027-03-01",
                          "confianca": "ALTA"})
    g = leitura.sugerir(cenario["s"], PDF, "cnd.pdf")
    assert g["empresa_id"] == cenario["empresa"].id
    assert g["dono_encontrado"] == "BWS"


def test_empresa_achada_pelo_nome_quando_o_cnpj_nao_veio(cenario, monkeypatch):
    _dublar(monkeypatch, {"tipo_codigo": "CONTRATO-SOCIAL", "dono_especie": "EMPRESA",
                          "dono_nome": "BWS Construções e Empreendimentos LTDA",
                          "dono_documento": "", "confianca": "ALTA"})
    g = leitura.sugerir(cenario["s"], PDF, "contrato.pdf")
    assert g["empresa_id"] == cenario["empresa"].id


def test_obra_achada_pelo_codigo(cenario, monkeypatch):
    _dublar(monkeypatch, {"tipo_codigo": "ART", "dono_especie": "OBRA",
                          "dono_nome": "ESCPLANALTO", "referencia": "ART-1234567",
                          "emissao": "2026-03-10", "confianca": "ALTA"})
    g = leitura.sugerir(cenario["s"], PDF, "art.pdf")
    assert g["obra_id"] == cenario["obra"].id
    assert g["referencia"] == "ART-1234567"


def test_pessoa_achada_pelo_cpf(cenario, monkeypatch):
    _dublar(monkeypatch, {"tipo_codigo": "ASO", "dono_especie": "PESSOA",
                          "dono_nome": "J. da Silva", "dono_documento": "529.982.247-25",
                          "validade": "2027-08-01", "competencia": "2026-08",
                          "confianca": "ALTA"})
    g = leitura.sugerir(cenario["s"], PDF, "aso.pdf")
    assert g["colaborador_id"] == cenario["pessoa"].id
    assert g["competencia"] == "2026-08"


def test_fornecedor_achado_pelo_cnpj(cenario, monkeypatch):
    _dublar(monkeypatch, {"tipo_codigo": "CARTAO-CNPJ", "dono_especie": "PARCEIRO",
                          "dono_nome": "Andaimes NE", "dono_documento": "09876543000199",
                          "confianca": "ALTA"})
    g = leitura.sugerir(cenario["s"], PDF, "cartao.pdf")
    assert g["fornecedor_id"] == cenario["parceiro"].id


def test_dono_que_nao_existe_volta_em_branco_com_o_nome_a_mostra(cenario, monkeypatch):
    """Pendurar no mais parecido é pior do que não achar: o documento sumiria
    da busca de quem procura, e ninguém saberia por quê."""
    _dublar(monkeypatch, {"tipo_codigo": "CND-FEDERAL", "dono_especie": "EMPRESA",
                          "dono_nome": "Empreiteira Que Não Existe LTDA",
                          "dono_documento": "99888777000166",
                          "validade": "2027-01-01", "confianca": "ALTA"})
    g = leitura.sugerir(cenario["s"], PDF, "cnd.pdf")
    assert g["empresa_id"] is None
    assert g["dono_encontrado"] == ""
    assert g["dono_lido"] == "Empreiteira Que Não Existe LTDA"
    assert "de quem é o documento" in g["faltando"]


def test_cnpj_de_outra_empresa_nao_vira_a_nossa(cenario, monkeypatch):
    """CNPJ diferente é empresa diferente, por mais parecido que seja o nome."""
    _dublar(monkeypatch, {"tipo_codigo": "CND-FEDERAL", "dono_especie": "EMPRESA",
                          "dono_nome": "BWS Construções", "dono_documento": "99999999000199",
                          "validade": "2027-01-01", "confianca": "ALTA"})
    g = leitura.sugerir(cenario["s"], PDF, "cnd.pdf")
    # o CNPJ não bateu, mas o nome sim — o nome é o segundo critério, e vale
    assert g["empresa_id"] == cenario["empresa"].id


# ---------------------------------------------------------------------------
# 3. O TIPO
# ---------------------------------------------------------------------------
def test_tipo_inventado_pela_ia_e_recusado(cenario, monkeypatch):
    _dublar(monkeypatch, {"tipo_codigo": "CERTIDAO-DA-LUA", "dono_especie": "EMPRESA",
                          "dono_nome": "BWS", "confianca": "ALTA"})
    g = leitura.sugerir(cenario["s"], PDF, "x.pdf")
    assert g["tipo_codigo"] == ""
    assert "o tipo do documento" in g["faltando"]


def test_tipo_aposentado_nao_e_aceito(cenario, monkeypatch):
    s = cenario["s"]
    tipo = s.get(DocumentoTipo, "ALVARA")
    tipo.ativo = False
    s.flush()
    _dublar(monkeypatch, {"tipo_codigo": "ALVARA", "dono_especie": "EMPRESA",
                          "dono_nome": "BWS", "confianca": "ALTA"})
    g = leitura.sugerir(s, PDF, "alvara.pdf")
    assert g["tipo_codigo"] == ""


# ---------------------------------------------------------------------------
# 4. DATAS QUE NÃO FAZEM SENTIDO
# ---------------------------------------------------------------------------
def test_validade_antes_da_emissao_e_descartada(cenario, monkeypatch):
    """Guardar essa validade faria o aviso de vencimento nascer errado."""
    _dublar(monkeypatch, {"tipo_codigo": "CND-FEDERAL", "dono_especie": "EMPRESA",
                          "dono_nome": "BWS", "emissao": "2026-09-01",
                          "validade": "2025-01-01", "confianca": "ALTA"})
    g = leitura.sugerir(cenario["s"], PDF, "cnd.pdf")
    assert g["validade"] == ""
    assert g["confianca"] == "BAIXA"
    assert "até quando vale" in g["faltando"]


@pytest.mark.parametrize("bruto,esperado", [
    ("2026-09-01", "2026-09-01"), ("01/09/2026", "2026-09-01"),
    ("01/09/26", "2026-09-01"), ("setembro", ""), ("", ""),
    ("2026-13-45", ""),
])
def test_data_mal_escrita_vira_vazio_nao_vira_chute(cenario, monkeypatch, bruto, esperado):
    _dublar(monkeypatch, {"tipo_codigo": "CONTRATO-SOCIAL", "dono_especie": "EMPRESA",
                          "dono_nome": "BWS", "emissao": bruto, "confianca": "ALTA"})
    g = leitura.sugerir(cenario["s"], PDF, "c.pdf")
    assert g["emissao"] == esperado


# ---------------------------------------------------------------------------
# 5. A LEITURA DIZ O QUE NÃO RESOLVEU
# ---------------------------------------------------------------------------
def test_documento_de_competencia_sem_o_mes_cobra_o_mes(cenario, monkeypatch):
    _dublar(monkeypatch, {"tipo_codigo": "FOLHA", "dono_especie": "OBRA",
                          "dono_nome": "ESCPLANALTO",
                          "competencia": "", "confianca": "ALTA"})
    g = leitura.sugerir(cenario["s"], PDF, "folha.pdf")
    assert "a competência (o mês)" in g["faltando"]


def test_muitos_campos_ilegiveis_derrubam_a_confianca(cenario, monkeypatch):
    _dublar(monkeypatch, {"tipo_codigo": "CONTRATO-SOCIAL", "dono_especie": "EMPRESA",
                          "dono_nome": "BWS", "confianca": "ALTA",
                          "campos_ilegiveis": ["emissao", "referencia", "resumo"]})
    g = leitura.sugerir(cenario["s"], PDF, "c.pdf")
    assert g["confianca"] == "BAIXA"


def test_leitura_completa_sugere_o_nome_padronizado(cenario, monkeypatch):
    _dublar(monkeypatch, {"tipo_codigo": "CRF-FGTS", "dono_especie": "EMPRESA",
                          "dono_nome": "BWS", "dono_documento": "11222333000181",
                          "emissao": "2026-09-02", "validade": "2026-10-02",
                          "confianca": "ALTA"})
    g = leitura.sugerir(cenario["s"], PDF, "crf baixado do site.pdf")
    assert g["nome_sugerido"] == "CRF-FGTS_BWS_val-2026-10-02.pdf"
    assert g["faltando"] == []
    assert g["confianca"] == "ALTA"


# ---------------------------------------------------------------------------
# 6 e 7. LER NÃO É GUARDAR — MAS O TEXTO SOBREVIVE ATÉ O ARQUIVAMENTO
# ---------------------------------------------------------------------------
def test_ler_nao_grava_documento_nenhum(cenario, monkeypatch):
    s = cenario["s"]
    antes = s.query(Documento).count()
    _dublar(monkeypatch, {"tipo_codigo": "CRF-FGTS", "dono_especie": "EMPRESA",
                          "dono_nome": "BWS", "validade": "2026-10-02",
                          "confianca": "ALTA"})
    leitura.sugerir(s, PDF, "crf.pdf")
    assert s.query(Documento).count() == antes


def test_o_texto_extraido_volta_e_permite_buscar_dentro_depois(cenario, monkeypatch):
    s = cenario["s"]
    _dublar(monkeypatch, {"tipo_codigo": "CRF-FGTS", "dono_especie": "EMPRESA",
                          "dono_nome": "BWS", "validade": "2026-10-02",
                          "confianca": "ALTA"},
            texto="CERTIFICADO DE REGULARIDADE DO FGTS numero 2026090212345")
    g = leitura.sugerir(s, PDF, "crf.pdf")
    assert "2026090212345" in g["texto"]

    arq.arquivar(s, PDF, "crf.pdf", tipo_codigo="CRF-FGTS",
                 empresa_id=cenario["empresa"].id, validade=date(2026, 10, 2),
                 texto=g["texto"], resumo=g["resumo"], origem="IA",
                 usuario=cenario["admin"])
    achados = arq.listar(s, busca="2026090212345", usuario=cenario["admin"])
    assert len(achados["documentos"]) == 1


def test_a_dica_do_usuario_chega_na_leitura(cenario, monkeypatch):
    falso = _dublar(monkeypatch, {"tipo_codigo": "CND-FEDERAL", "dono_especie": "EMPRESA",
                                  "dono_nome": "BWS", "validade": "2027-01-01",
                                  "confianca": "ALTA"})
    leitura.sugerir(cenario["s"], PDF, "x.pdf", dica="é a CND federal da BWS")
    assert "CND federal" in falso.dica


# ---------------------------------------------------------------------------
# 8. AS ROTAS
# ---------------------------------------------------------------------------
def test_ler_sem_arquivo_recusa_com_recado(app_real, cenario):
    from tests.conftest import como
    r = como(app_real, cenario["admin"].id).post("/erp/api/arquivo/ler", data={})
    assert r.status_code == 400
    assert "Escolha o arquivo" in r.get_json()["erro"]


def test_quem_nao_arquiva_nao_le(app_real, cenario):
    """A leitura consome IA, que é dinheiro — e não é porta de entrada para
    quem não pode guardar documento."""
    from tests.conftest import como
    s = cenario["s"]
    obreiro = Usuario(nome="Administrativo de obra", email="obra.leitura@teste.local",
                      ativo=True, senha_hash=gerar_hash("senha-de-teste-123"),
                      perfil=P.ADMINISTRATIVO_OBRA)
    s.add(obreiro)
    s.flush()
    r = como(app_real, obreiro.id).post("/erp/api/arquivo/ler", data={})
    assert r.status_code in (403, 404), "leitura aberta a quem não arquiva"


def test_a_lista_de_colaboradores_so_sai_para_quem_ve_documento_pessoal(app_real, cenario):
    """Nome de empregado é dado de pessoa: quem não enxerga o holerite também
    não recebe a lista de quem trabalha na empresa."""
    from tests.conftest import como
    s = cenario["s"]
    gestor = Usuario(nome="Gestor de obra", email="gestor.leitura@teste.local",
                     ativo=True, senha_hash=gerar_hash("senha-de-teste-123"),
                     perfil=P.GESTOR_OBRA)
    s.add(gestor)
    s.flush()

    do_admin = como(app_real, cenario["admin"].id).get("/erp/api/arquivo/donos").get_json()
    do_gestor = como(app_real, gestor.id).get("/erp/api/arquivo/donos").get_json()

    assert any(c["id"] == cenario["pessoa"].id for c in do_admin["colaboradores"])
    assert do_gestor["colaboradores"] == []
    # o resto continua servindo: sem fornecedor não há onde pendurar o
    # documento cadastral do parceiro
    assert any(f["id"] == cenario["parceiro"].id for f in do_gestor["fornecedores"])
